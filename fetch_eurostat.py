#!/usr/bin/env python3
"""
fetch_eurostat.py - Portugal energy-transition annual indicators from Eurostat.

Feeds the /transicao page (eew-transition.js). Produces data/transition-pt.json
with three blocks:

  - ghg         : national greenhouse-gas inventory (env_air_gge), total GHG in
                  CO2-equivalent. Includes 1990 baseline, latest year, a short
                  trend, and the 2030 Fit-for-55 reference (-55% vs 1990).
  - heat_pumps  : ambient heat captured by heat pumps (nrg_bal_c, SIEC RA600),
                  as a proxy for heating electrification uptake.
  - vehicles    : new passenger-car registrations by motor energy
                  (road_eqr_carpda): latest BEV new registrations + EV share
                  (BEV+PHEV) of new cars, plus an EV-share trend.

Source: Eurostat dissemination API (JSON-stat), public, no key:
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{code}?format=JSON&...
Eurostat republishes the GHG inventory reported to the EEA; it is annual and
lags ~2 years (year t-2). Heat-pump / energy-balance data is also annual.

Design - standalone producer, FAIL-SAFE: on any error it logs and exits 0
WITHOUT writing, so a bad run never overwrites a previously good
data/transition-pt.json. Stdlib only (urllib/json), like fetch_gb.py.

NOTE (codes to confirm on first live run, like fetch_gb.py was): the exact
Eurostat dimension codes below are best-known values. The fetcher tries a few
candidates per series and uses the first that returns data; it logs which one
won. If a block ends up null, check the logged candidates against the live
dataset dimensions in the Data Browser.
"""

import json
import ssl
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# --- Eurostat dissemination API --------------------------------------------
ES_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
GEO = "PT"
OUT_FILE = "data/transition-pt.json"

# GHG: total greenhouse gases in CO2-equivalent (env_air_gge).
#   airpol = GHG  (all greenhouse gases, CO2-eq)
#   unit   = THS_T (thousand tonnes) -> we report Mt = value / 1000
#   src_crf candidates for the headline national total (first that resolves wins)
GHG_DATASET = "env_air_gge"
GHG_AIRPOL = "GHG"
GHG_UNIT = "THS_T"
GHG_SRC_CANDIDATES = ["TOTX4_MEMO", "TOTXMEMO", "TOTX4_MEMONIA", "TOTXMEMONIA"]
GHG_TARGET_2030_PCT = -55  # Fit-for-55, vs 1990

# Heat pumps: ambient heat (heat pumps) primary production (nrg_bal_c).
#   siec    = RA600 (Ambient heat (heat pumps))
#   nrg_bal = PPRD  (Primary production)
#   unit candidates (first that resolves wins)
HP_DATASET = "nrg_bal_c"
HP_SIEC = "RA600"
HP_NRG_BAL = "PPRD"
HP_UNIT_CANDIDATES = ["GWH", "TJ", "KTOE"]

# Vehicles: new passenger-car registrations by motor energy (road_eqr_carpda).
#   unit = NR (number). We pick TOTAL, BEV (electricity) and any plug-in hybrid
#   categories by their human labels (robust to cryptic mot_nrg codes), then
#   report latest BEV new registrations + EV share (BEV+PHEV) of new cars.
VEH_DATASET = "road_eqr_carpda"
VEH_UNIT = "NR"

TREND_YEARS = 7  # most recent N years for the sparkline-style trend


# --- HTTP (stdlib, with SSL fallback like fetch_gb.py) ---------------------
def http_get_json(url, timeout=40):
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; EUPowerData/1.0; +https://eupowerdata.com)",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        # Retry once with a relaxed SSL context (some CI egress paths need it)
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e2:
            print(f"  HTTP error for {url}: {e2}")
            return None


def es_url(dataset, **params):
    q = {"format": "JSON", "lang": "EN", "geo": GEO}
    q.update(params)
    return ES_BASE + dataset + "?" + urllib.parse.urlencode(q, doseq=True)


# --- JSON-stat extraction ---------------------------------------------------
def _time_series(js, fixed):
    """Extract {year: value} from a JSON-stat cube, holding every non-time
    dimension at the code given in `fixed`. Returns {} if a code is missing."""
    if not js or "id" not in js or "size" not in js or "value" not in js:
        return {}
    dims = js["id"]
    sizes = js["size"]
    dimension = js["dimension"]

    # locate the time dimension
    time_dim = "time" if "time" in dims else next((d for d in dims if d.lower() in ("time", "time_period")), None)
    if not time_dim:
        return {}

    # row-major strides
    strides = {}
    acc = 1
    for i in range(len(dims) - 1, -1, -1):
        strides[dims[i]] = acc
        acc *= sizes[i]

    # resolve fixed positions for non-time dims
    fixed_pos = {}
    for d in dims:
        if d == time_dim:
            continue
        cat_index = dimension[d]["category"]["index"]
        if d in fixed:
            code = fixed[d]
            if code not in cat_index:
                return {}  # requested code not present
            fixed_pos[d] = cat_index[code]
        elif len(cat_index) == 1:
            fixed_pos[d] = next(iter(cat_index.values()))  # singleton dim
        else:
            return {}  # ambiguous: a non-time dim with >1 category not fixed

    values = js["value"]
    time_index = dimension[time_dim]["category"]["index"]  # {year: pos}
    out = {}
    for year, tpos in time_index.items():
        lin = tpos * strides[time_dim]
        for d, pos in fixed_pos.items():
            lin += pos * strides[d]
        v = values.get(str(lin))
        if v is not None:
            try:
                out[year] = float(v)
            except (TypeError, ValueError):
                pass
    return out


def _series_with_candidates(dataset, base_params, vary_key, candidates):
    """Fetch once per candidate code for `vary_key`; return (series, winning_code)
    for the first candidate that yields a non-empty time series."""
    for code in candidates:
        params = dict(base_params)
        params[vary_key] = code
        js = http_get_json(es_url(dataset, **params))
        if not js:
            continue
        fixed = {k: v for k, v in params.items() if k not in ("format", "lang", "geo")}
        series = _time_series(js, fixed)
        if series:
            return series, code
    return {}, None


def _shape(series, unit, extra=None):
    """Common output shape from a {year: value} series."""
    if not series:
        return None
    years = sorted(series.keys())
    latest_year = years[-1]
    trend = [{"year": int(y), "value": round(series[y], 2)} for y in years[-TREND_YEARS:]]
    out = {
        "unit": unit,
        "latest_year": int(latest_year),
        "latest_value": round(series[latest_year], 2),
        "trend": trend,
    }
    if extra:
        out.update(extra)
    return out


# --- Builders ---------------------------------------------------------------
def build_ghg():
    series, code = _series_with_candidates(
        GHG_DATASET,
        {"airpol": GHG_AIRPOL, "unit": GHG_UNIT},
        "src_crf", GHG_SRC_CANDIDATES,
    )
    if not series:
        print("  GHG: no series resolved (check src_crf candidates)")
        return None
    # THS_T (thousand tonnes) -> Mt CO2-eq
    series_mt = {y: v / 1000.0 for y, v in series.items()}
    base_1990 = series_mt.get("1990")
    extra = {"src_crf": code, "target_2030_pct": GHG_TARGET_2030_PCT}
    if base_1990 is not None:
        extra["base_1990"] = round(base_1990, 2)
        extra["target_2030_value"] = round(base_1990 * (1 + GHG_TARGET_2030_PCT / 100.0), 2)
        latest_year = sorted(series_mt.keys())[-1]
        extra["change_vs_1990_pct"] = round((series_mt[latest_year] / base_1990 - 1) * 100, 1)
    print(f"  GHG: src_crf={code}, {len(series)} years, latest={sorted(series)[-1]}")
    return _shape(series_mt, "Mt CO2e", extra)


def build_heat_pumps():
    series, unit = _series_with_candidates(
        HP_DATASET,
        {"siec": HP_SIEC, "nrg_bal": HP_NRG_BAL},
        "unit", HP_UNIT_CANDIDATES,
    )
    if not series:
        print("  Heat pumps: no series resolved (check siec/nrg_bal/unit)")
        return None
    print(f"  Heat pumps: unit={unit}, {len(series)} years, latest={sorted(series)[-1]}")
    return _shape(series, unit, {"siec": HP_SIEC, "nrg_bal": HP_NRG_BAL})


def build_vehicles():
    js = http_get_json(es_url(VEH_DATASET, unit=VEH_UNIT))
    if not js or "id" not in js or "dimension" not in js:
        print("  Vehicles: no response")
        return None

    dims = js["id"]
    mot_dim = "mot_nrg" if "mot_nrg" in dims else next(
        (d for d in dims if "nrg" in d.lower() or d.lower().startswith("mot")), None)
    if not mot_dim:
        print("  Vehicles: motor-energy dimension not found")
        return None

    cat = js["dimension"][mot_dim]["category"]
    index = cat.get("index", {})
    labels = cat.get("label", {})

    # Classify motor-energy categories by their human label (robust to codes)
    total_code, bev_code, phev_codes = None, None, []
    for code in index:
        lab = (labels.get(code, "") or "").lower()
        if total_code is None and (code in ("TOTAL", "TOT") or lab == "total"):
            total_code = code
        if bev_code is None and ("electric" in lab and "hybrid" not in lab and "plug" not in lab):
            bev_code = code
        if ("plug-in" in lab) or ("plug in" in lab) or ("phev" in lab):
            phev_codes.append(code)
    if bev_code is None and "ELC" in index:
        bev_code = "ELC"
    if total_code is None and "TOTAL" in index:
        total_code = "TOTAL"
    if not total_code or not bev_code:
        print(f"  Vehicles: couldn't identify total/BEV codes among {list(index)[:12]}")
        return None

    bev = _time_series(js, {mot_dim: bev_code})
    total = _time_series(js, {mot_dim: total_code})
    phev = {}
    for pc in phev_codes:
        for y, v in _time_series(js, {mot_dim: pc}).items():
            phev[y] = phev.get(y, 0.0) + v
    if not bev or not total:
        print("  Vehicles: empty BEV/total series")
        return None

    years = sorted(set(bev) & set(total))
    if not years:
        return None
    ly = years[-1]
    tot_ly = total.get(ly) or 0
    ev_share = round((bev.get(ly, 0) + phev.get(ly, 0)) / tot_ly * 100, 1) if tot_ly else None
    bev_share = round(bev.get(ly, 0) / tot_ly * 100, 1) if tot_ly else None
    trend_share = [
        {"year": int(y), "value": round((bev.get(y, 0) + phev.get(y, 0)) / total[y] * 100, 1)}
        for y in years[-TREND_YEARS:] if total.get(y)
    ]
    print(f"  Vehicles: total={total_code} bev={bev_code} phev={phev_codes}, latest={ly}, ev_share={ev_share}%")
    return {
        "unit": "registrations",
        "dataset": VEH_DATASET,
        "latest_year": int(ly),
        "bev_new": int(round(bev.get(ly, 0))),
        "ev_share_pct": ev_share,
        "bev_share_pct": bev_share,
        "trend_share": trend_share,
    }


def main():
    print("fetch_eurostat.py - PT energy-transition annual indicators")
    ghg = build_ghg()
    heat_pumps = build_heat_pumps()
    vehicles = build_vehicles()

    if ghg is None and heat_pumps is None and vehicles is None:
        print("ERROR: all blocks empty - leaving existing file untouched, exiting 0")
        return 0

    doc = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "source": "Eurostat",
        "geo": GEO,
        "ghg": ghg,
        "heat_pumps": heat_pumps,
        "vehicles": vehicles,
    }
    try:
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(doc, f, separators=(",", ":"))
        print(f"Wrote {OUT_FILE}")
    except Exception as e:
        print(f"ERROR writing {OUT_FILE}: {e} - exiting 0 (file untouched)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
