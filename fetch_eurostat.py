#!/usr/bin/env python3
"""
fetch_eurostat.py - multi-country energy-transition annual indicators from Eurostat.

Feeds the /transicao page (eew-transition.js). Produces data/transition.json,
indexed by site country code, each with three blocks:

  - ghg         : national greenhouse-gas inventory (env_air_gge), total GHG in
                  CO2-equivalent. Includes 1990 baseline, latest year, a short
                  trend, and the 2030 Fit-for-55 reference (-55% vs 1990).
  - heat_pumps  : ambient heat captured by heat pumps (nrg_bal_c, SIEC RA600),
                  as a proxy for heating electrification uptake.
  - vehicles    : new passenger-car registrations by motor energy
                  (road_eqr_carpda): latest BEV new registrations + EV share
                  (BEV+PHEV) of new cars, plus an EV-share trend.
  - industry    : industrial energy use (nrg_bal_c, nrg_bal=FC_IND_E): industrial
                  electrification rate (electricity/total), final consumption and
                  natural-gas share, plus an electrification-rate trend.
  - policy      : renewable share in gross final energy consumption (nrg_ind_ren,
                  nrg_bal=REN) vs the EU 2030 target (42.5%, RED III) for EU members.

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
OUT_FILE = "data/transition.json"

# All countries the rest of the site covers (24 core + GB + Phase A). "Todos".
COUNTRIES = ["PT","ES","DE","FR","IT","NL","BE","AT","CH","PL","NO","SE","DK","FI",
             "GR","IE","RO","BG","HU","CZ","EE","LV","LT","GB",
             "SI","HR","RS","ME","MK","AL","BA","XK"]

# Site code -> Eurostat geo code (Eurostat uses EL for Greece, UK for the UK).
GEO_MAP = {"GR": "EL", "GB": "UK"}

# Remembered winning dimension codes (set on first country that resolves),
# tried first for subsequent countries to cut request volume.
_ghg_winner = None
_hp_winner = None
_ind_unit_winner = None

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

# Industry: final energy consumption in industry (nrg_bal_c, nrg_bal=FC_IND_E).
#   We fetch three SIEC series and derive ratios at the latest common year:
#     electrification rate = electricity / total ; gas share = natural gas / total
#   Codes to confirm on first live run (graceful: any missing -> block null).
IND_DATASET = "nrg_bal_c"
IND_NRG_BAL = "FC_IND_E"          # final energy consumption - industry sector
IND_SIEC_TOTAL = "TOTAL"          # all products
IND_SIEC_ELEC = "E7000"           # electricity
IND_SIEC_GAS = "G3000"            # natural gas
IND_UNIT_CANDIDATES = ["KTOE", "TJ", "GWH"]

# Policy & targets: renewable share in gross final energy consumption (nrg_ind_ren).
#   nrg_bal = REN (overall RES share); unit = PC (percent).
#   The EU-wide binding 2030 target (RED III) is 42.5% — used as the reference for
#   EU members; non-EU countries show the share without a target line.
POLICY_DATASET = "nrg_ind_ren"
POLICY_NRG_BAL = "REN"
POLICY_UNIT = "PC"
EU_2030_RES_TARGET = 42.5
EU_CODES = {"PT", "ES", "DE", "FR", "IT", "NL", "BE", "AT", "PL", "SE", "DK", "FI",
            "GR", "IE", "RO", "BG", "HU", "CZ", "EE", "LV", "LT", "SI", "HR"}

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


def es_url(dataset, geo, **params):
    q = {"format": "JSON", "lang": "EN", "geo": geo}
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


def _series_with_candidates(dataset, geo, base_params, vary_key, candidates):
    """Fetch once per candidate code for `vary_key`; return (series, winning_code)
    for the first candidate that yields a non-empty time series."""
    for code in candidates:
        params = dict(base_params)
        params[vary_key] = code
        js = http_get_json(es_url(dataset, geo, **params))
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
def build_ghg(geo):
    global _ghg_winner
    cands = ([_ghg_winner] if _ghg_winner else []) + [c for c in GHG_SRC_CANDIDATES if c != _ghg_winner]
    series, code = _series_with_candidates(
        GHG_DATASET, geo,
        {"airpol": GHG_AIRPOL, "unit": GHG_UNIT},
        "src_crf", cands,
    )
    if not series:
        return None
    _ghg_winner = code
    # THS_T (thousand tonnes) -> Mt CO2-eq
    series_mt = {y: v / 1000.0 for y, v in series.items()}
    base_1990 = series_mt.get("1990")
    extra = {"src_crf": code, "target_2030_pct": GHG_TARGET_2030_PCT}
    if base_1990 is not None:
        extra["base_1990"] = round(base_1990, 2)
        extra["target_2030_value"] = round(base_1990 * (1 + GHG_TARGET_2030_PCT / 100.0), 2)
        latest_year = sorted(series_mt.keys())[-1]
        extra["change_vs_1990_pct"] = round((series_mt[latest_year] / base_1990 - 1) * 100, 1)
    return _shape(series_mt, "Mt CO2e", extra)


def build_heat_pumps(geo):
    global _hp_winner
    cands = ([_hp_winner] if _hp_winner else []) + [c for c in HP_UNIT_CANDIDATES if c != _hp_winner]
    series, unit = _series_with_candidates(
        HP_DATASET, geo,
        {"siec": HP_SIEC, "nrg_bal": HP_NRG_BAL},
        "unit", cands,
    )
    if not series:
        return None
    _hp_winner = unit
    return _shape(series, unit, {"siec": HP_SIEC, "nrg_bal": HP_NRG_BAL})


def build_vehicles(geo):
    js = http_get_json(es_url(VEH_DATASET, geo, unit=VEH_UNIT))
    if not js or "id" not in js or "dimension" not in js:
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
        return None

    bev = _time_series(js, {mot_dim: bev_code})
    total = _time_series(js, {mot_dim: total_code})
    phev = {}
    for pc in phev_codes:
        for y, v in _time_series(js, {mot_dim: pc}).items():
            phev[y] = phev.get(y, 0.0) + v
    if not bev or not total:
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
    return {
        "unit": "registrations",
        "dataset": VEH_DATASET,
        "latest_year": int(ly),
        "bev_new": int(round(bev.get(ly, 0))),
        "ev_share_pct": ev_share,
        "bev_share_pct": bev_share,
        "trend_share": trend_share,
    }


def _ind_series(geo, siec, unit):
    js = http_get_json(es_url(IND_DATASET, geo, nrg_bal=IND_NRG_BAL, siec=siec, unit=unit))
    return _time_series(js, {"nrg_bal": IND_NRG_BAL, "siec": siec, "unit": unit})


def build_industry(geo):
    global _ind_unit_winner
    cands = ([_ind_unit_winner] if _ind_unit_winner else []) + \
            [u for u in IND_UNIT_CANDIDATES if u != _ind_unit_winner]
    total, unit = _series_with_candidates(
        IND_DATASET, geo,
        {"nrg_bal": IND_NRG_BAL, "siec": IND_SIEC_TOTAL},
        "unit", cands,
    )
    if not total:
        return None
    _ind_unit_winner = unit
    elec = _ind_series(geo, IND_SIEC_ELEC, unit)
    gas = _ind_series(geo, IND_SIEC_GAS, unit)

    years = sorted(total.keys())
    ly = years[-1]
    tot_ly = total.get(ly) or 0
    elec_rate = round(elec.get(ly, 0) / tot_ly * 100, 1) if (tot_ly and elec) else None
    gas_share = round(gas.get(ly, 0) / tot_ly * 100, 1) if (tot_ly and gas) else None

    # KTOE -> report consumption in Mtoe; otherwise keep the raw unit
    if unit == "KTOE":
        cons_unit = "Mtoe"
        cons = {y: v / 1000.0 for y, v in total.items()}
    else:
        cons_unit = unit
        cons = total
    trend = [{"year": int(y), "value": round(cons[y], 2)} for y in years[-TREND_YEARS:]]
    rate_trend = [
        {"year": int(y), "value": round(elec[y] / total[y] * 100, 1)}
        for y in years[-TREND_YEARS:] if total.get(y) and elec.get(y) is not None
    ]
    return {
        "unit": cons_unit,
        "latest_year": int(ly),
        "consumption": round(cons[ly], 2),
        "electrification_rate_pct": elec_rate,
        "gas_share_pct": gas_share,
        "trend": trend,
        "trend_rate": rate_trend,
        "nrg_bal": IND_NRG_BAL,
    }


def build_policy(geo):
    js = http_get_json(es_url(POLICY_DATASET, geo, nrg_bal=POLICY_NRG_BAL, unit=POLICY_UNIT))
    series = _time_series(js, {"nrg_bal": POLICY_NRG_BAL, "unit": POLICY_UNIT})
    if not series:
        return None
    years = sorted(series.keys())
    ly = years[-1]
    trend = [{"year": int(y), "value": round(series[y], 1)} for y in years[-TREND_YEARS:]]
    return {
        "res_share_pct": round(series[ly], 1),
        "res_year": int(ly),
        "trend": trend,
        "dataset": POLICY_DATASET,
    }


def build_country(code):
    geo = GEO_MAP.get(code, code)
    ghg = build_ghg(geo)
    heat_pumps = build_heat_pumps(geo)
    vehicles = build_vehicles(geo)
    industry = build_industry(geo)
    policy = build_policy(geo)
    if policy and code in EU_CODES:
        policy["res_target_2030"] = EU_2030_RES_TARGET
    g = ghg["latest_year"] if ghg else "-"
    h = heat_pumps["latest_year"] if heat_pumps else "-"
    v = (str(vehicles["ev_share_pct"]) + "%") if vehicles and vehicles.get("ev_share_pct") is not None else "-"
    i = industry["latest_year"] if industry else "-"
    r = (str(policy["res_share_pct"]) + "%") if policy and policy.get("res_share_pct") is not None else "-"
    print(f"  {code} (geo={geo}): ghg={g} hp={h} veh_ev_share={v} ind={i} res={r}")
    if ghg is None and heat_pumps is None and vehicles is None and industry is None and policy is None:
        return None
    return {"geo": geo, "ghg": ghg, "heat_pumps": heat_pumps, "vehicles": vehicles,
            "industry": industry, "policy": policy}


def main():
    print("fetch_eurostat.py - multi-country energy-transition annual indicators")
    countries = {}
    for code in COUNTRIES:
        c = build_country(code)
        if c is not None:
            countries[code] = c

    if not countries:
        print("ERROR: no country resolved any block - leaving existing file untouched, exiting 0")
        return 0

    doc = {
        "updated": datetime.now(timezone.utc).isoformat(),
        "source": "Eurostat",
        "count": len(countries),
        "countries": countries,
    }
    try:
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(doc, f, separators=(",", ":"))
        print(f"Wrote {OUT_FILE} ({len(countries)}/{len(COUNTRIES)} countries)")
    except Exception as e:
        print(f"ERROR writing {OUT_FILE}: {e} - exiting 0 (file untouched)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
