#!/usr/bin/env python3
"""fetch_ree.py - Spanish real-time generation mix from REE REData (apidatos.ree.es).

Moved to the GitHub Actions feed because EasyWP can no longer reach apidatos.ree.es
directly (REE blocks the host's IP - the PHP fetcher already sent a browser User-Agent
and retried with SSL verification off, and still got nothing since 2026-05-27). The
runner has a clean outbound IP, so the fetch works here and the result is committed to
data/ree-generation.json, which eew-engine.php then mirrors like the ENTSO-E feeds.

Resilience (Option 2): if the REE fetch itself fails, we fall back to the ENTSO-E ES
zone already produced by fetch_entsoe.py in the same run (data/generation-mix.json), so
Spanish data never goes stale or blank. NOTE: during a fallback the per-technology
breakdown uses ENTSO-E's taxonomy rather than REE's; the headline figures (total MW,
renewable %) stay correct, and `source`/`fallback` flag the provenance.

Output shape is identical to what the PHP used to write, so the frontend is unchanged:
    {updated, source, mix:{technology: MW}, total_mw, renewable_pct[, fallback]}

Standard library only. The REE endpoint is keyless. apidatos.ree.es is not reachable
from the Claude sandbox, so live validation happens on the GitHub Action (the log prints
the technology count, total MW and renewable %, or the fallback line).
"""

import json
import os
import ssl
import urllib.error
import urllib.request
from datetime import datetime, timezone

DATA_DIR = "data"
OUT_PATH = os.path.join(DATA_DIR, "ree-generation.json")
GEN_MIX_PATH = os.path.join(DATA_DIR, "generation-mix.json")

# Allowlist of REE generation-technology families (case-insensitive substrings).
# estructura-generacion also returns an aggregate "Generacion total" row; without this
# filter it lands in the chart's "Other" bucket and double-counts the total. Matching on
# substrings survives REE's partial EN localisation (titles sometimes come back in
# Spanish). Mirrors eew-engine.php exactly so the output is identical to the old fetcher.
GEN_FAMILIES = [
    "hydro", "hidr\u00e1", "bombeo", "pumped", "nuclear", "coal", "carb\u00f3n",
    "diesel", "di\u00e9sel", "gas", "vapor", "steam", "ciclo", "combined", "cogen",
    "solar", "fotovolt", "t\u00e9rmic", "thermal", "wind", "e\u00f3lic", "renewabl",
    "renovabl", "waste", "residuo", "hidroe\u00f3l", "hydrowind",
]

RENEWABLE_TYPES = [
    "Wind", "Solar photovoltaic", "Solar thermal", "Hydro",
    "Renewable waste", "Biomass", "Other renewables", "Hydrowind",
]


def ree_url():
    """REE estructura-generacion for today, hourly. time_trunc=hour gives MW per
    technology for the latest published hour (time_trunc=day would give MWh energy)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        "https://apidatos.ree.es/en/datos/generacion/estructura-generacion"
        "?start_date=" + today + "T00:00&end_date=" + today + "T23:59"
        "&time_trunc=hour&geo_limit=peninsular"
    )


def http_get_json(url, timeout=30):
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; EUPowerData/3.0)",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError:
        # A real HTTP status (e.g. 403/blocked) won't be fixed by toggling SSL.
        raise
    except urllib.error.URLError:
        # Retry once without certificate verification (mirrors the PHP 2nd try).
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode("utf-8"))


def parse_ree(data):
    """Replicates eew_fetch_ree_generation()'s parsing. Returns a result dict, or None
    if there is nothing usable (so the caller can fall back to ENTSO-E)."""
    included = data.get("included")
    if not isinstance(included, list):
        return None

    mix = {}
    total = 0.0
    renewable = 0.0

    for item in included:
        attrs = item.get("attributes", {}) or {}
        title = attrs.get("title") or "Unknown"
        tl = title.lower()

        # Skip the aggregate "Generacion total" row.
        if "total" in tl:
            continue
        if not any(fam in tl for fam in GEN_FAMILIES):
            continue

        values = attrs.get("values") or []
        if not values:
            continue
        val = values[-1].get("value") or 0  # latest published hour

        mix[title] = round(val)
        total += val
        if any(rt.lower() in tl for rt in RENEWABLE_TYPES):
            renewable += val

    if not mix or total <= 0:
        return None

    return {
        "updated": datetime.now(timezone.utc).isoformat(),
        "source": "REE REData API",
        "mix": mix,
        "total_mw": round(total),
        "renewable_pct": round((renewable / total) * 100, 1),
    }


def fallback_from_entsoe():
    """REE unreachable: reuse the ENTSO-E ES zone already written by fetch_entsoe.py in
    this run, so Spanish data degrades gracefully instead of going stale/blank."""
    try:
        with open(GEN_MIX_PATH, encoding="utf-8") as f:
            gm = json.load(f)
    except (OSError, ValueError):
        return None

    es = (gm.get("zones") or {}).get("ES")
    if not es or not es.get("mix") or not es.get("total_mw"):
        return None

    return {
        "updated": datetime.now(timezone.utc).isoformat(),
        "source": "ENTSO-E Transparency Platform (REE unavailable)",
        "fallback": True,
        "mix": es["mix"],
        "total_mw": es["total_mw"],
        "renewable_pct": es.get("renewable_pct", 0),
    }


def main():
    result = None
    try:
        result = parse_ree(http_get_json(ree_url()))
        if result:
            print(
                "REE OK: %d technologies, %d MW, %.1f%% renewable"
                % (len(result["mix"]), result["total_mw"], result["renewable_pct"])
            )
        else:
            print("REE returned no usable rows - trying ENTSO-E ES fallback")
    except Exception as e:  # noqa: BLE001 - any failure should trigger the fallback
        print("REE fetch failed (%s) - trying ENTSO-E ES fallback" % e)

    if not result:
        result = fallback_from_entsoe()
        if result:
            print(
                "Fallback OK: ENTSO-E ES - %d MW, %.1f%% renewable"
                % (result["total_mw"], result["renewable_pct"])
            )

    if not result:
        # Neither source available: keep the last-good committed file untouched.
        print("No REE and no ENTSO-E ES fallback - keeping previous ree-generation.json")
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    # Single write at the very end so a crash never leaves a partial file.
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print("Wrote %s (source: %s)" % (OUT_PATH, result["source"]))


if __name__ == "__main__":
    main()
