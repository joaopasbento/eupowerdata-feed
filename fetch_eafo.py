#!/usr/bin/env python3
"""fetch_eafo.py - monthly battery-electric (BEV) passenger-car registrations from EAFO.

Augments the existing data/transition.json (produced by fetch_eurostat.py) by adding
a `vehicles.eafo` sub-block per covered country. It never touches any other field, and
it writes the file only once, at the end, so a failure mid-run leaves the Eurostat
output untouched (the workflow also runs this step with continue-on-error).

Source: the European Alternative Fuels Observatory (EAFO) has no public REST API. Each
country road page embeds the full chart data as Drupal settings JSON in the HTML:

    <script type="application/json" data-drupal-selector="drupal-settings-json">…</script>

Inside it, `drilldown["14"]` is the Passenger-cars (M1) graph; its "BEV" key holds the
monthly series, pre-loaded per year as {"2018": {"data": [[label, value], ...]}, ...}.
No API, no auth, no cookies. stdlib only (urllib + json + re).

Parsing notes (validated):
  - We index months by POSITION (0=Jan … 11=Dec), NOT by the label, because EAFO's
    labels are not consistently English (October comes through as the German "Okt", and
    other German labels could appear). Position is robust.
  - Trailing zero-months are "not yet reported" for the current year and are trimmed;
    interior zeros are kept (a genuine 0 in an early month for a small country is real).
"""

import json
import re
import time
import urllib.request
from datetime import datetime, timezone

OUT_FILE = "data/transition.json"
BASE = "https://alternative-fuels-observatory.ec.europa.eu"
GRAPH_M1 = "14"          # Passenger cars (M1) instance id in drupalSettings.drilldown
REQUEST_PAUSE = 0.4      # polite delay between country requests (seconds)
UA = "eupowerdata-feed (+https://eupowerdata.com)"

# Site country code -> EAFO page slug (English name, lowercase, hyphenated).
# 26 of the site's 32 are covered. RS/ME/MK/AL/BA/XK return 404 -> Eurostat-only.
# Watch-outs: CZ is "czech-republic" (czechia -> 404); GB is "united-kingdom".
SLUGS = {
    "PT": "portugal", "ES": "spain", "DE": "germany", "FR": "france",
    "IT": "italy", "NL": "netherlands", "BE": "belgium", "AT": "austria",
    "CH": "switzerland", "PL": "poland", "NO": "norway", "SE": "sweden",
    "DK": "denmark", "FI": "finland", "GR": "greece", "IE": "ireland",
    "RO": "romania", "BG": "bulgaria", "HU": "hungary", "CZ": "czech-republic",
    "EE": "estonia", "LV": "latvia", "LT": "lithuania", "GB": "united-kingdom",
    "SI": "slovenia", "HR": "croatia",
}

SETTINGS_RE = re.compile(
    r'<script type="application/json" data-drupal-selector="drupal-settings-json">'
    r'(.*?)</script>',
    re.DOTALL,
)


def fetch_drilldown(slug):
    """Return the drupalSettings.drilldown dict for a country page, or None."""
    url = f"{BASE}/transport-mode/road/{slug}/vehicles-and-fleet"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    m = SETTINGS_RE.search(html)
    if not m:
        return None
    settings = json.loads(m.group(1))
    return settings.get("drilldown") or None


def parse_monthly_bev(drilldown, graph_id=GRAPH_M1):
    """Return [{"period": "YYYY-MM", "value": int}, ...] with trailing zeros dropped."""
    graph = (drilldown or {}).get(graph_id)
    if not graph or "BEV" not in graph:
        return None
    try:
        bev_by_year = json.loads(graph["BEV"])
    except (TypeError, ValueError):
        return None

    series = []
    for year in sorted(bev_by_year, key=lambda y: int(y)):
        data = (bev_by_year[year] or {}).get("data") or []
        for i, pair in enumerate(data):
            if i > 11:  # guard against malformed years with >12 points
                break
            try:
                value = int(round(float(pair[1])))
            except (TypeError, ValueError, IndexError):
                value = 0
            series.append({"period": f"{int(year):04d}-{i + 1:02d}", "value": value})

    # Drop trailing zero-months (future / not-yet-reported); keep interior zeros.
    while series and series[-1]["value"] == 0:
        series.pop()
    return series or None


def annual_sums_bev(drilldown, graph_id=GRAPH_M1):
    """EAFO's own annual BEV aggregate {year: value}, used only for a sanity log."""
    try:
        sums = json.loads(drilldown[graph_id]["sums"])
        bev = next(s for s in sums if s.get("name") == "BEV")
        return {str(d["name"]): int(round(float(d["y"]))) for d in bev["data"]}
    except Exception:
        return {}


def sanity_check(code, series, drilldown):
    """Log a warning if the monthly sum diverges from EAFO's own annual aggregate."""
    annual = annual_sums_bev(drilldown)
    if not annual:
        return
    by_year = {}
    for pt in series:
        by_year[pt["period"][:4]] = by_year.get(pt["period"][:4], 0) + pt["value"]
    for y, monthly_total in by_year.items():
        ref = annual.get(y)
        # The current (incomplete) year will differ from the annual aggregate by design.
        if ref is not None and ref != 0 and monthly_total != ref:
            diff = monthly_total - ref
            print(f"  {code}: note {y} monthly-sum {monthly_total} vs annual {ref} (diff {diff:+d})")


def build_country_eafo(code, slug, drilldown):
    series = parse_monthly_bev(drilldown)
    if not series or len(series) < 2:
        return None
    sanity_check(code, series, drilldown)
    return {
        "source": "EAFO",
        "metric": "bev_registrations_m1",
        "granularity": "monthly",
        "latest_period": series[-1]["period"],
        "bev_monthly": series,
        "updated": datetime.now(timezone.utc).isoformat(),
    }


def main():
    # Read the Eurostat output. If it's not there, do nothing (don't create a bad file).
    try:
        with open(OUT_FILE, encoding="utf-8") as f:
            doc = json.load(f)
    except (FileNotFoundError, ValueError) as e:
        print(f"{OUT_FILE} not available ({e}) - nothing to augment, exiting 0")
        return

    countries = doc.get("countries") or {}
    added, skipped = 0, []

    for code, slug in SLUGS.items():
        country = countries.get(code)
        veh = country.get("vehicles") if isinstance(country, dict) else None
        if not isinstance(veh, dict):
            # No Eurostat vehicles block to attach to (the card would show the
            # placeholder anyway); skip rather than create a partial block.
            skipped.append(f"{code}(no annual)")
            continue
        try:
            drilldown = fetch_drilldown(slug)
            block = build_country_eafo(code, slug, drilldown)
        except Exception as e:  # noqa: BLE001 - one country must not break the rest
            skipped.append(f"{code}({type(e).__name__})")
            continue
        if block:
            veh["eafo"] = block
            added += 1
            print(f"  {code}: {len(block['bev_monthly'])} months, latest {block['latest_period']}")
        else:
            skipped.append(f"{code}(no series)")
        time.sleep(REQUEST_PAUSE)

    if added == 0:
        print("EAFO: no countries augmented - leaving transition.json unchanged")
        return

    try:
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(doc, f, separators=(",", ":"))
        print(f"EAFO: augmented {added} countries in {OUT_FILE}")
        if skipped:
            print(f"EAFO: skipped {len(skipped)} -> {', '.join(skipped)}")
    except OSError as e:
        print(f"ERROR writing {OUT_FILE}: {e} - exiting 0 (file untouched)")


if __name__ == "__main__":
    main()
