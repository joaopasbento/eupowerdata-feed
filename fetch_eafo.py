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

A country EAFO does not answer for in a run (some pages answer an HTTP error now and then:
3 to 8 of the 26 in each run in Nov 2026) keeps the block of the published file, as the
feed's rule for kept values does elsewhere: with the time it was obtained (`updated`) and
the mark carried_over, for MAX_AGE (35 days: five weekly runs, at most one monthly figure
behind) from that time; older, the country has no block until EAFO answers again. The
published block is read from the last commit (git show HEAD:data/transition.json), since
fetch_eurostat.py rewrites the file just before this step.
"""

import json
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from feed_common import CARRIED_OVER, parse_time

OUT_FILE = "data/transition.json"
BASE = "https://alternative-fuels-observatory.ec.europa.eu"
GRAPH_M1 = "14"          # Passenger cars (M1) instance id in drupalSettings.drilldown
REQUEST_PAUSE = 0.4      # polite delay between country requests (seconds)
UA = "eupowerdata-feed (+https://eupowerdata.com)"
MAX_AGE = timedelta(days=35)   # how long a country EAFO does not answer for keeps its published block

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


def published_blocks(path=OUT_FILE):
    """{country: eafo block} of the file as last committed, or {} when it cannot be read."""
    try:
        text = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True, text=True, timeout=60,
                              check=True).stdout
        countries = json.loads(text).get("countries") or {}
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}
    return {code: (c.get("vehicles") or {}).get("eafo") for code, c in countries.items()
            if isinstance(c, dict) and isinstance((c.get("vehicles") or {}).get("eafo"), dict)}


def kept_block(previous, now):
    """The published block of a country EAFO did not answer for, marked carried_over, while it is younger than
    MAX_AGE; None otherwise."""
    obtained = parse_time((previous or {}).get("updated"))
    if obtained is None or now - obtained >= MAX_AGE:
        return None
    return {**previous, CARRIED_OVER: True}


def main(previous=None, now=None, fetch=None):
    now = now or datetime.now(timezone.utc)
    fetch = fetch or fetch_drilldown
    previous = published_blocks() if previous is None else previous
    # Read the Eurostat output. If it's not there, do nothing (don't create a bad file).
    try:
        with open(OUT_FILE, encoding="utf-8") as f:
            doc = json.load(f)
    except (FileNotFoundError, ValueError) as e:
        print(f"{OUT_FILE} not available ({e}) - nothing to augment, exiting 0")
        return

    countries = doc.get("countries") or {}
    added, skipped, kept = 0, [], []

    for code, slug in SLUGS.items():
        country = countries.get(code)
        veh = country.get("vehicles") if isinstance(country, dict) else None
        if not isinstance(veh, dict):
            # No Eurostat vehicles block to attach to (the card would show the
            # placeholder anyway); skip rather than create a partial block.
            skipped.append(f"{code}(no annual)")
            continue
        try:
            drilldown = fetch(slug)
            block = build_country_eafo(code, slug, drilldown)
            why = None if block else "no series"
        except Exception as e:  # noqa: BLE001 - one country must not break the rest
            block, why = None, type(e).__name__
        if block:
            veh["eafo"] = block
            added += 1
            print(f"  {code}: {len(block['bev_monthly'])} months, latest {block['latest_period']}")
        else:
            skipped.append(f"{code}({why})")
            old = kept_block(previous.get(code), now)
            if old:
                veh["eafo"] = old
                kept.append(f"{code} (from {old['updated'][:10]})")
        time.sleep(REQUEST_PAUSE)

    if added == 0 and not kept:
        print("EAFO: no countries augmented - leaving transition.json unchanged")
        return

    try:
        with open(OUT_FILE, "w", encoding="utf-8") as f:
            json.dump(doc, f, separators=(",", ":"))
        print(f"EAFO: augmented {added} countries in {OUT_FILE}")
        if skipped:
            print(f"EAFO: skipped {len(skipped)} -> {', '.join(skipped)}")
        if kept:
            print(f"EAFO: kept, not obtained in this run: {', '.join(kept)}")
    except OSError as e:
        print(f"ERROR writing {OUT_FILE}: {e} - exiting 0 (file untouched)")


if __name__ == "__main__":
    main()
