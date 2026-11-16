#!/usr/bin/env python3
"""fetch_ren.py - Portuguese generation mix for data/ren-generation.json.

Python port of eew_fetch_ren_generation() / eew_parse_ren_response() in the
site's eew-engine.php: try REN's Data Hub API and, if that gives nothing
usable, fall back to the PT zone that fetch_entsoe.py wrote to
data/generation-mix.json earlier in the same run - as the PHP did, and as
fetch_ree.py does for Spain.

    {updated, source, fallback, mix: {technology: MW}, total_mw, renewable_pct[, url][, data_time]}

`source` and `fallback` say where the data came from. In a fallback, `updated`
is the time the PT zone itself was obtained from ENTSO-E (its fetched_at): not
the time of the copy, and not the time of generation-mix.json either, which is
later when the zone was kept from an earlier run (see carry_over.py); and
`data_time` is the zone's, the time the mix itself is for.
The site's JavaScript reads `mix`, `total_mw` and `renewable_pct` only.

Differences from the PHP, on purpose:

  - The requests identify the feed honestly (feed_common.USER_AGENT). The PHP
    sent a browser User-Agent and a Referer claiming to come from
    datahub.ren.pt, and retried with certificate checks off.
  - What REN answers is only used if it reads as power: its "Daily" endpoints
    report the energy of a day (GWh, field daily_Accumulation), which is not
    what total_mw means, so a total outside what Portugal can be generating at
    one moment is rejected rather than published as MW.
  - There is no empty file: the PHP wrote `mix: []` when everything failed,
    which blanks Portugal on some pages. Here the previous file is kept.
"""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from carry_over import DATA_TIME, entry_time
from feed_common import http_get, load_json, now_iso, write_json

OUT_PATH = 'data/ren-generation.json'
GEN_MIX_PATH = 'data/generation-mix.json'
REN_API = 'https://servicebus.ren.pt/datahubapi/electricity/ElectricityProductionBreakdownDaily?culture=en-US&date='
LISBON = ZoneInfo('Europe/Lisbon')

# Total generation Portugal can plausibly have at one moment, in MW. A day's
# energy in GWh (about 100-200) or in MWh (about 150 000) falls outside it.
MIN_TOTAL_MW, MAX_TOTAL_MW = 1000, 25000

# Same list as the PHP: a technology whose name contains one of these counts as renewable.
RENEWABLE_KEYWORDS = ['Eólica', 'Solar', 'Hídrica', 'Biomassa', 'Wind', 'Hydro', 'Biomass',
                      'Geotérmica', 'Fotovoltaica']


def _first(mapping, keys, default=None):
    """PHP's `$a['x'] ?? $a['y'] ?? default`: the first key present and not null."""
    for key in keys:
        if isinstance(mapping, dict) and mapping.get(key) is not None:
            return mapping[key]
    return default


def _number(value):
    """The value as a float if PHP's is_numeric() would accept it, else None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _is_renewable(name):
    low = str(name).lower()
    return any(keyword.lower() in low for keyword in RENEWABLE_KEYWORDS)


def parse_ren_response(data, url):
    """eew_parse_ren_response(): reads either {"Solar": 1200, ...} or
    [{"Name": "Solar", "Value": 1200}, ...], possibly inside Body / Data /
    Result / Items. Returns the result dict, or None when nothing adds up."""
    items = _first(data, ['Body', 'body', 'Data', 'data', 'Result', 'result', 'Items', 'items'], data)
    if isinstance(items, dict) and len(items) == 1:
        items = next(iter(items.values()))

    mix = {}
    total = 0.0
    renewable = 0.0

    if isinstance(items, dict):                       # {"Solar": 1200, "Wind": 3400}
        for key, raw in items.items():
            value = _number(raw)
            if value is not None and value > 0:
                mix[str(key)] = round(value)
                total += value
                if _is_renewable(key):
                    renewable += value

    if total == 0 and isinstance(items, (list, dict)):   # [{"Name": "Solar", "Value": 1200}]
        for item in (items.values() if isinstance(items, dict) else items):
            if not isinstance(item, dict):
                continue
            name = _first(item, ['Name', 'name', 'Type', 'type', 'Designation', 'Label'], 'Unknown')
            value = _number(_first(item, ['Value', 'value', 'Average', 'average', 'Total', 'total', 'Power'], 0))
            if value is None or value <= 0:
                continue
            mix[str(name)] = round(value)
            total += value
            if _is_renewable(name):
                renewable += value

    if total == 0:
        return None
    return {
        'updated': now_iso(),
        'source': 'REN DataHub',
        'fallback': False,
        'url': url,
        'mix': mix,
        'total_mw': round(total),
        'renewable_pct': round(renewable / total * 100, 1),
    }


def describe(data):
    """A short description of a JSON answer, for the log."""
    if isinstance(data, list):
        first = data[0] if data else None
        keys = sorted(first) if isinstance(first, dict) else type(first).__name__
        return f'list of {len(data)}, items with {keys}'
    if isinstance(data, dict):
        return f'object with keys {sorted(data)[:12]}'
    return type(data).__name__


def from_ren(get=http_get, today=None):
    """Ask REN for yesterday (more likely to be complete) and then today, as
    the PHP did. Returns a result dict or None; logs what REN answered.

    The PHP went on to ask the same again in Portuguese and then
    ElectricityConsumptionSupplyDaily. Neither is repeated here: the first is
    the same data, and the second only has a day's energy per source
    (daily_Accumulation, in GWh), which its parser could not read either."""
    today = today or datetime.now(LISBON).date()
    for day in (today - timedelta(days=1), today):
        url = REN_API + day.isoformat()
        resp = get(url, timeout=20, headers={'Accept': 'application/json'})
        print(f'  REN {day}: {resp.describe()}')
        if not resp.ok:
            if resp.body:
                print(f'    body: {resp.text()[:200]!r}')
            continue
        try:
            data = resp.json()
        except ValueError as e:
            print(f'    not JSON ({e}); starts {resp.text()[:120]!r}')
            continue
        print(f'    {describe(data)}')
        result = parse_ren_response(data, url)
        if not result:
            print('    nothing the parser can read as a generation mix')
            continue
        if not MIN_TOTAL_MW <= result['total_mw'] <= MAX_TOTAL_MW:
            print(f"    total {result['total_mw']} is not a plausible power in MW for Portugal "
                  f'(a daily energy figure?) - not used')
            continue
        return result
    return None


def from_generation_mix(gen_mix):
    """The PT zone of generation-mix.json, or None when it has no usable mix."""
    pt = ((gen_mix or {}).get('zones') or {}).get('PT')
    if not pt or not pt.get('mix'):
        return None
    mix = pt['mix']
    result = {
        'updated': entry_time(pt, gen_mix),           # when the zone was obtained
        'source': 'ENTSO-E Transparency Platform',
        'fallback': True,
        'mix': mix,
        'total_mw': pt.get('total_mw') if pt.get('total_mw') is not None else sum(mix.values()),
        'renewable_pct': pt.get('renewable_pct', 0),
    }
    if pt.get(DATA_TIME):
        result[DATA_TIME] = pt[DATA_TIME]             # the time the mix itself is for
    return result


def main():
    print('Portuguese generation: trying REN Data Hub...')
    result = from_ren()
    if result:
        print(f"REN OK: {len(result['mix'])} technologies, {result['total_mw']} MW, "
              f"{result['renewable_pct']}% renewable")
    else:
        result = from_generation_mix(load_json(GEN_MIX_PATH))
        if result:
            print(f"REN gave nothing usable - using the ENTSO-E PT zone (data of {result['updated']}): "
                  f"{result['total_mw']:.0f} MW, {result['renewable_pct']}% renewable")

    if not result:
        print(f'No REN data and no PT zone in {GEN_MIX_PATH} - keeping the previous {OUT_PATH}')
        return 1

    if load_json(OUT_PATH) == result:
        print(f'{OUT_PATH} already has this data - nothing to do')
        return 0
    write_json(OUT_PATH, result, indent=2)
    print(f"Wrote {OUT_PATH} (source: {result['source']})")
    return 0


if __name__ == '__main__':
    sys.exit(main())
