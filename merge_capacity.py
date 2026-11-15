#!/usr/bin/env python3
"""merge_capacity.py - installed renewable capacity per country.

Python port of eew_fetch_renewablenodes_capacity() in the site's eew-engine.php.
Merges two sources into data/renewablenodes-capacity.json, with the schema and
the 32 countries the site already reads:

  - data/capacity-installed.json (ENTSO-E A68, written by fetch_capacity.py):
    the capacity in MW, total and per technology. Primary source.
  - https://renewablenodes.com/wp-content/uploads/api-data.json: renewable
    share of generation, 7-year trend, target, population and country name -
    and the capacity, for a country ENTSO-E does not cover (Great Britain).

The numbers must match what the PHP produced for the same inputs: see
tests/test_merge_capacity.py, which checks them against what the site served.

Capacity changes once a year, so the merge only runs when the file is more
than 12 hours old (the PHP used the same 12-hour cache); pass --force to run
it regardless. If RenewableNodes cannot be reached, the previous file is kept.
"""

import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from feed_common import http_get, load_json, now_iso, write_json

ENTSOE_PATH = 'data/capacity-installed.json'
OUT_PATH = 'data/renewablenodes-capacity.json'
RN_URL = 'https://renewablenodes.com/wp-content/uploads/api-data.json'
MAX_AGE_S = 12 * 3600

# The 32 countries, in the order of the file (must match COUNTRIES in
# fetch_capacity.py, plus GB).
EU_CODES = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE', 'AT', 'CH', 'PL', 'DK', 'SE',
            'NO', 'FI', 'GR', 'IE', 'GB', 'RO', 'BG', 'HU', 'CZ', 'EE', 'LV', 'LT',
            'SI', 'HR', 'RS', 'ME', 'MK', 'AL', 'BA', 'XK']


def _float(value):
    """PHP floatval(): a number, or 0 for anything that is not one."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value, default=0):
    """PHP intval(), with `default` standing in for a missing value."""
    if value is None:
        return default
    return int(_float(value))


def php_round(value, digits=0):
    """PHP round(): halves go away from zero (Python's round() sends them to
    the even neighbour). Whole numbers come back as int."""
    step = Decimal(1).scaleb(-digits)
    rounded = Decimal(repr(float(value))).quantize(step, rounding=ROUND_HALF_UP)
    return int(rounded) if digits == 0 else _number(float(rounded))


def _number(value):
    """19.0 -> 19, so the JSON reads like the one the site serves."""
    return int(value) if float(value).is_integer() else value


def merge(entsoe_countries, rn_by_code):
    """{code: country} for the 32 countries, in EU_CODES order. A country with
    no capacity in either source is left out (the site shows "capacity
    unavailable")."""
    countries = {}
    for code in EU_CODES:
        e = entsoe_countries.get(code)
        r = rn_by_code.get(code)

        # Capacity (total and per-type MW): ENTSO-E first, RenewableNodes otherwise.
        if e and _float(e.get('capacity_mw')) > 0:
            cap_mw = php_round(_float(e['capacity_mw']))
            solar_mw = php_round(_float(e.get('solar_mw')))
            wind_mw = php_round(_float(e.get('wind_mw')))
            hydro_mw = php_round(_float(e.get('hydro_mw')))
            cap_src = 'entsoe'
        elif r and _float(r.get('capacity')) > 0:
            cap_mw = php_round(_float(r['capacity']) * 1000)      # GW -> MW
            solar_mw = php_round(cap_mw * _float(r.get('solar')) / 100)
            wind_mw = php_round(cap_mw * _float(r.get('wind')) / 100)
            hydro_mw = php_round(cap_mw * _float(r.get('hydro')) / 100)
            cap_src = 'rn'
        else:
            continue

        # Bar shares: each type's share of the installed renewable capacity.
        def share(mw):
            return php_round(mw / cap_mw * 100, 1) if cap_mw > 0 else 0

        name = r.get('name') if r else None
        trend = r.get('trend') if r else None
        countries[code] = {
            'name': name if name is not None else code,
            'capacity_mw': cap_mw,
            'renewable_pct': _number(_float(r.get('renewable'))) if r else 0,   # share of generation
            'solar_pct': share(solar_mw),
            'wind_pct': share(wind_mw),
            'hydro_pct': share(hydro_mw),
            'bio_pct': _number(_float(r.get('bio'))) if r else 0,
            'nuclear_pct': _number(_float(r.get('nuclear'))) if r else 0,
            'solar_mw': solar_mw,
            'wind_mw': wind_mw,
            'hydro_mw': hydro_mw,
            'target': _int(r.get('target')) if r else 0,
            'target_year': _int(r.get('targetYear'), 2030) if r else 2030,
            'trend': trend if trend is not None else [],
            'population_m': _number(_float(r.get('pop'))) if r else 0,
            'cap_source': cap_src,                                   # 'entsoe' | 'rn'
            'cap_year': e.get('year') if cap_src == 'entsoe' else None,
        }
    return countries


def rn_index(rn):
    """RenewableNodes' country list, keyed by its `id` (the country code)."""
    by_code = {}
    for c in (rn or {}).get('countries') or []:
        if isinstance(c, dict) and c.get('id'):
            by_code[c['id']] = c
    return by_code


def build(entsoe, rn, updated=None):
    countries = merge((entsoe or {}).get('countries') or {}, rn_index(rn))
    return {
        'updated': updated or now_iso(),
        'source': 'RenewableNodes.com',
        'countries': countries,
        'count': len(countries),
    }


def is_fresh(path, max_age_s):
    previous = load_json(path)
    try:
        updated = datetime.fromisoformat(previous['updated'])
    except (TypeError, KeyError, ValueError):
        return False
    return (datetime.now(timezone.utc) - updated).total_seconds() < max_age_s


def main(argv):
    if '--force' not in argv and is_fresh(OUT_PATH, MAX_AGE_S):
        print(f'{OUT_PATH} is less than 12 hours old - nothing to do')
        return 0

    entsoe = load_json(ENTSOE_PATH, {})
    if not entsoe.get('countries'):
        print(f'{ENTSOE_PATH} missing or empty - capacity will come from RenewableNodes only')

    resp = http_get(RN_URL, timeout=30)
    print(f'RenewableNodes: {resp.describe()}')
    rn = None
    if resp.ok:
        try:
            rn = resp.json()
        except ValueError as e:
            print(f'RenewableNodes: invalid JSON ({e})')
    if not rn_index(rn):
        # Without it every country would lose its name, share, trend and target.
        print(f'No RenewableNodes data - keeping the previous {OUT_PATH}')
        return 1

    result = build(entsoe, rn)
    if not result['countries']:
        print(f'No country with capacity - keeping the previous {OUT_PATH}')
        return 1

    write_json(OUT_PATH, result, indent=2)
    by_source = {}
    for c in result['countries'].values():
        by_source[c['cap_source']] = by_source.get(c['cap_source'], 0) + 1
    print(f"Wrote {OUT_PATH} ({result['count']} countries; capacity from: {by_source})")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
