#!/usr/bin/env python3
"""merge_capacity.py - installed renewable capacity per country.

Writes data/renewablenodes-capacity.json, with the schema and the 32 countries
the site already reads (it began as a port of the site's
eew_fetch_renewablenodes_capacity(), which is where the file's name and the
rounding come from). It makes no request: it puts together three things that
are in the repository.

  - data/capacity-installed.json (ENTSO-E A68, written by fetch_capacity.py):
    the installed renewable capacity in MW, total and per technology. Primary
    source of the capacity.
  - data/ember-yearly.json (Ember's Yearly Electricity Data, CC BY 4.0,
    written by fetch_ember.py): the share of renewables in the generation of
    the last complete year (`renewable_pct`) and of the seven years that end
    in it (`trend`, oldest first), and the share of bioenergy and of nuclear
    in the generation of that year (`bio_pct`, `nuclear_pct`), all rounded to
    one decimal and never over 100. And the capacity, for a country ENTSO-E
    publishes none for (today only the United Kingdom): Ember's capacity of
    each renewable source, with the definition of fetch_capacity.py (solar,
    wind, hydro, bioenergy and the other renewables), `cap_source` "ember"
    and, in `cap_year`, the year Ember's capacity is of.
  - COUNTRIES, below: each country's name, population, renewable target and
    the year of the target.

`solar_pct`, `wind_pct` and `hydro_pct` are shares of the installed renewable
capacity, whichever source it came from.

Capacity changes once a year, so the merge only runs when the file is more
than 12 hours old; pass --force to run it regardless. Without
data/ember-yearly.json the previous file is kept.
"""

import sys
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from feed_common import load_json, now_iso, parse_time, write_json

ENTSOE_PATH = 'data/capacity-installed.json'
EMBER_PATH = 'data/ember-yearly.json'
OUT_PATH = 'data/renewablenodes-capacity.json'
MAX_AGE_S = 12 * 3600

# The 32 countries, in the order of the file (COUNTRIES of fetch_capacity.py,
# plus GB): name, population in millions, renewable target in % of the
# generation and the year of the target. The values are the ones the file had
# on 18 November 2026, when they stopped being read from RenewableNodes.
COUNTRIES = {
    'PT': ('Portugal', 10.3, 85, 2030),
    'ES': ('Spain', 47.4, 74, 2030),
    'DE': ('Germany', 83.2, 80, 2030),
    'FR': ('France', 67.8, 44, 2030),
    'IT': ('Italy', 59, 65, 2030),
    'NL': ('Netherlands', 17.5, 73, 2030),
    'BE': ('Belgium', 11.6, 47, 2030),
    'AT': ('Austria', 9.1, 100, 2030),
    'CH': ('Switzerland', 8.7, 76, 2030),
    'PL': ('Poland', 37.7, 32, 2030),
    'DK': ('Denmark', 5.9, 100, 2030),
    'SE': ('Sweden', 10.5, 100, 2040),
    'NO': ('Norway', 5.5, 100, 2030),
    'FI': ('Finland', 5.5, 80, 2030),
    'GR': ('Greece', 10.4, 70, 2030),
    'IE': ('Ireland', 5.1, 80, 2030),
    'GB': ('United Kingdom', 67.3, 100, 2035),
    'RO': ('Romania', 19, 70, 2030),
    'BG': ('Bulgaria', 6.5, 30, 2030),
    'HU': ('Hungary', 9.7, 24, 2030),
    'CZ': ('Czechia', 10.7, 30, 2030),
    'EE': ('Estonia', 1.3, 50, 2030),
    'LV': ('Latvia', 1.8, 80, 2030),
    'LT': ('Lithuania', 2.8, 85, 2030),
    'SI': ('Slovenia', 2.1, 35, 2030),
    'HR': ('Croatia', 3.9, 100, 2030),
    'RS': ('Serbia', 6.6, 40, 2030),
    'ME': ('Montenegro', 0.62, 50, 2030),
    'MK': ('North Macedonia', 2.1, 38, 2030),
    'AL': ('Albania', 2.8, 100, 2030),
    'BA': ('Bosnia Herzegovina', 3.2, 40, 2030),
    'XK': ('Kosovo', 1.8, 25, 2030),
}
EU_CODES = list(COUNTRIES)

# Ember's renewable sources, the ones fetch_capacity.py counts for ENTSO-E.
EMBER_RENEWABLES = ('solar', 'wind', 'hydro', 'bioenergy', 'other_renewables')


def _float(value):
    """PHP floatval(): a number, or 0 for anything that is not one."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def php_round(value, digits=0):
    """PHP round(): halves go away from zero (Python's round() sends them to
    the even neighbour). Whole numbers come back as int."""
    step = Decimal(1).scaleb(-digits)
    rounded = Decimal(repr(float(value))).quantize(step, rounding=ROUND_HALF_UP)
    return int(rounded) if digits == 0 else _number(float(rounded))


def _number(value):
    """19.0 -> 19, so the JSON reads like the one the site serves."""
    return int(value) if float(value).is_integer() else value


def _share(value):
    """A share of the generation as the file gives it: one decimal, never
    over 100 (Ember's shares of a country that is all renewable come as
    100.005), or None when there is no value."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return php_round(min(max(float(value), 0.0), 100.0), 1)


def ember_capacity(country):
    """(total, solar, wind, hydro, year) in MW of the renewable capacity
    Ember has for a country, or None when it has none."""
    capacity = (country or {}).get('capacity_gw')
    if not isinstance(capacity, dict):
        return None
    mw = {name: Decimal(repr(float(capacity[name]))) * 1000 for name in EMBER_RENEWABLES
          if isinstance(capacity.get(name), (int, float)) and not isinstance(capacity.get(name), bool)}
    total = sum(mw.values(), Decimal(0))
    if total <= 0:
        return None
    return (php_round(total), php_round(mw.get('solar', 0)), php_round(mw.get('wind', 0)),
            php_round(mw.get('hydro', 0)), capacity.get('year'))


def merge(entsoe_countries, ember_countries):
    """{code: country} for the 32 countries, in EU_CODES order. A country with
    no capacity in either source is left out (the site shows "capacity
    unavailable")."""
    countries = {}
    for code, (name, population, target, target_year) in COUNTRIES.items():
        e = entsoe_countries.get(code)
        m = ember_countries.get(code) or {}

        # Capacity (total and per-type MW): ENTSO-E first, Ember otherwise.
        from_ember = ember_capacity(m)
        if e and _float(e.get('capacity_mw')) > 0:
            cap_mw = php_round(_float(e['capacity_mw']))
            solar_mw = php_round(_float(e.get('solar_mw')))
            wind_mw = php_round(_float(e.get('wind_mw')))
            hydro_mw = php_round(_float(e.get('hydro_mw')))
            cap_src, cap_year = 'entsoe', e.get('year')
        elif from_ember:
            cap_mw, solar_mw, wind_mw, hydro_mw, cap_year = from_ember
            cap_src = 'ember'
        else:
            continue

        # Bar shares: each type's share of the installed renewable capacity.
        def share(mw):
            return php_round(mw / cap_mw * 100, 1) if cap_mw > 0 else 0

        shares = m.get('share_of_generation_pct') or {}
        trend = (m.get('renewables_trend') or {}).get('share_of_generation_pct') or []
        countries[code] = {
            'name': name,
            'capacity_mw': cap_mw,
            'renewable_pct': _share(shares.get('renewables')) or 0,      # share of generation
            'solar_pct': share(solar_mw),
            'wind_pct': share(wind_mw),
            'hydro_pct': share(hydro_mw),
            'bio_pct': _share(shares.get('bioenergy')) or 0,
            'nuclear_pct': _share(shares.get('nuclear')) or 0,
            'solar_mw': solar_mw,
            'wind_mw': wind_mw,
            'hydro_mw': hydro_mw,
            'target': target,
            'target_year': target_year,
            'trend': [_share(value) for value in trend],
            'population_m': population,
            'cap_source': cap_src,                                   # 'entsoe' | 'ember'
            'cap_year': cap_year,
        }
    return countries


def source_of(ember):
    """The file's `source`: where its numbers come from."""
    edition = parse_time((ember or {}).get('edition'))
    of = f', edition of {edition.date().isoformat()}' if edition else ''
    return ('ENTSO-E Transparency Platform, installed capacity per production type (capacity); '
            f'Ember, Yearly Electricity Data{of}, CC BY 4.0 (shares of the generation and their trend, '
            'and the capacity of a country ENTSO-E publishes none for)')


def build(entsoe, ember, updated=None):
    countries = merge((entsoe or {}).get('countries') or {}, (ember or {}).get('countries') or {})
    return {
        'updated': updated or now_iso(),
        'source': source_of(ember),
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
        print(f'{ENTSOE_PATH} missing or empty - capacity will come from Ember only')

    ember = load_json(EMBER_PATH, {})
    if not isinstance(ember, dict) or not isinstance(ember.get('countries'), dict) or not ember['countries']:
        # Without it every country would lose its share and its trend.
        print(f'{EMBER_PATH} missing or empty - keeping the previous {OUT_PATH}')
        return 1

    result = build(entsoe, ember)
    if not result['countries']:
        print(f'No country with capacity - keeping the previous {OUT_PATH}')
        return 1

    write_json(OUT_PATH, result, indent=2)
    by_source = {}
    for c in result['countries'].values():
        by_source[c['cap_source']] = by_source.get(c['cap_source'], 0) + 1
    print(f"Wrote {OUT_PATH} ({result['count']} countries; capacity from: {by_source}; "
          f"Ember's edition: {ember.get('edition')})")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
