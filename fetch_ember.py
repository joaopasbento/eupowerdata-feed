#!/usr/bin/env python3
"""fetch_ember.py - the yearly figures the feed takes from Ember.

Ember publishes its "Yearly Electricity Data" as one CSV file for the whole
world (16 MB), under CC BY 4.0, with no API key. merge_capacity.py needs very
little of it, for the 32 countries of data/renewablenodes-capacity.json:

  - the share of renewables in the generation of the last complete year, and
    of the six years before it (the trend);
  - the share of bioenergy and of nuclear in the generation of that year;
  - the installed capacity of each renewable source, which stands in for a
    country ENTSO-E publishes no capacity for (today only the United Kingdom).

This script reads the file and keeps only that, in data/ember-yearly.json:

    {updated, license: "cc-by-4.0", source, url, file, edition,
     countries: {PT: {iso3, year,
                      share_of_generation_pct: {renewables, bioenergy, nuclear},
                      renewables_trend: {years: [7], share_of_generation_pct: [7]},
                      capacity_gw: {year, solar, wind, hydro, bioenergy, other_renewables}}}}

`edition` is when Ember last changed the file (its Last-Modified). The values
are Ember's, as they come: merge_capacity.py does the rounding.

`year` is the last complete year: the last one before the year of the edition
that has a share of renewables for the country. A year of the trend Ember has
no share for is null. `capacity_gw` is of the last year, not after `year`, in
which Ember has the capacity of solar, wind and hydro, the three the feed
shows by name; bioenergy and other renewables are in it when Ember has them
for that year. An empty cell is no value, not zero, so a year one of the three
is missing in is not used: a sum of what is left would pass for the whole. A
country with no such year has no `capacity_gw`.

The file is only downloaded when Ember has changed it since the edition kept
(If-Modified-Since); --force downloads it regardless. When it cannot be had,
is not laid out as expected or lacks one of the 32 countries, the file kept
stays as it is and the script ends with an error, for the workflow to show it.

Run by .github/workflows/fetch-ember.yml, once a week.
"""

import csv
import io
import sys
from datetime import timezone
from email.utils import format_datetime, parsedate_to_datetime

from feed_common import http_get, iso, load_json, now_iso, parse_time, write_json

PATH = 'data/ember-yearly.json'
URL = 'https://files.ember-energy.org/public-downloads/generation/outputs/release_generation_yearly_global.csv'
PAGE = 'https://ember-energy.org/data/yearly-electricity-data/'
SOURCE = 'Ember, Yearly Electricity Data'
LICENSE = 'cc-by-4.0'
TREND_YEARS = 7

# The 32 countries of data/renewablenodes-capacity.json, in its order, with
# the code Ember knows each by (Kosovo has no ISO code: Ember uses XKX).
ISO3 = {
    'PT': 'PRT', 'ES': 'ESP', 'DE': 'DEU', 'FR': 'FRA', 'IT': 'ITA', 'NL': 'NLD', 'BE': 'BEL', 'AT': 'AUT',
    'CH': 'CHE', 'PL': 'POL', 'DK': 'DNK', 'SE': 'SWE', 'NO': 'NOR', 'FI': 'FIN', 'GR': 'GRC', 'IE': 'IRL',
    'GB': 'GBR', 'RO': 'ROU', 'BG': 'BGR', 'HU': 'HUN', 'CZ': 'CZE', 'EE': 'EST', 'LV': 'LVA', 'LT': 'LTU',
    'SI': 'SVN', 'HR': 'HRV', 'RS': 'SRB', 'ME': 'MNE', 'MK': 'MKD', 'AL': 'ALB', 'BA': 'BIH', 'XK': 'XKX',
}

CODE, YEAR, WHAT = 'ISO 3 code', 'Year', 'Electricity source'
SHARE, CAPACITY = 'Share of generation (%)', 'Capacity (GW)'
# Ember's name of each row the feed reads -> the name it has in the file.
SHARES = {'Renewables': 'renewables', 'Bioenergy': 'bioenergy', 'Nuclear': 'nuclear'}
# The renewable sources, as fetch_capacity.py counts them for ENTSO-E: solar,
# wind, hydro without pumped storage, biomass, and geothermal, marine and the
# other renewables, which Ember gives together.
CAPACITIES = {'Solar': 'solar', 'Wind': 'wind', 'Hydro': 'hydro', 'Bioenergy': 'bioenergy',
              'Other renewables': 'other_renewables'}
SHOWN = ('solar', 'wind', 'hydro')


def _number(text):
    """A cell of the file as a number, or None when it is empty or not one."""
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def read(text, before_year, iso3=None):
    """What the feed uses of Ember's file: {code: its entry}, in the order of
    `iso3`, for the countries of it the file has a share of renewables for.
    `before_year` is the year of the edition: only years before it are
    complete. Raises ValueError when the file has not the columns it reads."""
    iso3 = ISO3 if iso3 is None else iso3
    rows = csv.DictReader(io.StringIO(text))
    missing = [name for name in (CODE, YEAR, WHAT, SHARE, CAPACITY) if name not in (rows.fieldnames or [])]
    if missing:
        raise ValueError(f'columns missing from the file: {missing}')
    codes = {theirs: ours for ours, theirs in iso3.items()}
    shares = {code: {} for code in iso3}          # code -> year -> name -> %
    capacities = {code: {} for code in iso3}      # code -> year -> name -> GW
    for row in rows:
        code = codes.get(row[CODE])
        if code is None:
            continue
        try:
            year = int(row[YEAR])
        except (TypeError, ValueError):
            continue
        for names, column, kept in ((SHARES, SHARE, shares), (CAPACITIES, CAPACITY, capacities)):
            name, value = names.get(row[WHAT]), _number(row[column])
            if name is not None and value is not None:
                kept[code].setdefault(year, {})[name] = value
    countries = {}
    for code in iso3:
        entry = _country(iso3[code], shares[code], capacities[code], before_year)
        if entry is not None:
            countries[code] = entry
    return countries


def _country(iso3, shares, capacities, before_year):
    complete = [year for year, values in shares.items() if year < before_year and 'renewables' in values]
    if not complete:
        return None
    last = max(complete)
    years = list(range(last - TREND_YEARS + 1, last + 1))
    entry = {
        'iso3': iso3,
        'year': last,
        'share_of_generation_pct': {name: shares[last].get(name) for name in SHARES.values()},
        'renewables_trend': {'years': years,
                             'share_of_generation_pct': [shares.get(year, {}).get('renewables') for year in years]},
    }
    whole = [year for year, values in capacities.items() if year <= last and all(name in values for name in SHOWN)]
    if whole:
        year = max(whole)
        entry['capacity_gw'] = {'year': year, **{name: capacities[year][name] for name in CAPACITIES.values()
                                                 if name in capacities[year]}}
    return entry


def edition_of(response):
    """When Ember last changed the file, from the answer's Last-Modified, or None."""
    try:
        changed = parsedate_to_datetime(response.headers.get('last-modified'))
    except (TypeError, ValueError):
        return None
    return changed if changed.tzinfo else changed.replace(tzinfo=timezone.utc)


def build(text, edition, updated=None):
    """The file to write, from Ember's CSV and the time of its edition."""
    return {
        'updated': updated or now_iso(),
        'license': LICENSE,
        'source': SOURCE,
        'url': PAGE,
        'file': URL,
        'edition': iso(edition),
        'countries': read(text, edition.astimezone(timezone.utc).year),
    }


def main(argv=(), get=http_get, path=PATH):
    previous = load_json(path) or {}
    kept = parse_time(previous.get('edition'))
    headers = {}
    if kept is not None and previous.get('countries') and '--force' not in argv:
        headers['If-Modified-Since'] = format_datetime(kept.astimezone(timezone.utc), usegmt=True)
    response = get(URL, timeout=120, headers=headers)
    print(f'Ember: {response.describe()}')
    if response.status == 304:
        print(f"Ember has not changed the file since {previous.get('edition')} - nothing to do")
        return 0
    if not response.ok:
        print(f'No file from Ember - keeping the previous {path}')
        return 1
    edition = edition_of(response)
    if edition is None:
        print(f'The answer does not say when the file was last changed - keeping the previous {path}')
        return 1
    try:
        doc = build(response.text(), edition)
    except (ValueError, csv.Error) as e:
        print(f'The file from Ember cannot be read ({e}) - keeping the previous {path}')
        return 1
    missing = [code for code in ISO3 if code not in doc['countries']]
    if missing:
        print(f'The file from Ember has no share of renewables for {missing} - keeping the previous {path}')
        return 1
    write_json(path, doc, indent=1)
    years = sorted({entry['year'] for entry in doc['countries'].values()})
    without = [code for code, entry in doc['countries'].items() if 'capacity_gw' not in entry]
    print(f"Wrote {path}: edition of {doc['edition']}, {len(doc['countries'])} countries, last complete year "
          f"{years}, no capacity for {without or 'none'}")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
