#!/usr/bin/env python3
"""fetch_price_history.py - the history of the day-ahead prices, from open sources only, for the Pro plan.

The Pro plan sells charts and exports (CSV, Excel) of the history of the
day-ahead prices; the feed prepares the data, in data/price-history/, and the
site draws and exports them. Only sources whose licence allows that:

    zone            source                                   licence     from
    15 zones        SMARD (Bundesnetzagentur)                cc-by-4.0   1 Jan 2015 (DE-LU and AT: see below)
                    and, for the days SMARD lacks,
                    Energy-Charts (republishing SMARD)       cc-by-4.0
    PT, ES          OMIE, day-ahead market (marginalpdbc)    omie        1 Jan 2018

SMARD. The same series fetch_open_prices.py reads (SMARD_ZONES there), from
the weekly files of SMARD: by the hour up to the week of the move to
quarter-hours (1 Oct 2025), and by the quarter-hour from then on. A day whose
four quarters of each hour have the same price (SMARD repeats the hourly price
where a zone had no quarter-hours, as Switzerland until 4 Nov 2026) is kept by
the hour. Germany-Luxembourg and Austria were one bidding zone until 30 Sep
2018: for the days before, both have SMARD's series of that zone (DE-AT-LU,
filter 251), and each of those days says so (`series`). A day SMARD has with a
missing price is not taken: it is asked of Energy-Charts, with the rules of
data/open-prices.json (taken only under CC BY 4.0 and whole; see
fetch_open_prices.py), at most EC_PER_RUN ranges of days a run in the daily
update, and 2 a minute in the first fill (its limit).

OMIE. Its public files of the day-ahead prices of Portugal and Spain go back to
2018: a zip a year from 2018 to 2022 (marginalpdbc_YYYY.zip) and a file a day
from 2023 (marginalpdbc_YYYYMMDD.N); of a day with more than one version, the
last one is taken. Hours up to 30 Sep 2025, quarter-hours from 1 Oct 2025.

Ember (European Wholesale Electricity Price Data) is not used: on 24 Nov 2026
its page gave no licence of its own for these prices, which it says it takes
from ENTSO-E, whose day-ahead prices are not free to reuse (see feed_common.py).
A decision for the owner of the site.

The files (times in UTC, prices in EUR/MWh; a day is a delivery day on the
market's clock, so it has 23, 24 or 25 hours, or 92, 96 or 100 quarter-hours):

    data/price-history/index.json
        {updated, unit, timezone, zones: {ZONE: {license, attribution, first, last, years,
         sources: [{source, from, to, days[, series]}], missing_days}}, files}
    data/price-history/<ZONE>/<YEAR>.json       one zone, one year, every interval
        {zone, year, unit, license, attribution, updated,
         days: {"2026-11-24": {start, step_minutes, prices: [...], source[, series][, license_info]}}}
    data/price-history/daily/<YEAR>.json        the daily means of every zone, one year
        {year, unit, zones: {ZONE: {license, attribution, first, values: [...], sources}}}
    data/price-history/monthly.json             the monthly means of every zone, every year
        {unit, zones: {ZONE: {license, attribution, first, values: [...], days: [...], sources}}}

A chart of a year is one file (daily/<YEAR>.json, or the zone's year for every
interval); an export of a range of dates is the zone's files of those years.
In daily and monthly, `values` starts on `first` (a day, or a month YYYY-MM)
and has one value for each day or month from there, null where there is no
price; a mean is the mean of the day's (or month's) intervals, each weighted
by its length, and `days` says how many days a month's mean has. `sources`
says, in spans of days, where the values came from. A file is only written
when what it holds changes.

Runs:
    python fetch_price_history.py            the daily update (price-history.yml): the days of the last
                                             UPDATE_DAYS up to tomorrow that are missing
    python fetch_price_history.py --fill [ZONE ...]
                                             the first fill (fill-price-history.yml, by hand): every year
                                             of each zone from its first day, committing and pushing a
                                             zone at a time; ends in error if it published nothing or a
                                             push failed. A second start takes up where the first left off.
"""

import io
import os
import sys
import time
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import carry_over
import fetch_omie
import fetch_open_prices as op
from feed_common import CC_BY, OMIE_TERMS, Budget, http_get, iso, load_json, write_json_if_changed

OUT_DIR = 'data/price-history'
FIRST_DAY = date(2015, 1, 1)
OMIE_FIRST_DAY = date(2018, 1, 1)
OMIE_ZIP_YEARS = range(2018, 2023)          # a zip a year; from 2023, a file a day
OMIE_ZIP_URL = 'https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_{year}.zip'
OMIE_ATTRIBUTION = 'OMIE'
JOINT = (251, 'DE')                          # SMARD's DE-AT-LU, for DE-LU and AT until JOINT_UNTIL
JOINT_UNTIL = date(2018, 9, 30)
QUARTERS_FROM = date(2025, 9, 29)            # the week of the move to quarter-hours (1 Oct 2025)
UPDATE_DAYS = 10
EC_PER_RUN = 2
EC_GAP_S = 31                                # between requests to Energy-Charts in the first fill
EC_LONGEST = 31                              # days in one request to Energy-Charts
OMIE_PAUSE_S = fetch_omie.PAUSE_S            # between requests to OMIE
UNIT = 'EUR/MWh'
SMARD_ZONES = list(op.SMARD_ZONES)
ZONES = SMARD_ZONES + ['PT', 'ES']


# --- Days ---

def days_between(first, last):
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def first_day(zone):
    return OMIE_FIRST_DAY if zone in ('PT', 'ES') else FIRST_DAY


def hourly_if_repeated(block):
    """A quarter-hour day block by the hour when each hour's four quarters have the same price."""
    prices = block['prices']
    if block['step_minutes'] != 15 or len(prices) % 4:
        return block
    if any(len(set(prices[i:i + 4])) != 1 for i in range(0, len(prices), 4)):
        return block
    return {**block, 'step_minutes': 60, 'prices': prices[::4]}


def mean(block):
    """The mean price of a day block (its intervals all have the same length)."""
    values = [v for v in block['prices'] if v is not None]
    return round(sum(values) / len(values), 2) if values else None


# --- SMARD ---

def smard_days(zone, days, get, budget, pause=op.PAUSE_S):
    """({day: block}, [problems]) of the whole days of `days` that SMARD has for a zone."""
    found, problems = {}, []
    groups = defaultdict(list)                    # (filter, region, resolution) -> days
    for day in days:
        series = JOINT if zone in ('DE-LU', 'AT') and day <= JOINT_UNTIL else op.SMARD_ZONES[zone]
        groups[(series, 'quarterhour' if day >= QUARTERS_FROM else 'hour')].append(day)
    for ((filter_id, region), resolution), wanted in groups.items():
        step = timedelta(minutes=15 if resolution == 'quarterhour' else 60)
        first, end = carry_over.day_bounds(wanted[0])[0], carry_over.day_bounds(wanted[-1])[1]
        try:
            values, trouble = op.smard_series(filter_id, region, resolution, first, end, get, budget, pause)
        except op.SourceDown as e:
            problems.append(f'SMARD {filter_id} {resolution}: {e}')
            continue
        problems += trouble
        for day in wanted:
            block = op.day_block(day, step, values)
            if block:
                block = hourly_if_repeated({**block, 'source': 'smard'})
                if (filter_id, region) == JOINT:
                    block['series'] = 'DE-AT-LU'
                found[day.isoformat()] = block
    return found, problems


def ranges(days, longest=EC_LONGEST):
    """Days in runs of consecutive days, none longer than `longest`."""
    runs = []
    for day in sorted(days):
        if runs and day - runs[-1][-1] == timedelta(days=1) and len(runs[-1]) < longest:
            runs[-1].append(day)
        else:
            runs.append([day])
    return runs


def energy_charts_days(zone, days, get, budget, at_most, gap_s=0, log=print):
    """({day: block}, asked) of the days of `days` that Energy-Charts has whole for a SMARD zone under CC BY 4.0,
    in at most `at_most` requests (one for each run of days), `gap_s` apart; stops at the first 429."""
    found, asked = {}, 0
    for run in ranges(days):
        if asked >= at_most:
            log(f'  {zone}: Energy-Charts not asked for {run[0]}..{run[-1]} in this run ({at_most} requests at most)')
            continue
        if asked and gap_s:
            time.sleep(gap_s)
        asked += 1
        try:
            got = op.ec_days(zone, run, get, budget)
        except op.RateLimited as e:
            log(f'  {zone}: Energy-Charts did not give {run[0]}..{run[-1]} ({e}); no more requests to it')
            break
        except op.SourceDown as e:
            log(f'  {zone}: Energy-Charts did not give {run[0]}..{run[-1]} ({e})')
            continue
        found.update({name: hourly_if_repeated({key: value for key, value in block.items() if key != 'license'})
                      for name, block in got.items()})
        log(f'  {zone}: Energy-Charts (CC BY 4.0), {len(got)} of the {len(run)} days {run[0]}..{run[-1]}')
    return found, asked


# --- OMIE ---

def omie_blocks(text):
    """{(day, zone): block} of a marginalpdbc file."""
    day, rows = fetch_omie.parse_marginalpdbc(text)
    starts, minutes = fetch_omie.period_starts(day, len(rows))
    found = {}
    for index, zone in ((1, 'PT'), (2, 'ES')):
        values = {local.astimezone(timezone.utc): op._round(row[index]) for local, row in zip(starts, rows)}
        block = op.day_block(day, timedelta(minutes=minutes), values)
        if block:
            found[(day, zone)] = {**block, 'source': 'omie'}
    return found


def omie_year_zip(year, get):
    """{(day, zone): block} of a year's zip of OMIE, the last version of each day."""
    response = get(OMIE_ZIP_URL.format(year=year), timeout=120)
    if not response.ok:
        raise op.SourceDown(f'zip of {year}: {response.describe()}')
    try:
        archive = zipfile.ZipFile(io.BytesIO(response.body))
    except zipfile.BadZipFile:
        raise op.SourceDown(f'zip of {year}: not a zip')
    latest = {}
    for name in archive.namelist():
        stem, _, version = name.rpartition('.')
        if stem.startswith('marginalpdbc_') and version.isdigit():
            if int(version) > int(latest.get(stem, '0').rpartition('.')[2] or 0):
                latest[stem] = name
    found = {}
    for name in latest.values():
        try:
            found.update(omie_blocks(archive.read(name).decode('latin-1')))
        except fetch_omie.OmieFormatError as e:
            print(f'  OMIE {name}: not usable ({e})')
    return found


def omie_day(day, get, pause=None):
    """{(day, zone): block} of one day's file of OMIE (the last version there is), {} when there is none."""
    found = {}
    for version in range(1, fetch_omie.MAX_VERSION + 1):
        response = get(fetch_omie.URL.format(ymd=day.strftime('%Y%m%d'), version=version), timeout=30)
        time.sleep(OMIE_PAUSE_S if pause is None else pause)
        if response.status == 404:
            break
        if not response.ok:
            raise op.SourceDown(f'{day} version {version}: {response.describe()}')
        try:
            found = omie_blocks(response.text('latin-1'))
        except fetch_omie.OmieFormatError as e:
            print(f'  OMIE {day} version {version}: not usable ({e})')
    return found


_omie_read = {}         # what this run already read of OMIE (its files have both zones): year or day -> blocks


def omie_days(days, get, pause=None):
    """({(day, zone): block}, [problems]) of OMIE for `days`: the zips of their years up to 2022, then a file a day.
    What was read for one zone is not asked again for the other."""
    found, problems = {}, []
    for year in sorted({day.year for day in days if day.year in OMIE_ZIP_YEARS}):
        if year not in _omie_read:
            try:
                _omie_read[year] = omie_year_zip(year, get)
            except op.SourceDown as e:
                problems.append(str(e))
                continue
        found.update(_omie_read[year])
    for day in days:
        if day.year in OMIE_ZIP_YEARS or day < OMIE_FIRST_DAY:
            continue
        if day not in _omie_read:
            try:
                _omie_read[day] = omie_day(day, get, pause)
            except op.SourceDown as e:
                problems.append(str(e))
                continue
        found.update(_omie_read[day])
    wanted = set(days)
    return {key: block for key, block in found.items() if key[0] in wanted}, problems


# --- The files ---

def zone_path(zone, year, folder=OUT_DIR):
    return os.path.join(folder, zone, f'{year}.json')


def licence(zone):
    return OMIE_TERMS if zone in ('PT', 'ES') else CC_BY


def attribution(zone, days):
    if zone in ('PT', 'ES'):
        return OMIE_ATTRIBUTION
    through = any(block.get('source') == 'energy-charts' for block in days.values())
    return op.ATTRIBUTION + (f', {op.EC_ATTRIBUTION}' if through else '')


def load_zone_year(zone, year, folder=OUT_DIR):
    doc = load_json(zone_path(zone, year, folder)) or {}
    return {name: block for name, block in (doc.get('days') or {}).items() if isinstance(block, dict)}


def merge(have, new):
    """`have` with the day blocks of `new`: a block of the source itself (SMARD, OMIE) replaces what was there; one
    of Energy-Charts only fills a day there was nothing for."""
    merged = dict(have)
    for name, block in new.items():
        if block.get('source') != 'energy-charts' or name not in merged:
            merged[name] = block
    return dict(sorted(merged.items()))


def write_zone_year(zone, year, days, now, folder=OUT_DIR):
    """Writes a zone's year when its days change; returns whether it was written."""
    path = zone_path(zone, year, folder)
    before = load_json(path) or {}
    if before.get('days') == days:
        return False
    doc = {'zone': zone, 'year': year, 'unit': UNIT, 'license': licence(zone),
           'attribution': attribution(zone, days), 'updated': iso(now), 'days': days}
    return write_json_if_changed(path, doc, separators=(',', ':'))


def spans(items):
    """[{source, from, to, days[, series]}] of (day, block) in order: a span for each run of the same source."""
    out = []
    for name, block in items:
        key = (block.get('source'), block.get('series'))
        if out and (out[-1]['source'], out[-1].get('series')) == key:
            out[-1]['to'] = name
            out[-1]['days'] += 1
        else:
            out.append({'source': key[0], 'from': name, 'to': name, 'days': 1, **({'series': key[1]} if key[1] else {})})
    return out


def zone_years(zone, folder=OUT_DIR):
    path = os.path.join(folder, zone)
    if not os.path.isdir(path):
        return []
    return sorted(int(name[:-5]) for name in os.listdir(path) if name.endswith('.json') and name[:-5].isdigit())


def build_summaries(now, folder=OUT_DIR):
    """Writes daily/<YEAR>.json, monthly.json and index.json from the zones' files; returns how many were written."""
    written = 0
    all_days = {}                                 # zone -> [(name, block)]
    for zone in ZONES:
        items = []
        for year in zone_years(zone, folder):
            items += sorted(load_zone_year(zone, year, folder).items())
        if items:
            all_days[zone] = items
    years = sorted({int(name[:4]) for items in all_days.values() for name, _ in items})
    for year in years:
        zones = {}
        for zone, items in all_days.items():
            in_year = [(name, block) for name, block in items if name.startswith(f'{year}-')]
            if not in_year:
                continue
            first = date.fromisoformat(in_year[0][0])
            by_day = dict(in_year)
            values = [mean(by_day[d.isoformat()]) if d.isoformat() in by_day else None
                      for d in days_between(first, date.fromisoformat(in_year[-1][0]))]
            zones[zone] = {'license': licence(zone), 'attribution': attribution(zone, by_day),
                           'first': first.isoformat(), 'values': values, 'sources': spans(in_year)}
        written += write_json_if_changed(os.path.join(folder, 'daily', f'{year}.json'),
                                         {'year': year, 'unit': UNIT, 'zones': zones}, separators=(',', ':'))
    monthly, index = {}, {}
    for zone, items in all_days.items():
        months = defaultdict(lambda: [0.0, 0.0, 0])   # YYYY-MM -> [sum of price x minutes, minutes, days]
        for name, block in items:
            priced = [v for v in block['prices'] if v is not None]
            kept = months[name[:7]]
            kept[0] += sum(priced) * block['step_minutes']
            kept[1] += len(priced) * block['step_minutes']
            kept[2] += 1
        first, last = items[0][0][:7], items[-1][0][:7]
        names, cursor = [], date.fromisoformat(first + '-01')
        while cursor.strftime('%Y-%m') <= last:
            names.append(cursor.strftime('%Y-%m'))
            cursor = (cursor + timedelta(days=32)).replace(day=1)
        by_day = dict(items)
        monthly[zone] = {'license': licence(zone), 'attribution': attribution(zone, by_day), 'first': first,
                         'values': [round(months[m][0] / months[m][1], 2) if months.get(m) and months[m][1] else None
                                    for m in names],
                         'days': [months[m][2] if m in months else 0 for m in names], 'sources': spans(items)}
        expected = len(days_between(date.fromisoformat(items[0][0]), date.fromisoformat(items[-1][0])))
        index[zone] = {'license': licence(zone), 'attribution': attribution(zone, by_day),
                       'first': items[0][0], 'last': items[-1][0], 'years': zone_years(zone, folder),
                       'missing_days': expected - len(items), 'sources': spans(items)}
    written += write_json_if_changed(os.path.join(folder, 'monthly.json'),
                                     {'unit': UNIT, 'zones': monthly}, separators=(',', ':'))
    doc = {'unit': UNIT, 'timezone': 'UTC', 'zones': {zone: index[zone] for zone in ZONES if zone in index},
           'files': {'zone_year': 'data/price-history/<ZONE>/<YEAR>.json',
                     'daily': 'data/price-history/daily/<YEAR>.json',
                     'monthly': 'data/price-history/monthly.json'}}
    before = load_json(os.path.join(folder, 'index.json')) or {}
    if {k: v for k, v in before.items() if k != 'updated'} != doc:
        written += write_json_if_changed(os.path.join(folder, 'index.json'), {'updated': iso(now), **doc}, indent=1)
    return written


# --- The runs ---

def missing_days(zone, days, folder=OUT_DIR):
    have = {}
    for year in sorted({day.year for day in days}):
        have.update(load_zone_year(zone, year, folder))
    return [day for day in days if day.isoformat() not in have]


def fetch_zone(zone, days, get, budget, ec_at_most, ec_gap_s=0, pause=op.PAUSE_S):
    """({day: block}, requests to Energy-Charts) of a zone for `days`, from its source (and, for a SMARD zone,
    Energy-Charts for the days SMARD lacks)."""
    if zone in ('PT', 'ES'):
        found, problems = omie_days(days, get)
        found = {key[0].isoformat(): block for key, block in found.items() if key[1] == zone}
    else:
        found, problems = smard_days(zone, days, get, budget, pause)
    for problem in problems[:5]:
        print(f'  {zone}: {problem}')
    lacking = [day for day in days if day.isoformat() not in found]
    asked = 0
    if zone in op.SMARD_ZONES and lacking and ec_at_most > 0:
        got, asked = energy_charts_days(zone, lacking, get, budget, ec_at_most, ec_gap_s)
        found.update(got)
    return found, asked


def store(zone, found, now, folder=OUT_DIR):
    """Merges the blocks found into the zone's files; returns the years written."""
    by_year = defaultdict(dict)
    for name, block in found.items():
        by_year[int(name[:4])][name] = block
    written = []
    for year, blocks in sorted(by_year.items()):
        if write_zone_year(zone, year, merge(load_zone_year(zone, year, folder), blocks), now, folder):
            written.append(year)
    return written


def update(now=None, get=http_get, budget=None, folder=OUT_DIR, pause=op.PAUSE_S):
    """The daily update: the missing days of the last UPDATE_DAYS up to tomorrow."""
    now = now or datetime.now(timezone.utc)
    budget = budget or Budget(float(os.environ.get('PRICE_HISTORY_BUDGET_SECONDS', '600')))
    today = carry_over.delivery_day(now)
    window = days_between(today - timedelta(days=UPDATE_DAYS), today + timedelta(days=1))
    ec_left = EC_PER_RUN
    for zone in ZONES:
        lacking = missing_days(zone, window, folder)
        if not lacking:
            continue
        found, asked = fetch_zone(zone, lacking, get, budget, ec_left, pause=pause)
        ec_left -= asked
        written = store(zone, found, now, folder)
        print(f"  {zone}: {len(found)} of the {len(lacking)} missing days"
              + (f"; wrote {', '.join(map(str, written))}" if written else ''))
    summaries = build_summaries(now, folder)
    print(f'Price history: {summaries} summary files written')
    return 0


def fill(zones=None, now=None, get=http_get, folder=OUT_DIR, publish=None, ec_gap_s=EC_GAP_S, pause=op.PAUSE_S):
    """The first fill: every missing day of each zone from its first day up to today, a zone at a time, each
    published (`publish(message)`) before the next. Returns 1 when nothing was published or a publication failed."""
    now = now or datetime.now(timezone.utc)
    today = carry_over.delivery_day(now)
    published, failed, tried = 0, 0, 0
    for zone in zones or ZONES:
        started = time.monotonic()
        lacking = missing_days(zone, days_between(first_day(zone), today), folder)
        if not lacking:
            print(f'{zone}: nothing missing')
            continue
        print(f'{zone}: {len(lacking)} days missing, {lacking[0]} to {lacking[-1]}')
        tried += 1
        found, _asked = fetch_zone(zone, lacking, get, Budget(3 * 3600), ec_at_most=40, ec_gap_s=ec_gap_s,
                                   pause=pause)
        written = store(zone, found, now, folder)
        build_summaries(now, folder)
        print(f'{zone}: {len(found)} days found, years written {written or "none"} '
              f'[{time.monotonic() - started:.0f}s]')
        if publish and written:
            if publish(f'Price history of {zone}: {len(found)} days ({lacking[0]} to {lacking[-1]})'):
                published += 1
            else:
                failed += 1
    if not tried:
        print('Price history: nothing missing in any zone')
        return 0
    if publish is None:                           # a trial on this computer: nothing to publish
        return 0
    if failed or not published:
        print(f'::error::price history: {published} zones published, {failed} not')
        return 1
    return 0


def main(argv):
    if argv[:1] == ['--fill']:
        import backfill_archive
        return fill(argv[1:] or None,
                    publish=lambda message: backfill_archive._commit_and_push(message, path=OUT_DIR + '/'))
    return update()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
