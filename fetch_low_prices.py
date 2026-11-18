#!/usr/bin/env python3
"""fetch_low_prices.py - how long the price was negative, zero or low, and the
price difference between Portugal and Spain.

Writes data/low-prices.json, with two blocks that are kept apart because they
are under different licences (see feed_common.py):

    omie     omie           From OMIE's files, for Portugal and Spain: the
                            day-ahead market (marginalpdbc) and the three
                            European intraday auctions (marginalpibc, sessions
                            01 to 03: IDA1, IDA2 and IDA3).
    entsoe   source-terms   The same count for every zone of the feed, from
                            the day-ahead prices fetch_entsoe.py already gets.

and one record a day in data/archive/low-prices-YYYY-MM-DD.json, from which
the totals of the year (1 January to the day in course) and of each month are
added up.

The count, which is that of the low-prices dashboard:

    negative   price below 0 EUR/MWh
    zero       price of exactly 0 EUR/MWh
    low        price above 0 and up to 5 EUR/MWh

in equivalent hours: an hourly period counts 1 hour and a quarter-hour 0.25
(OMIE's day-ahead market is by the quarter-hour since 1 October 2025). The
percentages are over the hours of the period, the days of 23 and 25 hours
counted as they are. From 1 January to 18 November 2026 that gives 7 728
hours, negative or zero for 11.8 % of them in Portugal and 13.7 % in Spain.

OMIE's files (see fetch_omie.py for the day-ahead one):

  - A row is year;month;day;period;price of Portugal;price of Spain. The
    order of the two prices is the same in the auctions' file: on the days
    they differ, its fifth column goes with Portugal's day-ahead price and
    the sixth with Spain's (checked on 2 Feb and 18 Aug 2026).
  - The day-ahead file of a day comes out the day before, at about 13:30.
    The three auctions of a day are published together, in the small hours
    of the day after (now and then days later): the last day that has them
    is yesterday.
  - IDA1 and IDA2 cover the whole day. IDA3 covers its last twelve hours,
    numbered as periods of the day: 49 to 96 (45 to 92 and 53 to 100 on the
    days the clocks change).
  - An auction with no result has a file with a header and no rows (40 in
    2026): it counts for nothing, and is marked `empty`. Two sessions of 2026
    have no file at all (IDA1 of 17 March and of 12 August): `missing`.
  - Each run gets what the records do not have yet, oldest first, a few
    files at a time: a first run fills the year in from 1 January.

`spread` is Portugal's price minus Spain's, period by period (`values`), with
how many periods differ (`differ_periods`, `differ_h`), the mean, the mean of
the absolute difference and the largest.

For the other zones the count is made on data/spot-prices-intervals.json,
which fetch_entsoe.py keeps: the prices of the delivery day at the resolution
the market publishes them in, before the hourly mean (`basis` is
"market-resolution"). The days before this file existed are counted from the
archive of the hourly prices instead (`basis` "hourly-means": an hour only
counts as negative when its mean is), and the totals of the year say how many
days of each kind they add up. Norway, Sweden and Denmark are the mean of the
feed's two zones of each, interval by interval, as their price is.

The step has its own time budget, runs after the files the site already reads
have been published, and never fails the run. Nothing here is lost when a
source does not answer: the records of the days already read stay, and the
file is only rewritten when a run brings something new.

    {updated, data_time, timezone, thresholds,
     omie: {license, source, spread, fetched_at,
         days: {"2026-11-18": {day_ahead: {resolution_minutes, first_period,
             periods, hours, version, PT: {negative_h, zero_h, low_h, min,
             avg, max}, ES: {...}, spread: {differ_periods, differ_h, mean,
             mean_abs, max_abs, values: [...]}}, ida1: {...}, ida2, ida3}},
         year: {year, from, to, day_ahead: {days, hours, PT: {negative_h,
             zero_h, low_h, negative_pct, zero_pct, low_pct,
             negative_or_zero_pct}, ES: {...}, spread: {periods,
             differ_periods, differ_h, differ_pct, mean_abs}}, ida1, ...},
         months: {"2026-01": {...as year...}}},
     entsoe: {license, source, delivery_date, fetched_at, zones: {DE: {day:
         {resolution_minutes, periods, hours, negative_h, zero_h, low_h,
         min, basis}, year: {days, hours, negative_h, ..., negative_or_zero_pct,
         market_resolution_days, hourly_days}}}}}

`omie.days` has the day in course, the day before and, once published, the
day after. `updated` is the time of the run that last brought something new;
`omie.fetched_at` the time a file of OMIE was last read and `entsoe.fetched_at`
the time the prices of the day were obtained. `data_time` is the start of the
last period that has a price.
"""

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

import carry_over
import fetch_entsoe as fe
import fetch_omie
import price_intervals
from feed_common import (HOURLY, OMIE_TERMS, SOURCE_TERMS, Budget, asked_recently, http_get, iso, latest, load_json,
                         parse_time, write_json)

OUT_PATH = 'data/low-prices.json'
ARCHIVE_PATH = 'data/archive/low-prices-{day}.json'
PRICE_ARCHIVE = 'data/archive/spot-prices-{day}.json'
BUDGET_S = float(os.environ.get('LOW_PRICES_BUDGET_SECONDS', '40'))
MAX_FILES = int(os.environ.get('LOW_PRICES_MAX_FILES', '30'))       # OMIE files a run asks for, at most
PAUSE_S = 0.5                                                        # between requests to OMIE
KEEP_DAYS = 1830

OMIE = 'omie'
ENTSOE = 'entsoe'
MARKETS = ('day_ahead', 'ida1', 'ida2', 'ida3')
SESSIONS = {'ida1': '01', 'ida2': '02', 'ida3': '03'}
ZONES = ('PT', 'ES')
MADRID = fetch_omie.MADRID
LOW = Decimal(5)
MAX_VERSION = fetch_omie.MAX_VERSION
PUBLICATION_HOUR = fetch_omie.PUBLICATION_HOUR
RECENT = timedelta(days=7)        # an auction file not there yet is asked for again for this long
DAY_AHEAD_URL = fetch_omie.URL
AUCTION_URL = 'https://www.omie.es/en/file-download?parents=marginalpibc&filename=marginalpibc_{ymd}{session}.{version}'
OMIE_SOURCE = ('OMIE: day-ahead market (marginalpdbc) and European intraday auctions IDA1, IDA2 and IDA3 '
               '(marginalpibc, sessions 01 to 03)')
ENTSOE_SOURCE = 'ENTSO-E Transparency Platform, item 12.1.D: day-ahead prices'
THRESHOLDS = {'negative': 'price < 0', 'zero': 'price = 0', 'low': '0 < price <= 5', 'unit': 'EUR/MWh'}
COUNTED = ('negative_h', 'zero_h', 'low_h')
# Moldova's exchange opened on 10 Dec 2025 and until June 2026 published the prices of a market with
# hardly any trade (2.5 to 6 times Romania's): it is counted from 1 July 2026 on. And never on a day it
# has one hour only, which is a day it has no prices for (see ONE_HOUR_IS_NO_DAY in fetch_entsoe.py).
COUNTED_FROM = {'MD': date(2026, 7, 1)}


def is_counted(zone, day, count):
    """Whether a zone's count of a day goes into the records and the totals."""
    if zone in COUNTED_FROM and day < COUNTED_FROM[zone]:
        return False
    return not (zone in fe.ONE_HOUR_IS_NO_DAY and count.get('hours', 0) <= 1)


# --- Counting ---

def _hours(value):
    """Hours as the files have them: whole when they are, else to the quarter."""
    value = round(float(value), 2)
    return int(value) if value.is_integer() else value


def _money(value):
    return fetch_omie._money(Decimal(str(value)) if not isinstance(value, Decimal) else value)


def tally(prices, each):
    """How long the price was negative, zero and low, in hours, for a list of
    prices that each stand for `each` hours; with the lowest, the mean and
    the highest."""
    prices = [Decimal(str(p)) if not isinstance(p, Decimal) else p for p in prices]
    return {
        'negative_h': _hours(sum(each for p in prices if p < 0)),
        'zero_h': _hours(sum(each for p in prices if p == 0)),
        'low_h': _hours(sum(each for p in prices if 0 < p <= LOW)),
        'min': _money(min(prices)),
        'avg': _money(sum(prices) / len(prices)),
        'max': _money(max(prices)),
    }


def day_hours(day):
    """How many hours a day has on the market's clock: 24, or 23 and 25."""
    start, end = carry_over.day_bounds(day)
    return round((end - start).total_seconds() / 3600)


# --- OMIE's files ---

def parse_marginal(text):
    """(day, [(period, price of Portugal, price of Spain)]) of a marginalpdbc
    or marginalpibc file, the prices as Decimals. An auction with no result
    has a header and no rows: (None, []). The periods are those of the day,
    in order, and need not begin at 1 (IDA3 covers the second half of the
    day). Raises fetch_omie.OmieFormatError for anything else."""
    day, rows = None, []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('*') or not line[0].isdigit():
            continue
        parts = [p.strip() for p in line.split(';')]
        if len(parts) < 6:
            raise fetch_omie.OmieFormatError(f'row with {len(parts)} fields: {line!r}')
        try:
            row_day = date(int(parts[0]), int(parts[1]), int(parts[2]))
            row = (int(parts[3]), Decimal(parts[4].replace(',', '.')), Decimal(parts[5].replace(',', '.')))
        except (ValueError, InvalidOperation) as e:
            raise fetch_omie.OmieFormatError(f'unreadable row {line!r}: {e}') from None
        if day not in (None, row_day):
            raise fetch_omie.OmieFormatError(f'more than one day in the file: {day} and {row_day}')
        day = row_day
        rows.append(row)
    if rows and [r[0] for r in rows] != list(range(rows[0][0], rows[0][0] + len(rows))):
        raise fetch_omie.OmieFormatError('periods are not in order')
    return day, rows


def market_record(day, rows, version):
    """What a file says of one market of one day: {resolution_minutes,
    first_period, periods, hours, version, PT, ES, spread}."""
    if not rows:
        return {'periods': 0, 'hours': 0, 'version': version, 'empty': True}
    whole, last = day_hours(day), rows[-1][0]
    if last == whole:
        minutes = 60
    elif last == whole * 4:
        minutes = 15
    else:
        raise fetch_omie.OmieFormatError(f'last period {last} on {day}, a day of {whole} hours')
    each = Decimal(minutes) / 60
    portugal, spain = [r[1] for r in rows], [r[2] for r in rows]
    spread = [p - s for p, s in zip(portugal, spain)]
    differ = sum(1 for d in spread if d != 0)
    return {
        'resolution_minutes': minutes,
        'first_period': rows[0][0],
        'periods': len(rows),
        'hours': _hours(len(rows) * each),
        'version': version,
        'PT': tally(portugal, each),
        'ES': tally(spain, each),
        'spread': {
            'differ_periods': differ,
            'differ_h': _hours(differ * each),
            'mean': _money(sum(spread) / len(spread)),
            'mean_abs': _money(sum(abs(d) for d in spread) / len(spread)),
            'max_abs': _money(max(abs(d) for d in spread)),
            'values': [_money(d) for d in spread],
        },
    }


def fetch_market(day, market, get, pause=PAUSE_S, versions=MAX_VERSION):
    """Ask OMIE for one market of one day. Returns its record, 'missing' when
    OMIE has no such file (HTTP 404 for each of the first `versions`
    versions), or None when OMIE could not be reached or the file could not
    be read."""
    for version in range(1, versions + 1):
        if market == 'day_ahead':
            url = DAY_AHEAD_URL.format(ymd=day.strftime('%Y%m%d'), version=version)
        else:
            url = AUCTION_URL.format(ymd=day.strftime('%Y%m%d'), session=SESSIONS[market], version=version)
        if version > 1:
            time.sleep(pause)
        response = get(url, timeout=20)
        if response.status == 404:
            continue
        if not response.ok:
            print(f'  {day} {market}: {response.describe()}')
            return None
        try:
            file_day, rows = parse_marginal(response.text('latin-1'))
            if file_day not in (None, day):
                raise fetch_omie.OmieFormatError(f'the file is for {file_day}')
            return market_record(day, rows, version)
        except fetch_omie.OmieFormatError as e:
            print(f'  {day} {market} version {version}: not usable ({e})')
            return None
    return 'missing'


# --- The records of the days ---

def load_record(day):
    record = load_json(ARCHIVE_PATH.format(day=day.isoformat()))
    return record if isinstance(record, dict) else {}


def _block(record, name, licence, source):
    block = record.get(name)
    if not isinstance(block, dict):
        block = record[name] = {'license': licence, 'source': source}
    return block


def wanted(now, records, today):
    """What the records lack of OMIE, in the order it is asked for: the
    day-ahead market of the day in course and, after its publication hour, of
    the day after; yesterday's auctions; then everything missing since 1
    January, oldest first. An auction file that was not there is asked for
    again once an hour while it is recent."""
    def lacks(day, market):
        entry = ((records.get(day) or {}).get(OMIE) or {}).get(market)
        if not isinstance(entry, dict):
            return True
        return bool(entry.get('pending')) and not asked_recently(entry.get('checked'), now, HOURLY)

    tomorrow, yesterday = today + timedelta(days=1), today - timedelta(days=1)
    todo = []
    if lacks(today, 'day_ahead'):
        todo.append((today, 'day_ahead'))
    if now.astimezone(MADRID).hour >= PUBLICATION_HOUR and lacks(tomorrow, 'day_ahead'):
        todo.append((tomorrow, 'day_ahead'))
    todo += [(yesterday, market) for market in MARKETS[1:] if lacks(yesterday, market)]
    day = date(today.year, 1, 1)
    while day <= today:
        for market in MARKETS:
            if (day < today or market == 'day_ahead') and lacks(day, market) and (day, market) not in todo:
                todo.append((day, market))
        day += timedelta(days=1)
    return todo


def update_omie(now, records, today, get, budget, pause=PAUSE_S, limit=None):
    """Get from OMIE what the records lack, within the budget. Returns the
    days whose record changed and how many files were read."""
    changed, read = set(), 0
    stamp = iso(now)
    todo = wanted(now, records, today)
    for number, (day, market) in enumerate(todo[:MAX_FILES if limit is None else limit]):
        if budget.left() < 5:
            break
        if number:
            time.sleep(pause)
        # A file that is not out yet is a version 1 when it comes: no point in asking for the others.
        found = fetch_market(day, market, get, pause, 1 if today - day <= RECENT and market != 'day_ahead' else MAX_VERSION)
        if found is None:
            break                                    # OMIE in trouble: do not insist
        block = _block(records.setdefault(day, {'date': day.isoformat()}), OMIE, OMIE_TERMS, OMIE_SOURCE)
        if found == 'missing':
            # Not there yet, for a recent day; never published, for an older one.
            found = ({'pending': True, 'checked': stamp} if today - day <= RECENT else {'missing': True})
            print(f'  {day} {market}: OMIE has no such file' + (' yet' if found.get('pending') else ''))
        else:
            read += 1
            print(f"  {day} {market}: {found['periods']} periods" + (
                f", PT {found['PT']['negative_h']}/{found['PT']['zero_h']}/{found['PT']['low_h']} h negative/zero/low, "
                f"ES {found['ES']['negative_h']}/{found['ES']['zero_h']}/{found['ES']['low_h']} h, "
                f"{found['spread']['differ_periods']} periods with different prices" if found['periods'] else ''))
        if block.get(market) != found:
            block[market] = found
            changed.add(day)
    left = len(todo) - min(len(todo), MAX_FILES if limit is None else limit)
    if left > 0:
        print(f'  {left} files of earlier days left for the next runs')
    return changed, read


# --- The other zones, from the prices the feed already has ---

def zone_count(prices, minutes, basis, weights=None):
    """A zone's count for a day from its prices, each of `minutes` (or of the
    hours `weights` gives it)."""
    known = [(Decimal(str(p)), Decimal(str(w))) for p, w in zip(prices, weights or [Decimal(minutes) / 60] * len(prices))
             if p is not None]
    if not known:
        return None
    return {
        'resolution_minutes': minutes,
        'periods': len(known),
        'hours': _hours(sum(w for _p, w in known)),
        'negative_h': _hours(sum(w for p, w in known if p < 0)),
        'zero_h': _hours(sum(w for p, w in known if p == 0)),
        'low_h': _hours(sum(w for p, w in known if 0 < p <= LOW)),
        'min': _money(min(p for p, _w in known)),
        'basis': basis,
    }


def counts_from_intervals(kept):
    """{zone: count} from what price_intervals.read() gives: the feed's zones
    at the market's resolution, with Norway, Sweden and Denmark as the mean of
    their zones, interval by interval, as their price is."""
    counts = {}
    for zone, (_start, step, prices) in kept.items():
        if zone not in fe.SUBZONES:
            counts[zone] = zone_count(prices, int(step.total_seconds() // 60), 'market-resolution')
    for country, parts in fe.CONSOLIDATE.items():
        series = [kept.get(zone) for zone in parts]
        if not all(series) or len({(start, step, len(prices)) for start, step, prices in series}) != 1:
            continue
        mean = [None if any(v is None for v in values) else sum(values) / len(values)
                for values in zip(*(prices for _start, _step, prices in series))]
        counts[country] = zone_count(mean, int(series[0][1].total_seconds() // 60), 'market-resolution')
    return {zone: count for zone, count in counts.items() if count}


def counts_from_archive(day):
    """{zone: count} of a past day from the archive of the hourly prices: an
    hour counts by its mean. On the day the clocks go back the entry of 02:00
    stands for two hours."""
    doc = load_json(PRICE_ARCHIVE.format(day=day.isoformat()))
    zones = (doc or {}).get('zones')
    counts = {}
    for zone, entry in (zones.items() if isinstance(zones, dict) else []):
        rows = entry.get('prices') if isinstance(entry, dict) else None
        if not rows or entry.get('delivery_date') != day.isoformat():
            continue
        twice = day_hours(day) == 25
        count = zone_count([row['price'] for row in rows], 60, 'hourly-means',
                           [2 if twice and row.get('hour') == 2 else 1 for row in rows])
        if count:
            counts[zone] = count
    return counts


def update_entsoe(records, today, year_start):
    """Put in the records the count of the other zones: the day in course from
    the prices at the market's resolution, the days that have none yet from
    the archive of the hourly prices. Returns the days whose record changed
    and the time the prices of the day were obtained."""
    changed = set()
    delivery_date, kept = price_intervals.read()
    obtained = (load_json(price_intervals.PATH) or {}).get('updated')
    day = year_start
    while day <= today:
        block = (records.get(day) or {}).get(ENTSOE)
        counts = None
        if delivery_date == day.isoformat() and kept:
            counts = counts_from_intervals(kept)
        elif not isinstance(block, dict) or not block.get('zones'):
            counts = counts_from_archive(day)
        if counts:
            ordered = {zone: counts[zone] for zone in fe.PRICE_ORDER
                       if zone in counts and is_counted(zone, day, counts[zone])}
            record = records.setdefault(day, {'date': day.isoformat()})
            block = _block(record, ENTSOE, SOURCE_TERMS, ENTSOE_SOURCE)
            if block.get('zones') != ordered:
                block['zones'] = ordered
                changed.add(day)
        day += timedelta(days=1)
    return changed, (obtained if delivery_date == today.isoformat() else None)


# --- Totals ---

def _share(part, whole):
    return round(float(part) / float(whole) * 100, 2) if whole else None


def _with_shares(total):
    hours = total['hours']
    out = {name: _hours(total[name]) for name in COUNTED}
    out.update({name.replace('_h', '_pct'): _share(total[name], hours) for name in COUNTED})
    out['negative_or_zero_pct'] = _share(total['negative_h'] + total['zero_h'], hours)
    return out


def omie_totals(records, days):
    """The totals of some days for each market of OMIE."""
    out = {}
    for market in MARKETS:
        entries = [entry for day in days
                   if isinstance(entry := ((records.get(day) or {}).get(OMIE) or {}).get(market), dict) and entry.get('periods')]
        if not entries:
            continue
        hours = sum(entry['hours'] for entry in entries)
        total = {'days': len(entries), 'hours': _hours(hours)}
        for zone in ZONES:
            total[zone] = _with_shares({'hours': hours, **{name: sum(e[zone][name] for e in entries) for name in COUNTED}})
        periods = sum(e['periods'] for e in entries)
        differ_h = sum(e['spread']['differ_h'] for e in entries)
        total['spread'] = {
            'periods': periods,
            'differ_periods': sum(e['spread']['differ_periods'] for e in entries),
            'differ_h': _hours(differ_h),
            'differ_pct': _share(differ_h, hours),
            'mean_abs': round(sum(e['spread']['mean_abs'] * e['hours'] for e in entries) / hours, 2),
        }
        out[market] = total
    return out


def entsoe_totals(records, days):
    """The totals of some days for each zone of the feed."""
    sums = {}
    for day in days:
        zones = ((records.get(day) or {}).get(ENTSOE) or {}).get('zones') or {}
        for zone, count in zones.items():
            if not is_counted(zone, day, count):
                continue                      # a record written before the zone's rule
            total = sums.setdefault(zone, {'days': 0, 'hours': 0, 'market_resolution_days': 0, 'hourly_days': 0,
                                           **{name: 0 for name in COUNTED}})
            total['days'] += 1
            total['hours'] += count['hours']
            total['market_resolution_days' if count.get('basis') == 'market-resolution' else 'hourly_days'] += 1
            for name in COUNTED:
                total[name] += count[name]
    return {zone: {'days': total['days'], 'hours': _hours(total['hours']), **_with_shares(total),
                   'market_resolution_days': total['market_resolution_days'], 'hourly_days': total['hourly_days']}
            for zone, total in sums.items()}


def _last_period(record, day):
    """The start of the last period of a day that OMIE's day-ahead market has a price for."""
    entry = ((record or {}).get(OMIE) or {}).get('day_ahead')
    if not isinstance(entry, dict) or not entry.get('periods'):
        return None
    start = carry_over.day_bounds(day)[0]
    return start + timedelta(minutes=entry['resolution_minutes']) * (entry['first_period'] + entry['periods'] - 2)


def main(now=None, get=http_get, budget=None, pause=PAUSE_S, limit=None):
    now = now or datetime.now(timezone.utc)
    stamp = iso(now)
    today = carry_over.delivery_day(now)
    year_start = date(today.year, 1, 1)
    tomorrow = today + timedelta(days=1)
    previous = load_json(OUT_PATH) or {}

    records, day = {}, year_start
    while day <= tomorrow:
        record = load_record(day)
        if record:
            records[day] = record
        day += timedelta(days=1)
    before = {day: repr(record) for day, record in records.items()}

    print(f'Low prices: {len(records)} days of {today.year} on record')
    _changed, read = update_omie(now, records, today, get, budget or Budget(BUDGET_S), pause, limit)
    _changed, prices_obtained = update_entsoe(records, today, year_start)

    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    written = 0
    for day, record in records.items():
        if repr(record) != before.get(day):
            ordered = {'date': day.isoformat(), **{name: record[name] for name in (OMIE, ENTSOE) if name in record}}
            write_json(ARCHIVE_PATH.format(day=day.isoformat()), ordered, separators=(',', ':'))
            written += 1
    cutoff = (today - timedelta(days=KEEP_DAYS)).isoformat()
    for name in os.listdir(os.path.dirname(ARCHIVE_PATH)):
        if name.startswith('low-prices-') and name.endswith('.json') and name[len('low-prices-'):-len('.json')] < cutoff:
            os.remove(os.path.join(os.path.dirname(ARCHIVE_PATH), name))

    had_prices = (previous.get(ENTSOE) or {}).get('fetched_at')
    new_prices = prices_obtained is not None and prices_obtained != had_prices
    if not read and not new_prices and not written:
        print(f'Nothing new in this run: {OUT_PATH} is left exactly as it was')
        return 0

    days = [year_start + timedelta(days=n) for n in range((today - year_start).days + 1)]
    months = {}
    for day in days:
        months.setdefault(day.strftime('%Y-%m'), []).append(day)
    shown = {day.isoformat(): {market: records[day][OMIE][market] for market in MARKETS
                               if isinstance(records[day][OMIE].get(market), dict)}
             for day in (tomorrow, today, today - timedelta(days=1)) if isinstance((records.get(day) or {}).get(OMIE), dict)}
    today_zones = ((records.get(today) or {}).get(ENTSOE) or {}).get('zones') or {}
    # When a file of OMIE was last read: in this run, or as the file had it. Not there until one is
    # (the records of the year were filled in once, before this file existed).
    omie_read = stamp if read else (previous.get(OMIE) or {}).get('fetched_at')
    year_zones = entsoe_totals(records, days)
    last = [at for day in (tomorrow, today) if (at := _last_period(records.get(day), day))]
    doc = {
        'updated': stamp,
        'data_time': latest(([iso(max(last))] if last else [])
                            + [(load_json(price_intervals.PATH) or {}).get('data_time')]),
        'timezone': 'Europe/Madrid',
        'thresholds': THRESHOLDS,
        OMIE: {
            'license': OMIE_TERMS,
            'source': OMIE_SOURCE,
            'spread': 'price of Portugal minus price of Spain, EUR/MWh',
            **({'fetched_at': omie_read} if omie_read else {}),
            'days': shown,
            'year': {'year': today.year, 'from': year_start.isoformat(), 'to': today.isoformat(),
                     **omie_totals(records, days)},
            'months': {month: omie_totals(records, month_days) for month, month_days in months.items()},
        },
        ENTSOE: {
            'license': SOURCE_TERMS,
            'source': ENTSOE_SOURCE,
            'delivery_date': today.isoformat(),
            'fetched_at': prices_obtained or had_prices,
            'year': {'year': today.year, 'from': year_start.isoformat(), 'to': today.isoformat()},
            'zones': {zone: {**({'day': today_zones[zone]} if zone in today_zones else {}), 'year': year_zones[zone]}
                      for zone in fe.PRICE_ORDER if zone in year_zones},
        },
    }
    write_json(OUT_PATH, doc, separators=(',', ':'))
    year = doc[OMIE]['year'].get('day_ahead') or {}
    print(f"Wrote {OUT_PATH}: {read} files of OMIE read, {written} records written; "
          f"{today.year} to {today}: negative or zero for "
          f"{(year.get('PT') or {}).get('negative_or_zero_pct')} % of the time in Portugal and "
          f"{(year.get('ES') or {}).get('negative_or_zero_pct')} % in Spain "
          f"({year.get('days')} days, {year.get('hours')} hours)")
    return 0


# --- Counting a year again, at the market's resolution ---
#
# The records of the days before 18 Nov 2026 were counted from the archive of
# the hourly prices, which was all there was: an hour counted as negative only
# when its mean was, and the 31 days the archive lacked were not counted at
# all. Over the same 291 days Portugal came to 11.46 % of the time at zero or
# below, against 12.58 % by OMIE's quarter-hours.
#
# `python fetch_low_prices.py --refill-entsoe [year]` (started by hand through
# rewrite-archive.yml, where the key is) counts the year again from ENTSO-E
# itself: one request for each zone for the whole year, about 35, each day of
# the answer read as the regular runs read the day in course (the same
# auction, the same hours, converted to euros: fetch_entsoe._complete_hours)
# and counted at the resolution the market publishes. Only the `entsoe` part
# of each record is written; every day ENTSO-E has prices for gets
# "market-resolution", and a day it has none for a zone leaves the zone out.
#
# The proof is Portugal and Spain, which OMIE's files count too: over the
# same days the two blocks must agree (compare_blocks). Where they do not,
# the days are listed and neither count is changed to fit the other.

def intervals_of_span(base, code, first, last, expected, ask, sleep):
    """{day: [(start, step, price)]} of a zone for the delivery days `first` to `last`."""
    import backfill_archive

    def read(text, first, last):
        return {day: [(at, step, value) for _hour, step, values in hours for at, value in sorted(values.items())]
                for day, hours in fe._complete_hours(fe.read_series(text)).items() if first <= day <= last}

    return backfill_archive.ask_span(
        base, {'documentType': 'A44', 'in_Domain': fe.ZONES[code], 'out_Domain': fe.ZONES[code]},
        code, first, last, read, expected, ask, sleep)


def compare_blocks(records, days, zones=ZONES):
    """Where the count of the feed's own prices (the `entsoe` part of the
    records) and OMIE's day-ahead count differ, for Portugal and Spain:
    ({zone: {'omie': totals, 'entsoe': totals}}, [(day, zone, omie, entsoe)]),
    the totals being (negative, zero, low) hours over the days that have both."""
    totals = {zone: {'omie': [0, 0, 0], 'entsoe': [0, 0, 0], 'days': 0} for zone in zones}
    differ = []
    for day in days:
        record = records.get(day) or {}
        ahead = (record.get(OMIE) or {}).get('day_ahead') or {}
        for zone in zones:
            ours = ((record.get(ENTSOE) or {}).get('zones') or {}).get(zone)
            theirs = ahead.get(zone)
            if not ours or not theirs:
                continue
            a, b = [theirs[name] for name in COUNTED], [ours[name] for name in COUNTED]
            totals[zone]['days'] += 1
            for i in range(3):
                totals[zone]['omie'][i] += a[i]
                totals[zone]['entsoe'][i] += b[i]
            if a != b:
                differ.append((day, zone, a, b))
    return totals, differ


def refill_entsoe(base, year=None, now=None, ask=None, sleep=time.sleep, commit=None):
    """Count a year again at the market's resolution (see above). Returns the
    exit code: 0, or 1 when a zone could not be had and nothing was written."""
    import backfill_archive
    ask = ask or backfill_archive._ask
    now = now or datetime.now(timezone.utc)
    today = carry_over.delivery_day(now)
    year = year or today.year
    first = date(year, 1, 1)
    last = min(today - timedelta(days=1), date(year, 12, 31))
    days = [first + timedelta(days=n) for n in range((last - first).days + 1)]
    records = {day: load_record(day) or {'date': day.isoformat()} for day in days}
    had = {zone for record in records.values() for zone in ((record.get(ENTSOE) or {}).get('zones') or {})}
    parent = {zone: country for country, zones in fe.CONSOLIDATE.items() for zone in zones}
    print(f'Counting {year} again at the market\'s resolution: {first} to {last}, {len(days)} days')

    found = {}
    try:
        for code in fe.ZONES:
            if code in fe.ELEXON_ZONES:
                continue
            started = time.monotonic()
            found[code] = intervals_of_span(base, code, first, last, parent.get(code, code) in had, ask, sleep)
            print(f'  {code}: {len(found[code])} days with prices [{time.monotonic() - started:.0f}s]')
    except backfill_archive.RewriteFailed as e:
        print(f'{year} NOT counted again: {e}')
        return 1

    written, bases, without = 0, {}, []
    for day in days:
        kept = {}
        for code, by_day in found.items():
            entry = price_intervals.zone_entry(by_day[day], day) if by_day.get(day) else None
            if entry:
                kept[code] = (parse_time(entry['start']), timedelta(minutes=entry['step_minutes']), entry['prices'])
        counts = counts_from_intervals(kept)
        ordered = {zone: counts[zone] for zone in fe.PRICE_ORDER if zone in counts and is_counted(zone, day, counts[zone])}
        record = records[day]
        before = (record.get(ENTSOE) or {}).get('zones') or {}
        for zone, count in before.items():
            bases[count.get('basis')] = bases.get(count.get('basis'), 0) + 1
        if not ordered:
            without.append(day)
        if ordered != before:
            _block(record, ENTSOE, SOURCE_TERMS, ENTSOE_SOURCE)['zones'] = ordered
            ordered_record = {'date': day.isoformat(), **{name: record[name] for name in (OMIE, ENTSOE) if name in record}}
            os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
            write_json(ARCHIVE_PATH.format(day=day.isoformat()), ordered_record, separators=(',', ':'))
            written += 1
    print(f'{written} records written; zone-days before, by basis: {bases}')
    if without:
        print(f'Days ENTSO-E has no prices for any zone: {len(without)} ({", ".join(str(day) for day in without[:20])})')
    totals, differ = compare_blocks(records, days)
    for zone, total in totals.items():
        agree = total['omie'] == total['entsoe']
        print(f"  {zone}, {total['days']} days: OMIE {[_hours(v) for v in total['omie']]} hours negative, zero and low; "
              f"from ENTSO-E {[_hours(v) for v in total['entsoe']]} - {'the same' if agree else 'NOT the same'}")
    for day, zone, omie, entsoe in differ[:60]:
        print(f'    {day} {zone}: OMIE {omie}, ENTSO-E {entsoe}')
    if len(differ) > 60:
        print(f'    ... and {len(differ) - 60} more')
    if commit and written:
        commit(f"Count the low prices of {year} again at the market's resolution ({written} records)")
    return 0


if __name__ == '__main__':
    if '--refill-entsoe' in sys.argv:
        import backfill_archive
        if not fe.API_KEY:
            sys.exit('ERROR: ENTSOE_API_KEY environment variable is not set')
        wanted = [int(arg) for arg in sys.argv[1:] if arg.isdigit()]
        backfill_archive._run_git(['config', 'user.name', 'github-actions[bot]'])
        backfill_archive._run_git(['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'])
        sys.exit(max(refill_entsoe(fe.ALT_URLS[0], year, commit=backfill_archive._commit_and_push)
                     for year in (wanted or [None])))
    sys.exit(main())
