#!/usr/bin/env python3
"""fetch_omie.py - OMIE day-ahead prices for Portugal and Spain.

Writes data/omie-prices.json from OMIE's public "marginalpdbc" file, one file
per delivery day:

    https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_YYYYMMDD.1

    MARGINALPDBC;
    2026;11;16;1;145;145;        year;month;day;period;PT price;ES price;
    ...
    *

It replaces eew_fetch_omie_daily_prices() in the site's eew-engine.php, and
deliberately differs from it in three ways:

1. Periods. Since 1 October 2025 the file has one row per quarter-hour: 96 a
   day, 92 and 100 on the days the clocks change (before that, 24, 23 and 25
   hourly rows). The PHP kept periods 1 to 24 only, so it showed the first six
   hours as if they were the whole day. Here `prices` has one value per hour
   of the day (the mean of that hour's quarter-hours), `avg` is the mean of
   every period of the day, and the quarter-hours are in `prices_15min`.
2. Which day. The PHP used tomorrow's file as soon as it was published, in the
   early afternoon, without saying so. Here PT and ES at the top level are
   always the current day in Madrid, `date` says which day that is, and
   tomorrow goes in `next_day` once OMIE has published it.
3. Which column. The first price column is Portugal and the second is Spain.
   The PHP read them the other way round (it only shows on the days the two
   prices differ). Checked against ENTSO-E's PT and ES prices for 2026-02-01,
   2025-10-01 and 2025-03-30: see tests/test_fetch_omie.py.

Output:

    {updated, source, url, date, timezone, resolution_minutes, periods, version,
     PT: {prices: [{time: "HH:00", price}], avg, prices_15min: [{period, time, price}]},
     ES: {...},
     next_day: null | {date, url, resolution_minutes, periods, version, PT, ES}}

`prices` and `avg` are what the site's JavaScript reads today; the rest is new.
`prices` has 24 entries, except on the spring clock-change day, which only has
23 hours (there is no 02:00). On the autumn one, 02:00 is the mean of both
02:00 hours.

Versions. Each day has one file. When OMIE republishes a day with corrections
the suffix goes up (.2, .3...) and the earlier file is removed (it answers
404): in OMIE's public listing, 3 of the 1416 days since 2023 are like that
(2023-05-28 as .2, 2025-10-30 as .3, 2025-11-27 as .2) and none has two
versions. So the script asks for the version it last saw (.1 the first time)
and, on a 404, for the next ones.

If OMIE cannot be reached, a day already in the previous file is reused; if
there is nothing for the current day, the previous file is kept.
"""

import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zoneinfo import ZoneInfo

from feed_common import http_get, load_json, now_iso, write_json

OUT_PATH = 'data/omie-prices.json'
URL = 'https://www.omie.es/en/file-download?parents=marginalpdbc&filename=marginalpdbc_{ymd}.{version}'
MADRID = ZoneInfo('Europe/Madrid')
MAX_VERSION = 4          # how far to look for republished versions of a day
PAUSE_S = 1.0            # between requests to OMIE
PUBLICATION_HOUR = 13    # Madrid time: the next day's file comes out between 13:30 and 15:00

# Columns of a data row: year;month;day;period;PT;ES;
COL_PERIOD, COL_PT, COL_ES = 3, 4, 5


class OmieFormatError(ValueError):
    pass


def parse_marginalpdbc(text):
    """Parse a marginalpdbc file. Returns (date, [(period, pt, es)]) with the
    prices as Decimal, or raises OmieFormatError."""
    day = None
    rows = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('*') or not line[0].isdigit():
            continue                     # header ("MARGINALPDBC;"), terminator, blanks
        parts = [p.strip() for p in line.split(';')]
        if len(parts) < 6:
            raise OmieFormatError(f'row with {len(parts)} fields: {line!r}')
        try:
            row_day = datetime(int(parts[0]), int(parts[1]), int(parts[2])).date()
            period = int(parts[COL_PERIOD])
            pt = Decimal(parts[COL_PT].replace(',', '.'))
            es = Decimal(parts[COL_ES].replace(',', '.'))
        except (ValueError, InvalidOperation) as e:
            raise OmieFormatError(f'unreadable row {line!r}: {e}') from None
        if day is None:
            day = row_day
        elif row_day != day:
            raise OmieFormatError(f'more than one day in the file: {day} and {row_day}')
        rows.append((period, pt, es))

    if not rows:
        raise OmieFormatError('no data rows')
    if [r[0] for r in rows] != list(range(1, len(rows) + 1)):
        raise OmieFormatError('periods are not 1..N in order')
    return day, rows


def period_starts(day, count):
    """Local (Madrid) start time of each of the `count` periods of `day`, and
    the period length in minutes. Raises OmieFormatError if `count` is neither
    the day's number of hours nor of quarter-hours."""
    start = datetime(day.year, day.month, day.day, tzinfo=MADRID).astimezone(timezone.utc)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=MADRID).astimezone(timezone.utc)
    hours = round((end - start).total_seconds() / 3600)      # 24, or 23 / 25 on clock-change days
    if count == hours:
        minutes = 60
    elif count == hours * 4:
        minutes = 15
    else:
        raise OmieFormatError(f'{count} periods on {day}, a day of {hours} hours')
    return [(start + timedelta(minutes=minutes * i)).astimezone(MADRID) for i in range(count)], minutes


def _money(value):
    """Decimal -> JSON number with 2 decimals at most (halves round up)."""
    rounded = float(value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    return int(rounded) if rounded.is_integer() else rounded


def _mean(values):
    return sum(values) / len(values)


def zone_block(starts, minutes, values):
    """{prices, avg[, prices_15min]} for one zone."""
    by_hour = {}
    for local, value in zip(starts, values):
        by_hour.setdefault(local.hour, []).append(value)
    block = {
        'prices': [{'time': f'{hour:02d}:00', 'price': _money(_mean(vals))}
                   for hour, vals in sorted(by_hour.items())],
        'avg': _money(_mean(values)),
    }
    if minutes == 15:
        block['prices_15min'] = [
            {'period': i + 1, 'time': local.strftime('%H:%M'), 'price': _money(value)}
            for i, (local, value) in enumerate(zip(starts, values))
        ]
    return block


def build_day(day, rows, url, version):
    """The block for one delivery day, from the parsed rows of its file."""
    starts, minutes = period_starts(day, len(rows))
    return {
        'date': day.isoformat(),
        'url': url,
        'resolution_minutes': minutes,
        'periods': len(rows),
        'version': version,
        'PT': zone_block(starts, minutes, [r[1] for r in rows]),
        'ES': zone_block(starts, minutes, [r[2] for r in rows]),
    }


def fetch_day(day, known_version=None, get=http_get, pause=PAUSE_S):
    """Download `day`. Returns the day block, or None when OMIE has no file
    for that day or cannot be reached.

    Starts at `known_version` (the one in the previous file) or .1 and, while
    the answer is 404, goes on to the next version."""
    first = known_version or 1
    for version in range(first, MAX_VERSION + 1):
        url = URL.format(ymd=day.strftime('%Y%m%d'), version=version)
        if version > first:
            time.sleep(pause)
        resp = get(url, timeout=20)
        if resp.status == 404:
            print(f'  {day} version {version}: not there (HTTP 404)')
            continue
        if not resp.ok:
            print(f'  {day} version {version}: {resp.describe()}')
            break                        # OMIE in trouble: do not insist
        try:
            file_day, rows = parse_marginalpdbc(resp.text('latin-1'))
            if file_day != day:
                raise OmieFormatError(f'the file is for {file_day}')
            block = build_day(day, rows, url, version)
        except OmieFormatError as e:
            print(f'  {day} version {version}: not usable ({e})')
            break
        print(f"  {day} version {version}: {len(rows)} periods, "
              f"PT avg {block['PT']['avg']}, ES avg {block['ES']['avg']}")
        return block
    return None


def previous_day_block(previous, day):
    """The block for `day` already in the previous file (as the current day or
    as its next_day), or None."""
    if not isinstance(previous, dict):
        return None
    for block in (previous, previous.get('next_day')):
        if isinstance(block, dict) and block.get('date') == day.isoformat() \
                and (block.get('PT') or {}).get('prices') and (block.get('ES') or {}).get('prices'):
            keys = ('date', 'url', 'resolution_minutes', 'periods', 'version', 'PT', 'ES')
            return {k: block.get(k) for k in keys}
    return None


def assemble(today_block, next_block, updated):
    result = {
        'updated': updated,
        'source': 'OMIE',
        'url': today_block['url'],
        'date': today_block['date'],
        'timezone': 'Europe/Madrid',
        'resolution_minutes': today_block['resolution_minutes'],
        'periods': today_block['periods'],
        'version': today_block['version'],
        'PT': today_block['PT'],
        'ES': today_block['ES'],
        'next_day': next_block,
    }
    return result


def main(now=None, get=http_get, pause=PAUSE_S):
    now = now or datetime.now(MADRID)
    today = now.date()
    tomorrow = today + timedelta(days=1)
    previous = load_json(OUT_PATH)
    had_today = previous_day_block(previous, today)
    had_next = previous_day_block(previous, tomorrow)

    print(f'OMIE day-ahead prices for {today} (Madrid)...')
    fetched_today = fetch_day(today, (had_today or {}).get('version'), get=get, pause=pause)
    # Tomorrow's file does not exist before the publication hour, and OMIE's
    # server caches its "not found" answers, so it is not asked for until then.
    fetched_next = None
    if had_next or now.hour >= PUBLICATION_HOUR:
        time.sleep(pause)
        fetched_next = fetch_day(tomorrow, (had_next or {}).get('version'), get=get, pause=pause)

    # What OMIE did not give now may already be in the previous file: a day
    # once published stays published, and today was yesterday's next_day.
    today_block = fetched_today or had_today
    next_block = fetched_next or had_next
    if not today_block:
        print(f'No prices for {today} from OMIE - keeping the previous {OUT_PATH}')
        return 1

    # `updated` is when OMIE last answered for the current day.
    updated = now_iso() if fetched_today else (previous or {}).get('updated')
    write_json(OUT_PATH, assemble(today_block, next_block, updated), separators=(',', ':'))
    print(f"Wrote {OUT_PATH}: {today_block['date']} "
          f"({'from OMIE' if fetched_today else 'kept from the previous file'}), "
          f"{len(today_block['PT']['prices'])} hourly prices, next day: "
          f"{next_block['date'] if next_block else 'not published yet'}")
    return 0 if fetched_today else 1


if __name__ == '__main__':
    sys.exit(main())
