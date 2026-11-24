#!/usr/bin/env python3
"""fetch_open_prices.py - day-ahead prices from open sources only, for the site's paid alerts.

Writes data/open-prices.json: the prices of 18 zones in one place, from sources
whose terms allow their use in something paid. ENTSO-E's day-ahead prices are
not on its list of data free to reuse, so they are not used here (nor mixed
in): the feed keeps them in its other files.

    zone            source                                       licence
    15 zones        SMARD (Bundesnetzagentur), day-ahead prices  cc-by-4.0
    GB              Elexon, Market Index Data (MID)              elexon
    PT, ES          OMIE, day-ahead market                       omie

SMARD. smard.de publishes, with the attribution it asks for (ATTRIBUTION),
the day-ahead prices of Germany-Luxembourg and of its neighbours, in the JSON
files its own site reads: an index of weekly files for each filter, region and
resolution, and the weekly files, each a list of [start in ms UTC, EUR/MWh or
null]. The finest resolution is taken: the quarter-hour, which SMARD has
since 1 Oct 2025 (since 4 Nov 2026 for Switzerland; before, each hour's price
is repeated in its four quarters), or the hour (the mean of the quarters) when
the quarter-hours cannot be had. See SMARD_ZONES for the filter and region of
each zone. Tomorrow's prices go into the latest weekly file, followed by nulls,
once they are out; SMARD has them between about 13:00 and 15:00 Brussels time.

On the licence (checked on smard.de/en/datennutzung on 23 Nov 2026): SMARD's
market data are under CC BY 4.0, attributed "Bundesnetzagentur | SMARD.de",
but the page says the Bundesnetzagentur is reviewing the licensing of the
wholesale prices (its HEDWIG determination) and means to set a new concept.
SMARD names ENTSO-E as the source of these prices; on 23 Nov 2026 they matched
ENTSO-E's, interval by interval, in the 14 zones the feed also has.

Energy-Charts, the stand-in for SMARD (since 24 Nov 2026). SMARD sometimes
stops publishing for a while: on 24 Nov 2026 it had not put out that day's
prices of the 14 coupled zones (as from 2 to 9 Mar 2026, and when it moved to
quarter-hours in Oct 2025). Energy-Charts (Fraunhofer ISE) republishes SMARD's
prices unchanged, and says the licence of each answer in `license_info`: CC BY
4.0 from Bundesnetzagentur | SMARD.de for the 15 zones of SMARD, private and
internal use only for the others. So, for a SMARD zone that is asked for and
lacks today (or, from 12:30 Brussels time, tomorrow) in SMARD, the missing days
are asked of Energy-Charts' /price, in one request for the zone, and taken only
when the answer says CC BY 4.0 and has the whole day; under another licence, or
incomplete, the zone stays without that day and the log says so. /price allows
2 requests a minute from one address, in a burst of 2 (api.energy-charts.info,
checked on 24 Nov 2026), so a run asks it for 2 zones at most (EC_PER_RUN),
those without today first, and stops at its first 429. A day from Energy-Charts
is final: when SMARD still lacks it an hour later, it is kept as it was, with
no new request. Each day of a SMARD zone says where it came from (`source`,
smard or energy-charts) and its licence, a day of Energy-Charts with the words
it was given under (`license_info`); the zone's attribution adds
"energy-charts.info" while one of its days came from there.

GB. The values fetch_gb.py reads from Elexon: the Market Index Data (MID), a
half-hourly, volume-weighted index of the short-term market (N2EX and APX),
not a day-ahead auction. Each half-hour is the volume-weighted mean of the two
providers, with the placeholder rows (price missing or 0) left out, as in
fetch_gb.py. MID is published as the periods go by, so GB has the half-hours
of today so far and never tomorrow. The prices are in pounds; they are put in
euros with the ECB's reference rate of the delivery day (or the last one before
it) from data/fx-rates.json, as the balancing prices are (fx_rates.py), with
the pounds beside them (`prices_gbp`, `fx_rate`, `fx_date`).

PT and ES. OMIE's prices, from data/omie-prices.json, which fetch_omie.py
writes earlier in the same run: no request is made for them here.

    {updated, data_time, timezone, unit,
     zones: {"DE-LU": {source, license[, attribution], currency, days:
                {"2026-11-23": {start, step_minutes, prices: [...], source, license[, license_info]},
                 "2026-11-24": {...}},
              data_time, fetched_at[, carried_over]},
             ...,
             "GB": {..., currency_original, fx_rate, fx_date,
                    days: {"2026-11-23": {start, step_minutes, prices, prices_gbp}}},
             "PT": {...}, "ES": {...}}}

`days` has today and, once it is out, tomorrow: delivery days on the market's
clock (Europe/Brussels), so a day has 92, 96 or 100 quarter-hours, or 23, 24
or 25 hours. `start` is the first instant of the day in UTC, and `prices` has
one value for each interval of `step_minutes` from there, in EUR/MWh. A day of
SMARD or OMIE is only there when it has every interval; GB's day has null for
the half-hours still to come. `data_time` is the start of the last interval
with a price, `fetched_at` when the zone's prices were obtained.

When it asks. A SMARD zone is asked for once an hour, and in every run from
12:30 Brussels time (the auction's results come out at about 12:45) until it
has tomorrow, of SMARD and, for the days SMARD lacks, of Energy-Charts (2 zones
a run at most); GB in every run (one request); PT and ES are read in every run. A
zone that was asked for and did not answer keeps what was published, with
the time it was obtained and the mark carried_over, for 24 hours from then
(MAX_AGE); the other zones are published all the same. The days that are no
longer today or tomorrow are taken out of every zone, kept or not.

The step has its own time budget, runs after the files the site already reads
are published, and never fails the run.
"""

import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import carry_over
import fetch_gb
import fetch_omie
import fx_rates
from feed_common import (CARRIED_OVER, CC_BY, DATA_TIME, ELEXON_TERMS, FETCHED_AT, HOURLY, OMIE_TERMS, Budget, carry,
                         due, http_get, iso, latest, load_json, parse_time, write_json)

OUT_PATH = 'data/open-prices.json'
OMIE_PATH = 'data/omie-prices.json'
BUDGET_S = float(os.environ.get('OPEN_PRICES_BUDGET_SECONDS', '60'))
MAX_AGE = timedelta(hours=24)        # how long a zone that does not answer keeps what was published
TOMORROW_FROM = (12, 30)             # Brussels time: from then on, a zone without tomorrow is asked in every run
PAUSE_S = 0.1                        # between requests to SMARD

SMARD_URL = 'https://www.smard.de/app/chart_data'
SMARD_SOURCE = 'SMARD (Bundesnetzagentur), day-ahead prices'
ATTRIBUTION = 'Bundesnetzagentur | SMARD.de'
# {zone: (filter, region)} of SMARD's day-ahead price of each zone ("Großhandelspreise" in the configuration its
# site loads, market_data_configuration.json; checked on 23 Nov 2026). The region does not change the numbers
# (DE, DE-LU, AT... give the same series): DE, the site's own, is allowed for every filter.
SMARD_ZONES = {
    'DE-LU': (4169, 'DE'), 'AT': (4170, 'DE'), 'BE': (4996, 'DE'), 'CH': (259, 'DE'), 'CZ': (261, 'DE'),
    'DK1': (252, 'DE'), 'DK2': (253, 'DE'), 'FR': (254, 'DE'), 'HU': (262, 'DE'), 'IT-North': (255, 'DE'),
    'NL': (256, 'DE'), 'NO2': (4997, 'DE'), 'PL': (257, 'DE'), 'SE4': (258, 'DE'), 'SI': (260, 'DE'),
}
RESOLUTIONS = (('quarterhour', 15), ('hour', 60))

# Energy-Charts, for the days SMARD lacks. Its zones have the names of SMARD_ZONES.
EC_URL = 'https://api.energy-charts.info/price'
EC_ATTRIBUTION = 'energy-charts.info'
EC_PER_RUN = 2                       # /price: 2 requests a minute from one address, burst 2
EC_LICENCE = re.compile(r'\s*CC BY 4\.0\b')     # how license_info starts when the prices may be reused

GB_SOURCE = 'Elexon, Market Index Data (MID, N2EX and APX): a half-hourly index of the short-term market'
OMIE_SOURCE = 'OMIE, day-ahead market'
ORDER = list(SMARD_ZONES) + ['GB', 'PT', 'ES']


# --- Days and intervals ---

def window(now):
    """[today, tomorrow] on the market's clock."""
    today = carry_over.delivery_day(now)
    return [today, today + timedelta(days=1)]


def day_block(day, step, values, whole=True):
    """{start, step_minutes, prices} of a day from {start of an interval (UTC): price}; None when the day has no
    price, or (with `whole`) when it lacks one."""
    start, end = carry_over.day_bounds(day)
    count = int((end - start) / step)
    prices = [values.get(start + step * i) for i in range(count)]
    if all(v is None for v in prices) or (whole and any(v is None for v in prices)):
        return None
    return {'start': iso(start), 'step_minutes': int(step.total_seconds() // 60), 'prices': prices}


def last_priced(days):
    """The start of the last interval of some day blocks that has a price, or None."""
    last = None
    for block in days.values():
        start, step = parse_time(block.get('start')), timedelta(minutes=block.get('step_minutes') or 0)
        priced = [i for i, v in enumerate(block.get('prices') or []) if v is not None]
        if start is not None and priced and step:
            at = start + step * priced[-1]
            last = at if last is None else max(last, at)
    return last


def _round(value):
    return round(float(value), 2)


# --- SMARD ---

class SourceDown(Exception):
    """The source did not give what was asked for."""


def _smard_json(url, get, budget):
    if budget.left() <= 0:
        raise SourceDown('out of time')
    response = get(url, timeout=min(20, max(1, budget.left())))
    if not response.ok:
        raise SourceDown(response.describe())
    try:
        return response.json()
    except ValueError:
        raise SourceDown('not JSON')


def smard_series(filter_id, region, resolution, first, end, get, budget, pause=PAUSE_S):
    """({start of an interval (UTC): EUR/MWh}, [problems]) of a SMARD series from `first` to `end`, from the
    weekly files that cover them (an index and one or two files; each starts on a Monday, Brussels time). A
    weekly file that does not answer is a problem, not a failure: the other may have the day wanted. Raises
    SourceDown when the index does not answer."""
    base = f'{SMARD_URL}/{filter_id}/{region}'
    stamps = (_smard_json(f'{base}/index_{resolution}.json', get, budget) or {}).get('timestamps') or []
    stamps = sorted(s for s in stamps if isinstance(s, int))
    begin_ms, end_ms = int(first.timestamp() * 1000), int(end.timestamp() * 1000)
    wanted = [s for i, s in enumerate(stamps)
              if s < end_ms and (i + 1 == len(stamps) or stamps[i + 1] > begin_ms)]
    values, problems = {}, []
    for stamp in wanted:
        time.sleep(pause)
        try:
            series = (_smard_json(f'{base}/{filter_id}_{region}_{resolution}_{stamp}.json', get, budget)
                      or {}).get('series') or []
        except SourceDown as e:
            problems.append(f'week of {datetime.fromtimestamp(stamp / 1000, timezone.utc):%Y-%m-%d}: {e}')
            continue
        for point in series:
            if (isinstance(point, list) and len(point) == 2 and isinstance(point[0], int)
                    and isinstance(point[1], (int, float)) and not isinstance(point[1], bool)
                    and begin_ms <= point[0] < end_ms):
                values[datetime.fromtimestamp(point[0] / 1000, timezone.utc)] = _round(point[1])
    return values, problems


def smard_days(zone, days, get, budget, pause=PAUSE_S):
    """(blocks, answered, reasons) of a SMARD zone for `days` (today and tomorrow): {day: block} of the days it
    has whole, at the finest resolution that has today whole (or, with no today, that has some day); whether
    SMARD answered at all (an index); and why a day is missing. A missing price is null in SMARD's files (a whole
    day can be), so a day with one is not taken."""
    filter_id, region = SMARD_ZONES[zone]
    first, end = carry_over.day_bounds(days[0])[0], carry_over.day_bounds(days[-1])[1]
    reasons, answered, best = [], False, {}
    for resolution, minutes in RESOLUTIONS:
        try:
            values, problems = smard_series(filter_id, region, resolution, first, end, get, budget, pause)
        except SourceDown as e:
            reasons.append(f'{resolution}: {e}')
            continue
        answered = True
        step = timedelta(minutes=minutes)
        blocks = {day.isoformat(): {**block, 'source': 'smard', 'license': CC_BY}
                  for day in days if (block := day_block(day, step, values))}
        if days[0].isoformat() in blocks:
            return blocks, True, reasons
        best = best or blocks
        reasons.append(f'{resolution}: no whole day of {days[0]}' + (f" ({'; '.join(problems)})" if problems else ''))
        if budget.left() <= 0:
            break
    return best, answered, reasons


def smard_zone(zone, days, get, budget, pause=PAUSE_S):
    """A SMARD zone's entry for `days`, from SMARD alone; raises SourceDown when it has no whole today."""
    blocks, _answered, reasons = smard_days(zone, days, get, budget, pause)
    if days[0].isoformat() not in blocks:
        raise SourceDown('; '.join(reasons) or 'nothing')
    return smard_entry(zone, blocks)


def smard_entry(zone, blocks):
    """The entry of a SMARD zone with these day blocks, its attribution as their sources ask."""
    filter_id, region = SMARD_ZONES[zone]
    entry = {'source': SMARD_SOURCE, 'license': CC_BY, 'attribution': ATTRIBUTION, 'currency': 'EUR',
             'filter': filter_id, 'region': region, 'days': blocks}
    return attributed(entry)


def attributed(entry):
    """A SMARD zone's entry with the attribution of the days it has: SMARD's, and Energy-Charts' while one of its
    days came from there. A day of a file written before the days said their source is SMARD's."""
    days = {name: {**block, 'source': block.get('source', 'smard'), 'license': block.get('license', CC_BY)}
            for name, block in (entry.get('days') or {}).items()}
    through = any(block.get('source') == 'energy-charts' for block in days.values())
    return {**entry, 'attribution': ATTRIBUTION + (f', {EC_ATTRIBUTION}' if through else ''), 'days': days}


# --- Energy-Charts, for the days SMARD lacks ---

class RateLimited(SourceDown):
    """Energy-Charts answered 429: no more requests to it in this run."""


class OtherLicence(SourceDown):
    """Energy-Charts gave the prices under a licence that does not allow their reuse."""


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def ec_days(zone, wanted, get, budget):
    """{day: block} of the days of `wanted` (one or two days in a row) that Energy-Charts has whole for a SMARD zone
    under CC BY 4.0, from one request; {} when it has no price for them yet (404). Raises OtherLicence when the
    answer is under another licence, RateLimited on 429, SourceDown when it does not answer."""
    if budget.left() <= 0:
        raise SourceDown('out of time')
    url = f'{EC_URL}?bzn={zone}&start={wanted[0].isoformat()}&end={wanted[-1].isoformat()}'
    response = get(url, timeout=min(20, max(1, budget.left())))
    if response.status == 404:
        return {}
    if response.status == 429:
        raise RateLimited(f"HTTP 429, retry after {response.headers.get('retry-after', '?')} s")
    if not response.ok:
        raise SourceDown(response.describe())
    try:
        doc = response.json()
    except ValueError:
        raise SourceDown('not JSON')
    info = doc.get('license_info') if isinstance(doc, dict) else None
    if not isinstance(info, str) or not EC_LICENCE.match(info):
        raise OtherLicence(f'under another licence: "{str(info)[:90]}"')
    if 'EUR' not in str(doc.get('unit')) or 'MWh' not in str(doc.get('unit')):
        raise SourceDown(f"prices in {doc.get('unit')}, not EUR/MWh")
    stamps = [s for s in doc.get('unix_seconds') or [] if isinstance(s, int)]
    values = {datetime.fromtimestamp(s, timezone.utc): _round(p)
              for s, p in zip(doc.get('unix_seconds') or [], doc.get('price') or [])
              if isinstance(s, int) and _number(p)}
    gaps = sorted({b - a for a, b in zip(sorted(stamps), sorted(stamps)[1:])})
    if not values or not gaps or gaps[0] not in (900, 3600):
        return {}
    step = timedelta(seconds=gaps[0])
    return {day.isoformat(): {**block, 'source': 'energy-charts', 'license': CC_BY, 'license_info': info}
            for day in wanted if (block := day_block(day, step, values))}


# --- GB: Elexon MID, as fetch_gb.py reads it ---

def gb_zone(days, now, get, budget, rates):
    """GB's entry for today: the half-hours of MID so far, in euros and in pounds."""
    today = days[0]
    start, end = carry_over.day_bounds(today)
    if budget.left() <= 0:
        raise SourceDown('out of time')
    url = f"{fetch_gb.EP_MARKET_INDEX}?from={start.strftime('%Y-%m-%dT%H:%MZ')}&to={end.strftime('%Y-%m-%dT%H:%MZ')}"
    response = get(url, timeout=min(30, max(1, budget.left())))
    if not response.ok:
        raise SourceDown(response.describe())
    try:
        records = fetch_gb._records(response.json())
    except ValueError:
        raise SourceDown('not JSON')
    periods = {}                                      # start -> [sum of price x volume, volume, [prices]]
    for record in records:
        price = fetch_gb._num(fetch_gb._ci(record, 'price'))
        volume = fetch_gb._num(fetch_gb._ci(record, 'volume')) or 0.0
        at = parse_time(fetch_gb._ci(record, 'startTime', 'start_time'))
        if price is None or price == 0 or at is None or not start <= at < end:
            continue                                  # the placeholders of the periods not settled yet
        kept = periods.setdefault(at, [0.0, 0.0, []])
        kept[0] += price * volume
        kept[1] += volume
        kept[2].append(price)
    pounds = {at: _round(s / v if v > 0 else sum(p) / len(p)) for at, (s, v, p) in periods.items()}
    if not pounds:
        raise SourceDown('no half-hour with a price')
    found = rates.rate('GBP', today)
    if found is None:
        raise SourceDown('no exchange rate for the pound')
    rate, rate_day = found
    euros = {at: _round(Decimal(str(value)) / rate) for at, value in pounds.items()}
    step = timedelta(minutes=30)
    block = day_block(today, step, euros, whole=False)
    block['prices_gbp'] = day_block(today, step, pounds, whole=False)['prices']
    return {'source': GB_SOURCE, 'license': ELEXON_TERMS, 'currency': 'EUR', 'currency_original': 'GBP',
            'fx_rate': float(rate), 'fx_date': rate_day, 'days': {today.isoformat(): block}}


# --- PT and ES: OMIE's file, as fetch_omie.py wrote it in this run ---

def omie_zones(days, path=OMIE_PATH):
    """{PT: entry, ES: entry} from data/omie-prices.json, for the days of `days` it has; zones with no today left
    out."""
    doc = load_json(path) or {}
    found = {}
    for block in (doc, doc.get('next_day')):
        if not isinstance(block, dict) or not isinstance(block.get('date'), str):
            continue
        try:
            day = datetime.strptime(block['date'], '%Y-%m-%d').date()
        except ValueError:
            continue
        if day not in days:
            continue
        for zone in ('PT', 'ES'):
            entry = block.get(zone) or {}
            quarters = entry.get('prices_15min') if block.get('resolution_minutes') == 15 else None
            listed = [p.get('price') for p in quarters] if quarters else [p.get('price') for p in entry.get('prices') or []]
            try:
                starts, minutes = fetch_omie.period_starts(day, len(listed))
            except fetch_omie.OmieFormatError:
                continue
            values = {local.astimezone(timezone.utc): value for local, value in zip(starts, listed)
                      if isinstance(value, (int, float))}
            day_prices = day_block(day, timedelta(minutes=minutes), values)
            if day_prices:
                found.setdefault(zone, {})[day.isoformat()] = day_prices
    return {zone: {'source': OMIE_SOURCE, 'license': OMIE_TERMS, 'currency': 'EUR', 'days': blocks}
            for zone, blocks in found.items() if days[0].isoformat() in blocks}


# --- The run ---

def asked(zone, entry, now, days):
    """Whether a SMARD zone is asked for in this run."""
    if due(entry, now, HOURLY):
        return True
    have = (entry or {}).get('days') or {}
    if days[0].isoformat() not in have:
        return True
    local = now.astimezone(carry_over.MARKET_TZ)
    return days[1].isoformat() not in have and (local.hour, local.minute) >= TOMORROW_FROM


def _in_window(entry, days):
    """An entry with only the days of `days`, and its data_time; None when none is left."""
    kept = {day.isoformat(): entry['days'][day.isoformat()] for day in days
            if isinstance((entry.get('days') or {}).get(day.isoformat()), dict)}
    if not kept:
        return None
    entry = {**entry, 'days': kept}
    if 'filter' in entry:                             # a SMARD zone: its attribution as its days now ask
        entry = attributed(entry)
    last = last_priced(kept)
    if last is not None:
        entry[DATA_TIME] = iso(last)
    return entry


def main(now=None, get=http_get, budget=None, pause=PAUSE_S, rates=None):
    now = now or datetime.now(timezone.utc)
    stamp = iso(now)
    days = window(now)
    budget = budget or Budget(BUDGET_S)
    previous = ((load_json(OUT_PATH) or {}).get('zones')) or {}
    previous = {zone: entry for zone, entry in previous.items() if isinstance(entry, dict)}
    print(f'Open prices: delivery days {days[0]} and {days[1]}')

    obtained, failed = {}, set()
    local = now.astimezone(carry_over.MARKET_TZ)
    tomorrow_due = (local.hour, local.minute) >= TOMORROW_FROM
    smard = {}                                        # zone -> [blocks, answered, days still missing]
    for zone in SMARD_ZONES:
        if not asked(zone, previous.get(zone), now, days):
            continue
        started = time.monotonic()
        blocks, answered, reasons = smard_days(zone, days, get, budget, pause)
        if blocks:
            print(f"  {zone}: SMARD, {', '.join(sorted(blocks))} by {next(iter(blocks.values()))['step_minutes']} min "
                  f"[{time.monotonic() - started:.1f}s]")
        else:
            print(f"  {zone}: SMARD did not give it ({'; '.join(reasons) or 'nothing'})")
        before = ((previous.get(zone) or {}).get('days')) or {}
        missing = []
        for day in days if tomorrow_due else days[:1]:
            name = day.isoformat()
            if name in blocks:
                continue
            if (before.get(name) or {}).get('source') == 'energy-charts':
                blocks[name] = before[name]           # final: kept as it was, with no new request
                print(f'  {zone}: {name} as Energy-Charts gave it before (SMARD does not have it yet)')
            else:
                missing.append(day)
        smard[zone] = [blocks, answered, missing]
        time.sleep(pause)

    # Energy-Charts, for the days SMARD lacks: the zones without today first, EC_PER_RUN at most.
    waiting = sorted((zone for zone, (_b, _a, missing) in smard.items() if missing),
                     key=lambda zone: (days[0] not in smard[zone][2], ORDER.index(zone)))
    ec_asked, stopped = 0, None
    for zone in waiting:
        blocks, answered, missing = smard[zone]
        names = ', '.join(day.isoformat() for day in missing)
        if stopped or ec_asked >= EC_PER_RUN:
            print(f'  {zone}: Energy-Charts not asked for {names} in this run '
                  f'({stopped or f"{EC_PER_RUN} zones a run at most"})')
            continue
        ec_asked += 1
        try:
            got = ec_days(zone, missing, get, budget)
        except RateLimited as e:
            stopped = f'Energy-Charts asked to wait: {e}'
            print(f'  {zone}: Energy-Charts did not give {names} ({e}); no more requests to it in this run')
            continue
        except SourceDown as e:
            print(f'  {zone}: Energy-Charts did not give {names} ({e}); the zone stays without '
                  f"{'them' if len(missing) > 1 else 'it'}")
            continue
        finally:
            time.sleep(pause)
        smard[zone][1] = True
        blocks.update(got)
        lacking = [day.isoformat() for day in missing if day.isoformat() not in got]
        print(f"  {zone}: Energy-Charts (CC BY 4.0), {', '.join(sorted(got)) or 'nothing'}"
              + (f'; no whole day of {", ".join(lacking)} there yet' if lacking else ''))

    for zone, (blocks, answered, _missing) in smard.items():
        # Obtained when SMARD or Energy-Charts answered and there is today; otherwise the zone keeps what it had.
        if answered and days[0].isoformat() in blocks:
            obtained[zone] = smard_entry(zone, {day.isoformat(): blocks[day.isoformat()]
                                                for day in days if day.isoformat() in blocks})
        else:
            failed.add(zone)
    try:
        obtained['GB'] = gb_zone(days, now, get, budget, rates or fx_rates.load())
        print(f"  GB: Elexon MID, {sum(v is not None for v in obtained['GB']['days'][days[0].isoformat()]['prices'])} "
              f"half-hours of today, at {obtained['GB']['fx_rate']} pounds a euro ({obtained['GB']['fx_date']})")
    except SourceDown as e:
        failed.add('GB')
        print(f'  GB: Elexon did not give it ({e})')
    omie = omie_zones(days)
    for zone in ('PT', 'ES'):
        if zone in omie:
            obtained[zone] = omie[zone]
            print(f"  {zone}: OMIE, {', '.join(sorted(omie[zone]['days']))}")
        else:
            failed.add(zone)
            print(f'  {zone}: no day of {days[0]} in {OMIE_PATH}')

    obtained = {zone: kept for zone, entry in obtained.items() if (kept := _in_window(entry, days))}
    before = {zone: kept for zone, entry in previous.items() if (kept := _in_window(entry, days))}
    zones, kept, dropped = carry(before, obtained, failed, stamp,
                                 lambda zone, entry: (at := parse_time(entry.get(FETCHED_AT))) is not None
                                 and now - at < MAX_AGE)
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(f'{z} (from {zones[z][FETCHED_AT]})' for z in kept))
    if dropped:
        print('  Left out, older than 24 hours or of other days: ' + ', '.join(dropped))
    if not zones:
        print(f'Nothing to publish: {OUT_PATH} is left as it was')
        return 1
    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry.get(DATA_TIME) for entry in zones.values()),
        'timezone': 'Europe/Brussels',
        'unit': 'EUR/MWh',
        'zones': {zone: zones[zone] for zone in ORDER if zone in zones},
    }
    write_json(OUT_PATH, doc, separators=(',', ':'))
    with_tomorrow = [zone for zone, entry in doc['zones'].items() if days[1].isoformat() in entry['days']]
    print(f"Wrote {OUT_PATH}: {len(doc['zones'])} zones ({len(obtained)} from this run, {len(kept)} kept); "
          f"tomorrow for {len(with_tomorrow)}")
    return 0 if obtained else 1


if __name__ == '__main__':
    sys.exit(main())
