#!/usr/bin/env python3
"""fetch_balancing.py - the price of balancing energy (aFRR), by country.

Writes data/balancing-prices.json, with two blocks that are kept apart because
they are under different licences (see feed_common.py):

    afrr_activated   cc-by-4.0      ENTSO-E item 17.1.F, "prices of activated
                                    balancing energy": the aFRR price of each
                                    settlement period, upward and downward.
    afrr_cbmp        source-terms   the cross-border marginal price of the
                                    aFRR platform (PICASSO), which ENTSO-E
                                    publishes every 4 seconds: here only its
                                    summary by quarter-hour (lowest, mean,
                                    highest and how many values had a price).
                                    The series itself is never kept.

What ENTSO-E has, as its answers of 18 Nov 2026 showed:

  - 17.1.F is asked for by control area, with documentType A84 and
    businessType A96 (aFRR), and comes under two processes. processType A16
    ("realised") is what the countries that settle aFRR by themselves publish:
    Portugal, Poland, Finland, Switzerland, Germany's operators, Norway,
    Sweden, Romania, Hungary, Serbia... processType A68 ("aFRR local
    selection") is what Spain and Austria publish, since they joined PICASSO,
    in place of A16: Spain has had nothing under A16 since then. Both are item
    17.1.F. A zone is asked for both and shows the one that has prices (A16
    when both do); `process` says which. Italy is the exception: when both
    processes have prices, the one with more periods priced in the days the
    file keeps stays (A16 when they tie). Its North zone publishes A68 for
    whole days and A16 only for some hours.
  - Not every country is up to date. Spain's last period is half an hour old;
    Portugal publishes a whole delivery day at a time, two days later (at 18:00
    UTC of the 18th its last period was 22:45 UTC of the 16th). So the request
    covers the delivery day in course and the three before it, and each zone
    shows the two most recent delivery days ENTSO-E has prices for: the day in
    course and the one before for most, the two before those for Portugal.
    `data_time` says where each zone's prices end.
  - A period with no activation has no price: it is left out of the document
    (the series comes in pieces) and is null here. Zero is a price.
  - Prices are in the currency the operator publishes them in: zloty for
    Poland, lei for Romania, convertible marks for Bosnia. `currency` says
    which, and `up` and `down` are as published. Such a zone also has the
    prices in euros, `up_eur` and `down_eur`, with the rate they were
    converted at and the day that rate is of (`fx_rate`, `fx_date`) in each
    day: the ECB's reference rate of the delivery day or the last one
    published before it, and a fixed 1.95583 for the Bosnian mark, for which
    `fx_date` is null (fx_rates.py, which this step brings up to date).
  - Italy is asked for twice: by the control area of the country, which is
    what its PICASSO series is published under, and by the North bidding zone
    the feed shows its prices for, which had three periods in two days when
    it was the only one asked. The one that has prices stays (the most recent
    when both do), and `eic` and `area` say which.
  - The PICASSO series (processType A67) has one value every 4 seconds, with
    curveType A03: a position left out repeats the one before, and a point
    written without a price means "no price from here on". It is published in
    blocks of a quarter of an hour, 15 to 30 minutes after each ends. One hour
    is 50 to 160 kB of XML, a day 3 to 4 MB, so each run asks only for what is
    new since the last quarter-hour it summarised (`summarized_until`), 12
    hours at most at a time: a zone seen for the first time catches up with the
    day before in a few runs.
  - Portugal is not on PICASSO (REN's accession is planned for October 2027)
    and has no A68: it has the 17.1.F block only. A zone ENTSO-E has no data
    for is listed under `not_published`, with the time it was last asked, and
    is asked again once a day (Portugal and Spain once an hour).

Portugal and Spain are asked for in every run; the other zones of the feed
once an hour. The step has its own time budget, runs after the files the site
already reads have been published, and never fails the run. A zone that was
asked for and did not answer keeps what was published, with the time it was
obtained and the mark carried_over, while it still has a day of the window; a
run that obtains nothing leaves the file exactly as it was.

    {updated, data_time, timezone,
     afrr_activated: {license, source, zones: {PT: {eic, process, currency,
         resolution_minutes, price_category, days: {"2026-11-16": {start,
         periods, up: [...], down: [...][, up_eur, down_eur, fx_rate,
         fx_date]}}, data_time, fetched_at[, area]}},
         not_published: {A16: {zone: time}, A68: {zone: time}}},
     afrr_cbmp: {license, source, zones: {ES: {eic, currency,
         resolution_seconds, days: {"2026-11-18": {start, quarters,
         up: {min: [...], avg: [...], max: [...], count: [...]}, down: {...}}},
         summarized_until, data_time, fetched_at}}, not_published: {zone: time}}}

`start` is the first instant of the delivery day (midnight on the market's
clock, in UTC) and the lists have one value for each period from there on: 96
quarter-hours, 92 and 100 on the days the clocks change.
"""

import collections
import os
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from xml.etree import ElementTree as ET

import carry_over
import fetch_entsoe as fe
import fx_rates
from feed_common import (CC_BY, DAILY, DATA_TIME, HOURLY, SOURCE_TERMS, Budget, asked_recently, carry, due,
                         entsoe_answer, entsoe_ask, http_get, iso, latest, load_json, parse_time, period_intervals,
                         step_of, write_json)

OUT_PATH = 'data/balancing-prices.json'
BUDGET_S = float(os.environ.get('BALANCING_BUDGET_SECONDS', '75'))

ACTIVATED = 'afrr_activated'
CBMP = 'afrr_cbmp'
ALWAYS = ('PT', 'ES')            # asked for in every run; the rest once an hour
PROCESSES = ('A16', 'A68')       # 17.1.F: realised, and aFRR local selection
DAYS_BACK = 3                    # delivery days before the one in course that 17.1.F is asked for
DAYS_SHOWN = 2                   # the most recent of them that have prices
QUARTER = timedelta(minutes=15)
CATCH_UP = timedelta(hours=12)   # the most of the 4-second series one request asks for
STAMP = '%Y%m%d%H%M'
DIRECTIONS = {'A01': 'up', 'A02': 'down'}

# The area each zone's balancing prices are asked for: the feed's own zones,
# but for Germany, whose price is one for the country and is published under
# each of its control areas (TenneT's here), not under the bidding zone, and
# for Italy's PICASSO series, published under the country. The United Kingdom
# is not on ENTSO-E. Moldova is left out: ENTSO-E's list of data free to
# reuse excludes Moldova's, so its prices could not go in the cc-by-4.0 block.
AREAS = {code: eic for code, eic in fe.ZONES.items() if code not in fe.ELEXON_ZONES and code != 'MD'}
AREAS['DE'] = '10YDE-EON------1'
CBMP_AREAS = dict(AREAS, IT='10YIT-GRTN-----B')
# Zones for which, when both processes have prices, the one with more periods priced stays (see above).
MOST_PERIODS = ('IT',)
# Zones whose 17.1.F prices are asked for by more than one area: [(EIC, what it is)], the first preferred.
ALTERNATIVES = {'IT': [(CBMP_AREAS['IT'], 'control area of the country'), (AREAS['IT'], 'North bidding zone')]}


def areas_of(zone):
    """[(EIC, what it is or None)] of the areas a zone's 17.1.F prices are asked for."""
    return ALTERNATIVES.get(zone) or [(AREAS[zone], None)]
ORDER = list(ALWAYS) + [code for code in AREAS if code not in ALWAYS]


def _cents(value):
    return round(value + 0.0, 2)


# --- 17.1.F: the price of each settlement period ---

def read_activated(xml):
    """The aFRR prices of a 17.1.F document:

        {'up': {start: price}, 'down': {start: price}, 'step': timedelta,
         'currency': 'EUR', 'category': 'A06'}

    or None when it has none. A period with no price is not in the dicts.
    When a direction comes twice for the same period (once as a standard
    product, once as an original or as no product at all), the standard
    product stands."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    ranked = []
    for series in root.findall('{*}TimeSeries'):
        direction = DIRECTIONS.get((series.findtext('{*}flowDirection.direction') or '').strip())
        if direction is None or (series.findtext('{*}businessType') or '').strip() != 'A96':
            continue
        rank = (0 if series.find('{*}standard_MarketProduct.marketProductType') is not None
                else 1 if series.find('{*}original_MarketProduct.marketProductType') is not None else 2)
        ranked.append((rank, direction, series))
    found = {'up': {}, 'down': {}}
    steps, currencies, categories = [], collections.Counter(), collections.Counter()
    for _rank, direction, series in sorted(ranked, key=lambda item: -item[0]):      # the standard product last: it stands
        variable = (series.findtext('{*}curveType') or '').strip() == 'A03'
        currencies[(series.findtext('{*}currency_Unit.name') or '').strip() or None] += 1
        for period in series.findall('{*}Period'):
            step = step_of(period.findtext('{*}resolution'))
            if step is None or step > timedelta(hours=1):
                continue
            had = False
            for start, value in period_intervals(period, 'activation_Price.amount', variable):
                if value is not None:
                    found[direction][start] = (value, step)
                    had = True
            if had:
                steps.append(step)
                for point in period.findall('{*}Point'):
                    categories[(point.findtext('{*}imbalance_Price.category') or '').strip() or None] += 1
    if not steps:
        return None
    finest = min(steps)
    # A price published for a longer period (an hour, in a document that also
    # has quarter-hours) stands for every period it covers.
    spread = {direction: {start + finest * i: value
                          for start, (value, step) in sorted(prices.items()) for i in range(int(step / finest))}
              for direction, prices in found.items()}
    return {**spread, 'step': finest, 'currency': currencies.most_common(1)[0][0],
            'category': categories.most_common(1)[0][0] if categories else None}


def day_series(prices, day, step):
    """A day's list of prices from {start: price}: one value for each period
    of `step` from the start of the delivery day, None where there is none."""
    start, end = carry_over.day_bounds(day)
    values = [None] * int((end - start) / step)
    for at, value in prices.items():
        if start <= at < end:
            values[int((at - start) / step)] = _cents(value)
    return values


def in_euros(values, rate):
    return [None if value is None else _cents(float(Decimal(str(value)) / rate)) for value in values]


def priced_periods(entry):
    """How many periods of a zone's days have a price, upward or downward."""
    return sum(1 for day in entry['days'].values() for up, down in zip(day['up'], day['down'])
               if up is not None or down is not None)


def activated_entry(eic, process, read, today, rates=None, area=None):
    """A zone of the afrr_activated block from what read_activated() gave: the
    two most recent delivery days that have a price, of the day in course and
    the DAYS_BACK before it. None when none of them has. A zone that is not
    in euros has each day in euros too, when `rates` (fx_rates.Rates) has a
    rate for its currency."""
    days = {}
    last = None
    for back in range(DAYS_BACK + 1):
        day = today - timedelta(days=back)
        start, end = carry_over.day_bounds(day)
        up, down = day_series(read['up'], day, read['step']), day_series(read['down'], day, read['step'])
        if all(v is None for v in up) and all(v is None for v in down):
            continue
        block = {'start': iso(start), 'periods': len(up), 'up': up, 'down': down}
        if read['currency'] not in (None, 'EUR') and rates is not None:
            found = rates.rate(read['currency'], day)
            if found:
                rate, rate_day = found
                block.update(up_eur=in_euros(up, rate), down_eur=in_euros(down, rate),
                             fx_rate=float(rate), fx_date=rate_day)
        days[day.isoformat()] = block
        priced = [at for prices in (read['up'], read['down']) for at in prices if start <= at < end]
        last = max([last] + priced) if last else max(priced)
        if len(days) == DAYS_SHOWN:
            break
    if not days:
        return None
    return {
        'eic': eic,
        **({'area': area} if area else {}),
        'process': process,
        'currency': read['currency'],
        'resolution_minutes': int(read['step'].total_seconds() // 60),
        'price_category': read['category'],
        'days': days,
        DATA_TIME: iso(last),
    }


# --- PICASSO: the 4-second series, summarised by quarter-hour ---

def read_cbmp(xml):
    """The 4-second prices of a PICASSO document, summarised:

        {'quarters': {start of a quarter-hour: {'up': stats, 'down': stats}},
         'until': end of the last quarter-hour the document covers whole,
         'currency': 'EUR', 'step_seconds': 4}

    with stats = (lowest, mean, highest, how many values had a price), or
    (None, None, None, 0) for a quarter-hour in which a direction never had
    one. Only quarter-hours the document covers from beginning to end are
    there. None when the document has no such series."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    seen = {}            # (direction, quarter) -> [positions, with a price, sum, lowest, highest]
    step_seen, currencies = None, collections.Counter()
    for series in root.findall('{*}TimeSeries'):
        direction = DIRECTIONS.get((series.findtext('{*}flowDirection.direction') or '').strip())
        if direction is None:
            continue
        variable = (series.findtext('{*}curveType') or '').strip() == 'A03'
        currencies[(series.findtext('{*}currency_Unit.name') or '').strip() or None] += 1
        for period in series.findall('{*}Period'):
            step = step_of(period.findtext('{*}resolution'))
            if step is None or step >= QUARTER:
                continue
            step_seen = step if step_seen is None else min(step_seen, step)
            for start, value in period_intervals(period, 'activation_Price.amount', variable):
                quarter = start - timedelta(minutes=start.minute % 15, seconds=start.second, microseconds=start.microsecond)
                tally = seen.setdefault((direction, quarter), [0, 0, 0.0, None, None])
                tally[0] += 1
                if value is not None:
                    tally[1] += 1
                    tally[2] += value
                    tally[3] = value if tally[3] is None else min(tally[3], value)
                    tally[4] = value if tally[4] is None else max(tally[4], value)
    if step_seen is None:
        return None
    whole = int(QUARTER / step_seen)
    directions = sorted({direction for direction, _quarter in seen})
    quarters = {}
    for quarter in sorted({quarter for _direction, quarter in seen}):
        tallies = {direction: seen.get((direction, quarter)) for direction in directions}
        if any(tally is None or tally[0] < whole for tally in tallies.values()):
            continue                         # not whole yet: asked for again in the next run
        quarters[quarter] = {
            direction: ((_cents(tally[3]), _cents(tally[2] / tally[1]), _cents(tally[4]), tally[1]) if tally[1]
                        else (None, None, None, 0))
            for direction, tally in tallies.items()}
    return {'quarters': quarters, 'until': max(quarters) + QUARTER if quarters else None,
            'currency': currencies.most_common(1)[0][0] if currencies else None,
            'step_seconds': int(step_seen.total_seconds())}


STATS = ('min', 'avg', 'max', 'count')


def _summary_day(day, kept=None):
    """A day of the afrr_cbmp block: the one published before (`kept`), when
    it has the shape it should, or an empty one."""
    start, end = carry_over.day_bounds(day)
    count = int((end - start) / QUARTER)
    block = {'start': iso(start), 'quarters': count}
    for direction in ('up', 'down'):
        had = kept.get(direction) if isinstance(kept, dict) else None
        block[direction] = {
            name: (list(had[name]) if isinstance(had, dict) and isinstance(had.get(name), list)
                   and len(had[name]) == count else [None] * count)
            for name in STATS}
    return block


def cbmp_entry(eic, before, read, today, frontier):
    """A zone of the afrr_cbmp block: what was published for it (`before`, or
    None) with the quarter-hours of `read` put in. Only the delivery day in
    course and the one before are kept."""
    entry = {'eic': eic, 'currency': (before or {}).get('currency'),
             'resolution_seconds': (before or {}).get('resolution_seconds'), 'days': {}}
    if read:
        entry['currency'] = read['currency'] or entry['currency']
        entry['resolution_seconds'] = read['step_seconds']
    for day in (today, today - timedelta(days=1)):
        entry['days'][day.isoformat()] = _summary_day(day, ((before or {}).get('days') or {}).get(day.isoformat()))
    last = parse_time((before or {}).get(DATA_TIME))
    for quarter, stats in ((read or {}).get('quarters') or {}).items():
        day = carry_over.delivery_day(quarter)
        block = entry['days'].get(day.isoformat())
        if block is None:
            continue
        index = int((quarter - parse_time(block['start'])) / QUARTER)
        for direction in ('up', 'down'):
            for name, value in zip(STATS, stats.get(direction, (None, None, None, 0))):
                block[direction][name][index] = value
        last = quarter if last is None else max(last, quarter)
    entry['days'] = {name: block for name, block in entry['days'].items()
                     if any(count is not None for d in ('up', 'down') for count in block[d]['count'])}
    entry['summarized_until'] = iso(frontier)
    if last is not None:
        entry[DATA_TIME] = iso(last)
    return entry


def cbmp_window(entry, now, today):
    """(start, end) of the 4-second series a zone is asked for: from the last
    quarter-hour summarised for it (the start of the day before, the first
    time) up to now, CATCH_UP at most."""
    earliest = carry_over.day_bounds(today - timedelta(days=1))[0]
    done = parse_time((entry or {}).get('summarized_until'))
    start = max(earliest, done) if done else earliest
    ceiling = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return start, min(ceiling, start + CATCH_UP)


# --- The run ---

def _days_in_window(entry, oldest):
    """An entry with the delivery days before `oldest` taken out, or None when none is left."""
    days = {name: block for name, block in (entry.get('days') or {}).items() if name >= oldest.isoformat()}
    return dict(entry, days=days) if days else None


def _zones(doc, block):
    zones = ((doc or {}).get(block) or {}).get('zones')
    return {name: entry for name, entry in zones.items() if isinstance(entry, dict)} if isinstance(zones, dict) else {}


def _memory(doc, block):
    memory = ((doc or {}).get(block) or {}).get('not_published')
    return memory if isinstance(memory, dict) else {}


def plan(previous, now):
    """The requests of this run, Portugal and Spain first: [(key, name, params)]
    with key (block, zone, process)."""
    today = carry_over.delivery_day(now)
    hour = now.replace(minute=0, second=0, microsecond=0)
    window = {'periodStart': carry_over.day_bounds(today - timedelta(days=DAYS_BACK))[0].strftime(STAMP),
              'periodEnd': (hour + timedelta(hours=1)).strftime(STAMP)}
    activated, summaries = _zones(previous, ACTIVATED), _zones(previous, CBMP)
    no_price, no_series = _memory(previous, ACTIVATED), _memory(previous, CBMP)
    first, rest = [], []
    for zone in ORDER:
        jobs = first if zone in ALWAYS else rest
        again = HOURLY if zone in ALWAYS else DAILY
        if zone in ALWAYS or due(activated.get(zone), now):
            for process in PROCESSES:
                if asked_recently((no_price.get(process) or {}).get(zone), now, again):
                    continue
                for eic, what in areas_of(zone):
                    jobs.append(((ACTIVATED, zone, (process, eic)),
                                 f'{zone} 17.1.F {process}' + (f' ({what})' if what else ''),
                                 dict(documentType='A84', processType=process, businessType='A96',
                                      controlArea_Domain=eic, **window)))
        entry = summaries.get(zone)
        start, end = cbmp_window(entry, now, today)
        behind = now - start > timedelta(hours=3)
        if entry is None and asked_recently(no_series.get(zone), now, again):
            continue
        if zone in ALWAYS or behind or due(entry, now):
            jobs.append(((CBMP, zone, 'A67'), f'{zone} PICASSO {start:%d %H:%M}-{end:%d %H:%M}',
                         dict(documentType='A84', processType='A67', businessType='A96',
                              controlArea_Domain=CBMP_AREAS[zone], periodStart=start.strftime(STAMP),
                              periodEnd=end.strftime(STAMP))))
    return first + rest


def main(now=None, get=http_get, budget=None, interval=0.25, update_rates=fx_rates.update):
    now = now or datetime.now(timezone.utc)
    today = carry_over.delivery_day(now)
    stamp = iso(now)
    previous = load_json(OUT_PATH)
    try:
        update_rates(now)                      # the ECB's rates of the day, when they may be out
    except Exception as e:  # noqa: BLE001 - the rates the file has are used
        print(f'  Exchange rates not brought up to date: {type(e).__name__}: {e}')
    rates = fx_rates.load()
    jobs = plan(previous, now)
    print(f'Balancing prices: {len(jobs)} requests for delivery day {today} and the {DAYS_BACK} before')
    if not jobs:
        print('  Nothing due in this run')
        return 0
    answers = {key: entsoe_answer(response)
               for key, response in entsoe_ask(jobs, budget or Budget(BUDGET_S), get=get, interval=interval,
                                                label='Balancing').items()}
    asked = collections.defaultdict(dict)                     # (block, zone) -> {process or (process, area): Answer}
    for (block, zone, process), answer in answers.items():
        asked[(block, zone)][process] = answer

    # 17.1.F
    oldest = today - timedelta(days=DAYS_BACK)
    before = {zone: trimmed for zone, entry in _zones(previous, ACTIVATED).items()
              if (trimmed := _days_in_window(entry, oldest))}
    no_price = {process: dict(_memory(previous, ACTIVATED).get(process) or {}) for process in PROCESSES}
    obtained, failed = {}, set()
    for zone in ORDER:
        given = asked.get((ACTIVATED, zone))
        if not given:
            continue
        # Of each area, the first process that has prices (for Italy, the one with more periods priced, the
        # first when they tie); of the areas, the one whose prices go furthest.
        entries = []
        for eic, what in areas_of(zone):
            of_the_area = []
            for process in PROCESSES:
                answer = given.get((process, eic))
                read = read_activated(answer.documents[0]) if answer is not None and answer.documents else None
                entry = activated_entry(eic, process, read, today, rates, what) if read else None
                if entry:
                    of_the_area.append(entry)
                    if zone not in MOST_PERIODS:
                        break
            if len(of_the_area) > 1:
                counts = ', '.join(f"{e['process']} {priced_periods(e)}" for e in of_the_area)
                print(f'  {zone}: periods priced by the {what or eic}: {counts}')
            if of_the_area:
                entries.append(max(of_the_area, key=priced_periods))           # the first of them, when they tie
        for process in PROCESSES:
            answers_of = [given[(process, eic)] for eic, _what in areas_of(zone) if (process, eic) in given]
            if answers_of and all(a.problem is None and not a.documents for a in answers_of):
                no_price[process][zone] = stamp             # ENTSO-E has no such prices for the zone
            elif any(a.documents for a in answers_of):
                no_price[process].pop(zone, None)
        if entries:
            entry = max(entries, key=lambda e: parse_time(e[DATA_TIME]))       # the first of them, when they tie
            obtained[zone] = entry
            euros = next((f", in euros at {day['fx_rate']}" for day in entry['days'].values() if 'fx_rate' in day), '')
            print(f"  {zone}: 17.1.F {entry['process']}" + (f" by the {entry['area']}" if entry.get('area') else '')
                  + f", {', '.join(sorted(entry['days']))}, up to {entry[DATA_TIME]}, {entry['currency']}{euros}")
        else:
            failed.add(zone)
    activated, kept, dropped = carry(before, obtained, failed, stamp, lambda zone, entry: True)
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(kept))

    # PICASSO
    yesterday = today - timedelta(days=1)
    before = {zone: trimmed for zone, entry in _zones(previous, CBMP).items()
              if (trimmed := _days_in_window(entry, yesterday))}
    no_series = dict(_memory(previous, CBMP))
    summaries, missed = {}, set()
    for zone in ORDER:
        answer = (asked.get((CBMP, zone)) or {}).get('A67')
        if answer is None:
            continue
        if answer.problem is not None:
            missed.add(zone)
            continue
        start, end = cbmp_window(_zones(previous, CBMP).get(zone), now, today)
        read = read_cbmp(answer.documents[0]) if answer.documents else None
        had = before.get(zone)
        if read is None and had is None:
            no_series[zone] = stamp                       # ENTSO-E has no such series for this area
            continue
        no_series.pop(zone, None)
        frontier = start
        if read and read['until']:
            frontier = max(start, read['until'])
        elif end <= now - timedelta(hours=2):
            frontier = end                                # nothing published for that stretch: go on past it
        entry = cbmp_entry(CBMP_AREAS[zone], had, read, today, frontier)
        if not entry['days']:
            continue
        summaries[zone] = entry
        added = len((read or {}).get('quarters') or {})
        print(f"  {zone}: PICASSO, {added} quarter-hours summarised, up to {entry.get(DATA_TIME)}")
    cbmp, kept, _dropped = carry(before, summaries, missed, stamp, lambda zone, entry: True)
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(kept))

    if not obtained and not summaries:
        print(f'Nothing obtained in this run: {OUT_PATH} is left exactly as it was')
        return 1

    def ordered(entries):
        return {zone: entries[zone] for zone in fe.ZONES if zone in entries}

    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry.get(DATA_TIME) for entries in (activated, cbmp) for entry in entries.values()),
        'timezone': 'Europe/Brussels',
        ACTIVATED: {
            'license': CC_BY,
            'source': 'ENTSO-E Transparency Platform, item 17.1.F: prices of activated balancing energy (aFRR)',
            'zones': ordered(activated),
            'not_published': {process: ordered(no_price[process]) for process in PROCESSES},
        },
        CBMP: {
            'license': SOURCE_TERMS,
            'source': 'ENTSO-E Transparency Platform: cross-border marginal prices for aFRR, central selection '
                      '(PICASSO), summarised by quarter-hour',
            'zones': ordered(cbmp),
            'not_published': ordered(no_series),
        },
    }
    write_json(OUT_PATH, doc, separators=(',', ':'))
    print(f"Wrote {OUT_PATH}: {len(activated)} zones with the 17.1.F price ({len(obtained)} from this run), "
          f"{len(cbmp)} with the PICASSO summary ({len(summaries)} from this run)")
    return 0


if __name__ == '__main__':
    sys.exit(main())
