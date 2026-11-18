"""price_intervals.py - the day-ahead prices of the day at the market's own resolution.

spot-prices.json has one price for each hour of the delivery day: the mean of
the intervals the market publishes, which since October 2025 are quarter-hours
for most zones. Counting how long the price was negative, zero or low
(fetch_low_prices.py) has to be done on those intervals, before the hourly
mean hides them: an hour with three quarters at 10 and one at -40 has a mean
of -2.5, and an hour with one quarter at -1 and three at 20 has no negative
price at all in the hourly file.

No further request is made. From the answers fetch_entsoe.py already gets for
spot-prices.json, this module keeps each zone's prices of the delivery day in
course, as read (the same auction, the same hours: see _complete_hours() in
fetch_entsoe.py), in data/spot-prices-intervals.json:

    {updated, data_time, delivery_date, timezone,
     day_ahead_prices: {license, source, zones: {DE: {start, step_minutes,
         prices: [...], data_time, fetched_at[, carried_over]}}}}

`start` is the first instant of the delivery day (midnight on the market's
clock, in UTC) and `prices` has one value for each interval of `step_minutes`
from there on, in EUR/MWh: 96 quarter-hours, 92 and 100 on the days the clocks
change; null for an interval of an hour that is not complete. A zone that
publishes part of the day by the hour and part by the quarter-hour has the
hourly price repeated in its four quarter-hours.

A zone that did not answer in a run keeps its prices while they are of the
delivery day in course, with the time they were obtained and the mark
carried_over (the feed's rule for prices). The licence is the source's terms:
day-ahead prices (12.1.D) are not on ENTSO-E's list of data free to reuse.

fetch_entsoe.py calls save() after spot-prices.json is published and never
depends on it: a failure here is logged and changes nothing else.
"""

from datetime import timedelta

import carry_over
from feed_common import CARRIED_OVER, DATA_TIME, FETCHED_AT, SOURCE_TERMS, iso, latest, load_json, parse_time, write_json

PATH = 'data/spot-prices-intervals.json'
BLOCK = 'day_ahead_prices'
SOURCE = 'ENTSO-E Transparency Platform, item 12.1.D: day-ahead prices'


def zone_entry(intervals, day):
    """A zone of the file from fetch_entsoe's [(start, step, price)] of the
    delivery day: its prices from midnight on, at the finest step it has."""
    start, end = carry_over.day_bounds(day)
    step = min(step for _at, step, _price in intervals)
    prices = [None] * int((end - start) / step)
    last = None
    for at, length, price in intervals:
        for part in range(int(length / step)):                # an hourly price stands for its four quarter-hours
            moment = at + step * part
            if start <= moment < end:
                prices[int((moment - start) / step)] = float(round(price, 2))
                last = moment if last is None else max(last, moment)
    if last is None:
        return None
    return {'start': iso(start), 'step_minutes': int(step.total_seconds() // 60), 'prices': prices, DATA_TIME: iso(last)}


def save(obtained, day, now, path=PATH):
    """Write the file with this run's prices ({zone: [(start, step, price)]})
    and those kept from before for the same delivery day. Returns how many
    zones it has; with nothing obtained it is left as it was."""
    fresh = {zone: entry for zone, intervals in obtained.items() if intervals and (entry := zone_entry(intervals, day))}
    if not fresh:
        return 0
    stamp = iso(now)
    previous = load_json(path) or {}
    before = (previous.get(BLOCK) or {}).get('zones') if previous.get('delivery_date') == day.isoformat() else None
    zones = {}
    for zone in list(dict.fromkeys(list(before or {}) + list(fresh))):
        if zone in fresh:
            zones[zone] = dict(fresh[zone], **{FETCHED_AT: stamp})
        elif isinstance(before[zone], dict):
            zones[zone] = dict(before[zone], **{CARRIED_OVER: True})
    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry.get(DATA_TIME) for entry in zones.values()),
        'delivery_date': day.isoformat(),
        'timezone': 'Europe/Brussels',
        BLOCK: {'license': SOURCE_TERMS, 'source': SOURCE, 'zones': zones},
    }
    write_json(path, doc, separators=(',', ':'))
    return len(zones)


def read(path=PATH):
    """(delivery day as text, {zone: (start, step, [prices])}) of the file;
    (None, {}) when it is missing or cannot be read."""
    doc = load_json(path) or {}
    zones = (doc.get(BLOCK) or {}).get('zones')
    found = {}
    for zone, entry in (zones.items() if isinstance(zones, dict) else []):
        try:
            start, step = parse_time(entry['start']), timedelta(minutes=int(entry['step_minutes']))
            prices = entry['prices']
        except (KeyError, TypeError, ValueError):
            continue
        if start is not None and step > timedelta(0) and isinstance(prices, list):
            found[zone] = (start, step, prices)
    return doc.get('delivery_date'), found
