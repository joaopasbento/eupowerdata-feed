"""wind_solar_actual.py - the actual generation of wind and solar, kept through the day.

fetch_entsoe.py asks ENTSO-E for each zone's actual generation (A75) of the
last 24 hours and publishes the last value of each technology. The forecast
error (fetch_forecast.py) needs more than the last value: the whole of the day
in course and of the day before, for solar, wind onshore and wind offshore.
No further request is made for it. From the answers the feed already gets,
this module keeps those three series in data/wind-solar-actual.json, run after
run: each run brings the last 24 hours, so the file fills up with the delivery
day in course and, from its second day on, with the whole of the day before.
(A gap of more than a day in the feed leaves a hole that is not filled.)

    {updated, data_time,
     actual_generation: {license, source, window: {start, end},
         zones: {PT: {solar: {step_minutes, values: [...]},
                      wind_onshore: {...}, wind_offshore: {...},
                      data_time, fetched_at[, carried_over]}}}}

`window` is the delivery day before and the delivery day in course on the
market's clock, in UTC. A technology's `values` has one value for each
interval of `step_minutes` from `window.start` on, in MW, null where ENTSO-E
has none (yet). A value is the latest ENTSO-E gave for its interval: the
actuals are revised, and the 24 hours of each run overwrite what was kept.

A zone that did not answer in a run keeps its series, with the time they were
obtained and the mark carried_over. The licence is the source's terms: actual
generation (16.1.B&C) is not on ENTSO-E's list of data free to reuse.

fetch_entsoe.py calls save() after the generation mix is published and never
depends on it: a failure here is logged and changes nothing else.
"""

from datetime import timedelta

import carry_over
from feed_common import (CARRIED_OVER, DATA_TIME, FETCHED_AT, SOURCE_TERMS, iso, latest, load_json, parse_time,
                         write_json)

PATH = 'data/wind-solar-actual.json'
BLOCK = 'actual_generation'
TECHNOLOGIES = {'B16': 'solar', 'B19': 'wind_onshore', 'B18': 'wind_offshore'}
NAMES = tuple(TECHNOLOGIES.values())
SOURCE = 'ENTSO-E Transparency Platform, items 16.1.B&C: actual generation per production type'


def series_of(all_series, now, every_interval):
    """The wind and solar series of an A75 answer, from fetch_entsoe's
    read_series(): {name: (step, {start of an interval: MW})}, only intervals
    that have begun. `every_interval` is fetch_entsoe's reading of a Period,
    which puts back the points curveType A03 leaves out. Consumption series
    are not generation and are left out. A technology that comes in pieces
    (several TimeSeries) is put together; its step is the finest of them."""
    found = {}
    for series in all_series:
        name = TECHNOLOGIES.get(series['psr'])
        if name is None or series['out']:
            continue
        for period in series['periods']:
            values = {at: value for at, value in every_interval(period) if at <= now}
            if not values:
                continue
            step, kept = found.get(name, (period['step'], {}))
            kept.update(values)
            found[name] = (min(step, period['step']), kept)
    return found


def window_of(now):
    """(start, end) of what is kept: the delivery day before and the one in course."""
    today = carry_over.delivery_day(now)
    return carry_over.day_bounds(today - timedelta(days=1))[0], carry_over.day_bounds(today)[1]


def _unpack(entry, start):
    """A technology of the file as (step, {time: MW}), or None when it cannot be read."""
    try:
        step = timedelta(minutes=int(entry['step_minutes']))
        values = entry['values']
    except (KeyError, TypeError, ValueError):
        return None
    if step <= timedelta(0) or not isinstance(values, list):
        return None
    return step, {start + step * i: value for i, value in enumerate(values) if isinstance(value, (int, float))}


def _pack(step, values, start, end):
    count = int((end - start) / step)
    listed = [None] * count
    for at, value in values.items():
        if start <= at < end and (at - start) % step == timedelta(0):
            listed[int((at - start) / step)] = round(value, 1)
    while listed and listed[-1] is None:
        listed.pop()                     # the part of the day still to come
    return {'step_minutes': int(step.total_seconds() // 60), 'values': listed}


def read(path=PATH):
    """What the file has: ({zone: {name: (step, {time: MW})}}, {zone: its entry}).
    Empty when the file is missing or cannot be read."""
    doc = load_json(path)
    block = (doc or {}).get(BLOCK) or {}
    start = parse_time((block.get('window') or {}).get('start'))
    zones = block.get('zones') if isinstance(block.get('zones'), dict) else {}
    series = {}
    if start is not None:
        for zone, entry in zones.items():
            if isinstance(entry, dict):
                kept = {name: unpacked for name in NAMES if (unpacked := _unpack(entry.get(name), start))}
                if kept:
                    series[zone] = kept
    return series, {zone: entry for zone, entry in zones.items() if isinstance(entry, dict)}


def save(obtained, now, order=None, path=PATH):
    """Put this run's series ({zone: series_of()}) into the file, over what it
    had for the same intervals, and take out what has left the window. Returns
    how many zones the file has; with nothing obtained it is left as it was."""
    if not obtained:
        return 0
    start, end = window_of(now)
    before, entries = read(path)
    stamp = iso(now)
    zones = {}
    for zone in list(dict.fromkeys(list(order or []) + list(before) + list(obtained))):
        merged = {}
        for name in NAMES:
            old, new = (before.get(zone) or {}).get(name), (obtained.get(zone) or {}).get(name)
            if new is None and old is None:
                continue
            step = (new or old)[0]
            values = dict(old[1]) if old and old[0] == step else {}       # a change of resolution starts again
            values.update((new or (None, {}))[1])
            values = {at: value for at, value in values.items() if start <= at < end}
            if values:
                merged[name] = (step, values)
        if not merged:
            continue
        entry = {name: _pack(step, values, start, end) for name, (step, values) in merged.items()}
        entry[DATA_TIME] = iso(max(at for _step, values in merged.values() for at in values))
        if zone in obtained:
            entry[FETCHED_AT] = stamp
        else:
            entry[FETCHED_AT] = (entries.get(zone) or {}).get(FETCHED_AT)
            entry[CARRIED_OVER] = True
        zones[zone] = entry
    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry[DATA_TIME] for entry in zones.values()),
        BLOCK: {'license': SOURCE_TERMS, 'source': SOURCE, 'window': {'start': iso(start), 'end': iso(end)},
                'zones': zones},
    }
    write_json(path, doc, separators=(',', ':'))
    return len(zones)
