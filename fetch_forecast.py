#!/usr/bin/env python3
"""fetch_forecast.py - the forecast error of wind and solar, by country.

Writes data/wind-solar-forecast.json: for each country and each hour of the
delivery day in course and of the day before, the day-ahead forecast and the
actual generation of solar, wind onshore and wind offshore, and the difference
between the two. And, for the mean error of the last 7 and 30 days, one record
a day in data/archive/wind-solar-forecast-YYYY-MM-DD.json.

Where the two halves come from:

  - The forecast is ENTSO-E item 14.1.D (documentType A69, processType A01,
    the day-ahead forecast), asked for here: one request for each zone, for
    the two days. It is published the day before, by 18:00 Brussels time, and
    is not revised afterwards.
  - The actual generation is item 16.1.B&C (A75), which fetch_entsoe.py
    already asks for in every run. No request is made for it here: the day's
    series of the three technologies are kept in data/wind-solar-actual.json
    (see wind_solar_actual.py) and read from there. Each run brings the last
    24 hours, so the day in course is whole as it goes and the day before is
    whole from the second day of this file on.

Both come in intervals of 15, 30 or 60 minutes, not always the same for the
forecast and for the actual (Belgium forecasts solar by the quarter-hour and
measures it by the hour). They are compared by the hour: an hour's value is
the mean of its intervals, in MW, and an hour with an interval missing has no
value. `error` is actual minus forecast: positive when there was more than
was forecast. `summary` has, for each technology and over the hours that have
both: how many they are, the energy forecast and generated in them, the mean
error (bias) and the mean absolute error.

A zone is asked for the same area its generation is asked for, so that the
two are of the same territory: Italy as the country, Norway, Sweden and
Denmark by the two zones of each that the feed follows, which are then added
up, hour by hour, as in generation-mix.json (and a country is not put
together from part of its zones). A technology is there when the forecast has
it: Spain forecasts no offshore wind, the Czech Republic and Slovenia only
solar. Moldova and Kosovo publish no forecast: they are listed under
`not_published` and asked again once a day.

Portugal and Spain are asked for in every run; the other zones once an hour.
The actuals and the error of every zone are worked out again in every run,
with the forecast the file already has. The step has its own time budget, runs
after the files the site already reads have been published, and never fails
the run. A zone that was asked for and did not answer keeps what was
published, with the time its forecast was obtained and the mark carried_over,
while it has a day of the two; a run that obtains nothing from ENTSO-E still
brings the actuals up to date.

The licence is the source's terms: neither 14.1.D nor 16.1.B&C is on ENTSO-E's
list of data free to reuse.

    {updated, data_time, timezone,
     wind_solar: {license, source, zones: {PT: {eic, days: {"2026-11-18":
         {start, hours, solar: {forecast: [...], actual: [...], error: [...]},
          wind_onshore: {...}, wind_offshore: {...},
          summary: {solar: {hours, forecast_mwh, actual_mwh, mean_error_mw,
                            mean_abs_error_mw}, ...}}},
         forecast_zones: {solar: [zones], ...}, data_time, actual_updated,
         fetched_at}}, not_published: {zone: time}}}

`start` is the first instant of the delivery day (midnight on the market's
clock, in UTC) and the lists have one value for each hour from there on: 24,
or 23 and 25 on the days the clocks change. `forecast_zones` says which of
the zones asked for a technology's numbers are of (for Norway, Sweden and
Denmark, the zones that forecast it). `updated` is the time of the run,
`fetched_at` the time a zone's forecast was obtained, `actual_updated` the
time its actuals were, and `data_time` the start of the last hour that has an
actual value.

The last 30 days, one file for each zone (for the "Radar" page, decided on
20 Nov 2026, which asks only for the file of the country shown):

    data/forecast/ES.json:
    {zone, timezone, data_time,
     wind_solar: {license, source, error, eic, forecast_zones,
         days: {"2026-10-21": {start, hours, solar: {forecast, actual,
             error}, ..., summary: {...}}, ... "2026-11-20": {...}}}}

    data/forecast/index.json:
    {updated, data_time, timezone, days: {from, to}, zones: [zones with a file],
     wind_solar: {license, source, zones: {ES: {days, first, last, data_time,
         fetched_at, actual_updated[, carried_over]}}, not_published}}

`days` has the day in course and the 30 before it, each as in the file above,
oldest first. They come from the records of the archive, which already keep
each day as it was when it closed: every run writes there the day in course
and the day before (with the actuals of the whole day by then), and leaves the
older ones as they are. A run still asks ENTSO-E for those two days only. The
oldest day leaves the window by the date alone, so a run that obtains nothing
takes no day out; a day the archive has no record of for a zone is not in its
file. A zone's file has no time of the run in it (those are in the index) and
is only written when what it says changes.

The 30 days before the files existed were filled in once, from ENTSO-E itself:
`python fetch_forecast.py --fill-days`, started by hand through
rewrite-archive.yml (where the key is). For each zone it makes one request for
the forecast and one for the actual generation of the whole period, one
request at a time and no more than 100 a minute (as backfill_archive.py), and
writes the records of the archive of the days before yesterday, each zone's
day worked out as a run works out the day before.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone

import carry_over
import fetch_entsoe as fe
import wind_solar_actual
from feed_common import (CARRIED_OVER, DAILY, DATA_TIME, FETCHED_AT, HOURLY, SOURCE_TERMS, Budget, asked_recently,
                         carry, due, entsoe_answer, entsoe_ask, http_get, iso, latest, load_json, parse_time,
                         write_json, write_json_if_changed)

OUT_PATH = 'data/wind-solar-forecast.json'
ARCHIVE_PATH = 'data/archive/wind-solar-forecast-{day}.json'
OUT_DIR = 'data/forecast'
ZONE_PATH = OUT_DIR + '/{zone}.json'
INDEX_PATH = OUT_DIR + '/index.json'
WINDOW_DAYS = 30                 # the days before the one in course that the files of the zones have
BUDGET_S = float(os.environ.get('FORECAST_BUDGET_SECONDS', '45'))

BLOCK = 'wind_solar'
ALWAYS = ('PT', 'ES')
NAMES = wind_solar_actual.NAMES
HOUR = timedelta(hours=1)
KEEP_DAYS = 1830
STAMP = '%Y%m%d%H%M'
SOURCE = ('ENTSO-E Transparency Platform, items 14.1.D (day-ahead forecast of wind and solar generation) and '
          '16.1.B&C (actual generation per production type)')

# The zones asked for, as the generation is: {published name: [zone codes]}.
_ASKED = [code for code in fe.GEN_COUNTRIES if code in fe.ZONES and code not in fe.ELEXON_ZONES]
PARTS = {**{code: [code] for code in _ASKED}, **{country: list(zones) for country, zones in fe.CONSOLIDATE.items()}}
ORDER = list(ALWAYS) + [name for name in PARTS if name not in ALWAYS]


def _mw(value):
    return None if value is None else round(value, 1)


def hourly(series, start, hours):
    """The mean of each hour from `start`, for a series (step, {time: MW}): a
    list of `hours` values, None for an hour that does not have all of its
    intervals."""
    step, values = series
    if step > HOUR or HOUR % step:
        return [None] * hours
    each = int(HOUR / step)
    means = []
    for hour in range(hours):
        found = [values.get(start + HOUR * hour + step * i) for i in range(each)]
        means.append(None if any(v is None for v in found) else sum(found) / each)
    return means


def read_forecast(xml, end):
    """The forecast series of an A69 answer: {name: (step, {time: MW})}."""
    text = xml.decode('utf-8', errors='replace') if isinstance(xml, bytes) else xml
    return wind_solar_actual.series_of(fe.read_series(text), end, fe._every_interval)


def add_up(lists):
    """Hour by hour, the sum of several lists; None where any of them has none."""
    return [None if any(v is None for v in values) else sum(values) for values in zip(*lists)]


def summary(forecast, actual):
    """What the hours that have both a forecast and an actual say."""
    pairs = [(f, a) for f, a in zip(forecast, actual) if f is not None and a is not None]
    if not pairs:
        return {'hours': 0}
    errors = [a - f for f, a in pairs]
    return {
        'hours': len(pairs),
        'forecast_mwh': round(sum(f for f, _a in pairs), 1),
        'actual_mwh': round(sum(a for _f, a in pairs), 1),
        'mean_error_mw': round(sum(errors) / len(errors), 1),
        'mean_abs_error_mw': round(sum(abs(e) for e in errors) / len(errors), 1),
    }


def day_block(day, forecasts, actuals):
    """A day of a zone: {start, hours, technology: {forecast, actual, error},
    summary}, from its hourly forecasts and actuals ({name: [...]}). None when
    the day has no forecast."""
    start, end = carry_over.day_bounds(day)
    hours = int((end - start) / HOUR)
    block = {'start': iso(start), 'hours': hours}
    totals = {}
    for name in NAMES:
        forecast = forecasts.get(name)
        if not forecast or len(forecast) != hours or all(v is None for v in forecast):
            continue
        # The error and the summary are worked out from the values as published (one decimal), so that
        # they come out the same when a later run starts from the forecast the file has.
        forecast = [_mw(v) for v in forecast]
        actual = [_mw(v) for v in (actuals.get(name) or [None] * hours)]
        block[name] = {
            'forecast': forecast,
            'actual': actual,
            'error': [_mw(a - f) if f is not None and a is not None else None for f, a in zip(forecast, actual)],
        }
        totals[name] = summary(forecast, actual)
    if not totals:
        return None
    block['summary'] = totals
    return block


def actual_hours(name, technology, day, kept, forecast_zones):
    """The hourly actuals of a technology for a published name: the sum of
    those of its zones that forecast it."""
    start, end = carry_over.day_bounds(day)
    hours = int((end - start) / HOUR)
    lists = []
    for zone in forecast_zones:
        series = (kept.get(zone) or {}).get(technology)
        lists.append(hourly(series, start, hours) if series else [None] * hours)
    return add_up(lists) if lists else [None] * hours


def _days(now):
    today = carry_over.delivery_day(now)
    return [today, today - timedelta(days=1)]


def stored_forecast(entry, day):
    """{technology: ([hourly forecast], [zones that forecast it])} of a day of a published entry."""
    block = ((entry or {}).get('days') or {}).get(day.isoformat()) or {}
    zones = (entry or {}).get('forecast_zones') or {}
    return {name: (block[name]['forecast'], zones.get(name) or [])
            for name in NAMES if isinstance(block.get(name), dict) and isinstance(block[name].get('forecast'), list)}


def build(name, forecasts, kept, stamps, now):
    """A zone's entry from its hourly forecasts ({day: {technology: ([values],
    [zones])}}) and the actuals kept (`kept`, by zone). None when no day has a
    forecast."""
    days, zones_of, last = {}, {}, None
    for day in _days(now):
        given = forecasts.get(day) or {}
        actuals = {technology: actual_hours(name, technology, day, kept, zones)
                   for technology, (_values, zones) in given.items()}
        block = day_block(day, {technology: values for technology, (values, _zones) in given.items()}, actuals)
        if block is None:
            continue
        days[day.isoformat()] = block
        start = parse_time(block['start'])
        for technology, (_values, zones) in given.items():
            zones_of[technology] = zones
            measured = [hour for hour, value in enumerate(actuals[technology]) if value is not None]
            if measured:
                at = start + HOUR * measured[-1]
                last = at if last is None else max(last, at)
    if not days:
        return None
    entry = {'eic': ', '.join(fe.GEN_ZONES[zone] for zone in PARTS[name]), 'days': days, 'forecast_zones': zones_of}
    if last is not None:
        entry[DATA_TIME] = iso(last)
    updated = [stamps[zone] for zone in PARTS[name] if stamps.get(zone)]
    if updated:
        entry['actual_updated'] = min(updated, key=parse_time)       # as old as the oldest of its zones
    return entry


def forecasts_of(name, read, day):
    """{technology: ([hourly forecast of the day], [zones])} of a published
    name, from the forecast series of each of its zones ({zone: {technology:
    (step, {time: MW})}}): the sum of the zones that forecast the technology."""
    start, end = carry_over.day_bounds(day)
    hours = int((end - start) / HOUR)
    found = {}
    for technology in NAMES:
        zones = [zone for zone in PARTS[name] if technology in read[zone]]
        if zones:
            found[technology] = (add_up([hourly(read[zone][technology], start, hours) for zone in zones]), zones)
    return found


def plan(previous, now):
    """The requests of this run, Portugal and Spain first: [((name, zone), log name, params)]."""
    today = carry_over.delivery_day(now)
    window = {'periodStart': carry_over.day_bounds(today - timedelta(days=1))[0].strftime(STAMP),
              'periodEnd': carry_over.day_bounds(today)[1].strftime(STAMP)}
    zones = _zones(previous)
    silent = _memory(previous)
    jobs = []
    for name in ORDER:
        if name not in ALWAYS and not due(zones.get(name), now):
            continue
        if name not in zones and asked_recently(silent.get(name), now, HOURLY if name in ALWAYS else DAILY):
            continue
        for zone in PARTS[name]:
            jobs.append(((name, zone), f'{zone} forecast',
                         dict(documentType='A69', processType='A01', in_Domain=fe.GEN_ZONES[zone], **window)))
    return jobs


def _zones(doc):
    zones = ((doc or {}).get(BLOCK) or {}).get('zones')
    return {name: entry for name, entry in zones.items() if isinstance(entry, dict)} if isinstance(zones, dict) else {}


def _memory(doc):
    memory = ((doc or {}).get(BLOCK) or {}).get('not_published')
    return dict(memory) if isinstance(memory, dict) else {}


def main(now=None, get=http_get, budget=None, interval=0.25):
    now = now or datetime.now(timezone.utc)
    stamp = iso(now)
    days = _days(now)
    window_end = carry_over.day_bounds(days[0])[1]
    previous = load_json(OUT_PATH)
    kept, kept_entries = wind_solar_actual.read()
    stamps = {zone: entry.get(FETCHED_AT) for zone, entry in kept_entries.items()}

    jobs = plan(previous, now)
    print(f'Wind and solar forecast: {len(jobs)} requests for delivery days {days[1]} and {days[0]}; '
          f'actual generation kept for {len(kept)} zones')
    answers = {}
    if jobs:
        answers = {key: entsoe_answer(response)
                   for key, response in entsoe_ask(jobs, budget or Budget(BUDGET_S), get=get, interval=interval,
                                                   label='Forecast').items()}

    # What the file had, with the days that left the window taken out and the actuals brought up to date.
    before = {}
    for name, entry in _zones(previous).items():
        if name not in PARTS:
            continue
        forecasts = {day: stored_forecast(entry, day) for day in days}
        rebuilt = build(name, forecasts, kept, stamps, now)
        if rebuilt:
            before[name] = {**{k: entry[k] for k in (FETCHED_AT, 'carried_over') if k in entry}, **rebuilt}

    silent = _memory(previous)
    obtained, failed = {}, set()
    for name in ORDER:
        given = {zone: answers.get((name, zone)) for zone in PARTS[name]}
        if not any(given.values()):
            continue                                                   # not due, or no time for it
        if any(answer is None for answer in given.values()):
            continue                                                   # only part of it was asked: left as it was
        if any(answer.problem is not None for answer in given.values()):
            failed.add(name)
            continue
        read = {zone: read_forecast(answer.documents[0], window_end) if answer.documents else {}
                for zone, answer in given.items()}
        if not any(read.values()):
            silent[name] = stamp                                       # ENTSO-E has no forecast for it
            continue
        if not all(read.values()):
            missing = [zone for zone, series in read.items() if not series]
            print(f"  {name}: no forecast for {'+'.join(missing)} - not put together from part of the country")
            failed.add(name)
            continue
        silent.pop(name, None)
        forecasts = {day: forecasts_of(name, read, day) for day in days}
        entry = build(name, forecasts, kept, stamps, now)
        if entry is None:
            failed.add(name)
            continue
        obtained[name] = entry
        shown = ', '.join(f"{technology} {entry['days'][days[0].isoformat()]['summary'][technology]['hours']} h"
                          for technology in NAMES
                          if technology in (entry['days'].get(days[0].isoformat()) or {}).get('summary', {}))
        print(f"  {name}: forecast for {', '.join(sorted(entry['days']))}; hours of today with both: {shown or 'none'}")

    zones, held, _dropped = carry(before, obtained, failed, stamp, lambda name, entry: True)
    if held:
        print('  Kept, not obtained in this run: ' + ', '.join(held))
    if not zones:
        print(f'Nothing to publish: {OUT_PATH} is left exactly as it was')
        return 1 if jobs else 0

    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry.get(DATA_TIME) for entry in zones.values()),
        'timezone': 'Europe/Brussels',
        BLOCK: {
            'license': SOURCE_TERMS,
            'source': SOURCE,
            'error': 'actual minus forecast, in MW; hourly means',
            'zones': {name: zones[name] for name in PARTS if name in zones},
            'not_published': {name: silent[name] for name in PARTS if name in silent},
        },
    }
    write_json(OUT_PATH, doc, separators=(',', ':'))
    print(f'Wrote {OUT_PATH}: {len(zones)} zones ({len(obtained)} forecasts from this run)')
    archive(doc, now)
    written, names, window = publish(doc, now)
    spain = ((load_json(ZONE_PATH.format(zone='ES')) or {}).get(BLOCK) or {}).get('days') or {}
    print(f'Wrote {INDEX_PATH} and {written} of the {len(names)} files of the zones (the others had not changed), '
          f'for the days {window[0]} to {window[-1]}; Spain has {len(spain)} of them')
    return 0 if (obtained or not jobs or not failed) else 1


# --- The last 30 days, a file for each zone ---

def window_of(now):
    """The days the files of the zones have: the 30 before the one in course and that one, oldest first."""
    today = carry_over.delivery_day(now)
    return [today - timedelta(days=back) for back in range(WINDOW_DAYS, -1, -1)]


def last_actual(days):
    """The start of the last hour that has an actual value, in the days of a zone's file; None with none."""
    last = None
    for block in days.values():
        start = parse_time(block.get('start'))
        for name in NAMES:
            actual = (block.get(name) or {}).get('actual') or []
            measured = [hour for hour, value in enumerate(actual) if value is not None]
            if start is not None and measured:
                at = start + HOUR * measured[-1]
                last = at if last is None else max(last, at)
    return iso(last) if last else None


def publish(doc, now):
    """Write the files of the zones that changed, from the records of the
    archive of the days of the window, and the index. Returns (how many files
    of zones were written, the zones that have one, the days)."""
    window = window_of(now)
    records = {}
    for day in window:
        zones = (((load_json(ARCHIVE_PATH.format(day=day.isoformat())) or {}).get(BLOCK) or {}).get('zones'))
        records[day] = zones if isinstance(zones, dict) else {}
    entries = doc[BLOCK]['zones']
    names = [name for name in PARTS if any(isinstance(records[day].get(name), dict) for day in window)]
    written, listed = 0, {}
    for name in names:
        days = {day.isoformat(): records[day][name] for day in window if isinstance(records[day].get(name), dict)}
        entry = entries.get(name) or {}
        data_time = last_actual(days)
        zone_file = {
            'zone': name,
            'timezone': doc['timezone'],
            DATA_TIME: data_time,
            BLOCK: {
                'license': doc[BLOCK]['license'],
                'source': doc[BLOCK]['source'],
                'error': doc[BLOCK]['error'],
                'eic': ', '.join(fe.GEN_ZONES[zone] for zone in PARTS[name]),
                **({'forecast_zones': entry['forecast_zones']} if entry.get('forecast_zones') else {}),
                'days': days,
            },
        }
        written += write_json_if_changed(ZONE_PATH.format(zone=name), zone_file, separators=(',', ':'))
        listed[name] = {'days': len(days), 'first': min(days), 'last': max(days), DATA_TIME: data_time,
                        **{key: entry[key] for key in (FETCHED_AT, 'actual_updated', CARRIED_OVER) if key in entry}}
    write_json(INDEX_PATH, {
        'updated': doc['updated'],
        DATA_TIME: latest(entry[DATA_TIME] for entry in listed.values()),
        'timezone': doc['timezone'],
        'days': {'from': window[0].isoformat(), 'to': window[-1].isoformat()},
        'zones': names,
        BLOCK: {'license': doc[BLOCK]['license'], 'source': doc[BLOCK]['source'], 'zones': listed,
                'not_published': doc[BLOCK]['not_published']},
    }, separators=(',', ':'))
    for file_name in os.listdir(OUT_DIR):
        if (file_name.endswith('.json') and file_name != os.path.basename(INDEX_PATH)
                and file_name[:-len('.json')] not in names):
            os.remove(os.path.join(OUT_DIR, file_name))
    return written, names, window


# --- The archive: one record for each delivery day ---

def write_record(day, blocks, licence=SOURCE_TERMS, source=SOURCE):
    """Put the day blocks of some zones ({name: block}) into the record of a
    day, over what it had for them; the other zones of the record stay as
    they are. Written only when it changes. Returns whether it was."""
    path = ARCHIVE_PATH.format(day=day.isoformat())
    before = load_json(path)
    zones = dict((((before or {}).get(BLOCK) or {}).get('zones')) or {})
    zones.update(blocks)
    record = {'date': day.isoformat(),
              BLOCK: {'license': licence, 'source': source, 'zones': {name: zones[name] for name in sorted(zones)}}}
    if not zones or record == before:
        return False
    write_json(path, record, separators=(',', ':'))
    return True


def archive(doc, now):
    """Write the records of the delivery day in course and of the one before
    (each zone's day as the file has it; a zone the file no longer has keeps
    the record it had), and take out those older than five years. A record has
    no time of its own: it only changes when its numbers do."""
    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    for day in _days(now):
        write_record(day, {name: entry['days'][day.isoformat()] for name, entry in doc[BLOCK]['zones'].items()
                           if (entry.get('days') or {}).get(day.isoformat())},
                     doc[BLOCK]['license'], doc[BLOCK]['source'])
    cutoff = (carry_over.delivery_day(now) - timedelta(days=KEEP_DAYS)).isoformat()
    folder, prefix = os.path.dirname(ARCHIVE_PATH), 'wind-solar-forecast-'
    for name in os.listdir(folder):
        if name.startswith(prefix) and name.endswith('.json') and name[len(prefix):-len('.json')] < cutoff:
            os.remove(os.path.join(folder, name))


# --- Filling in the 30 days before the files existed, once ---

def _by_day(series, first, last):
    """{day: {technology: (step, {time: MW})}} of series ({technology: (step, {time: MW})}), for the delivery
    days `first` to `last`."""
    found = {}
    day = first
    while day <= last:
        start, end = carry_over.day_bounds(day)
        of_day = {name: (step, {at: value for at, value in values.items() if start <= at < end})
                  for name, (step, values) in series.items()}
        of_day = {name: kept for name, kept in of_day.items() if kept[1]}
        if of_day:
            found[day] = of_day
        day += timedelta(days=1)
    return found


def fill_days(base, now=None, ask=None, sleep=time.sleep, commit=None, previous=None):
    """Write the records of the archive of the 30 days before the day in
    course, but for the day before (which the runs write), from ENTSO-E: for
    each zone, one request for the forecast and one for the actual generation
    of the whole period. Returns the exit code: 0, or 1 when a zone could not
    be had (the others are written) or the records could not be published."""
    import backfill_archive
    backfill_archive._gently()
    ask = ask or backfill_archive._ask
    now = now or datetime.now(timezone.utc)
    today = carry_over.delivery_day(now)
    first, last = today - timedelta(days=WINDOW_DAYS), today - timedelta(days=2)
    end = carry_over.day_bounds(last)[1]
    silent = _memory(load_json(OUT_PATH) if previous is None else previous)
    print(f'Filling in the forecast error of {first} to {last}, from ENTSO-E')

    def forecast(text, first, last):
        return _by_day(read_forecast(text, end), first, last)

    def actual(text, first, last):
        return _by_day(wind_solar_actual.series_of(fe.read_series(text), end, fe._every_interval), first, last)

    blocks, failed = {}, []
    for name in ORDER:
        if name in silent:
            print(f'  {name}: publishes no forecast - not asked for')
            continue
        started = time.monotonic()
        try:
            forecasts, actuals = {}, {}
            for zone in PARTS[name]:
                area = {'in_Domain': fe.GEN_ZONES[zone]}
                forecasts[zone] = backfill_archive.ask_span(base, {'documentType': 'A69', 'processType': 'A01', **area},
                                                            f'{zone} forecast', first, last, forecast, True, ask, sleep)
                actuals[zone] = backfill_archive.ask_span(base, {'documentType': 'A75', 'processType': 'A16', **area},
                                                          f'{zone} actual', first, last, actual, True, ask, sleep)
        except backfill_archive.RewriteFailed as e:
            print(f'  {name}: NOT filled in ({e})')
            failed.append(name)
            continue
        day, count = first, 0
        while day <= last:
            read = {zone: forecasts[zone].get(day) or {} for zone in PARTS[name]}
            if all(read.values()):                        # never a country put together from part of its zones
                given = forecasts_of(name, read, day)
                kept = {zone: actuals[zone].get(day) or {} for zone in PARTS[name]}
                block = day_block(day, {technology: values for technology, (values, _zones) in given.items()},
                                  {technology: actual_hours(name, technology, day, kept, zones)
                                   for technology, (_values, zones) in given.items()})
                if block:
                    blocks.setdefault(day, {})[name] = block
                    count += 1
            day += timedelta(days=1)
        print(f'  {name}: {count} days [{time.monotonic() - started:.0f}s]')

    written = sum(write_record(day, zones) for day, zones in sorted(blocks.items()))
    print(f'{written} records of the archive written; zones not filled in: {failed or "none"}')
    if commit and written and commit(f'Fill in the forecast error of wind and solar of {first} to {last}') is False:
        print('The records were written here but NOT published: start the run again')
        return 1
    return 1 if failed else 0


if __name__ == '__main__':
    if '--fill-days' in sys.argv:
        import backfill_archive
        if not fe.API_KEY:
            sys.exit('ERROR: ENTSOE_API_KEY environment variable is not set')
        backfill_archive._run_git(['config', 'user.name', 'github-actions[bot]'])
        backfill_archive._run_git(['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'])
        sys.exit(fill_days(fe.ALT_URLS[0], commit=backfill_archive._commit_and_push))
    sys.exit(main())
