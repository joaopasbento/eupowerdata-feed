#!/usr/bin/env python3
"""fetch_records.py - data/records.json: the renewable and price records of each zone, for the site's Insights.

The site wrote its records by hand (46 lines, the last of 2 May 2026). The feed
now works them out once a day from the data it already gathers; it gives data
only, and the site writes the sentences in its five languages, by the stable
names of the types:

    by zone (ENTSO-E, licence source-terms: free plan, with attribution, never the Pro plan)
      solar_mw_max                    the highest solar generation in an interval            MW
      wind_mw_max                     the highest wind generation (onshore + offshore)       MW
      renewable_share_interval_max    the highest renewable share of generation, interval    %
      renewable_share_day_max         the highest renewable share of a day's generation      %
      price_interval_min              the lowest day-ahead price of an hour                  EUR/MWh
      price_day_avg_min               the lowest mean day-ahead price of a day               EUR/MWh
      negative_zero_hours_day_max     the most hours with a price below or at zero in a day  h
    by country (Ember, Monthly Electricity Data, cc-by-4.0, attributed "Ember")
      solar_share_month_max           the month with the highest share of solar              %
      wind_share_month_max            the month with the highest share of wind               %
      renewable_share_month_max       the month with the highest share of renewables         %

    {updated, data_until, attribution, types: {TYPE: {unit, scope, best, source, license}},
     zones: {ZONE: {TYPE: {value, unit, at, source[, step_minutes][, basis],
                           previous: {value, at} | null}}},
     countries: {COUNTRY: {TYPE: {...}}},
     recent: [{area, scope: zone | country, type, value, unit, at, previous: {value, at} | null}]}

`at` is the start of the interval in UTC, the delivery day (YYYY-MM-DD, on the
market's clock, Europe/Brussels) or the month (YYYY-MM). `previous` is the
record the current one beat. `recent` lists the records beaten in the last 12
months, the most recent first; a record only counts as beaten once the zone or
country has a year of data of that type before it (so that the first months of
a zone are not a string of records).

Where the numbers come from:
  - generation: ENTSO-E's actual generation per production type (A75), interval
    by interval, as fetch_entsoe.py reads it (the same zones, Norway, Sweden and
    Denmark as the sum of the zones the feed asks for, NO1+NO2, SE1+SE3,
    DK1+DK2; renewable as RENEWABLE_TYPES there), since 1 Jan 2021: a one-off
    fill (fill-records.yml, by hand: one request per zone and year) and then,
    each day, the last DAILY_DAYS days. A day's numbers are kept in
    data/records/days/<ZONE>/<YEAR>.json;
  - prices: the feed's archive of the hourly prices (data/archive/spot-prices-*,
    since 20 Nov 2021) and, for the hours below or at zero, the count of the low
    prices (data/archive/low-prices-*, at the market's resolution, since
    1 Jan 2026; before, counted the same way, fetch_low_prices.zone_count, from
    the hourly means);
  - Ember's Monthly Electricity Data, since Jan 2015, kept in
    data/records/ember-monthly.json and downloaded again only when it changes.

Guards: a value above the zone's installed capacity (data/capacity-installed.json,
with a margin of CAPACITY_MARGIN), or a peak more than SPIKE_RATIO times both
intervals around it (SPIKE_PP points for a share), does not count; an interval
where a technology the day has is missing does not count; a day with an
interval missing does not count for the records of the day; a day the archive
leaves out (Moldova before 1 Jul 2026, archive_fixes.WRONG_AT_THE_SOURCE) stays
out; the days of 23 and 25 hours are counted as they are.

Runs:
    python fetch_records.py                   the daily update (records.yml)
    python fetch_records.py --fill [ZONE ...] the one-off fill of the generation since 2021 (fill-records.yml),
                                              a zone at a time, each committed and pushed; ends in error if it
                                              published nothing or a push failed
    python fetch_records.py --sample DIR      saves real answers of ENTSO-E for the tests (fill-records.yml)
"""

import csv
import gzip
import io
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import archive_fixes
import carry_over
import fetch_ember
import fetch_entsoe as fe
import fetch_low_prices as flp
from feed_common import (CC_BY, SOURCE_TERMS, Budget, entsoe_answer, entsoe_ask, http_get, iso, load_json,
                         write_json_if_changed)

OUT_PATH = 'data/records.json'
DAYS_DIR = 'data/records/days'
EMBER_PATH = 'data/records/ember-monthly.json'
EMBER_URL = 'https://files.ember-energy.org/public-downloads/generation/outputs/release_generation_monthly_global.csv'
EMBER_SOURCE = 'Ember, Monthly Electricity Data'
ENTSOE_SOURCE = 'ENTSO-E Transparency Platform'
GEN_FROM = date(2021, 1, 1)
DAILY_DAYS = 4                       # the days asked for again each day: data come in late
SPIKE_RATIO = 1.25
SPIKE_PP = 15.0
CAPACITY_MARGIN = 1.05
STORAGE = {'B10', 'B25'}             # pumped storage and other storage: they report only when they generate
SMALL_SHARE = 0.01                   # a technology under 1 % of the day's total is 0 where it has no value
MIN_SHARE_OF_TOTAL = 0.5             # an interval whose total is under half the day's median is not a share record
A_YEAR = timedelta(days=365)

SUBZONES = fe.CONSOLIDATE                                    # NO, SE, DK: the sum of their zones
# The site's zones of generation: fetch_entsoe's countries asked of ENTSO-E (not GB, which is Elexon's), and
# Norway, Sweden and Denmark, which it makes from their zones.
GEN_ZONES = [code for code in fe.GEN_COUNTRIES if fe.ZONES.get(code) and code not in fe.ELEXON_ZONES] + list(SUBZONES)
RENEWABLE = {code for code, name in fe.PSR_MAP.items() if name in fe.RENEWABLE_TYPES}

TYPES = {
    'solar_mw_max': ('MW', 'interval', 'max', 'generation'),
    'wind_mw_max': ('MW', 'interval', 'max', 'generation'),
    'renewable_share_interval_max': ('%', 'interval', 'max', 'generation'),
    'renewable_share_day_max': ('%', 'day', 'max', 'generation'),
    'price_interval_min': ('EUR/MWh', 'interval', 'min', 'prices'),
    'price_day_avg_min': ('EUR/MWh', 'day', 'min', 'prices'),
    'negative_zero_hours_day_max': ('h', 'day', 'max', 'prices'),
}
COUNTRY_TYPES = {
    'solar_share_month_max': ('%', 'month', 'max', 0),
    'wind_share_month_max': ('%', 'month', 'max', 1),
    'renewable_share_month_max': ('%', 'month', 'max', 2),
}
# Where each record of a zone's day is kept: (part, key).
DAY_KEYS = {'solar_mw_max': ('gen', 'solar'), 'wind_mw_max': ('gen', 'wind'),
            'renewable_share_interval_max': ('gen', 'ren'), 'renewable_share_day_max': ('gen', 'ren_day'),
            'price_interval_min': ('price', 'min'), 'price_day_avg_min': ('price', 'avg'),
            'negative_zero_hours_day_max': ('price', 'neg_zero_h')}


# --- Days ---

def days_between(first, last):
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def left_out(zone, day):
    """A day the archive leaves out for a zone."""
    if zone == 'MD' and day < archive_fixes.MOLDOVA_FROM:
        return True
    return archive_fixes.WRONG_AT_THE_SOURCE.get(day) == zone


# --- Generation: from ENTSO-E's A75 to a day's numbers ---

def technologies(answers):
    """{psr: (step, {start: MW})} of the generation in the answers (bytes) of one zone; consumption left out, a
    technology in several pieces kept by the value of each interval."""
    found = {}
    for body in answers:
        for series in fe.read_series(body):
            if series['out'] or not series['psr']:
                continue
            for period in series['periods']:
                step, values = found.setdefault(series['psr'], (period['step'], {}))
                if period['step'] < step:                  # a finer resolution from then on
                    found[series['psr']] = (period['step'], dict(values))
                    step, values = found[series['psr']]
                for start, value in fe._every_interval(period):
                    values[start] = value
    return found


def add_zones(parts):
    """The technologies of a country from those of its zones: interval by interval, where every zone has one."""
    if any(part is None for part in parts):
        return None
    total = {}
    for psr in set().union(*parts):
        steps = [part[psr][0] for part in parts if psr in part]
        step = min(steps)
        values = defaultdict(float)
        for part in parts:
            if psr not in part:
                continue
            own_step, own = part[psr]
            for start, value in own.items():
                for k in range(int(own_step / step)):
                    values[start + step * k] += value
        total[psr] = (step, dict(values))
    return total


def _at(values, step, day_start, moment):
    """A technology's value at `moment` on a grid of its own `step` from the day's start."""
    offset = (moment - day_start).total_seconds() % step.total_seconds()
    return values.get(moment - timedelta(seconds=offset))


def _not_a_spike(series, i, ratio=SPIKE_RATIO, points=None):
    """Whether series[i] is not more than `ratio` times (or `points` above) the larger of its neighbours."""
    around = [series[j] for j in (i - 1, i + 1) if 0 <= j < len(series) and series[j] is not None]
    if not around:
        return False
    if points is not None:
        return series[i] - max(around) <= points
    return max(around) > 0 and series[i] <= ratio * max(around)


def day_numbers(techs, day, capacity=None):
    """{solar, wind, ren: [value, at], ren_day, step, complete} of a zone's day, or None when it has no interval."""
    start, end = carry_over.day_bounds(day)
    present = {psr: (step, values) for psr, (step, values) in techs.items()
               if any(start <= t < end for t in values)}
    if not present:
        return None
    step = min(s for s, _v in present.values())
    grid = [start + step * i for i in range(int((end - start) / step))]
    # Every technology is required in each interval, but storage, which reports only when it generates (pumped
    # storage in Belgium), and a small one (under SMALL_SHARE of the total, as Hungary's oil, which leaves out some
    # quarter-hours): those are 0 where they have no value. When a zone's data stop in the middle of the day (Italy
    # on 19 Nov 2026, at 09:45), the rest of the day is missing, not 0.
    seen = {psr: [_at(values, s, start, moment) for moment in grid] for psr, (s, values) in present.items()}
    medians = {psr: sorted(v for v in values if v is not None)[sum(v is not None for v in values) // 2]
               for psr, values in seen.items()}
    whole = sum(medians.values())
    required = {psr for psr in seen if psr not in STORAGE and medians[psr] >= SMALL_SHARE * whole}
    rows = []
    for i in range(len(grid)):
        if any(seen[psr][i] is None for psr in required):
            rows.append(None)
        else:
            rows.append({psr: (values[i] if values[i] is not None else 0.0) for psr, values in seen.items()})
    complete = all(rows)
    totals = [sum(r.values()) if r else None for r in rows]
    median = sorted(t for t in totals if t)[len([t for t in totals if t]) // 2] if any(totals) else 0
    solar = [r.get('B16') if r and 'B16' in r else None for r in rows]
    wind = [(r.get('B18', 0) + r.get('B19', 0)) if r and ('B18' in r or 'B19' in r) else None for r in rows]
    share = [100 * sum(v for psr, v in r.items() if psr in RENEWABLE) / t if r and t and t >= MIN_SHARE_OF_TOTAL * median
             else None for r, t in zip(rows, totals)]

    def best(series, limit=None, points=None):
        found = None
        for i, value in enumerate(series):
            if value is None or (limit is not None and value > limit * CAPACITY_MARGIN):
                continue
            if not _not_a_spike(series, i, points=points):
                continue
            if found is None or value > found[0]:
                found = (value, grid[i])
        return found

    numbers = {'step': int(step.total_seconds() // 60), 'complete': complete}
    for key, series, limit, points, digits in (('solar', solar, (capacity or {}).get('solar_mw'), None, 0),
                                               ('wind', wind, (capacity or {}).get('wind_mw'), None, 0),
                                               ('ren', share, None, SPIKE_PP, 1)):
        found = best(series, limit, points)
        if found:
            numbers[key] = [round(found[0], digits) if digits else int(round(found[0])), iso(found[1])]
    if complete and sum(totals) > 0:
        numbers['ren_day'] = round(100 * sum(sum(v for psr, v in r.items() if psr in RENEWABLE) for r in rows)
                                   / sum(totals), 1)
    return numbers


# --- Prices: from the feed's archives ---

def price_numbers(day, zone_entry, low_count=None):
    """{min: [EUR/MWh, at], avg, neg_zero_h, basis, step} of a zone's day from its entry in the archive of the hourly
    prices (and its count in the archive of the low prices); the day's records only when it has every hour."""
    rows = zone_entry.get('prices') or []
    hours = flp.day_hours(day)
    start = carry_over.day_bounds(day)[0]
    entries = 24 if hours == 25 else hours
    if not rows:
        return None
    weights = [2 if hours == 25 and row.get('hour') == 2 else 1 for row in rows]
    starts, at = [], start
    for weight in weights:
        starts.append(at)
        at += timedelta(hours=weight)
    priced = [(row.get('price'), moment) for row, moment in zip(rows, starts) if isinstance(row.get('price'), (int, float))]
    if not priced:
        return None
    low = min(priced, key=lambda item: item[0])
    numbers = {'min': [low[0], iso(low[1])], 'step': 60}
    if len(rows) == entries and len(priced) == entries:
        numbers['avg'] = round(sum(p * w for (p, _m), w in zip(priced, weights)) / sum(weights), 2)
        count = low_count or flp.zone_count([p for p, _m in priced], 60, 'hourly-means', weights)
        if count and float(count.get('hours') or 0) >= hours - 0.01:
            numbers['neg_zero_h'] = round(float(count['negative_h']) + float(count['zero_h']), 2)
            numbers['basis'] = count.get('basis')
    return numbers


def price_days(days):
    """{zone: {day: numbers}} of `days` from the archives (no request)."""
    found = defaultdict(dict)
    for day in days:
        doc = load_json(flp.PRICE_ARCHIVE.format(day=day.isoformat())) or {}
        low = ((load_json(f'data/archive/low-prices-{day.isoformat()}.json') or {}).get('entsoe') or {}).get('zones') or {}
        for zone, entry in (doc.get('zones') or {}).items():
            if not isinstance(entry, dict) or entry.get('delivery_date', day.isoformat()) != day.isoformat():
                continue
            if left_out(zone, day):
                continue
            count = low.get(zone) if isinstance(low.get(zone), dict) and flp.is_counted(zone, day, low[zone]) else None
            numbers = price_numbers(day, entry, count)
            if numbers:
                found[zone][day.isoformat()] = numbers
    return found


# --- The days files ---

def days_path(zone, year):
    return os.path.join(DAYS_DIR, zone, f'{year}.json')


def load_days(zone, year):
    return ((load_json(days_path(zone, year)) or {}).get('days')) or {}


def store(zone, part, numbers, keep_complete=True):
    """Puts {day: numbers} of a part (gen or price) into the zone's days files; returns the years written. With
    `keep_complete` (the daily update), a day already complete is not replaced by an incomplete one; the fill
    replaces what was there."""
    by_year = defaultdict(dict)
    for name, value in numbers.items():
        by_year[int(name[:4])][name] = value
    written = []
    for year, new in sorted(by_year.items()):
        days = load_days(zone, year)
        for name, value in new.items():
            before = (days.get(name) or {}).get(part)
            if keep_complete and before and before.get('complete') and not value.get('complete', True):
                continue
            days.setdefault(name, {})[part] = value
        if write_json_if_changed(days_path(zone, year), {'zone': zone, 'year': year, 'days': dict(sorted(days.items()))},
                                 separators=(',', ':')):
            written.append(year)
    return written


def all_days(zone):
    folder = os.path.join(DAYS_DIR, zone)
    if not os.path.isdir(folder):
        return {}
    found = {}
    for name in sorted(os.listdir(folder)):
        if name.endswith('.json') and name[:-5].isdigit():
            found.update(load_days(zone, int(name[:-5])))
    return found


# --- Ember, monthly ---

def ember_monthly(get=http_get, path=EMBER_PATH):
    """data/records/ember-monthly.json up to date with Ember's file (downloaded only when it has changed): {country:
    {YYYY-MM: [solar %, wind %, renewables %]}}. Returns the document, or the one kept when Ember does not answer."""
    kept = load_json(path) or {}
    headers = {'If-Modified-Since': kept['edition']} if kept.get('edition') else {}
    response = get(EMBER_URL, timeout=120, headers=headers)
    if response.status == 304 or not response.ok:
        if not response.ok and response.status != 304:
            print(f'  Ember: {response.describe()}; the months kept are used')
        return kept
    iso2 = {v: k for k, v in fetch_ember.ISO3.items()}
    which = {'Solar': 0, 'Wind': 1, 'Renewables': 2}
    countries = defaultdict(lambda: defaultdict(lambda: [None, None, None]))
    for row in csv.DictReader(io.StringIO(response.text())):
        country = iso2.get(row.get('ISO 3 code'))
        if not country or row.get('Electricity source') not in which:
            continue
        try:
            share = round(float(row['Share of generation (%)']), 2)
        except (TypeError, ValueError):
            continue
        countries[country][row['Date'][:7]][which[row['Electricity source']]] = share
    doc = {'source': EMBER_SOURCE, 'license': CC_BY, 'url': EMBER_URL,
           'edition': response.headers.get('last-modified'),
           'countries': {c: dict(sorted(months.items())) for c, months in sorted(countries.items())}}
    write_json_if_changed(path, doc, separators=(',', ':'))
    print(f"  Ember: edition {doc['edition']}, {len(doc['countries'])} countries")
    return doc


# --- The records ---

def progression(candidates, best):
    """[(value, at, extra)] of the records in order, from (day, value, at, extra) in day order: each one beats the
    one before."""
    out = []
    better = (lambda a, b: a > b) if best == 'max' else (lambda a, b: a < b)
    for _day, value, at, extra in candidates:
        if not out or better(value, out[-1][0]):
            out.append((value, at, extra))
    return out


def zone_candidates(days, kind):
    """(day, value, at, extra) of a zone's days for a type."""
    part, key = DAY_KEYS[kind]
    out = []
    for name in sorted(days):
        numbers = (days[name] or {}).get(part) or {}
        value = numbers.get(key)
        if value is None:
            continue
        extra = {'step_minutes': numbers.get('step')}
        if kind == 'negative_zero_hours_day_max' and numbers.get('basis'):
            extra['basis'] = numbers['basis']
        if isinstance(value, list):
            out.append((name, value[0], value[1], extra))
        else:
            out.append((name, value, name, extra))
    return out


def build(now=None, ember=None):
    """data/records.json from the days files and Ember's months; returns the document."""
    now = now or datetime.now(timezone.utc)
    since = (now - A_YEAR).date().isoformat()
    zones, countries, recent = {}, {}, []
    names = sorted(name for name in os.listdir(DAYS_DIR)) if os.path.isdir(DAYS_DIR) else []
    data_until = None
    for zone in names:
        days = all_days(zone)
        if days:
            data_until = max(data_until or '', max(days))
        for kind, (unit, scope, best, part) in TYPES.items():
            candidates = zone_candidates(days, kind)
            steps = progression(candidates, best)
            if not steps:
                continue
            value, at, extra = steps[-1]
            previous = {'value': steps[-2][0], 'at': steps[-2][1]} if len(steps) > 1 else None
            zones.setdefault(zone, {})[kind] = {'value': value, 'unit': unit, 'at': at, 'source': ENTSOE_SOURCE,
                                                **{k: v for k, v in extra.items() if v is not None},
                                                'previous': previous}
            first = candidates[0][0]
            for i, (v, a, _e) in enumerate(steps):
                day = a[:10]
                if i and day >= since and day >= (date.fromisoformat(first) + A_YEAR).isoformat():
                    recent.append({'area': zone, 'scope': 'zone', 'type': kind, 'value': v, 'unit': unit, 'at': a,
                                   'previous': {'value': steps[i - 1][0], 'at': steps[i - 1][1]}})
    for country, months in ((ember or {}).get('countries') or {}).items():
        for kind, (unit, scope, best, index) in COUNTRY_TYPES.items():
            candidates = [(m, values[index], m, {}) for m, values in sorted(months.items()) if values[index] is not None]
            steps = progression(candidates, best)
            if not steps:
                continue
            value, at, _ = steps[-1]
            countries.setdefault(country, {})[kind] = {
                'value': value, 'unit': unit, 'at': at, 'source': EMBER_SOURCE,
                'previous': {'value': steps[-2][0], 'at': steps[-2][1]} if len(steps) > 1 else None}
            for i, (v, a, _e) in enumerate(steps):
                if i and a >= since[:7] and a >= (date.fromisoformat(candidates[0][0] + '-01') + A_YEAR).isoformat()[:7]:
                    recent.append({'area': country, 'scope': 'country', 'type': kind, 'value': v, 'unit': unit,
                                   'at': a, 'previous': {'value': steps[i - 1][0], 'at': steps[i - 1][1]}})
    recent.sort(key=lambda r: (r['at'] if len(r['at']) > 7 else r['at'] + '-99'), reverse=True)
    types = {kind: {'unit': unit, 'scope': scope, 'best': best, 'source': ENTSOE_SOURCE, 'license': SOURCE_TERMS}
             for kind, (unit, scope, best, _part) in TYPES.items()}
    types.update({kind: {'unit': unit, 'scope': scope, 'best': best, 'source': EMBER_SOURCE, 'license': CC_BY}
                  for kind, (unit, scope, best, _i) in COUNTRY_TYPES.items()})
    doc = {'data_until': data_until,
           'attribution': {'entsoe': 'ENTSO-E Transparency Platform', 'ember': 'Ember'},
           'types': types, 'zones': zones, 'countries': countries, 'recent': recent}
    before = load_json(OUT_PATH) or {}
    if {k: v for k, v in before.items() if k != 'updated'} != doc:
        write_json_if_changed(OUT_PATH, {'updated': iso(now), **doc}, separators=(',', ':'))
    return doc


# --- Asking ENTSO-E ---

def _stamp(moment):
    return moment.astimezone(timezone.utc).strftime('%Y%m%d%H%M')


def generation_answers(codes, first, last, budget, get=http_get, timeout=180, workers=4):
    """{code: [answer bodies] | None} of A75 for the codes from the start of `first` to the end of `last`."""
    start, end = carry_over.day_bounds(first)[0], carry_over.day_bounds(last)[1]
    jobs = [(code, code, {'documentType': 'A75', 'processType': 'A16', 'in_Domain': fe.GEN_ZONES[code],
                          'periodStart': _stamp(start), 'periodEnd': _stamp(end)}) for code in codes]
    responses = entsoe_ask(jobs, budget, get=get, workers=workers, timeout=timeout, label='Generation')
    found = {}
    for code in codes:
        answer = entsoe_answer(responses.get(code))
        if answer.problem:
            print(f'  {code} {first}..{last}: {answer.problem}')
            found[code] = None
        else:
            found[code] = answer.documents
    return found


def zone_techs(zone, answers):
    """The technologies of a site zone from the answers by code (a country of SUBZONES from its zones)."""
    if zone in SUBZONES:
        parts = [technologies(answers[code]) if answers.get(code) is not None else None for code in SUBZONES[zone]]
        return add_zones(parts)
    return technologies(answers[zone]) if answers.get(zone) is not None else None


def generation_days(zone, first, last, budget, get=http_get, capacity=None, timeout=180):
    """{day: numbers} of a zone's generation from `first` to `last`."""
    codes = SUBZONES.get(zone, [zone])
    techs = zone_techs(zone, generation_answers(codes, first, last, budget, get, timeout))
    if techs is None:
        return {}
    found = {}
    for day in days_between(first, last):
        if left_out(zone, day):
            continue
        numbers = day_numbers(techs, day, capacity)
        if numbers:
            found[day.isoformat()] = numbers
    return found


def capacities():
    return ((load_json('data/capacity-installed.json') or {}).get('countries')) or {}


# --- The runs ---

def update(now=None, get=http_get, budget=None):
    """The daily update: the generation of the last DAILY_DAYS days, the prices of the last 10 from the archives,
    Ember's months, and data/records.json."""
    now = now or datetime.now(timezone.utc)
    budget = budget or Budget(float(os.environ.get('RECORDS_BUDGET_SECONDS', '900')))
    today = carry_over.delivery_day(now)
    first, last = today - timedelta(days=DAILY_DAYS), today - timedelta(days=1)
    caps = capacities()
    for zone in GEN_ZONES:
        found = generation_days(zone, first, last, budget, get, caps.get(zone), timeout=60)
        written = store(zone, 'gen', found)
        print(f"  {zone}: generation of {len(found)} days ({sum(1 for n in found.values() if n.get('complete'))} "
              f"complete)" + (f"; wrote {written}" if written else ''))
    for zone, numbers in price_days(days_between(today - timedelta(days=10), last)).items():
        store(zone, 'price', numbers)
    before = load_json(OUT_PATH)
    doc = build(now, ember_monthly(get))
    print(f"{OUT_PATH} {'written' if load_json(OUT_PATH) != before else 'unchanged (nothing new)'}: "
          f"{len(doc['zones'])} zones, {len(doc['countries'])} countries, "
          f"{len(doc['recent'])} records beaten in the last 12 months")
    return 0


def fill(zones=None, now=None, get=http_get, publish=None):
    """The one-off fill: the prices of every day of the archive, and the generation of each zone since GEN_FROM,
    one request per zone (or each of its zones) and year, a zone at a time, each published before the next.
    Returns 1 when nothing was published or a publication failed."""
    now = now or datetime.now(timezone.utc)
    today = carry_over.delivery_day(now)
    started = time.monotonic()
    published, failed = 0, 0
    if not zones:
        for zone, numbers in price_days(days_between(date(2021, 11, 20), today - timedelta(days=1))).items():
            store(zone, 'price', numbers)
        build(now, ember_monthly(get))
        if publish:
            ok = publish('Records: the prices of the archive and Ember\'s months')
            published, failed = published + ok, failed + (not ok)
    caps = capacities()
    asked = 0
    for zone in zones or GEN_ZONES:
        zone_started = time.monotonic()
        found = {}
        for year in range(GEN_FROM.year, today.year + 1):
            first = max(GEN_FROM, date(year, 1, 1))
            last = min(date(year, 12, 31), today - timedelta(days=1))
            found.update(generation_days(zone, first, last, Budget(1800), get, caps.get(zone)))
            asked += len(SUBZONES.get(zone, [zone]))
        written = store(zone, 'gen', found, keep_complete=False)
        build(now, load_json(EMBER_PATH))
        print(f"{zone}: {len(found)} days, {sum(1 for n in found.values() if n.get('complete'))} complete, "
              f"years written {written or 'none'} [{time.monotonic() - zone_started:.0f}s]")
        if publish and written:
            ok = publish(f'Records: the generation of {zone} since {GEN_FROM.year}')
            published, failed = published + ok, failed + (not ok)
    print(f'Records fill: {asked} requests to ENTSO-E in {(time.monotonic() - started) / 60:.0f} minutes')
    if publish is None:
        return 0
    if failed or not published:
        print(f'::error::records: {published} parts published, {failed} not')
        return 1
    return 0


SAMPLES = (('PT', date(2025, 10, 26)), ('PT', date(2026, 3, 29)), ('DE', date(2026, 11, 20)),
           ('NO1', date(2026, 11, 20)), ('NO2', date(2026, 11, 20)))


def sample(folder, get=http_get):
    """Saves, gzipped, real answers of A75 for whole days (the clock changes among them), for the tests."""
    os.makedirs(folder, exist_ok=True)
    asked = SAMPLES
    if os.environ.get('RECORDS_SAMPLES'):              # "HU:2026-11-20 BE:2026-11-20", to look at an answer
        asked = [(item.split(':')[0], date.fromisoformat(item.split(':')[1]))
                 for item in os.environ['RECORDS_SAMPLES'].split()]
    for code, day in asked:
        answers = generation_answers([code], day, day, Budget(120), get)
        for i, body in enumerate(answers.get(code) or []):
            path = os.path.join(folder, f'generation_A75_{code}_{day.isoformat()}{"_" + str(i) if i else ""}.xml.gz')
            with open(path, 'wb') as f:
                f.write(gzip.compress(body, mtime=0))
            print(f'  saved {path}')
    return 0


def main(argv):
    if argv[:1] == ['--fill']:
        import backfill_archive
        return fill(argv[1:] or None, publish=lambda message: backfill_archive._commit_and_push(message, path='data/records*'))
    if argv[:1] == ['--sample']:
        return sample(argv[1] if len(argv) > 1 else 'samples')
    return update()


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
