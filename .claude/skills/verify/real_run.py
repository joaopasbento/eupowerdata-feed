#!/usr/bin/env python3
"""real_run.py - read what a real run of fetch.yml published. See SKILL.md.

    GH_TOKEN=$(gh auth token --user joaopasbento) python3 .claude/skills/verify/real_run.py

Waits for the first run of fetch.yml that started from a commit (HEAD unless
--commit says another) and actually fetched, then checks the files on
origin/main: the three the site has always read, and the newer datasets
(balancing prices, outages, forecast error, low prices and the two files
fetch_entsoe.py keeps for them), each block with its licence, and the shape
of the files a run reads or rewrites only now and then (the exchange rates,
Ember's yearly figures, the installed capacity). Exit 0 REAL RUN
OK, 1 REAL RUN BROKEN, 2 REAL RUN INCONCLUSIVE (a source was down: says
nothing about the change; run it again).
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

ROOT = subprocess.run(['git', 'rev-parse', '--show-toplevel'], capture_output=True, text=True).stdout.strip()
sys.path.insert(0, ROOT)
os.chdir(ROOT)
import carry_over  # noqa: E402

REPOSITORY = 'joaopasbento/eupowerdata-feed'
FETCH_STEP = 'Fetch ENTSO-E data'


def gh(path):
    done = subprocess.run(['gh', 'api', path], capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f'gh api {path} failed: {done.stderr.strip()[:200]}')
    return json.loads(done.stdout)


def git(*args):
    return subprocess.run(['git', *args], capture_output=True, text=True)


def published(path):
    """A file as it is on origin/main, or None when it is not valid JSON."""
    try:
        return json.loads(git('show', f'origin/main:{path}').stdout)
    except ValueError:
        return None


def find_run(commit, wait_minutes):
    """The first completed run of fetch.yml from `commit` (or a later commit)
    whose fetch step ran: (run, its steps)."""
    since = datetime.fromisoformat(git('show', '-s', '--format=%cI', commit).stdout.strip()) - timedelta(minutes=1)
    deadline = time.monotonic() + wait_minutes * 60
    seen = set()
    while True:
        git('fetch', '-q', 'origin')
        runs = gh(f'repos/{REPOSITORY}/actions/workflows/fetch.yml/runs?per_page=20')['workflow_runs']
        for run in sorted(runs, key=lambda r: r['created_at']):
            created = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
            if created < since or run['id'] in seen or run['status'] != 'completed':
                continue
            if git('merge-base', '--is-ancestor', commit, run['head_sha']).returncode != 0:
                continue
            jobs = gh(f"repos/{REPOSITORY}/actions/runs/{run['id']}/jobs")['jobs']
            steps = {s['name']: s['conclusion'] for s in (jobs[0]['steps'] if jobs else [])}
            if steps.get(FETCH_STEP) in (None, 'skipped'):
                seen.add(run['id'])
                print(f"Run {run['run_number']} skipped the fetch (the data were fresh): waiting for the next one")
                continue
            return run, steps, jobs[0]
        if time.monotonic() > deadline:
            finish([], [f'no run of fetch.yml from {commit[:8]} finished in {wait_minutes} minutes'], [])
        time.sleep(20)


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def check_prices(doc, broken):
    if not doc or list(doc)[:4] != ['updated', 'source', 'endpoint', 'zones'] or not doc['zones']:
        return broken.append('spot-prices.json: top-level keys changed, or no zones')
    day = carry_over.delivery_day(doc['updated'])
    for code, zone in doc['zones'].items():
        where = f'spot-prices.json {code}'
        if any(field not in zone for field in ('eic', 'prices', 'latest', 'avg', 'min', 'max', 'fetched_at')):
            broken.append(f'{where}: a field the site reads is missing')
            continue
        rows = zone['prices']
        if not rows or any(sorted(r) != ['hour', 'price', 'time'] or not number(r['price']) for r in rows):
            broken.append(f'{where}: prices are not a list of time, hour and price')
            continue
        if not (number(zone['avg']) and zone['min'] <= zone['avg'] <= zone['max']):
            broken.append(f'{where}: avg is not between min and max')
        if 'delivery_date' in zone:                     # a zone of ENTSO-E: one value for each hour of the day
            hours = [r['hour'] for r in rows]
            if hours != sorted(set(hours)) or not set(hours) <= set(range(24)) \
                    or any(r['time'] != f"{r['hour']:02d}:00" for r in rows):
                broken.append(f'{where}: hours are not 00:00 to 23:00, each once and in order')
            if zone['delivery_date'] != day.isoformat():
                broken.append(f"{where}: delivery_date {zone['delivery_date']} is not the day in course ({day})")


def check_generation(doc, broken):
    if not doc or list(doc)[:3] != ['updated', 'source', 'zones'] or not doc['zones']:
        return broken.append('generation-mix.json: top-level keys changed, or no zones')
    for code, zone in doc['zones'].items():
        where = f'generation-mix.json {code}'
        mix = zone.get('mix')
        if not isinstance(mix, dict) or not mix or not all(number(v) for v in mix.values()):
            broken.append(f'{where}: mix is not technology -> MW')
        elif not number(zone.get('total_mw')) or abs(zone['total_mw'] - sum(mix.values())) > 1:
            broken.append(f'{where}: total_mw is not the sum of the mix')
        elif not number(zone.get('renewable_pct')) or not 0 <= zone['renewable_pct'] <= 100:
            broken.append(f'{where}: renewable_pct is not a percentage')
        if 'fetched_at' not in zone:
            broken.append(f'{where}: no fetched_at')


def check_flows(doc, broken):
    if not doc or list(doc)[:4] != ['updated', 'source', 'corridors', 'net'] or not doc['corridors']:
        return broken.append('cross-border-flows.json: top-level keys changed, or no corridors')
    for name, corridor in doc['corridors'].items():
        where = f'cross-border-flows.json {name}'
        if any(field not in corridor for field in ('from', 'to', 'latest', 'values', 'data_time', 'fetched_at')):
            broken.append(f'{where}: a field is missing')
        elif not isinstance(corridor['values'], list) or not number(corridor['latest'].get('mw')):
            broken.append(f'{where}: latest or values changed shape')
    for pair, net in doc['net'].items():
        if any(field not in net for field in ('from', 'to', 'net_mw', 'data_time', 'fetched_at')):
            broken.append(f'cross-border-flows.json net {pair}: a field is missing')
        elif f"{net['from']}→{net['to']}" not in doc['corridors'] and f"{net['to']}→{net['from']}" not in doc['corridors']:
            broken.append(f'cross-border-flows.json net {pair}: neither of its corridors is in the file')


# --- The newer datasets ---
#
# Made by steps of their own, after the three files above are committed, and
# published by a second commit. A step of theirs that got nothing leaves its
# file as it was: that is a note here, never a failure of the change. What is
# a failure is a file that is not valid, or that lost its shape or a licence.

def exists(path):
    return git('cat-file', '-e', f'origin/main:{path}').returncode == 0


def _blocks(doc, path, licences, broken):
    """The blocks of a newer file, each with the licence it must have. False when the file is not what it should be."""
    if not isinstance(doc, dict) or 'updated' not in doc or 'data_time' not in doc:
        broken.append(f'{path}: no `updated` or no `data_time`')
        return False
    for name, licence in licences.items():
        block = doc.get(name)
        if not isinstance(block, dict) or block.get('license') != licence:
            broken.append(f'{path}: block {name} is missing or its licence is not {licence}')
            return False
    return True


def _same_length(lists, count):
    return all(isinstance(values, list) and len(values) == count for values in lists)


notes_of_balancing = []


def _priced(entry):
    """How many periods of a zone's days have a price, upward or downward."""
    return sum(1 for day in (entry.get('days') or {}).values()
               for up, down in zip(day.get('up') or [], day.get('down') or []) if up is not None or down is not None)


def check_balancing(doc, path, broken):
    if not _blocks(doc, path, {'afrr_activated': 'cc-by-4.0', 'afrr_cbmp': 'source-terms'}, broken):
        return 'broken'
    for zone, entry in doc['afrr_activated']['zones'].items():
        days = entry.get('days') or {}
        if 'fetched_at' not in entry or not days or not all(
                _same_length([day.get('up'), day.get('down')], day.get('periods')) for day in days.values()):
            broken.append(f'{path} afrr_activated {zone}: a day does not have one price for each period')
        if entry.get('currency') != 'EUR' and days and not all(
                _same_length([day.get('up_eur'), day.get('down_eur')], day.get('periods'))
                and number(day.get('fx_rate')) and 'fx_date' in day for day in days.values()):
            # a day with no rate has no values in euros; the step's log says which
            notes_of_balancing.append(f"{zone} ({entry.get('currency')}) has a day with no values in euros")
    for zone, entry in doc['afrr_cbmp']['zones'].items():
        for day in (entry.get('days') or {}).values():
            if not all(isinstance(day.get(d), dict) and _same_length(
                    [day[d].get(name) for name in ('min', 'avg', 'max', 'count')], day.get('quarters')) for d in ('up', 'down')):
                broken.append(f'{path} afrr_cbmp {zone}: a day does not have one summary for each quarter-hour')
    zones = doc['afrr_activated']['zones']
    in_euros = sorted(zone for zone, entry in zones.items() if entry.get('currency') != 'EUR'
                      and any('up_eur' in day for day in (entry.get('days') or {}).values()))
    more = notes_of_balancing[:]
    del notes_of_balancing[:]
    if in_euros:
        more.append(f"also in euros: {', '.join(in_euros)}")
    more += [f"{zone} by the {entry['area']}, process {entry.get('process')}, "
             f"{_priced(entry)} periods priced in {len(entry.get('days') or {})} days"
             for zone, entry in zones.items() if entry.get('area')]
    return (f"{len(zones)} zones with the 17.1.F price, {len(doc['afrr_cbmp']['zones'])} with the PICASSO summary"
            + ''.join(f'; {line}' for line in more))


OUTAGE = ('id', 'revision', 'asset_type', 'name', 'start', 'end', 'planned', 'status', 'published', 'phase')


def _in_force_now(outages, moment):
    return [o for o in outages if carry_over.parse_time(o['start']) <= moment < carry_over.parse_time(o['end'])]


def check_outages(doc, path, broken):
    """data/outages/index.json and the file of each zone it lists (since 20 Nov 2026 there is no single file)."""
    if not _blocks(doc, path, {'generation': 'source-terms', 'transmission': 'cc-by-4.0'}, broken):
        return 'broken'
    if exists('data/outages.json'):
        broken.append('data/outages.json is still published: the step should have taken it out')
    zones, total, said = doc.get('zones') or [], 0, []
    moment = carry_over.parse_time(doc['updated'])
    for zone in zones:
        where = f'data/outages/{zone}.json'
        file = published(where) if exists(where) else None
        if not isinstance(file, dict) or file.get('zone') != zone or 'data_time' not in file:
            broken.append(f'{where}: missing, or not the file of the zone')
            continue
        generation, transmission = file.get('generation') or {}, file.get('transmission') or {}
        if generation.get('license') != 'source-terms' or transmission.get('license') != 'cc-by-4.0' \
                or 'known_issue' not in generation or not isinstance(transmission.get('borders'), dict):
            broken.append(f'{where}: a block is missing, or its licence is not the one it must have')
            continue
        now_out = generation.get('unavailable_now') or {}
        if not number(now_out.get('unavailable_mw')) or not isinstance(now_out.get('units'), int):
            broken.append(f'{where}: no summary of what is out now (unavailable_now)')
        if 'fetched_at' in json.dumps(file):
            broken.append(f'{where}: a time of the run is in the file of the zone (it belongs in the index)')
        lists = [('generation', generation.get('outages'), ('zone', 'nominal_mw', 'unavailable_mw'))] + [
            (f'border {name}', border.get('outages'), ('border', 'ids', 'available_mw'))
            for name, border in transmission['borders'].items()]
        for what, outages, more in lists:
            if not isinstance(outages, list) or any(field not in o for o in outages for field in OUTAGE + more):
                broken.append(f'{where} {what}: an outage lost a field')
            elif any(o['status'] != 'active' for o in outages):
                broken.append(f'{where} {what}: a cancelled or withdrawn outage is published')
        listed = (doc['generation'].get('zones') or {}).get(zone)
        if listed and (listed.get('outages') != len(generation.get('outages') or [])
                       or listed.get('unavailable_now') != now_out or 'fetched_at' not in listed):
            broken.append(f'{path} {zone}: the index does not say what the file of the zone has, or has no fetched_at')
        total += len(generation.get('outages') or [])
        if zone in ('ES', 'PT') and moment is not None and isinstance(generation.get('outages'), list):
            each = sum(o.get('unavailable_mw') or 0 for o in _in_force_now(generation['outages'], moment))
            said.append(f"{zone} {now_out.get('unavailable_mw')} MW out now in {now_out.get('units')} units "
                        f"(the outages in force added up: {each:.1f} MW)")
    return (f"{len(zones)} files of zones, {len(doc['generation'].get('zones') or {})} with outages of units, "
            f"{len(doc['transmission'].get('borders') or {})} borders, {total} outages of units; " + '; '.join(said))


def _day_is_whole(day):
    series = [day[name] for name in ('solar', 'wind_onshore', 'wind_offshore') if name in day]
    return bool(series) and 'summary' in day and all(
        _same_length([one.get('forecast'), one.get('actual'), one.get('error')], day.get('hours')) for one in series)


def check_forecast(doc, path, broken):
    if not _blocks(doc, path, {'wind_solar': 'source-terms'}, broken):
        return 'broken'
    for zone, entry in doc['wind_solar']['zones'].items():
        if not all(_day_is_whole(day) for day in (entry.get('days') or {}).values()):
            broken.append(f'{path} {zone}: a day does not have forecast, actual and error for each hour')
    return f"{len(doc['wind_solar']['zones'])} zones"


def check_forecast_days(doc, path, broken):
    """data/forecast/index.json and the file of each zone it lists: the day in course and the 30 before it."""
    if not _blocks(doc, path, {'wind_solar': 'source-terms'}, broken):
        return 'broken'
    window = doc.get('days') or {}
    first, last = window.get('from'), window.get('to')
    if not first or not last or (datetime.fromisoformat(last) - datetime.fromisoformat(first)).days != 30:
        broken.append(f'{path}: `days` is not the day in course and the 30 before it')
        return 'broken'
    said = []
    for zone in doc.get('zones') or []:
        where = f'data/forecast/{zone}.json'
        file = published(where) if exists(where) else None
        block = (file or {}).get('wind_solar') if isinstance(file, dict) else None
        if not isinstance(block, dict) or block.get('license') != 'source-terms' or not isinstance(block.get('days'), dict):
            broken.append(f'{where}: missing, or no block wind_solar with its licence and days')
            continue
        days = list(block['days'])
        if days != sorted(days) or not all(first <= day <= last for day in days):
            broken.append(f'{where}: days out of order or out of the window {first} to {last}')
        if not all(_day_is_whole(day) for day in block['days'].values()):
            broken.append(f'{where}: a day does not have forecast, actual and error for each hour')
        if (doc['wind_solar'].get('zones') or {}).get(zone, {}).get('days') != len(days):
            broken.append(f'{path} {zone}: the index does not say how many days the file of the zone has')
        if zone in ('ES', 'PT'):
            said.append(f"{zone} {len(days)} days ({days[0] if days else '-'} to {days[-1] if days else '-'})")
    return f"{len(doc.get('zones') or [])} files of zones, days {first} to {last}; " + '; '.join(said)


def check_low_prices(doc, path, broken):
    if not _blocks(doc, path, {'omie': 'omie', 'entsoe': 'source-terms'}, broken):
        return 'broken'
    year = (doc['omie'].get('year') or {}).get('day_ahead') or {}
    if not doc['omie'].get('days') or any(not isinstance((year.get(zone) or {}).get('negative_or_zero_pct'), (int, float))
                                          for zone in ('PT', 'ES')):
        broken.append(f'{path}: OMIE has no day, or no total of the year for Portugal and Spain')
        return 'broken'
    checked = doc['omie'].get('version_check') or {}
    hourly = sorted(zone for zone, entry in (doc['entsoe'].get('zones') or {}).items()
                    if ((entry.get('year') or {}).get('hourly_days') or 0) > 0)
    # Since 20 Nov 2026 each zone of ENTSO-E has its months, which add up to its year.
    zones = doc['entsoe'].get('zones') or {}
    for zone, entry in zones.items():
        months = entry.get('months')
        if not isinstance(months, dict) or not months or any(
                abs(sum(month.get(name) or 0 for month in months.values()) - (entry.get('year') or {}).get(name, -1)) > 1e-6
                for name in ('days', 'hours', 'negative_h', 'zero_h', 'low_h')):
            broken.append(f'{path} entsoe {zone}: no months, or they do not add up to the year')
    spain = (zones.get('ES') or {}).get('months') or {}
    return (f"OMIE {year.get('days')} days of the year, negative or zero {year['PT']['negative_or_zero_pct']} % in "
            f"Portugal and {year['ES']['negative_or_zero_pct']} % in Spain; {len(zones)} zones of ENTSO-E, "
            f"each with its months ({len(spain)} for Spain)"
            + (f"; OMIE's later versions looked for on {checked.get('date')}" if checked.get('date') else '')
            + (f"; zones whose year still has days counted on hourly means: {', '.join(hourly)}" if hourly else ''))


def check_kept(block):
    def check(doc, path, broken):
        if not _blocks(doc, path, {block: 'source-terms'}, broken):
            return 'broken'
        return f"{len(doc[block].get('zones') or {})} zones"
    return check


CAPACITY_FIELDS = ['name', 'capacity_mw', 'renewable_pct', 'solar_pct', 'wind_pct', 'hydro_pct', 'bio_pct', 'nuclear_pct',
                   'solar_mw', 'wind_mw', 'hydro_mw', 'target', 'target_year', 'trend', 'population_m', 'cap_source',
                   'cap_year']


def check_rates(doc, path, broken):
    """data/fx-rates.json: the ECB's reference rates the prices not in euros are converted with."""
    rates = doc.get('rates') if isinstance(doc, dict) else None
    fixed = doc.get('fixed') if isinstance(doc, dict) else None
    if not isinstance(rates, dict) or not isinstance(fixed, dict) or not {'PLN', 'RON'} <= set(rates) \
            or not {'BGN', 'BAM'} <= set(fixed) or any(not days or not all(number(v) for v in days.values())
                                                       for days in rates.values()):
        broken.append(f'{path}: no rates, or a currency with no day or with a value that is not a number')
        return 'broken'
    last = max(max(days) for days in rates.values())
    return f"{len(rates)} currencies and {len(fixed)} fixed rates, the last day {last}"


def check_ember(doc, path, broken):
    """data/ember-yearly.json: what merge_capacity.py reads of Ember's yearly data."""
    countries = doc.get('countries') if isinstance(doc, dict) else None
    if not isinstance(countries, dict) or doc.get('license') != 'cc-by-4.0' or not doc.get('edition') \
            or not doc.get('source') or len(countries) != 32 or any(
                not number((entry.get('share_of_generation_pct') or {}).get('renewables'))
                or len((entry.get('renewables_trend') or {}).get('share_of_generation_pct') or []) != 7
                for entry in countries.values()):
        broken.append(f'{path}: not the licence, the edition and the 32 countries with their share and trend')
        return 'broken'
    return f"edition of {doc['edition'][:10]}, {len(countries)} countries, last complete year " \
           f"{sorted({entry.get('year') for entry in countries.values()})}"


def check_capacity(doc, path, broken):
    """data/renewablenodes-capacity.json, which the site reads: the same countries and fields as ever."""
    countries = doc.get('countries') if isinstance(doc, dict) else None
    if not isinstance(countries, dict) or list(doc) != ['updated', 'source', 'countries', 'count'] \
            or doc['count'] != len(countries) or any(list(country) != CAPACITY_FIELDS for country in countries.values()):
        broken.append(f'{path}: top-level keys changed, or a country lost a field the site reads')
        return 'broken'
    sources = sorted({country['cap_source'] for country in countries.values()})
    if not set(sources) <= {'entsoe', 'ember'} or 'Ember' not in str(doc['source']):
        broken.append(f'{path}: the capacity comes from {sources}, or `source` does not name Ember')
    return f"{len(countries)} countries, capacity from {', '.join(sources)}"


# Files a run reads, or rewrites only now and then: their shape is checked, not their time.
STANDING = {
    'data/fx-rates.json': check_rates,
    'data/ember-yearly.json': check_ember,
    'data/renewablenodes-capacity.json': check_capacity,
}

NEWER = {
    'data/wind-solar-actual.json': check_kept('actual_generation'),
    'data/spot-prices-intervals.json': check_kept('day_ahead_prices'),
    'data/balancing-prices.json': check_balancing,
    'data/outages/index.json': check_outages,
    'data/wind-solar-forecast.json': check_forecast,
    'data/forecast/index.json': check_forecast_days,
    'data/low-prices.json': check_low_prices,
}
NEWER_STEPS = ('Balancing prices', 'Outages of power plants', 'Forecast error', 'Negative, zero and low prices',
               'Commit and push the newer datasets')
CADENCE_MINUTES = 15


def finish(broken, inconclusive, notes):
    for line in notes:
        print(f'  {line}')
    if broken:
        print('\n'.join(f'  BROKEN: {line}' for line in broken[:30]))
        print('REAL RUN BROKEN')
        sys.exit(1)
    if inconclusive:
        print('\n'.join(f'  SOURCE DOWN: {line}' for line in inconclusive))
        print('REAL RUN INCONCLUSIVE')
        sys.exit(2)
    print('REAL RUN OK')
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description='Read what a real run of fetch.yml published.')
    parser.add_argument('--commit', default='HEAD', help='the pushed commit the run must include (default HEAD)')
    parser.add_argument('--wait', type=int, default=25, help='minutes to wait for the run (default 25)')
    args = parser.parse_args()
    commit = git('rev-parse', args.commit).stdout.strip()
    git('fetch', '-q', 'origin')
    if git('merge-base', '--is-ancestor', commit, 'origin/main').returncode != 0:
        sys.exit(f'{commit[:8]} is not on origin/main: push it first')

    run, steps, job = find_run(commit, args.wait)
    started = datetime.fromisoformat(job['started_at'].replace('Z', '+00:00'))
    print(f"Run {run['run_number']} ({run['html_url']}), asked for at {run['created_at']}, ended {run['conclusion']}")
    git('fetch', '-q', 'origin')
    broken, inconclusive, notes = [], [], []

    files = {'data/spot-prices.json': ('zones', check_prices), 'data/generation-mix.json': ('zones', check_generation),
             'data/cross-border-flows.json': ('corridors', check_flows)}
    for path, (key, check) in files.items():
        doc = published(path)
        if doc is None:
            broken.append(f'{path} is not valid JSON')
            continue
        check(doc, broken)
        updated = carry_over.parse_time(doc.get('updated'))
        entries = doc.get(key) or {}
        kept = [name for name, entry in entries.items() if entry.get('carried_over')]
        if updated is None or updated < started:
            inconclusive.append(f'ENTSO-E gave nothing for {path} in this run: it was left as it was, of {doc.get("updated")}')
        notes.append(f'{path}: {len(entries)} {key}, updated {doc.get("updated", "?")[:19]}'
                     + (f'; kept because the source did not answer: {", ".join(kept)}' if kept else ''))

    for path, check in NEWER.items():
        if not exists(path):
            notes.append(f'{path}: not published (its step has not run yet, or has never obtained anything)')
            continue
        doc = published(path)
        if doc is None:
            broken.append(f'{path} is not valid JSON')
            continue
        found = check(doc, path, broken)
        updated = carry_over.parse_time(doc.get('updated'))
        fresh = updated is not None and updated >= started
        notes.append(f'{path}: {found}, updated {str(doc.get("updated"))[:19]}'
                     + ('' if fresh else ' - not written in this run (nothing new, or its source did not answer)'))

    for path, check in STANDING.items():
        doc = published(path) if exists(path) else None
        if doc is None:
            broken.append(f'{path} is missing or is not valid JSON')
            continue
        notes.append(f'{path}: {check(doc, path, broken)}, updated {str(doc.get("updated"))[:19]}')

    # The whole run must fit in the cadence: the next one starts 15 minutes after this one was asked for.
    ended = datetime.fromisoformat(job['completed_at'].replace('Z', '+00:00')) if job.get('completed_at') else None
    if ended is not None:
        minutes = (ended - started).total_seconds() / 60
        notes.append(f'the job took {minutes:.1f} minutes')
        if minutes > CADENCE_MINUTES - 2:
            broken.append(f'the job took {minutes:.1f} minutes: it does not fit in the cadence of {CADENCE_MINUTES}')
    newer = {name: conclusion for name, conclusion in steps.items() if name.startswith(NEWER_STEPS)}
    if newer:
        notes.append('newer steps: ' + ', '.join(f'{name.split(" (")[0]} {conclusion}' for name, conclusion in newer.items()))

    status = published('data/status.json') or {}
    if status.get('status') != 'ok':
        inconclusive.append(f"status.json says {status.get('status')}: ENTSO-E refused the key or could not be asked")
    failed = [name for name, conclusion in steps.items() if conclusion == 'failure']
    if 'Commit and push' in failed:
        broken.append('the run could not commit and push')
    if FETCH_STEP in failed:
        log = subprocess.run(['gh', 'run', 'view', str(run['id']), '--log-failed'], capture_output=True, text=True).stdout
        if 'Traceback' in log:
            broken.append('fetch_entsoe.py stopped with an error (Traceback in the log of the run)')
        elif not inconclusive:
            inconclusive.append('the fetch step failed with nothing obtained from ENTSO-E')
    if failed:
        notes.append(f'steps that failed: {", ".join(failed)}')
    finish(broken, inconclusive, notes)


if __name__ == '__main__':
    main()
