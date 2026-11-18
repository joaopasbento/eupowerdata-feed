#!/usr/bin/env python3
"""real_run.py - read what a real run of fetch.yml published. See SKILL.md.

    GH_TOKEN=$(gh auth token --user joaopasbento) python3 .claude/skills/verify/real_run.py

Waits for the first run of fetch.yml that started from a commit (HEAD unless
--commit says another) and actually fetched, then checks the files on
origin/main: the three the site has always read, and the newer datasets
(balancing prices, outages, forecast error, low prices and the two files
fetch_entsoe.py keeps for them), each block with its licence. Exit 0 REAL RUN
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


def check_balancing(doc, path, broken):
    if not _blocks(doc, path, {'afrr_activated': 'cc-by-4.0', 'afrr_cbmp': 'source-terms'}, broken):
        return 'broken'
    for zone, entry in doc['afrr_activated']['zones'].items():
        days = entry.get('days') or {}
        if 'fetched_at' not in entry or not days or not all(
                _same_length([day.get('up'), day.get('down')], day.get('periods')) for day in days.values()):
            broken.append(f'{path} afrr_activated {zone}: a day does not have one price for each period')
    for zone, entry in doc['afrr_cbmp']['zones'].items():
        for day in (entry.get('days') or {}).values():
            if not all(isinstance(day.get(d), dict) and _same_length(
                    [day[d].get(name) for name in ('min', 'avg', 'max', 'count')], day.get('quarters')) for d in ('up', 'down')):
                broken.append(f'{path} afrr_cbmp {zone}: a day does not have one summary for each quarter-hour')
    return f"{len(doc['afrr_activated']['zones'])} zones with the 17.1.F price, {len(doc['afrr_cbmp']['zones'])} with the PICASSO summary"


OUTAGE = ('id', 'revision', 'asset_type', 'name', 'start', 'end', 'planned', 'status', 'published', 'phase')


def check_outages(doc, path, broken):
    if not _blocks(doc, path, {'generation': 'source-terms', 'transmission': 'cc-by-4.0'}, broken):
        return 'broken'
    total = 0
    for block, key, more in (('generation', 'zones', ('zone', 'nominal_mw', 'unavailable_mw')),
                             ('transmission', 'borders', ('border', 'ids', 'available_mw'))):
        for name, entry in doc[block][key].items():
            outages = entry.get('outages')
            if 'fetched_at' not in entry or not isinstance(outages, list) or any(
                    field not in outage for outage in outages for field in OUTAGE + more):
                broken.append(f'{path} {block} {name}: an outage lost a field, or the entry its time')
            total += len(outages or [])
    return f"{len(doc['generation']['zones'])} zones, {len(doc['transmission']['borders'])} borders, {total} outages"


def check_forecast(doc, path, broken):
    if not _blocks(doc, path, {'wind_solar': 'source-terms'}, broken):
        return 'broken'
    for zone, entry in doc['wind_solar']['zones'].items():
        for day in (entry.get('days') or {}).values():
            series = [day[name] for name in ('solar', 'wind_onshore', 'wind_offshore') if name in day]
            if not series or 'summary' not in day or not all(
                    _same_length([one.get('forecast'), one.get('actual'), one.get('error')], day.get('hours')) for one in series):
                broken.append(f'{path} {zone}: a day does not have forecast, actual and error for each hour')
    return f"{len(doc['wind_solar']['zones'])} zones"


def check_low_prices(doc, path, broken):
    if not _blocks(doc, path, {'omie': 'omie', 'entsoe': 'source-terms'}, broken):
        return 'broken'
    year = (doc['omie'].get('year') or {}).get('day_ahead') or {}
    if not doc['omie'].get('days') or any(not isinstance((year.get(zone) or {}).get('negative_or_zero_pct'), (int, float))
                                          for zone in ('PT', 'ES')):
        broken.append(f'{path}: OMIE has no day, or no total of the year for Portugal and Spain')
        return 'broken'
    return (f"OMIE {year.get('days')} days of the year, negative or zero {year['PT']['negative_or_zero_pct']} % in "
            f"Portugal and {year['ES']['negative_or_zero_pct']} % in Spain; {len(doc['entsoe'].get('zones') or {})} zones of ENTSO-E")


def check_kept(block):
    def check(doc, path, broken):
        if not _blocks(doc, path, {block: 'source-terms'}, broken):
            return 'broken'
        return f"{len(doc[block].get('zones') or {})} zones"
    return check


NEWER = {
    'data/wind-solar-actual.json': check_kept('actual_generation'),
    'data/spot-prices-intervals.json': check_kept('day_ahead_prices'),
    'data/balancing-prices.json': check_balancing,
    'data/outages.json': check_outages,
    'data/wind-solar-forecast.json': check_forecast,
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
