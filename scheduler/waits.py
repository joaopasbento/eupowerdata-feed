#!/usr/bin/env python3
"""waits.py - how long GitHub held each run of the feed before starting it.

The scheduler asks GitHub for a run every 15 minutes. GitHub creates the run
at once, but sometimes leaves it sitting for 15 to 25 minutes before starting
its job, with no other run in the way (three of the eleven runs on the morning
of 16 Nov 2026). This script reads the runs of fetch.yml through the GitHub
API and prints, for each one, the wait between the request and the start, and
then a summary by variant of the experiment that is looking for the cause.

    GH_TOKEN=$(gh auth token --user joaopasbento) python3 scheduler/waits.py
    GH_TOKEN=$(gh auth token --user joaopasbento) python3 scheduler/waits.py --since 2026-11-16T18:00Z

Without --since it covers the last 24 hours. It only reads: one request for
the list of runs and one for the job of each run, through the `gh` CLI.

What the columns mean:

  requested  when the run was asked for (its created_at), UTC
  wait       from the request to the start of its job
  queue      the part of that wait during which another run of this workflow
             was going: the workflow's own queue doing its job
  held       the rest: time GitHub kept the run although no other run was
             going. A run counts as held from 60 seconds up
  variant    what the scheduler varied on that run (see src/index.js), read
             from the run's title; "-" for a run started by hand or by
             GitHub's own schedule
  outcome    ran, skipped (the published data were fresh: skip_if_fresh.py),
             failed or cancelled
"""

import argparse
import json
import re
import statistics
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

REPOSITORY = 'joaopasbento/eupowerdata-feed'
WORKFLOW = 'fetch.yml'
HELD_FROM = 60                 # seconds with nothing in the way from which a run counts as held
FETCH_STEP = 'Fetch ENTSO-E data'


def gh(path):
    """One GET to the GitHub API through the gh CLI; the parsed JSON."""
    done = subprocess.run(['gh', 'api', path], capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f'gh api {path} failed: {done.stderr.strip()}')
    return json.loads(done.stdout)


def when(text):
    """'2026-11-16T09:37:54Z' as a time, or None (GitHub writes year 1 for "not yet")."""
    if not text or text.startswith('0001'):
        return None
    return datetime.fromisoformat(text.replace('Z', '+00:00'))


def list_runs(since, until):
    """The runs of the workflow created in [since, until], oldest first."""
    runs, page = [], 1
    span = f'{since:%Y-%m-%dT%H:%M:%SZ}..{until:%Y-%m-%dT%H:%M:%SZ}'
    while True:
        batch = gh(f'repos/{REPOSITORY}/actions/workflows/{WORKFLOW}/runs?per_page=100&page={page}&created={span}')
        runs.extend(batch.get('workflow_runs', []))
        if len(batch.get('workflow_runs', [])) < 100:
            break
        page += 1
    return sorted(runs, key=lambda run: run['created_at'])


def describe(run, jobs):
    """One run as the plain facts this script works with."""
    job = jobs[0] if jobs else {}
    steps = {step['name']: step.get('conclusion') for step in job.get('steps') or []}
    if run.get('conclusion') == 'success':
        outcome = 'skipped' if steps.get(FETCH_STEP) == 'skipped' else 'ran'
    else:
        outcome = run.get('conclusion') or run.get('status') or '?'
    found = re.search(r'\[([^\]]+)\]\s*$', run.get('display_title') or '')
    return {
        'number': run['run_number'],
        'event': run['event'],
        'requested': when(run['created_at']),
        'started': when(job.get('started_at')),
        'ended': when(job.get('completed_at')),
        'variant': found.group(1) if found else '-',
        'outcome': outcome,
    }


def waits(laps):
    """Add `wait`, `queue` and `held` (seconds) to every run that has started.

    The queue is the part of the wait during which some other run's job was
    going; held is the part during which none was, and GitHub could have
    started this one."""
    for lap in laps:
        if lap['started'] is None:
            continue
        busy = sorted((max(other['started'], lap['requested']), min(other['ended'], lap['started']))
                      for other in laps
                      if other is not lap and other['started'] is not None and other['ended'] is not None
                      and other['started'] < lap['started'] and other['ended'] > lap['requested'])
        queue, covered = timedelta(0), lap['requested']
        for start, end in busy:                      # the union of the intervals, without counting overlaps twice
            start = max(start, covered)
            if end > start:
                queue += end - start
                covered = end
        lap['wait'] = (lap['started'] - lap['requested']).total_seconds()
        lap['queue'] = queue.total_seconds()
        lap['held'] = lap['wait'] - lap['queue']
    return laps


def clock(seconds):
    minutes, rest = divmod(int(round(seconds)), 60)
    return f'{minutes}:{rest:02d}'


def summary(title, groups):
    """A block of the report: for each group, how many runs and how many were held."""
    lines = [title, f"  {'':<34}{'runs':>5}{'held':>6}{'share':>7}{'median held':>13}{'longest':>9}"]
    for name, laps in groups:
        laps = [lap for lap in laps if 'held' in lap]
        if not laps:
            continue
        held = [lap['held'] for lap in laps if lap['held'] >= HELD_FROM]
        lines.append(f"  {name:<34}{len(laps):>5}{len(held):>6}{100 * len(held) / len(laps):>6.0f}%"
                     f"{clock(statistics.median(held)) if held else '-':>13}{clock(max(held)) if held else '-':>9}")
    return lines


def report(laps):
    """The whole report, as a list of lines."""
    lines = [f"{'requested (UTC)':<17}{'run':>5}  {'event':<18}{'wait':>7}{'queue':>7}{'held':>7}  {'outcome':<12}variant"]
    for lap in laps:
        if 'held' in lap:
            times = f"{clock(lap['wait']):>7}{clock(lap['queue']):>7}{clock(lap['held']):>7}"
            mark = '  <- held' if lap['held'] >= HELD_FROM else ''
        else:
            times, mark = f"{'not started':>21}", ''
        lines.append(f"{lap['requested']:%d %b %H:%M:%S}  {lap['number']:>5}  {lap['event']:<18}{times}  "
                     f"{lap['outcome']:<12}{lap['variant']}{mark}")

    started = [lap for lap in laps if 'held' in lap]
    lines += ['']
    lines += summary('All runs', [('all', started)])

    tested = [lap for lap in started if lap['variant'] != '-']
    words = sorted({word for lap in tested for word in lap['variant'].split()})
    if words:
        lines += ['']
        lines += summary('By what the scheduler varied (each run counts once for each of its three words)',
                         [(word, [lap for lap in tested if word in lap['variant'].split()]) for word in words])
        lines += ['']
        lines += summary('By combination',
                         [(variant, [lap for lap in tested if lap['variant'] == variant])
                          for variant in sorted({lap['variant'] for lap in tested})])
        others = [lap for lap in started if lap['variant'] == '-']
        if others:
            lines += ['']
            lines += summary('Runs outside the experiment (by hand, or GitHub\'s own schedule)',
                             [(event, [lap for lap in others if lap['event'] == event])
                              for event in sorted({lap['event'] for lap in others})])
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description='How long GitHub held each run of the feed before starting it.')
    parser.add_argument('--since', help='first moment to cover, e.g. 2026-11-16T18:00Z (default: 24 hours ago)')
    parser.add_argument('--until', help='last moment to cover (default: now)')
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    since = when(args.since) if args.since else now - timedelta(hours=24)
    until = when(args.until) if args.until else now
    for moment in (since, until):
        if moment is None or moment.tzinfo is None:
            sys.exit('Times are written like 2026-11-16T18:00Z')

    runs = list_runs(since, until)
    if not runs:
        print('No runs in that period.')
        return 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs = list(pool.map(lambda run: gh(f"repos/{REPOSITORY}/actions/runs/{run['id']}/jobs").get('jobs', []), runs))
    laps = waits([describe(run, run_jobs) for run, run_jobs in zip(runs, jobs)])
    print('\n'.join(report(laps)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
