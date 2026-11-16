#!/usr/bin/env python3
"""skip_if_fresh.py - is there anything new for this run to fetch?

First step of the feed's workflow, before any source is asked. When the
published data are under 7 minutes old the run is skipped: it asks nothing of
any source, commits nothing and ends in success, with one line in the log.

Why. A run is started every 15 minutes, but GitHub sometimes holds one for 15
to 25 minutes and then lets it go right behind another (runs 2370/2371,
2376/2375 and 2379/2381 on 16 Nov 2026). The second repeated the 166 requests
to ENTSO-E minutes after the first and brought nothing new.

How old the data are. The age is that of the OLDEST of the three files ENTSO-E
feeds (spot-prices, generation-mix, cross-border-flows), by their own
`updated`, read from the branch as it is when the job starts. A file's
`updated` is the time its data were asked for, and it only moves when ENTSO-E
gave at least one entry for it in that run (see carry_over.py; the GB zone that
fetch_gb.py adds does not move it). So a run that ended in error, or that got
nothing from ENTSO-E, or nothing for one of the three, does not make the data
fresh: the next run is not skipped.

Why 7 minutes. Two cases must come out right, and the limit sits between them:

  - A run right behind another must be skipped. A run takes at most 300 s of
    requests from the moment its first file is dated, plus about 30 s to
    publish and for the next job to get here: the data are then at most about
    5 min 30 s old.
  - The next ordinary run, 15 minutes later, must never be skipped. The first
    file is dated within a minute of the start of its run, so 15 minutes later
    the data are at least 14 minutes old, however slow ENTSO-E was and however
    long the run took.

Forcing a run. The workflow_dispatch input `force` (off unless asked for; the
scheduler never sends it) arrives here as FORCE=true and the run goes ahead
whatever the age:  gh workflow run fetch.yml -f force=true

This script never fails the run: if the age cannot be told, the run goes ahead.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from carry_over import parse_time
from feed_common import load_json

FILES = (
    'data/spot-prices.json',
    'data/generation-mix.json',
    'data/cross-border-flows.json',
)
FRESH_FOR = timedelta(minutes=7)


def utcnow():
    return datetime.now(timezone.utc)


def oldest_file(files=FILES):
    """(path, time) of the file whose data were asked for longest ago, or
    (path, None) for the first file with no usable `updated`."""
    oldest = None
    for path in files:
        doc = load_json(path)
        when = parse_time(doc.get('updated')) if isinstance(doc, dict) else None
        if when is None:
            return path, None
        if oldest is None or when < oldest[1]:
            oldest = (path, when)
    return oldest


def _spell(age):
    minutes, seconds = divmod(int(age.total_seconds()), 60)
    return f'{minutes} min {seconds:02d} s'


def decide(now, force=False, files=FILES):
    """(skip, line for the log)."""
    path, when = oldest_file(files)
    limit = f'{int(FRESH_FOR.total_seconds() // 60)} minutes'
    if when is None:
        return False, f'Running: the age of the published data cannot be told ({path} has no usable `updated`).'
    age = now - when
    if age < timedelta(0):
        return False, f'Running: {path} is dated {when.isoformat()}, after the time of this run.'
    what = f'the published data are {_spell(age)} old (the oldest is {path}, of {when.isoformat()})'
    if force:
        return False, f'Running: forced by hand; {what}.'
    if age < FRESH_FOR:
        return True, (f'Skipped: {what}, under the {limit} it takes for a run to bring something new. '
                      f'No source was asked for anything.')
    return False, f'Running: {what}, over the limit of {limit}.'


def main():
    skip, line = False, 'Running: the age of the published data could not be worked out.'
    try:
        force = os.environ.get('FORCE', '').strip().lower() == 'true'
        skip, line = decide(utcnow(), force)
    except Exception as e:  # noqa: BLE001 - never the reason a run fails
        line = f'Running: the age of the published data could not be worked out ({type(e).__name__}: {e}).'
    # A notice shows on the run's page as well as in the log.
    print(f'::notice::{line}' if skip else line)
    output = os.environ.get('GITHUB_OUTPUT')
    if output:
        with open(output, 'a', encoding='utf-8') as f:
            f.write(f"skip={'true' if skip else 'false'}\n")
    return 0


if __name__ == '__main__':
    sys.exit(main())
