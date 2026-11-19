#!/usr/bin/env python3
"""build_meta.py - data/meta.json: the state of the files the site reads.

The site's status page (/site-status) and its embed widget read one small
file that says, for each file the site reads, whether it is there, when its
data are of, how big it is and, for the prices, the generation and the flows,
how many zones or corridors it has. On the WordPress site that file was
written by the PHP (eew_update_meta() in eew-engine.php), from the copies it
kept on its own disk. The site on Cloudflare keeps no copies: it reads the
feed's files, so the feed writes this one too, with the same shape:

    {
      "last_update": "2026-11-19T17:24:58+00:00",   when this run wrote it
      "trigger": "feed_workflow_dispatch",           what started the run
      "datasets": {
        "spot-prices": {"exists": true, "updated": "...", "size": 120939, "zones": 32},
        "cross-border-flows": {"exists": true, "updated": "...", "size": 290249, "corridors": 92},
        "ren-generation": {"exists": true, "updated": "...", "size": 459},
        ...
      },
      "run": "https://github.com/.../actions/runs/123",
      "steps": {"entsoe": "success", "omie": "failure", ...}
    }

Two things differ from the PHP's file, both for the better:

  - `updated` is the time the file's own `updated` field gives (when its data
    were obtained; a file kept from an earlier run keeps its time, by the
    rules of carry_over.py), not when a copy of it was written to a disk.
  - `run` and `steps` say which run wrote the file and how each of its steps
    ended (success, failure, skipped or cancelled): what failed can be seen
    on the status page, not only in the logs of GitHub Actions. The outcomes
    come from the workflow, in environment variables STEP_<ID>.

It makes no request and never fails the run: the step that calls it is the
last one before the second commit of fetch.yml, with continue-on-error.
"""

import os
from datetime import datetime, timezone

from feed_common import iso, load_json, write_json

OUT_PATH = 'data/meta.json'

# The files of the PHP's meta.json, in its order.
FILES = (
    'spot-prices', 'generation-mix', 'cross-border-flows', 'ren-generation', 'ree-generation',
    'omie-prices', 'forward-curves', 'capacity-installed', 'renewablenodes-capacity',
    'transition', 'datacenters',
)


def describe(name, folder='data'):
    """What meta.json says about one file."""
    path = os.path.join(folder, f'{name}.json')
    if not os.path.isfile(path):
        return {'exists': False, 'updated': None, 'size': 0}
    doc = load_json(path, {})
    info = {
        'exists': True,
        'updated': doc.get('updated') if isinstance(doc, dict) else None,
        'size': os.path.getsize(path),
    }
    if name == 'cross-border-flows':
        corridors = doc.get('corridors') if isinstance(doc, dict) else None
        info['corridors'] = len(corridors) if isinstance(corridors, (list, dict)) else 0
    elif name in ('spot-prices', 'generation-mix'):
        zones = doc.get('zones') if isinstance(doc, dict) else None
        info['zones'] = len(zones) if isinstance(zones, (list, dict)) else 0
    return info


def steps(environ):
    """{step id: outcome} from the STEP_<ID> variables the workflow passes."""
    found = {}
    for key, value in environ.items():
        if key.startswith('STEP_') and value:
            found[key[5:].lower()] = value
    return dict(sorted(found.items()))


def build(environ=None, now=None, folder='data'):
    environ = os.environ if environ is None else environ
    now = now or datetime.now(timezone.utc)
    meta = {
        'last_update': iso(now),
        'trigger': 'feed_' + (environ.get('GITHUB_EVENT_NAME') or 'local'),
        'datasets': {name: describe(name, folder) for name in FILES},
    }
    if environ.get('GITHUB_RUN_ID') and environ.get('GITHUB_REPOSITORY'):
        server = environ.get('GITHUB_SERVER_URL') or 'https://github.com'
        meta['run'] = f"{server}/{environ['GITHUB_REPOSITORY']}/actions/runs/{environ['GITHUB_RUN_ID']}"
    outcomes = steps(environ)
    if outcomes:
        meta['steps'] = outcomes
    return meta


def main():
    try:
        meta = build()
        write_json(OUT_PATH, meta, indent=2)
        present = sum(1 for info in meta['datasets'].values() if info['exists'])
        print(f'Wrote {OUT_PATH}: {present} of {len(FILES)} files present, trigger {meta["trigger"]}')
    except Exception as error:          # never fail the run for this file
        print(f'::warning::{OUT_PATH} not written: {error}')


if __name__ == '__main__':
    main()
