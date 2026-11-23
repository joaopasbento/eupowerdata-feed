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

After the eleven files of the PHP's list, in its order and shape (the status
page and the widget read them), come the other files the site reads (since
20 Nov 2026, known issue N87): fx-rates, low-prices, balancing-prices,
weather and (since 23 Nov 2026) open-prices, with how many zones it has,
described as the others, and the two datasets kept in one file per
zone, outages and forecast, one entry each, made from their index:

        "outages": {"exists": true, "updated": "...", "zones": 35, "size": 968123},

`exists` says the index is there, `updated` is the index's own, `zones` how
many zones it lists (a count, as the `zones` of the prices) and `size` the
index and the files of those zones together.

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
# The other files the site reads (20 Nov 2026), after those of the PHP; and, since 23 Nov 2026, the prices
# of open sources for the alerts (fetch_open_prices.py).
MORE_FILES = ('fx-rates', 'low-prices', 'balancing-prices', 'weather', 'open-prices')
# The datasets kept in one file per zone: data/<name>/index.json and data/<name>/<ZONE>.json.
ZONED = ('outages', 'forecast')


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
    elif name in ('spot-prices', 'generation-mix', 'open-prices'):
        zones = doc.get('zones') if isinstance(doc, dict) else None
        info['zones'] = len(zones) if isinstance(zones, (list, dict)) else 0
    return info


def describe_zoned(name, folder='data'):
    """What meta.json says about a dataset kept in one file per zone, from its index."""
    index_path = os.path.join(folder, name, 'index.json')
    if not os.path.isfile(index_path):
        return {'exists': False, 'updated': None, 'zones': 0, 'size': 0}
    doc = load_json(index_path, {})
    zones = doc.get('zones') if isinstance(doc, dict) else None
    zones = [zone for zone in zones if isinstance(zone, str)] if isinstance(zones, list) else []
    size = os.path.getsize(index_path)
    for zone in zones:
        path = os.path.join(folder, name, f'{zone}.json')
        if os.path.isfile(path):
            size += os.path.getsize(path)
    return {'exists': True, 'updated': doc.get('updated') if isinstance(doc, dict) else None,
            'zones': len(zones), 'size': size}


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
        'datasets': {**{name: describe(name, folder) for name in FILES + MORE_FILES},
                     **{name: describe_zoned(name, folder) for name in ZONED}},
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
        print(f'Wrote {OUT_PATH}: {present} of {len(meta["datasets"])} datasets present, trigger {meta["trigger"]}')
    except Exception as error:          # never fail the run for this file
        print(f'::warning::{OUT_PATH} not written: {error}')


if __name__ == '__main__':
    main()
