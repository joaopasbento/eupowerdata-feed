#!/usr/bin/env python3
"""fetch_ree.py - Spanish generation mix for data/ree-generation.json.

The file keeps its name and shape, but the data no longer comes from REE: the
terms of REE's REData API exclude commercial use, so this script makes no
request to REE (or to anyone). It copies the ES zone that fetch_entsoe.py
wrote to data/generation-mix.json earlier in the same run:

    {updated, source, mix: {technology: MW}, total_mw, renewable_pct}

`updated` is the time of the ENTSO-E data it was copied from, not the time of
the copy, so the file never looks fresher than its source. The `fallback` flag
is gone (nothing on the site reads it): ENTSO-E is now the only source.

The technology names are ENTSO-E's ("Wind Onshore", "Fossil Gas"...), which
the site already maps to its categories.

Standard library only. If generation-mix.json has no usable ES zone, the
previous ree-generation.json is kept.
"""

import sys

from feed_common import load_json, write_json

OUT_PATH = 'data/ree-generation.json'
GEN_MIX_PATH = 'data/generation-mix.json'
SOURCE = 'ENTSO-E Transparency Platform'


def from_generation_mix(gen_mix):
    """The ES zone of generation-mix.json in the ree-generation.json shape, or
    None when there is nothing usable."""
    es = ((gen_mix or {}).get('zones') or {}).get('ES')
    if not es or not es.get('mix') or not es.get('total_mw'):
        return None
    return {
        'updated': gen_mix.get('updated'),
        'source': SOURCE,
        'mix': es['mix'],
        'total_mw': es['total_mw'],
        'renewable_pct': es.get('renewable_pct', 0),
    }


def main():
    result = from_generation_mix(load_json(GEN_MIX_PATH))
    if not result:
        print(f'No ES zone in {GEN_MIX_PATH} - keeping the previous {OUT_PATH}')
        return 1

    if load_json(OUT_PATH) == result:
        print(f'{OUT_PATH} already has the ES data of {result["updated"]} - nothing to do')
        return 0

    write_json(OUT_PATH, result, indent=2)
    print('Wrote %s (ES from ENTSO-E, data of %s): %d MW, %.1f%% renewable'
          % (OUT_PATH, result['updated'], result['total_mw'], result['renewable_pct']))
    return 0


if __name__ == '__main__':
    sys.exit(main())
