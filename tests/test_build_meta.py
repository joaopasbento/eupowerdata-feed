import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import build_meta  # noqa: E402


class BuildMeta(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = folder.name

    def put(self, name, doc):
        with open(os.path.join(self.folder, f'{name}.json'), 'w', encoding='utf-8') as f:
            json.dump(doc, f)

    def test_it_has_the_shape_of_the_php_file(self):
        self.put('spot-prices', {'updated': '2026-11-19T17:00:00+00:00', 'zones': {'PT': {}, 'ES': {}}})
        self.put('cross-border-flows', {'updated': '2026-11-19T16:00:00+00:00', 'corridors': [{}, {}, {}]})
        self.put('ren-generation', {'updated': '2026-11-19T15:00:00+00:00', 'mix': {}})
        environ = {'GITHUB_EVENT_NAME': 'workflow_dispatch', 'GITHUB_RUN_ID': '42',
                   'GITHUB_REPOSITORY': 'joaopasbento/eupowerdata-feed',
                   'STEP_OMIE': 'failure', 'STEP_ENTSOE': 'success', 'STEP_EMPTY': ''}
        meta = build_meta.build(environ, datetime(2026, 11, 19, 17, 30, tzinfo=timezone.utc), self.folder)

        # The eleven of the PHP first, in its order; then the other files the site reads
        self.assertEqual(list(meta['datasets']), list(build_meta.FILES) + ['fx-rates', 'low-prices', 'balancing-prices',
                                                                           'weather', 'open-prices', 'outages', 'forecast'])
        self.assertEqual((meta['last_update'], meta['trigger']), ('2026-11-19T17:30:00+00:00', 'feed_workflow_dispatch'))
        spot = meta['datasets']['spot-prices']
        self.assertEqual((spot['exists'], spot['updated'], spot['zones']), (True, '2026-11-19T17:00:00+00:00', 2))
        self.assertEqual(spot['size'], os.path.getsize(os.path.join(self.folder, 'spot-prices.json')))
        self.assertEqual(meta['datasets']['cross-border-flows']['corridors'], 3)
        self.assertNotIn('zones', meta['datasets']['ren-generation'])
        self.assertEqual(meta['datasets']['datacenters'], {'exists': False, 'updated': None, 'size': 0})
        self.assertEqual(meta['run'], 'https://github.com/joaopasbento/eupowerdata-feed/actions/runs/42')
        self.assertEqual(meta['steps'], {'entsoe': 'success', 'omie': 'failure'})

    def test_the_other_files_the_site_reads_are_described_as_the_others(self):
        self.put('low-prices', {'updated': '2026-11-20T17:43:01+00:00', 'entsoe': {}})
        meta = build_meta.build({}, None, self.folder)
        low = meta['datasets']['low-prices']
        self.assertEqual(low, {'exists': True, 'updated': '2026-11-20T17:43:01+00:00',
                               'size': os.path.getsize(os.path.join(self.folder, 'low-prices.json'))})
        self.assertEqual(meta['datasets']['weather'], {'exists': False, 'updated': None, 'size': 0})

    def test_the_open_prices_say_how_many_zones_they_have(self):
        self.put('open-prices', {'updated': '2026-11-23T17:00:00+00:00', 'zones': {'DE-LU': {}, 'GB': {}, 'PT': {}}})
        info = build_meta.build({}, None, self.folder)['datasets']['open-prices']
        self.assertEqual((info['exists'], info['updated'], info['zones']), (True, '2026-11-23T17:00:00+00:00', 3))

    def test_a_dataset_kept_in_one_file_per_zone_is_one_entry_made_from_its_index(self):
        os.makedirs(os.path.join(self.folder, 'outages'))
        files = {'index': {'updated': '2026-11-20T17:41:13+00:00', 'zones': ['PT', 'ES', 'EE']},
                 'PT': {'zone': 'PT'}, 'ES': {'zone': 'ES', 'generation': {}}, 'FR': {'zone': 'FR'}}   # EE has no file; FR is not listed
        for name, doc in files.items():
            self.put(f'outages/{name}', doc)
        size = sum(os.path.getsize(os.path.join(self.folder, 'outages', f'{n}.json')) for n in ('index', 'PT', 'ES'))
        meta = build_meta.build({}, None, self.folder)
        self.assertEqual(meta['datasets']['outages'], {'exists': True, 'updated': '2026-11-20T17:41:13+00:00', 'zones': 3, 'size': size})
        self.assertEqual(meta['datasets']['forecast'], {'exists': False, 'updated': None, 'zones': 0, 'size': 0})

    def test_an_unreadable_file_counts_as_present_with_nothing_in_it(self):
        with open(os.path.join(self.folder, 'generation-mix.json'), 'w') as f:
            f.write('{broken')
        info = build_meta.build({}, None, self.folder)['datasets']['generation-mix']
        self.assertEqual((info['exists'], info['updated'], info['zones']), (True, None, 0))

    def test_outside_github_there_is_no_run_and_no_steps(self):
        meta = build_meta.build({}, None, self.folder)
        self.assertEqual(meta['trigger'], 'feed_local')
        self.assertNotIn('run', meta)
        self.assertNotIn('steps', meta)


if __name__ == '__main__':
    unittest.main()
