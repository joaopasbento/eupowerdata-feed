"""Tests for what fetch_eafo.py does when EAFO does not answer for a country:
the country keeps its published block, marked carried_over, for 35 days from
when it was obtained. On the real settings of EAFO's page of Slovenia
(tests/fixtures/eafo, taken on 26 Nov 2026), the part the feed reads. No network.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import gzip
import io
import json
import os
import tempfile
import unittest
import urllib.error
import unittest.mock
from datetime import datetime, timezone

import fetch_eafo as fe

HERE = os.path.dirname(os.path.abspath(__file__))
NOW = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)


def drilldown():
    with open(os.path.join(HERE, 'fixtures', 'eafo', 'slovenia_drupal_settings.json.gz'), 'rb') as f:
        return json.loads(gzip.decompress(f.read()))['drilldown']


def old(updated):
    return {'source': 'EAFO', 'metric': 'bev_registrations_m1', 'granularity': 'monthly', 'latest_period': '2026-09',
            'bev_monthly': [{'period': '2026-08', 'value': 1}, {'period': '2026-09', 'value': 2}], 'updated': updated}


class EAFO(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')
        # As fetch_eurostat.py leaves it: every country with its vehicles block, no eafo yet.
        doc = {'countries': {code: {'vehicles': {'unit': 'x'}} for code in fe.SLUGS}}
        with open(fe.OUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(doc, f)

    def run_with(self, answers, previous):
        def fetch(slug):
            code = {s: c for c, s in fe.SLUGS.items()}[slug]
            if answers.get(code) == 'error':
                raise urllib.error.HTTPError('u', 503, 'Service Unavailable', {}, None)
            return answers.get(code)
        with contextlib.redirect_stdout(io.StringIO()) as log, unittest.mock.patch.object(fe.time, 'sleep'):
            fe.main(previous=previous, now=NOW, fetch=fetch)
        with open(fe.OUT_FILE, encoding='utf-8') as f:
            return json.load(f)['countries'], log.getvalue()

    def test_a_country_that_fails_keeps_its_block_with_its_date_and_the_mark(self):
        previous = {'ES': old('2026-11-19T05:00:00+00:00'), 'HR': old('2026-10-01T05:00:00+00:00')}
        countries, log = self.run_with({'SI': drilldown(), 'ES': 'error', 'HR': 'error'}, previous)
        self.assertNotIn('carried_over', countries['SI']['vehicles']['eafo'])
        self.assertEqual(countries['SI']['vehicles']['eafo']['source'], 'EAFO')
        kept = countries['ES']['vehicles']['eafo']
        self.assertEqual((kept['updated'], kept['carried_over'], kept['bev_monthly']),
                         ('2026-11-19T05:00:00+00:00', True, previous['ES']['bev_monthly']))
        self.assertNotIn('eafo', countries['HR']['vehicles'])            # 56 days old: past the 35
        self.assertIn('EAFO: kept, not obtained in this run: ES (from 2026-11-19)', log)

    def test_when_every_country_fails_the_kept_blocks_are_still_published(self):
        previous = {'PT': old('2026-11-20T05:00:00+00:00')}
        countries, _log = self.run_with({code: 'error' for code in fe.SLUGS}, previous)
        self.assertTrue(countries['PT']['vehicles']['eafo']['carried_over'])

    def test_thirty_five_days(self):
        self.assertIsNotNone(fe.kept_block(old('2026-10-23T10:00:01+00:00'), NOW))
        self.assertIsNone(fe.kept_block(old('2026-10-22T10:00:00+00:00'), NOW))


if __name__ == '__main__':
    unittest.main()
