"""Tests for the yearly figures taken from Ember (fetch_ember.py).

Run from the repository root:  python -m unittest discover -s tests -v

tests/fixtures/ember/yearly-2026-11-17-excerpt.csv is Ember's file of the
edition of 17 November 2026, cut down to seven of the feed's countries (GBR,
PRT, MKD, ALB, NLD, XKX, FRA), the years from 2017 on, and rows the feed must
ignore: a country outside the list (USA) and two regions (EU, World). The
numbers the tests expect were taken from the file by hand (grep), not with
the code under test.
"""

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

import fetch_ember as fe
from feed_common import Response

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'ember')
SEVEN = {code: fe.ISO3[code] for code in ('GB', 'PT', 'MK', 'AL', 'NL', 'XK', 'FR')}
EDITION = 'Tue, 17 Nov 2026 18:34:40 GMT'


def excerpt():
    with open(os.path.join(FIXTURES, 'yearly-2026-11-17-excerpt.csv'), encoding='utf-8') as f:
        return f.read()


class WhatIsKept(unittest.TestCase):

    def setUp(self):
        self.countries = fe.read(excerpt(), 2026, SEVEN)

    def test_only_the_countries_asked_for_in_their_order(self):
        self.assertEqual(list(self.countries), list(SEVEN))
        self.assertEqual(self.countries['XK']['iso3'], 'XKX')

    def test_shares_of_the_last_complete_year(self):
        pt = self.countries['PT']
        self.assertEqual(pt['year'], 2025)
        self.assertEqual(pt['share_of_generation_pct'], {'renewables': 80.882, 'bioenergy': 6.618, 'nuclear': 0.0})
        self.assertEqual(self.countries['FR']['share_of_generation_pct'],
                         {'renewables': 26.636, 'bioenergy': 1.785, 'nuclear': 68.257})
        self.assertEqual(self.countries['GB']['share_of_generation_pct'],
                         {'renewables': 51.824, 'bioenergy': 13.939, 'nuclear': 12.388})

    def test_the_trend_is_the_seven_years_that_end_in_it(self):
        trend = self.countries['MK']['renewables_trend']
        self.assertEqual(trend['years'], [2019, 2020, 2021, 2022, 2023, 2024, 2025])
        self.assertEqual(trend['share_of_generation_pct'], [22.921, 27.919, 29.9, 27.566, 33.24, 40.482, 45.389])

    def test_a_year_that_is_not_over_is_not_the_last_complete_one(self):
        """An edition of 2025 would have 2025 unfinished: 2024 is the year."""
        mk = fe.read(excerpt(), 2025, SEVEN)['MK']
        self.assertEqual(mk['year'], 2024)
        self.assertEqual(mk['share_of_generation_pct']['renewables'], 40.482)
        self.assertEqual(mk['renewables_trend']['years'][0], 2018)

    def test_capacity_of_each_renewable_source(self):
        """The United Kingdom has no row of other renewables: it is not in the entry."""
        self.assertEqual(self.countries['GB']['capacity_gw'],
                         {'year': 2025, 'solar': 21.66, 'wind': 33.4, 'hydro': 2.19, 'bioenergy': 7.01})
        self.assertEqual(self.countries['PT']['capacity_gw'],
                         {'year': 2025, 'solar': 6.95, 'wind': 5.61, 'hydro': 8.37, 'bioenergy': 0.66,
                          'other_renewables': 0.04})

    def test_a_year_with_an_empty_cell_is_not_used_for_the_capacity(self):
        """The Netherlands' hydro of 2025 is empty: the capacity is 2024's, whole."""
        self.assertEqual(self.countries['NL']['capacity_gw'],
                         {'year': 2024, 'solar': 24.77, 'wind': 11.7, 'hydro': 0.04, 'bioenergy': 0.48,
                          'other_renewables': 0.4})
        self.assertEqual(self.countries['AL']['capacity_gw']['year'], 2024)

    def test_a_country_with_no_capacity_has_none(self):
        self.assertNotIn('capacity_gw', self.countries['XK'])
        self.assertEqual(self.countries['XK']['share_of_generation_pct']['renewables'], 10.752)

    def test_a_file_laid_out_otherwise_is_refused(self):
        with self.assertRaises(ValueError):
            fe.read(excerpt().replace('Share of generation (%)', 'Share', 1), 2026, SEVEN)

    def test_a_country_the_file_does_not_have_is_left_out(self):
        self.assertNotIn('DE', fe.read(excerpt(), 2026, dict(SEVEN, DE='DEU')))

    def test_the_file_written(self):
        edition = datetime(2026, 11, 17, 18, 34, 40, tzinfo=timezone.utc)
        doc = fe.build(excerpt(), edition, updated='2026-11-18T21:00:00+00:00')
        self.assertEqual([key for key in doc if key != 'countries'],
                         ['updated', 'license', 'source', 'url', 'file', 'edition'])
        self.assertEqual((doc['license'], doc['edition']), ('cc-by-4.0', '2026-11-17T18:34:40+00:00'))
        self.assertEqual(doc['file'], fe.URL)


class TheKeptFixture(unittest.TestCase):
    """tests/fixtures/ember/ember-yearly-2026-11-17.json, which the merge's
    tests read, is what this code made of the whole file of that edition."""

    def test_it_agrees_with_the_excerpt(self):
        with open(os.path.join(FIXTURES, 'ember-yearly-2026-11-17.json'), encoding='utf-8') as f:
            kept = json.load(f)
        self.assertEqual(list(kept['countries']), list(fe.ISO3))
        for code, entry in fe.read(excerpt(), 2026, SEVEN).items():
            self.assertEqual(kept['countries'][code], entry, code)


class Downloading(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, 'ember-yearly.json')
        self.asked = []

    def run_with(self, response, argv=(), all_countries=True):
        def get(url, timeout=None, headers=None):
            self.asked.append((url, dict(headers or {})))
            return response
        before = fe.ISO3
        fe.ISO3 = before if all_countries else SEVEN
        try:
            return fe.main(argv, get=get, path=self.path)
        finally:
            fe.ISO3 = before

    def answer(self):
        return Response(200, {'last-modified': EDITION}, excerpt().encode('utf-8'))

    def kept(self):
        with open(self.path, encoding='utf-8') as f:
            return json.load(f)

    def test_the_file_is_written_with_its_edition(self):
        self.assertEqual(self.run_with(self.answer(), all_countries=False), 0)
        doc = self.kept()
        self.assertEqual(doc['edition'], '2026-11-17T18:34:40+00:00')
        self.assertEqual(list(doc['countries']), list(SEVEN))
        self.assertEqual(self.asked, [(fe.URL, {})])

    def test_the_next_run_asks_only_for_a_newer_edition(self):
        self.run_with(self.answer(), all_countries=False)
        before = self.kept()
        self.assertEqual(self.run_with(Response(304, {}, b'', error='Not Modified'), all_countries=False), 0)
        self.assertEqual(self.asked[-1][1], {'If-Modified-Since': EDITION})
        self.assertEqual(self.kept(), before)

    def test_force_downloads_it_regardless(self):
        self.run_with(self.answer(), all_countries=False)
        self.run_with(self.answer(), argv=['--force'], all_countries=False)
        self.assertEqual(self.asked[-1][1], {})

    def test_a_file_that_lacks_a_country_is_not_kept(self):
        """The excerpt has seven of the 32 countries."""
        self.assertEqual(self.run_with(self.answer()), 1)
        self.assertFalse(os.path.exists(self.path))

    def test_no_answer_keeps_what_there_was(self):
        self.run_with(self.answer(), all_countries=False)
        before = self.kept()
        for response in (Response(None, error='timed out'), Response(503, {}, b'', error='Service Unavailable'),
                         Response(200, {}, excerpt().encode('utf-8')),               # no Last-Modified
                         Response(200, {'last-modified': EDITION}, b'not,the,file\n1,2,3\n')):
            self.assertEqual(self.run_with(response, argv=['--force'], all_countries=False), 1)
            self.assertEqual(self.kept(), before)


if __name__ == '__main__':
    unittest.main()
