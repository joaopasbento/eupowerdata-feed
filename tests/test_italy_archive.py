"""Tests for putting Italy's generation in the archive by the country
(backfill_archive.py --rewrite-italy-generation): one request for a year, and
each file's value taken at the instant the file is of.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are documents written by hand (tests/entsoe_xml.py).
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.parse
from datetime import date, timedelta
from unittest import mock

import backfill_archive as ba
import fetch_entsoe as fe
from entsoe_xml import NO_DATA, at, document, series

COUNTRY = '10YIT-GRTN-----B'


def long_answer(start='2026-08-31 00:00:00', hours=24 * 6):
    """A75 by the hour for six days from 31 Aug 2026: gas at 1000 + the hour's
    number, solar at 500 with the points left out (curveType A03), and pumps
    taking 300 from the grid."""
    return document(
        series(start, [1000.0 + n for n in range(hours)], psr='B04'),
        series(start, [500.0] + [None] * (hours - 1), psr='B16', curve='A03', end=at(start) + timedelta(hours=hours)),
        series(start, [300.0] * hours, psr='B10', out=True))


class TheHoursOfAnInstant(unittest.TestCase):

    def test_are_cut_out_as_entsoe_gives_them_when_asked_for_them_alone(self):
        all_series = fe.read_series(long_answer())
        cut = ba.window_of(all_series, at('2026-09-01 00:00:00'), at('2026-09-02 01:00:00'))
        gas, solar, pumps = cut
        self.assertEqual((gas['periods'][0]['start'], gas['periods'][0]['end']),
                         (at('2026-09-01 00:00:00'), at('2026-09-02 01:00:00')))
        self.assertEqual(gas['periods'][0]['points'][0], (1, 1024.0))            # the 25th hour of the answer
        self.assertEqual(len(gas['periods'][0]['points']), 25)
        self.assertEqual(solar['periods'][0]['points'], [(1, 500.0)])            # the value in force, as its first point
        self.assertTrue(pumps['out'])

    def test_the_mix_of_the_window_is_the_mix_of_the_instant(self):
        instant = at('2026-09-01 23:59:59')
        cut = ba.window_of(fe.read_series(long_answer()), at('2026-08-31 23:00:00'), at('2026-09-02 00:00:00'))
        got = fe.latest_generation(None, instant, cut)
        self.assertEqual(got['mix'], {'Fossil Gas': 1047.0, 'Solar': 500.0})      # 23:00 of 1 Sep is hour 47
        self.assertEqual(got['data_time'], '2026-09-01T23:00:00+00:00')

    def test_a_period_outside_the_window_is_left_out(self):
        self.assertEqual(ba.window_of(fe.read_series(long_answer()), at('2026-10-01 00:00:00'), at('2026-10-02 00:00:00')), [])


class InADirectory(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data/archive')

    def write(self, day, doc):
        with open(ba.GENERATION_FILES.format(day=day), 'w') as f:
            json.dump(doc, f)

    def read(self, day):
        with open(ba.GENERATION_FILES.format(day=day)) as f:
            return json.load(f)

    def rewrite(self, answer=None):
        asked = []

        def ask(url):
            asked.append(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)))
            return (long_answer() if answer is None else answer), None

        log = io.StringIO()
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(log):
            code = ba.rewrite_italy_generation('https://entsoe.test/api', ask=ask, sleep=lambda seconds: None)
        return code, asked, log.getvalue()


NORTH = {'mix': {'Fossil Gas': 400.0}, 'total_mw': 400.0, 'renewable_pct': 0}
FRANCE = {'mix': {'Nuclear': 40000.0}, 'total_mw': 40000.0, 'renewable_pct': 0}


class ItalyByTheCountry(InADirectory):

    def setUp(self):
        super().setUp()
        # A file of the backfill (the end of the UTC day) and one the regular runs wrote (the time of `updated`).
        self.write('2026-09-01', {'updated': '2026-10-02T03:00:00+00:00', 'date': '2026-09-01',
                                  'source': 'ENTSO-E Transparency Platform (backfilled)', 'zones': {'FR': FRANCE, 'IT': NORTH}})
        self.write('2026-09-03', {'updated': '2026-09-03T15:38:10+00:00', 'source': 'ENTSO-E Transparency Platform',
                                  'zones': {'FR': dict(FRANCE, fetched_at='2026-09-03T15:38:10+00:00'),
                                            'IT': dict(NORTH, fetched_at='2026-09-03T15:38:10+00:00')}})

    def test_each_file_has_the_country_at_the_instant_the_file_is_of(self):
        code, asked, _log = self.rewrite()
        self.assertEqual(code, 0)
        backfilled, live = self.read('2026-09-01'), self.read('2026-09-03')
        # 23:00 of 1 Sep is hour 47 of the answer; 15:00 of 3 Sep is hour 87.
        self.assertEqual(backfilled['zones']['IT'], {'mix': {'Fossil Gas': 1047.0, 'Solar': 500.0}, 'total_mw': 1547.0,
                                                    'renewable_pct': 32.3, 'data_time': '2026-09-01T23:00:00+00:00',
                                                    'eic': COUNTRY})
        italy = live['zones']['IT']
        self.assertEqual((italy['mix'], italy['data_time'], italy['eic']),
                         ({'Fossil Gas': 1087.0, 'Solar': 500.0}, '2026-09-03T15:00:00+00:00', COUNTRY))
        self.assertIn('fetched_at', italy)
        self.assertNotIn('fetched_at', backfilled['zones']['IT'])

    def test_one_request_for_the_year_by_the_country(self):
        _code, asked, _log = self.rewrite()
        self.assertEqual(len(asked), 1)
        self.assertEqual((asked[0]['documentType'], asked[0]['in_Domain']), ('A75', COUNTRY))
        # The days asked for and one more on each side, on the market's clock.
        self.assertEqual((asked[0]['periodStart'], asked[0]['periodEnd']), ('202608302200', '202609042200'))

    def test_nothing_else_in_the_file_changes(self):
        self.rewrite()
        backfilled, live = self.read('2026-09-01'), self.read('2026-09-03')
        self.assertEqual((backfilled['updated'], backfilled['date'], backfilled['zones']['FR']),
                         ('2026-10-02T03:00:00+00:00', '2026-09-01', FRANCE))
        self.assertEqual((live['updated'], live['zones']['FR']['fetched_at'], list(live['zones'])),
                         ('2026-09-03T15:38:10+00:00', '2026-09-03T15:38:10+00:00', ['FR', 'IT']))

    def test_a_file_already_done_is_left_alone(self):
        self.rewrite()
        code, asked, log = self.rewrite()
        self.assertEqual((code, asked), (0, []))
        self.assertIn('0 generation files with Italy by its North zone', log)

    def test_on_a_day_entsoe_has_nothing_for_the_country_italy_leaves_the_file(self):
        only_the_third = document(series('2026-09-02 15:00:00', [2000.0] * 30, psr='B04'))
        _code, _asked, log = self.rewrite(only_the_third)
        self.assertEqual(list(self.read('2026-09-01')['zones']), ['FR'])
        self.assertEqual(self.read('2026-09-03')['zones']['IT']['mix'], {'Fossil Gas': 2000.0})
        self.assertIn('Days Italy left, because ENTSO-E has no generation for the country: 1 (2026-09-01)', log)

    def test_the_days_the_country_was_already_asked_for_are_not_touched(self):
        self.write('2026-11-18', {'updated': '2026-11-18T23:50:00+00:00', 'zones': {'IT': NORTH}})
        self.write('2026-09-05', {'updated': '2026-11-18T21:00:00+00:00', 'date': '2026-09-05',
                                  'source': 'ENTSO-E Transparency Platform (backfilled)', 'zones': {'IT': NORTH}})
        self.assertEqual(sorted(str(day) for day in ba.italy_to_do()), ['2026-09-01', '2026-09-03'])

    def test_a_year_that_cannot_be_had_is_left_alone_and_the_run_fails(self):
        asked = []

        def ask(url):
            asked.append(url)
            return None, 'timed out'

        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(io.StringIO()):
            code = ba.rewrite_italy_generation('https://entsoe.test/api', ask=ask, sleep=lambda seconds: None)
        self.assertEqual(code, 1)
        self.assertEqual(self.read('2026-09-01')['zones']['IT'], NORTH)
        self.assertEqual(len(asked), 1 + len(ba.REWRITE_WAITS))               # repeated, never taken for "no data"

    def test_an_answer_with_no_data_is_believed_only_for_a_month_or_less(self):
        # Three days: believed, and Italy leaves the files.
        code, asked, _log = self.rewrite(NO_DATA)
        self.assertEqual((code, len(asked)), (0, 1))
        self.assertEqual(list(self.read('2026-09-01')['zones']), ['FR'])

    def test_a_longer_one_with_no_data_is_asked_again_in_halves(self):
        self.write('2026-03-01', {'updated': '2026-03-02T03:00:00+00:00', 'date': '2026-03-01',
                                  'source': 'ENTSO-E Transparency Platform (backfilled)', 'zones': {'IT': NORTH}})
        _code, asked, _log = self.rewrite(NO_DATA)
        self.assertGreater(len(asked), 2)                # 1 Mar to 3 Sep, then its halves, down to a month


if __name__ == '__main__':
    unittest.main()
