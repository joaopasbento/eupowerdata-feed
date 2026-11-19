"""Tests for the hours of negative, zero and low prices and the price
difference between Portugal and Spain (fetch_low_prices.py), and for the
prices at the market's resolution that fetch_entsoe.py keeps for them
(price_intervals.py). On real files of OMIE (tests/fixtures/omie) and real
answers of ENTSO-E (tests/fixtures/entsoe, 18 Nov 2026).

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers were counted from the files apart from the code under
test (the rows read as plain text).
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import feed_common as fc
import fetch_entsoe as fe
import fetch_low_prices as lp
import fetch_omie
import price_intervals as pi
from entsoe_real import sample
from entsoe_xml import NO_DATA, price_document, price_series
from test_carry_over import Feed as EntsoeFeed

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'omie')
QUARTER = Decimal('0.25')


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def omie_file(name):
    with open(os.path.join(FIXTURES, name), encoding='latin-1', newline='') as f:
        return f.read()


def record(name, day, version=1):
    file_day, rows = lp.parse_marginal(omie_file(name))
    assert file_day in (None, day)
    return lp.market_record(day, rows, version)


class Omie:
    """OMIE for a test: the kept files by their name, 404 for any other;
    `down` makes every request fail. `asked` has the names asked for."""

    def __init__(self, down=False, without=()):
        self.asked, self.down, self.without = [], down, set(without)

    def __call__(self, url, timeout=None, **more):
        name = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))['filename']
        self.asked.append(name)
        if self.down:
            return fc.Response(error='timed out')
        path = os.path.join(FIXTURES, name)
        if name in self.without or not os.path.exists(path):
            return fc.Response(404, {}, b'<html>Page not found', error='Not Found')
        with open(path, 'rb') as f:
            return fc.Response(200, {}, f.read())


class TheCount(unittest.TestCase):

    def test_negative_is_below_zero_zero_is_zero_and_low_is_up_to_five(self):
        prices = [Decimal(p) for p in ('-0.01', '0', '0.00', '0.01', '5', '5.01', '80')]
        count = lp.tally(prices, QUARTER)
        self.assertEqual((count['negative_h'], count['zero_h'], count['low_h']), (0.25, 0.5, 0.5))
        self.assertEqual((count['min'], count['max']), (-0.01, 80))

    def test_an_hourly_period_counts_an_hour_and_a_quarter_hour_a_quarter(self):
        self.assertEqual(lp.tally([Decimal(-1)] * 3, Decimal(1))['negative_h'], 3)
        self.assertEqual(lp.tally([Decimal(-1)] * 3, QUARTER)['negative_h'], 0.75)


class OmiesFiles(unittest.TestCase):

    def test_the_day_ahead_market_of_1_feb_2026(self):
        got = record('marginalpdbc_20260201.1', date(2026, 2, 1))
        self.assertEqual({k: got[k] for k in ('resolution_minutes', 'first_period', 'periods', 'hours', 'version')},
                         {'resolution_minutes': 15, 'first_period': 1, 'periods': 96, 'hours': 24, 'version': 1})
        # Portugal: every quarter-hour between 0.01 and 3.78. Spain: 60 of them up to 5.
        self.assertEqual(got['PT'], {'negative_h': 0, 'zero_h': 0, 'low_h': 24, 'min': 0.01, 'avg': 1.04, 'max': 3.78})
        self.assertEqual(got['ES'], {'negative_h': 0, 'zero_h': 0, 'low_h': 15, 'min': 0.09, 'avg': 24.58, 'max': 131.8})
        spread = got['spread']
        self.assertEqual({k: v for k, v in spread.items() if k != 'values'},
                         {'differ_periods': 66, 'differ_h': 16.5, 'mean': -23.54, 'mean_abs': 23.54, 'max_abs': 130.5})
        self.assertEqual((len(spread['values']), spread['values'][:4]), (96, [0, -1.28, 0, 0]))

    def test_the_first_auction_of_the_same_day(self):
        got = record('marginalpibc_2026020101.1', date(2026, 2, 1))
        self.assertEqual({k: got['PT'][k] for k in lp.COUNTED}, {'negative_h': 6, 'zero_h': 14.75, 'low_h': 3.25})
        self.assertEqual({k: got['ES'][k] for k in lp.COUNTED}, {'negative_h': 6, 'zero_h': 1, 'low_h': 7.75})
        self.assertEqual((got['spread']['differ_periods'], got['spread']['max_abs']), (67, 118.7))

    def test_the_third_auction_covers_the_last_twelve_hours_of_the_day(self):
        got = record('marginalpibc_2026020103.1', date(2026, 2, 1))
        self.assertEqual((got['resolution_minutes'], got['first_period'], got['periods'], got['hours']), (15, 49, 48, 12))
        self.assertEqual((got['PT']['negative_h'], got['PT']['max'], got['ES']['negative_h']), (12, -0.02, 0))
        self.assertEqual(got['spread']['differ_periods'], 48)
        self.assertEqual(got['spread']['values'][0], -7.7)        # Portugal's price minus Spain's

    def test_the_day_of_23_hours(self):
        day = date(2026, 3, 29)
        ahead, third = record('marginalpdbc_20260329.1', day), record('marginalpibc_2026032903.1', day)
        self.assertEqual((ahead['periods'], ahead['hours'], ahead['PT']['negative_h']), (92, 23, 10.5))
        self.assertEqual((third['first_period'], third['periods'], third['hours']), (45, 48, 12))
        self.assertEqual((third['ES']['min'], third['spread']['differ_periods'], third['spread']['max_abs']), (-35, 23, 33))

    def test_the_day_of_25_hours(self):
        day = date(2026, 10, 25)
        ahead, third = record('marginalpdbc_20261025.1', day), record('marginalpibc_2026102503.1', day)
        self.assertEqual((ahead['periods'], ahead['hours'], ahead['PT']['negative_h'], ahead['PT']['zero_h']),
                         (100, 25, 6.25, 0.75))
        self.assertEqual((third['first_period'], third['periods'], third['hours']), (53, 48, 12))

    def test_an_hourly_day_before_october_2025(self):
        got = record('marginalpdbc_20250930.1', date(2025, 9, 30))
        self.assertEqual((got['resolution_minutes'], got['periods'], got['hours'], got['PT']['avg']), (60, 24, 24, 82.8))

    def test_an_auction_with_no_result_is_a_file_with_no_rows(self):
        self.assertEqual(lp.parse_marginal(omie_file('marginalpibc_2026081802.1')), (None, []))
        self.assertEqual(record('marginalpibc_2026081802.1', date(2026, 8, 18)),
                         {'periods': 0, 'hours': 0, 'version': 1, 'empty': True})

    def test_a_price_written_without_its_zero_is_read(self):
        # The auctions' file writes Spain's ".00" and "-.81".
        _day, rows = lp.parse_marginal('MARGINALPIBC;\n2026;10;25;53;-0.81;-.81;\n2026;10;25;54;0.00;.00;\n*\n')
        self.assertEqual(rows, [(53, Decimal('-0.81'), Decimal('-0.81')), (54, Decimal('0'), Decimal('0'))])

    def test_a_file_that_is_not_what_it_should_be_is_refused(self):
        for text in ('MARGINALPIBC;\n2026;02;01;1;5;5;\n2026;02;01;3;5;5;\n*\n',          # a period missing
                     'MARGINALPIBC;\n2026;02;01;1;5;5;\n2026;02;02;2;5;5;\n*\n',          # two days
                     'MARGINALPIBC;\n2026;02;01;1;abc;5;\n*\n'):
            with self.assertRaises(fetch_omie.OmieFormatError):
                lp.parse_marginal(text)
        with self.assertRaises(fetch_omie.OmieFormatError):                             # 50 periods fit no day
            lp.market_record(date(2026, 2, 1), [(n, Decimal(1), Decimal(1)) for n in range(1, 51)], 1)


class AskingOmie(unittest.TestCase):

    def test_the_address_of_each_market(self):
        omie = Omie()
        with contextlib.redirect_stdout(io.StringIO()):
            lp.fetch_market(date(2026, 2, 1), 'day_ahead', omie, pause=0)
            lp.fetch_market(date(2026, 2, 1), 'ida3', omie, pause=0)
        self.assertEqual(omie.asked, ['marginalpdbc_20260201.1', 'marginalpibc_2026020103.1'])

    def test_a_republished_day_is_found_under_its_next_version(self):
        omie = Omie()
        with contextlib.redirect_stdout(io.StringIO()):
            got = lp.fetch_market(date(2025, 11, 27), 'day_ahead', omie, pause=0)
        self.assertEqual((omie.asked, got['version']), (['marginalpdbc_20251127.1', 'marginalpdbc_20251127.2'], 2))

    def test_no_file_is_missing_and_no_answer_is_none(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(lp.fetch_market(date(2026, 3, 17), 'ida1', Omie(), pause=0), 'missing')
            self.assertIsNone(lp.fetch_market(date(2026, 2, 1), 'ida1', Omie(down=True), pause=0))


class InADirectory(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data/archive')

    def doc(self, path=lp.OUT_PATH):
        with open(path, encoding='utf-8') as f:
            return json.load(f)

    def run_at(self, when, omie=None, limit=10000):
        omie = omie or Omie()
        log = io.StringIO()
        with contextlib.redirect_stdout(log):
            code = lp.main(now=when, get=omie, budget=fc.Budget(600), pause=0, limit=limit)
        return code, omie, log.getvalue()


MORNING = at('2026-02-02 09:00:00')        # 2 Feb 2026, 10:00 in Madrid: before the day-ahead market of the 3rd is out


class ARun(InADirectory):

    def test_writes_the_two_blocks_each_with_its_licence(self):
        code, _omie, _log = self.run_at(MORNING)
        doc = self.doc()
        self.assertEqual(code, 0)
        self.assertEqual(list(doc), ['updated', 'data_time', 'timezone', 'thresholds', 'omie', 'entsoe'])
        self.assertEqual((doc['omie']['license'], doc['entsoe']['license']), ('omie', 'source-terms'))
        self.assertEqual(doc['thresholds'], {'negative': 'price < 0', 'zero': 'price = 0', 'low': '0 < price <= 5',
                                             'unit': 'EUR/MWh'})
        self.assertEqual((doc['updated'], doc['omie']['fetched_at']), ('2026-02-02T09:00:00+00:00',) * 2)

    def test_yesterday_has_the_day_ahead_market_and_the_three_auctions(self):
        self.run_at(MORNING)
        day = self.doc()['omie']['days']['2026-02-01']
        self.assertEqual(list(day), ['day_ahead', 'ida1', 'ida2', 'ida3'])
        self.assertEqual((day['day_ahead']['PT']['low_h'], day['ida1']['PT']['zero_h'], day['ida2']['PT']['negative_h'],
                          day['ida3']['hours']), (24, 14.75, 23.5, 12))
        self.assertEqual(len(day['ida3']['spread']['values']), 48)

    def test_the_totals_of_the_year_are_over_the_hours_of_the_days_read(self):
        self.run_at(MORNING)
        year = self.doc()['omie']['year']
        self.assertEqual((year['year'], year['from'], year['to']), (2026, '2026-01-01', '2026-02-02'))
        ahead = year['day_ahead']
        self.assertEqual((ahead['days'], ahead['hours']), (1, 24))
        self.assertEqual(ahead['PT'], {'negative_h': 0, 'zero_h': 0, 'low_h': 24, 'negative_pct': 0.0, 'zero_pct': 0.0,
                                       'low_pct': 100.0, 'negative_or_zero_pct': 0.0})
        self.assertEqual(ahead['ES']['low_pct'], 62.5)                 # 15 of 24 hours
        self.assertEqual(ahead['spread'], {'periods': 96, 'differ_periods': 66, 'differ_h': 16.5, 'differ_pct': 68.75,
                                           'mean_abs': 23.54})
        # IDA1 of 1 Feb: negative for 6 hours and zero for 14.75, of 24.
        self.assertEqual(year['ida1']['PT']['negative_or_zero_pct'], 86.46)
        self.assertEqual((year['ida3']['hours'], year['ida3']['PT']['negative_pct']), (12, 100.0))
        self.assertEqual(self.doc()['omie']['months']['2026-02']['day_ahead'], ahead)
        self.assertEqual(self.doc()['omie']['months']['2026-01'], {})

    def test_each_day_has_its_record_in_the_archive(self):
        self.run_at(MORNING)
        saved = self.doc(lp.ARCHIVE_PATH.format(day='2026-02-01'))
        self.assertEqual(list(saved), ['date', 'omie'])
        self.assertEqual((saved['date'], saved['omie']['license']), ('2026-02-01', 'omie'))
        self.assertEqual(saved['omie']['day_ahead'], self.doc()['omie']['days']['2026-02-01']['day_ahead'])

    def test_a_file_omie_never_published_is_missing_and_one_not_out_yet_is_asked_for_again(self):
        _code, omie, _log = self.run_at(MORNING)
        old = self.doc(lp.ARCHIVE_PATH.format(day='2026-01-10'))['omie']
        self.assertEqual(old['day_ahead'], {'missing': True})
        today = self.doc(lp.ARCHIVE_PATH.format(day='2026-02-02'))['omie']
        self.assertEqual(today['day_ahead'], {'pending': True, 'checked': '2026-02-02T09:00:00+00:00'})
        # A quarter of an hour later nothing is asked for; an hour later, what was not out is.
        _code, omie, _log = self.run_at(MORNING + timedelta(minutes=15))
        self.assertEqual(omie.asked, [])
        _code, omie, _log = self.run_at(MORNING + timedelta(minutes=60))
        self.assertIn('marginalpdbc_20260202.1', omie.asked)
        # ...but only what is of the last seven days: what OMIE never published is not asked for again.
        self.assertEqual({name.split('_')[1][:8] for name in omie.asked},
                         {f'202601{day}' for day in range(26, 32)} | {'20260202'})

    def test_the_day_after_is_asked_for_from_one_in_the_afternoon(self):
        _code, omie, _log = self.run_at(MORNING)
        self.assertNotIn('marginalpdbc_20260203.1', omie.asked)
        _code, omie, _log = self.run_at(at('2026-02-02 12:30:00'))          # 13:30 in Madrid
        self.assertIn('marginalpdbc_20260203.1', omie.asked)

    def test_the_time_omie_was_last_read_stays_when_a_run_reads_nothing_of_it(self):
        self.run_at(MORNING)
        os.makedirs('data', exist_ok=True)
        pi.save({'ES': [(at('2026-02-01 23:00'), timedelta(hours=1), Decimal('40'))]}, date(2026, 2, 2),
                MORNING + timedelta(minutes=15))                    # new prices of the day: the file is written again
        _code, omie, _log = self.run_at(MORNING + timedelta(minutes=15))
        self.assertEqual(omie.asked, [])
        self.assertEqual((self.doc()['updated'], self.doc()['omie']['fetched_at']),
                         ('2026-02-02T09:15:00+00:00', '2026-02-02T09:00:00+00:00'))

    def test_the_auctions_of_the_day_in_course_are_not_asked_for(self):
        _code, omie, _log = self.run_at(MORNING)
        self.assertFalse([name for name in omie.asked if name.startswith('marginalpibc_20260202')])
        self.assertIn('marginalpibc_2026020101.1', omie.asked)

    def test_a_run_asks_for_thirty_files_at_most_the_newest_days_first(self):
        _code, omie, log = self.run_at(MORNING, limit=None)
        self.assertEqual(omie.asked[:1], ['marginalpdbc_20260202.1'])
        self.assertIn('marginalpibc_2026020103.1', omie.asked)
        self.assertIn('files of earlier days left for the next runs', log)
        days = {name.split('_')[1][:8] for name in omie.asked}
        self.assertLess(len(days), 12)

    def test_with_omie_not_answering_the_records_stay_and_nothing_is_rewritten(self):
        self.run_at(MORNING)
        with open(lp.OUT_PATH, 'rb') as f:
            before = (f.read(), os.stat(lp.OUT_PATH).st_ino)
        code, omie, log = self.run_at(MORNING + timedelta(hours=2), Omie(down=True))
        with open(lp.OUT_PATH, 'rb') as f:
            self.assertEqual((f.read(), os.stat(lp.OUT_PATH).st_ino), before)
        self.assertEqual((code, len(omie.asked)), (0, 1))                # one failure and it does not insist
        self.assertIn('left exactly as it was', log)


class ADayOmieCorrectsAfterwards(InADirectory):
    """Once a day, the next version of each market of the last ten days is asked for."""

    NOW = at('2025-11-28 09:00:00')
    DAY = date(2025, 11, 27)                           # OMIE's file of this day is a version 2

    def setUp(self):
        super().setUp()
        # A record read from a version 1 that said the whole day was at zero.
        _day, rows = lp.parse_marginal(omie_file('marginalpdbc_20251127.2'))
        first = lp.market_record(self.DAY, [(period, Decimal(0), Decimal(0)) for period, _pt, _es in rows], 1)
        with open(lp.ARCHIVE_PATH.format(day='2025-11-27'), 'w') as f:
            json.dump({'date': '2025-11-27', 'omie': {'license': 'omie', 'source': 's', 'day_ahead': first}}, f)

    def run_here(self, when, omie=None):
        return self.run_at(when, omie, limit=0)        # none of the run's ordinary work with OMIE

    def test_a_newer_version_is_read_and_the_totals_follow(self):
        code, omie, log = self.run_here(self.NOW)
        saved = self.doc(lp.ARCHIVE_PATH.format(day='2025-11-27'))['omie']['day_ahead']
        self.assertEqual(code, 0)
        self.assertEqual(omie.asked, ['marginalpdbc_20251127.2'])           # the only market there is a record of
        self.assertEqual((saved['version'], saved['PT']['avg'], saved['PT']['zero_h']), (2, 86.02, 0))
        self.assertIn('OMIE has version 2 (the record had 1) - read again', log)
        november = self.doc()['omie']['months']['2025-11']['day_ahead']
        self.assertEqual((november['days'], november['PT']['zero_h']), (1, 0))

    def test_it_is_done_once_a_day(self):
        self.run_here(self.NOW)
        self.assertEqual(self.doc()['omie']['version_check'], {'date': '2025-11-28', 'pending': []})
        _code, omie, _log = self.run_here(self.NOW + timedelta(hours=1))
        self.assertEqual(omie.asked, [])
        _code, omie, _log = self.run_here(self.NOW + timedelta(days=1))
        self.assertEqual(omie.asked, ['marginalpdbc_20251127.3'])           # the day after, the version after that

    def test_with_no_newer_version_the_record_stays(self):
        _code, _omie, _log = self.run_here(self.NOW, Omie(without=['marginalpdbc_20251127.2']))
        saved = self.doc(lp.ARCHIVE_PATH.format(day='2025-11-27'))['omie']['day_ahead']
        self.assertEqual((saved['version'], saved['PT']['zero_h']), (1, 24))

    def test_with_omie_not_answering_what_is_left_waits_for_the_next_run(self):
        _code, omie, _log = self.run_here(self.NOW, Omie(down=True))
        self.assertEqual(len(omie.asked), 1)                                # it does not insist
        self.assertEqual(self.doc()['omie']['version_check']['pending'][0], ['2025-11-27', 'day_ahead'])
        _code, omie, _log = self.run_here(self.NOW + timedelta(minutes=15))
        self.assertEqual(omie.asked, ['marginalpdbc_20251127.2'])
        self.assertEqual(self.doc(lp.ARCHIVE_PATH.format(day='2025-11-27'))['omie']['day_ahead']['version'], 2)

    def test_only_the_last_ten_days_are_looked_at(self):
        _code, omie, _log = self.run_here(self.NOW + timedelta(days=10))
        self.assertEqual(omie.asked, [])


class TheOtherZones(InADirectory):

    def prices(self, zone, values, day='2026-02-01'):
        """A zone of the archive of the hourly prices."""
        return {'prices': [{'time': f'{hour:02d}:00', 'hour': hour, 'price': price} for hour, price in values],
                'delivery_date': day}

    def archive(self, day, zones):
        with open(lp.PRICE_ARCHIVE.format(day=day), 'w') as f:
            json.dump({'zones': zones}, f)

    def test_a_past_day_is_counted_from_the_hourly_prices_of_the_archive(self):
        hours = [(h, p) for h, p in enumerate([-5, -0.01, 0, 0, 0.01, 5, 5.01] + [60] * 17)]
        self.archive('2026-02-01', {'DE': self.prices('DE', hours)})
        self.run_at(MORNING)
        saved = self.doc(lp.ARCHIVE_PATH.format(day='2026-02-01'))['entsoe']
        self.assertEqual(saved['license'], 'source-terms')
        self.assertEqual(saved['zones']['DE'], {'resolution_minutes': 60, 'periods': 24, 'hours': 24, 'negative_h': 2,
                                               'zero_h': 2, 'low_h': 2, 'min': -5, 'basis': 'hourly-means'})
        year = self.doc()['entsoe']['zones']['DE']['year']
        self.assertEqual((year['days'], year['hours'], year['negative_or_zero_pct'], year['low_pct']), (1, 24, 16.67, 8.33))
        self.assertEqual((year['market_resolution_days'], year['hourly_days']), (0, 1))

    def test_on_the_day_of_25_hours_the_entry_of_two_o_clock_stands_for_two(self):
        self.archive('2026-10-25', {'DE': self.prices('DE', [(h, -1 if h == 2 else 50) for h in range(24)], '2026-10-25')})
        count = lp.counts_from_archive(date(2026, 10, 25))['DE']
        self.assertEqual((count['hours'], count['negative_h']), (25, 2))

    def test_the_day_in_course_is_counted_at_the_markets_resolution(self):
        today = date(2026, 2, 2)
        start = at('2026-02-01 23:00')
        quarter = timedelta(minutes=15)
        spain = [(start + quarter * i, quarter, Decimal('-1') if i < 3 else Decimal('0') if i < 5 else Decimal('40'))
                 for i in range(96)]
        pi.save({'ES': spain}, today, MORNING)
        # By the hour the first hour averages 9.25 and the second 30: not one hour negative or zero.
        self.archive('2026-02-02', {'ES': self.prices('ES', [(0, 9.25), (1, 30)] + [(h, 40) for h in range(2, 24)], '2026-02-02')})
        self.run_at(MORNING)
        zone = self.doc()['entsoe']['zones']['ES']
        self.assertEqual(zone['day'], {'resolution_minutes': 15, 'periods': 96, 'hours': 24, 'negative_h': 0.75,
                                       'zero_h': 0.5, 'low_h': 0, 'min': -1, 'basis': 'market-resolution'})
        self.assertEqual((zone['year']['market_resolution_days'], zone['year']['negative_h']), (1, 0.75))
        self.assertEqual((self.doc()['entsoe']['delivery_date'], self.doc()['entsoe']['fetched_at']),
                         ('2026-02-02', '2026-02-02T09:00:00+00:00'))

    def test_moldova_is_counted_from_1_july_2026_and_never_on_a_day_of_one_hour(self):
        hours = [(h, -5) for h in range(24)]
        for day in ('2026-06-30', '2026-07-01', '2026-07-02'):
            self.archive(day, {'MD': self.prices('MD', hours if day != '2026-07-02' else [(23, -5)], day),
                               'RO': self.prices('RO', hours, day)})
        self.run_at(at('2026-07-03 09:00:00'))
        saved = {day: list(self.doc(lp.ARCHIVE_PATH.format(day=day))['entsoe']['zones'])
                 for day in ('2026-06-30', '2026-07-01', '2026-07-02')}
        self.assertEqual(saved, {'2026-06-30': ['RO'], '2026-07-01': ['RO', 'MD'], '2026-07-02': ['RO']})
        zones = self.doc()['entsoe']['zones']
        self.assertEqual((zones['MD']['year']['days'], zones['RO']['year']['days']), (1, 3))

    def test_a_record_written_before_that_rule_does_not_count_for_the_total(self):
        count = {'resolution_minutes': 60, 'periods': 24, 'hours': 24, 'negative_h': 24, 'zero_h': 0, 'low_h': 0,
                 'min': -5, 'basis': 'hourly-means'}
        totals = lp.entsoe_totals({date(2026, 3, 1): {'entsoe': {'zones': {'MD': count, 'RO': count}}}}, [date(2026, 3, 1)])
        self.assertEqual(list(totals), ['RO'])

    def test_a_country_of_two_zones_is_their_mean_interval_by_interval(self):
        today = date(2026, 2, 2)
        start, hour = at('2026-02-01 23:00'), timedelta(hours=1)
        north = [(start + hour * i, hour, Decimal('-10') if i == 0 else Decimal('30')) for i in range(24)]
        south = [(start + hour * i, hour, Decimal('4') if i == 0 else Decimal('-30') if i == 1 else Decimal('30')) for i in range(24)]
        pi.save({'NO1': north, 'NO2': south}, today, MORNING)
        counts = lp.counts_from_intervals(pi.read()[1])
        # 00:00: (-10 + 4) / 2 = -3, negative. 01:00: (30 - 30) / 2 = 0, zero.
        self.assertEqual((counts['NO']['negative_h'], counts['NO']['zero_h'], counts['NO']['min']), (1, 1, -3))
        self.assertNotIn('NO1', counts)


class CountingAYearAgain(InADirectory):
    """--refill-entsoe: one request for each zone for the year, counted at the market's resolution."""

    NOW = at('2026-01-04 10:00:00')                   # the year so far: 1 to 3 January

    def answer(self, quarters_by_day):
        """A44 for a zone: a list of 96 quarter-hourly prices for each day from 1 January."""
        prices = [price for day in quarters_by_day for price in day]
        return price_document(price_series('2025-12-31 23:00', prices, minutes=15))

    def refill(self, answers):
        asked = []

        def ask(url):
            params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
            asked.append(params)
            return answers.get(params['in_Domain'], NO_DATA), None

        log = io.StringIO()
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(log):
            code = lp.refill_entsoe('https://entsoe.test/api', now=self.NOW, ask=ask, sleep=lambda seconds: None)
        return code, asked, log.getvalue()

    def record(self, day, zones, omie=None):
        with open(lp.ARCHIVE_PATH.format(day=day), 'w') as f:
            json.dump({'date': day, **({'omie': omie} if omie else {}),
                       'entsoe': {'license': 'source-terms', 'source': 's', 'zones': zones}}, f)

    HOURLY = {'resolution_minutes': 60, 'periods': 24, 'hours': 24, 'negative_h': 0, 'zero_h': 0, 'low_h': 0,
              'min': 9.25, 'basis': 'hourly-means'}

    def test_every_day_is_counted_at_the_markets_resolution(self):
        # 1 Jan: three quarter-hours below zero in an hour whose mean is not. 2 Jan: nothing low. 3 Jan: no data.
        first = [-1.0] * 3 + [40.0] * 93
        self.record('2026-01-01', {'ES': self.HOURLY, 'FR': self.HOURLY})
        self.record('2026-01-02', {'ES': self.HOURLY})
        code, asked, log = self.refill({fe.ZONES['ES']: self.answer([first, [40.0] * 96])})
        saved = self.doc(lp.ARCHIVE_PATH.format(day='2026-01-01'))['entsoe']['zones']
        self.assertEqual(code, 0)
        self.assertEqual(saved, {'ES': {'resolution_minutes': 15, 'periods': 96, 'hours': 24, 'negative_h': 0.75,
                                        'zero_h': 0, 'low_h': 0, 'min': -1, 'basis': 'market-resolution'}})
        self.assertEqual(self.doc(lp.ARCHIVE_PATH.format(day='2026-01-02'))['entsoe']['zones']['ES']['basis'],
                         'market-resolution')
        self.assertIn("zone-days before, by basis: {'hourly-means': 3}", log)

    def test_a_count_that_is_not_published_ends_in_failure(self):
        """The records are written but the push did not go through: the run must not pass for done."""
        self.record('2026-01-01', {'ES': self.HOURLY})
        answers = {fe.ZONES['ES']: self.answer([[40.0] * 96, [40.0] * 96])}

        def ask(url):
            params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
            return answers.get(params['in_Domain'], NO_DATA), None

        for published, code in ((False, 1), (True, 0), (None, 0)):
            self.record('2026-01-01', {'ES': self.HOURLY})
            with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(lp.refill_entsoe('https://entsoe.test/api', now=self.NOW, ask=ask,
                                                  sleep=lambda seconds: None, commit=lambda message: published), code)

    def test_one_request_for_each_zone_for_the_year(self):
        self.record('2026-01-01', {'ES': self.HOURLY})
        _code, asked, _log = self.refill({fe.ZONES['ES']: self.answer([[40.0] * 96] * 3)})
        self.assertEqual(len(asked), len(fe.ZONES) - 1)                       # every zone but the United Kingdom
        spain = [p for p in asked if p['in_Domain'] == fe.ZONES['ES']]
        self.assertEqual((spain[0]['documentType'], spain[0]['periodStart'], spain[0]['periodEnd']),
                         ('A44', '202512302300', '202601042300'))

    def test_the_omie_part_of_a_record_is_not_touched(self):
        omie = {'license': 'omie', 'day_ahead': {'periods': 96}}
        self.record('2026-01-01', {'ES': self.HOURLY}, omie)
        self.refill({fe.ZONES['ES']: self.answer([[40.0] * 96] * 3)})
        self.assertEqual(self.doc(lp.ARCHIVE_PATH.format(day='2026-01-01'))['omie'], omie)

    def test_a_zone_that_cannot_be_had_stops_it_and_nothing_is_written(self):
        self.record('2026-01-01', {'ES': self.HOURLY})

        def ask(url):
            return None, 'timed out'

        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(io.StringIO()):
            code = lp.refill_entsoe('https://entsoe.test/api', now=self.NOW, ask=ask, sleep=lambda seconds: None)
        self.assertEqual(code, 1)
        self.assertEqual(self.doc(lp.ARCHIVE_PATH.format(day='2026-01-01'))['entsoe']['zones'], {'ES': self.HOURLY})

    def test_portugal_and_spain_are_set_against_omies_count_and_the_days_that_differ_are_named(self):
        def omie(negative):
            count = {'negative_h': negative, 'zero_h': 0, 'low_h': 0}
            return {'license': 'omie', 'day_ahead': {'periods': 96, 'hours': 24, 'PT': count, 'ES': count}}
        self.record('2026-01-01', {}, omie(0.75))
        self.record('2026-01-02', {}, omie(1))
        answer = self.answer([[-1.0] * 3 + [40.0] * 93, [40.0] * 96])
        _code, _asked, log = self.refill({fe.ZONES['ES']: answer, fe.ZONES['PT']: answer})
        self.assertIn('ES, 2 days: OMIE [1.75, 0, 0] hours negative, zero and low; from ENTSO-E [0.75, 0, 0] - NOT the same', log)
        self.assertIn('2026-01-02 ES: OMIE [1, 0, 0], ENTSO-E [0, 0, 0]', log)
        self.assertNotIn('2026-01-01 ES', log)
        # Neither count is changed to fit the other.
        self.assertEqual(self.doc(lp.ARCHIVE_PATH.format(day='2026-01-02'))['omie']['day_ahead']['ES']['negative_h'], 1)


class ThePricesOfTheDayAsTheMarketPublishesThem(unittest.TestCase):
    """What fetch_entsoe.py takes from the price answers it already gets."""

    DAY = date(2026, 11, 18)

    def read(self, zone):
        return fe.read_day_prices(sample(f'prices_A44_{zone}.xml').decode('utf-8'), self.DAY)

    def test_the_hourly_prices_are_what_they_were(self):
        for zone in ('ES', 'DE', 'NO1'):
            xml = sample(f'prices_A44_{zone}.xml').decode('utf-8')
            self.assertEqual(list(fe.read_day_prices(xml, self.DAY)), fe.read_prices(xml, self.DAY))

    def test_spain_has_96_quarter_hours_of_the_day_ahead_market_only(self):
        # The answer also has the three intraday auctions (contract A07): they are not read.
        entry = pi.zone_entry(self.read('ES').intervals, self.DAY)
        prices = entry['prices']
        self.assertEqual((entry['start'], entry['step_minutes'], len(prices)), ('2026-11-17T23:00:00+00:00', 15, 96))
        self.assertEqual((prices[:4], prices[-2:]), ([12.17, 11.2, 2.99, 1.0], [65.0, 57.07]))
        self.assertEqual((sum(p == 0 for p in prices), sum(0 < p <= 5 for p in prices), min(prices)), (14, 8, 0.0))
        self.assertEqual(entry['data_time'], '2026-11-18T22:45:00+00:00')

    def test_germany_is_read_from_the_first_of_its_two_auctions(self):
        prices = pi.zone_entry(self.read('DE').intervals, self.DAY)['prices']
        self.assertEqual((prices[:4], prices[-2:], min(prices)), ([108.86, 109.86, 114.34, 112.37], [174.59, 171.29], 78.45))

    def test_an_hourly_price_among_quarter_hours_is_repeated_in_its_four(self):
        start = at('2026-11-17 23:00')
        intervals = [(start, timedelta(hours=1), Decimal('10'))] + [
            (start + timedelta(minutes=60 + 15 * i), timedelta(minutes=15), Decimal(20 + i)) for i in range(4)]
        entry = pi.zone_entry(intervals, self.DAY)
        self.assertEqual((entry['step_minutes'], entry['prices'][:8]), (15, [10.0, 10.0, 10.0, 10.0, 20.0, 21.0, 22.0, 23.0]))
        self.assertEqual(entry['prices'][8:], [None] * 88)

    def test_spain_today_counted_by_the_quarter_hour_is_as_omie_has_it(self):
        entry = pi.zone_entry(self.read('ES').intervals, self.DAY)
        count = lp.zone_count(entry['prices'], 15, 'market-resolution')
        # OMIE's day-ahead file of 18 Nov 2026: zero for 3.5 hours, low for 2.
        self.assertEqual((count['hours'], count['negative_h'], count['zero_h'], count['low_h']), (24, 0, 3.5, 2))


class TheFileOfTheIntervals(InADirectory):

    DAY = date(2026, 11, 18)
    NOW = at('2026-11-18 18:08:40')

    def intervals(self, zone):
        return fe.read_day_prices(sample(f'prices_A44_{zone}.xml').decode('utf-8'), self.DAY).intervals

    def test_has_one_block_with_its_licence_and_the_day_it_is_of(self):
        self.assertEqual(pi.save({'ES': self.intervals('ES'), 'DE': self.intervals('DE')}, self.DAY, self.NOW), 2)
        doc = self.doc(pi.PATH)
        self.assertEqual(list(doc), ['updated', 'data_time', 'delivery_date', 'timezone', 'day_ahead_prices'])
        self.assertEqual((doc['updated'], doc['data_time'], doc['delivery_date']),
                         ('2026-11-18T18:08:40+00:00', '2026-11-18T22:45:00+00:00', '2026-11-18'))
        self.assertEqual(doc['day_ahead_prices']['license'], 'source-terms')
        self.assertEqual(doc['day_ahead_prices']['zones']['DE']['fetched_at'], '2026-11-18T18:08:40+00:00')

    def test_a_zone_that_did_not_answer_keeps_its_prices_while_they_are_of_the_day(self):
        pi.save({'ES': self.intervals('ES'), 'DE': self.intervals('DE')}, self.DAY, self.NOW)
        pi.save({'ES': self.intervals('ES')}, self.DAY, self.NOW + timedelta(minutes=15))
        zones = self.doc(pi.PATH)['day_ahead_prices']['zones']
        self.assertEqual((zones['DE']['fetched_at'], zones['DE'].get('carried_over')), ('2026-11-18T18:08:40+00:00', True))
        self.assertNotIn('carried_over', zones['ES'])
        # The next delivery day they are gone: yesterday's prices never pass for today's.
        tomorrow = [(at, step, price) for at, step, price in ((a + timedelta(days=1), s, p) for a, s, p in self.intervals('ES'))]
        pi.save({'ES': tomorrow}, self.DAY + timedelta(days=1), self.NOW + timedelta(hours=6))
        self.assertEqual(list(self.doc(pi.PATH)['day_ahead_prices']['zones']), ['ES'])

    def test_with_nothing_obtained_the_file_is_left_as_it_was(self):
        pi.save({'ES': self.intervals('ES')}, self.DAY, self.NOW)
        before = os.stat(pi.PATH).st_ino
        self.assertEqual(pi.save({}, self.DAY, self.NOW + timedelta(minutes=15)), 0)
        self.assertEqual(os.stat(pi.PATH).st_ino, before)


class ARunOfTheFeed(EntsoeFeed):
    """fetch_entsoe.py itself, with ENTSO-E answering as in the tests of carry_over."""

    def test_keeps_the_prices_at_the_markets_resolution_and_spot_prices_is_as_before(self):
        code, log = self.entsoe(at('2026-11-16 10:00:05'), prices={'PT': 50, 'ES': 60})
        self.assertEqual(code, 0)
        self.assertIn("Kept the prices of 16 Nov at the market's resolution for 2 zones", log)
        with open(pi.PATH, encoding='utf-8') as f:
            kept = json.load(f)
        self.assertEqual((kept['delivery_date'], kept['day_ahead_prices']['zones']['PT']['prices'][:3]),
                         ('2026-11-16', [50.0, 51.0, 52.0]))
        with open('data/spot-prices.json', encoding='utf-8') as f:
            zone = json.load(f)['zones']['PT']
        self.assertEqual(sorted(zone), ['avg', 'delivery_date', 'eic', 'fetched_at', 'latest', 'max', 'min', 'prices'])

    def test_a_failure_to_keep_them_changes_nothing_else(self):
        with mock.patch.object(pi, 'save', side_effect=OSError('disk full')):
            code, log = self.entsoe(at('2026-11-16 10:00:05'), prices={'PT': 50}, generation={'PT': 8000})
        self.assertEqual(code, 0)
        self.assertIn("Prices at the market's resolution not kept: OSError: disk full", log)
        self.assertTrue(os.path.exists('data/spot-prices.json'))
        self.assertTrue(os.path.exists('data/generation-mix.json'))


if __name__ == '__main__':
    unittest.main()
