"""Tests for the prices of backfill_archive.py, which fills the archive one
past day at a time: it reads them with the same code as the regular runs.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are A44 documents written by hand.
"""

import contextlib
import io
import unittest
from datetime import date, datetime
from unittest import mock

import backfill_archive
import fetch_entsoe as fe
from entsoe_xml import NO_DATA, price_document, price_series

DAY = date(2026, 11, 10)
START = '2026-11-09 23:00'                      # midnight of the 10th on the market's clock


class ThePricesOfAPastDay(unittest.TestCase):

    def backfill(self, answers):
        """fetch_prices_for_date() with ENTSO-E answering {EIC of the zone: XML}."""
        asked = []

        # The zones are asked for in parallel, through fetch_entsoe's requests (_get), at the backfill's pace:
        # the pace is made quick here, and what it changes in fetch_entsoe is put back after.
        def get(url, deadline=None, **_more):
            asked.append(url)
            return next((answer for eic, answer in answers.items() if f'in_Domain={eic}&' in url), NO_DATA), None

        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), mock.patch.object(fe, '_get', get), \
                mock.patch.object(fe, 'MIN_INTERVAL_S', 0), mock.patch.object(fe, '_throttle', fe._Throttle(0)), \
                mock.patch.object(fe, 'MAX_WORKERS', fe.MAX_WORKERS), \
                mock.patch.object(backfill_archive, 'BACKFILL_INTERVAL_S', 0.001), \
                contextlib.redirect_stdout(io.StringIO()):
            return backfill_archive.fetch_prices_for_date('https://entsoe.test/api', DAY), asked

    def test_are_read_as_the_regular_runs_read_them(self):
        spain = price_document(
            price_series(START, [77.54] * 96, minutes=15, contract='A07', position=2),      # an intraday auction
            price_series(START, [float(q) for q in range(96)], minutes=15))                 # the day-ahead market
        serbia = price_document(price_series(                                               # points left out
            START, [100.0 + q // 4 if q % 4 == 0 else None for q in range(96)], minutes=15,
            end='2026-11-10 23:00'))
        doc, asked = self.backfill({
            fe.ZONES['ES']: spain, fe.ZONES['RS']: serbia,
            fe.ZONES['NO1']: price_document(price_series(START, [40.0] * 24)),
            fe.ZONES['NO2']: price_document(price_series(START, [60.0] * 24)),
            fe.ZONES['SE1']: price_document(price_series(START, [40.0] * 24)),              # SE3 does not answer
        })

        self.assertEqual(list(doc), ['updated', 'date', 'source', 'endpoint', 'zones'])
        self.assertEqual((doc['date'], doc['source']), ('2026-11-10', 'ENTSO-E Transparency Platform (backfilled)'))
        self.assertEqual(list(doc['zones']), ['ES', 'RS', 'NO'])                            # the feed's order
        for code, zone in doc['zones'].items():
            self.assertEqual(list(zone), ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'delivery_date'], code)
            self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(24)], code)
            self.assertEqual(zone['delivery_date'], '2026-11-10', code)
        self.assertEqual((doc['zones']['ES']['avg'], doc['zones']['ES']['min'], doc['zones']['ES']['max']),
                         (47.5, 1.5, 93.5))
        self.assertEqual([p['price'] for p in doc['zones']['RS']['prices']], [100.0 + h for h in range(24)])
        self.assertEqual(doc['zones']['NO']['avg'], 50.0)

        # One request for each zone, for the delivery day on the market's clock.
        self.assertEqual(len(asked), len(fe.ZONES))
        for url in asked:
            self.assertIn('documentType=A44', url)
            self.assertIn('periodStart=202611092300&periodEnd=202611102300', url)

    def test_is_the_same_as_the_regular_reading_of_the_same_answers(self):
        answer = price_document(price_series(START, [50.25 + h for h in range(24)]))
        doc, _asked = self.backfill({fe.ZONES['PT']: answer})
        with contextlib.redirect_stdout(io.StringIO()):
            regular = fe.price_zones({'PT': fe.read_prices(answer, DAY)}, DAY)
        self.assertEqual(doc['zones'], regular)

    def test_a_day_with_no_answer_has_no_zones(self):
        doc, _asked = self.backfill({})
        self.assertEqual(doc['zones'], {})


if __name__ == '__main__':
    unittest.main()


# --- Rewriting the archive of the prices, a year at a time ---

import json
import os
import re
import tempfile
from datetime import timedelta

from entsoe_xml import day_start

OLD = {'eic': 'x', 'prices': [{'time': '23:00', 'hour': 95, 'price': 9.0}], 'latest': {}, 'avg': 9.0, 'min': 9.0,
       'max': 9.0}
GB = {'eic': 'gb', 'prices': [{'time': '00:00', 'hour': 0, 'price': 80.0}], 'avg': 80.0, 'source': 'Elexon MID'}
EIC = {eic: code for code, eic in fe.ZONES.items()}


def year_answer(days, price=50.0, **more):
    """An A44 answer with one hourly series for each of `days`."""
    return price_document(*[price_series(day_start(day), [price + hour for hour in range(24)], **more) for day in days])


class RewritingTheArchive(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data/archive')
        self.asked, self.slept, self.commits = [], [], []

    def old_file(self, day, zones, **top):
        doc = {'updated': '2026-04-23T10:00:00+00:00', 'source': 'ENTSO-E Transparency Platform (backfilled)',
               'endpoint': 'https://old.test/api', 'zones': zones, **top}
        with open(f'data/archive/spot-prices-{day}.json', 'w', encoding='utf-8') as f:
            json.dump(doc, f)

    def read(self, day):
        with open(f'data/archive/spot-prices-{day}.json', encoding='utf-8') as f:
            return json.load(f)

    def rewrite(self, answers, years=None):
        """answers: {zone: what ENTSO-E answers, in turn, to the requests for it}; an answer is XML, None for a
        failure, or a function of (first day, last day) of the request. A zone not given has no data."""
        turns = {}

        def ask(url):
            code = EIC[re.search(r'in_Domain=([^&]+)', url).group(1)]
            start, end = re.search(r'periodStart=(\d+)&periodEnd=(\d+)', url).groups()
            self.asked.append((code, start, end))
            given = answers.get(code, NO_DATA)
            given = given if isinstance(given, list) else [given]
            answer = given[min(turns.setdefault(code, 0), len(given) - 1)]
            turns[code] += 1
            if callable(answer):
                answer = answer(start, end)
            if isinstance(answer, tuple):
                return answer
            return (answer, None) if answer is not None else (None, 'timeout: response too slow')

        log = io.StringIO()
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), contextlib.redirect_stdout(log):
            code = backfill_archive.rewrite_prices('https://entsoe.test/api', years, ask=ask, sleep=self.slept.append,
                                                   commit=self.commits.append)
        return code, log.getvalue()

    def test_each_day_is_rewritten_from_the_answer_for_its_year(self):
        days = ['2024-06-14', '2024-06-15', '2024-06-16']
        for day in days:
            self.old_file(day, {'PT': dict(OLD), 'FR': dict(OLD), 'NO': dict(OLD), 'GB': dict(GB)}, date=day)
        code, log = self.rewrite({
            'PT': year_answer(days),
            'FR': year_answer(days[:2], 70.0),                         # nothing for the 16th
            'ES': year_answer(days, 60.0),                             # was not in the files
            'NO1': year_answer(days, 40.0), 'NO2': year_answer(days, 60.0)})
        self.assertEqual(code, 0)

        doc = self.read('2024-06-15')
        self.assertEqual(list(doc), ['updated', 'source', 'endpoint', 'zones', 'date'])      # what it had, no less
        self.assertEqual((doc['date'], doc['endpoint']), ('2024-06-15', 'https://entsoe.test/api'))
        self.assertNotEqual(doc['updated'], '2026-04-23T10:00:00+00:00')
        self.assertEqual(list(doc['zones']), ['PT', 'ES', 'FR', 'NO', 'GB'])                # the feed's order
        for name in ('PT', 'ES', 'FR', 'NO'):
            zone = doc['zones'][name]
            self.assertEqual(list(zone), ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'delivery_date',
                                          'fetched_at'], name)                               # as a run writes it
            self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(24)], name)
            self.assertEqual((zone['delivery_date'], zone['fetched_at']), ('2024-06-15', doc['updated']), name)
        self.assertEqual([doc['zones'][name]['avg'] for name in ('PT', 'ES', 'FR', 'NO')], [61.5, 71.5, 81.5, 61.5])
        self.assertEqual(doc['zones']['GB'], GB)                                             # not ENTSO-E's: as it was

        # A zone ENTSO-E has nothing for on a day leaves that day, and is counted.
        self.assertEqual(list(self.read('2024-06-16')['zones']), ['PT', 'ES', 'NO', 'GB'])
        self.assertIn('Zone-days that left a file, because ENTSO-E has no prices for them: 1', log)
        self.assertIn('FR: 1 days, 2024-06-16 to 2024-06-16', log)
        self.assertIn('ES: 3 days, 2024-06-14 to 2024-06-16', log)

        # One request for each zone of ENTSO-E, for the whole span and a day on each side.
        self.assertEqual(len(self.asked), len(fe.ZONES) - 1)
        self.assertEqual({(start, end) for _code, start, end in self.asked}, {('202406122200', '202406172200')})
        self.assertNotIn('GB', {code for code, _start, _end in self.asked})
        self.assertEqual(len(self.commits), 1)

    def test_days_of_the_new_reading_and_other_archives_are_left_alone(self):
        self.old_file('2026-11-10', {'PT': dict(OLD)})
        self.old_file('2026-11-17', {'PT': dict(OLD, delivery_date='2026-11-17')})           # already right
        with open('data/archive/generation-mix-2026-11-10.json', 'w', encoding='utf-8') as f:
            f.write('{"zones": {}}')
        before = self.read('2026-11-17')
        code, log = self.rewrite({'PT': year_answer(['2026-11-10', '2026-11-17'])})
        self.assertEqual(code, 0)
        self.assertEqual(self.read('2026-11-17'), before)
        self.assertEqual(self.read('2026-11-10')['zones']['PT']['delivery_date'], '2026-11-10')
        self.assertEqual(self.asked[0][1:], ('202611082300', '202611112300'))                # the one old day only
        with open('data/archive/generation-mix-2026-11-10.json', encoding='utf-8') as f:
            self.assertEqual(f.read(), '{"zones": {}}')
        # A second run finds nothing left to do, and asks for nothing.
        self.asked.clear()
        self.assertEqual(self.rewrite({})[0], 0)
        self.assertEqual(self.asked, [])

    def test_a_request_that_fails_is_repeated_and_is_not_no_data(self):
        self.old_file('2024-06-15', {'PT': dict(OLD)})
        code, log = self.rewrite({'PT': [None, None, year_answer(['2024-06-15'])]})
        self.assertEqual(code, 0)
        self.assertEqual(self.read('2024-06-15')['zones']['PT']['avg'], 61.5)
        self.assertEqual([code for code, _s, _e in self.asked].count('PT'), 3)
        self.assertEqual(self.slept, [15, 45])

    def test_a_zone_that_cannot_be_had_leaves_its_whole_year_as_it_was(self):
        self.old_file('2024-06-15', {'PT': dict(OLD), 'FR': dict(OLD)})
        self.old_file('2025-06-15', {'PT': dict(OLD)})
        before = self.read('2024-06-15')
        code, log = self.rewrite({'PT': lambda start, end: None if start.startswith('2024') else year_answer(['2025-06-15']),
                                  'FR': year_answer(['2024-06-15'])})
        self.assertEqual(code, 1)                                             # to be started again
        self.assertEqual(self.read('2024-06-15'), before)                     # not even FR, which answered
        self.assertEqual(self.read('2025-06-15')['zones']['PT']['delivery_date'], '2025-06-15')
        self.assertIn('2024 NOT rewritten: PT 2024-06-15 to 2024-06-15', log)
        self.assertEqual(self.commits, ['Rewrite the archive of the prices of 2025 with the new reading (1 days)'])

    def test_a_long_request_that_keeps_failing_is_asked_again_in_halves(self):
        days = [(date(2024, 1, 1) + timedelta(days=n)).isoformat() for n in range(0, 366, 30)]
        for day in days:
            self.old_file(day, {'PT': dict(OLD)})

        def entsoe(start, end):                                               # only answers for six months or less
            first = datetime.strptime(start, '%Y%m%d%H%M') + timedelta(days=2)
            last = datetime.strptime(end, '%Y%m%d%H%M') - timedelta(days=1)
            if (last - first).days > 190:
                return (None, 'HTTP 400')
            return year_answer([day for day in days if first.date().isoformat() <= day <= last.date().isoformat()])

        code, log = self.rewrite({'PT': entsoe})
        self.assertEqual(code, 0)
        self.assertEqual([code for code, _s, _e in self.asked].count('PT'), 3)   # the year, then its two halves
        self.assertEqual(self.slept, [])                                      # a refusal is not waited on
        for day in days:
            self.assertEqual(self.read(day)['zones']['PT']['delivery_date'], day)

    def test_an_empty_answer_for_a_zone_the_archive_has_is_not_believed_at_once(self):
        days = ['2024-02-10', '2024-11-20']
        for day in days:
            self.old_file(day, {'PT': dict(OLD)})
        code, log = self.rewrite({'PT': [NO_DATA, year_answer(days[:1]), year_answer(days[1:])]})
        self.assertEqual(code, 0)
        self.assertEqual([code for code, _s, _e in self.asked].count('PT'), 3)
        self.assertEqual([self.read(day)['zones']['PT']['avg'] for day in days], [61.5, 61.5])

    def test_only_the_years_asked_for(self):
        self.old_file('2023-05-05', {'PT': dict(OLD)})
        self.old_file('2024-05-05', {'PT': dict(OLD)})
        code, _log = self.rewrite({'PT': year_answer(['2023-05-05', '2024-05-05'])}, years=[2024])
        self.assertEqual(code, 0)
        self.assertNotIn('delivery_date', self.read('2023-05-05')['zones']['PT'])
        self.assertIn('delivery_date', self.read('2024-05-05')['zones']['PT'])

    def test_a_year_is_read_as_each_of_its_days(self):
        days = ['2025-09-30', '2025-10-01']
        answer = price_document(
            price_series(day_start(days[0]), [float(h) for h in range(24)]),
            price_series(day_start(days[1]), [float(q) for q in range(96)], minutes=15),
            price_series(day_start(days[1]), [500.0] * 96, minutes=15, contract='A07', position=1))
        by_day = fe.prices_by_day(fe.read_series(answer))
        self.assertEqual(sorted(by_day), [date(2025, 9, 30), date(2025, 10, 1)])
        for day in by_day:
            self.assertEqual(by_day[day], fe.read_prices(answer, day))
        self.assertEqual(fe.price_entry('x', by_day[date(2025, 10, 1)], date(2025, 10, 1))['avg'], 47.5)
