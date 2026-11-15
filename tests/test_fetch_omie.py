"""Tests for the OMIE parser, on real marginalpdbc files (tests/fixtures/omie).

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers were worked out by hand from the files, not with the code
under test.
"""

import os
import unittest
from datetime import date
from decimal import Decimal

import fetch_omie

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'omie')


def load(name):
    with open(os.path.join(FIXTURES, name), encoding='latin-1', newline='') as f:
        return f.read()


def day_block(name):
    day, rows = fetch_omie.parse_marginalpdbc(load(name))
    return fetch_omie.build_day(day, rows, 'https://example.invalid/' + name, 1)


class QuarterHourDay(unittest.TestCase):
    """2026-11-16: an ordinary day of 96 quarter-hours."""

    def setUp(self):
        self.block = day_block('marginalpdbc_20261116.1')

    def test_shape(self):
        b = self.block
        self.assertEqual(b['date'], '2026-11-16')
        self.assertEqual((b['periods'], b['resolution_minutes']), (96, 15))
        for zone in ('PT', 'ES'):
            self.assertEqual(len(b[zone]['prices']), 24)
            self.assertEqual([p['time'] for p in b[zone]['prices']],
                             [f'{h:02d}:00' for h in range(24)])
            self.assertEqual(len(b[zone]['prices_15min']), 96)

    def test_hourly_price_is_the_mean_of_its_quarter_hours(self):
        prices = self.block['PT']['prices']
        # (145 + 129.93 + 121.7 + 113.88) / 4 = 127.6275
        self.assertEqual(prices[0], {'time': '00:00', 'price': 127.63})
        self.assertEqual(prices[23], {'time': '23:00', 'price': 155.83})

    def test_avg_is_over_the_whole_day(self):
        # The PHP averaged the first 24 quarter-hours only: 84.76.
        self.assertEqual(self.block['PT']['avg'], 132.55)

    def test_quarter_hours_are_kept_as_published(self):
        q = self.block['PT']['prices_15min']
        self.assertEqual(q[0], {'period': 1, 'time': '00:00', 'price': 145})
        self.assertEqual(q[1], {'period': 2, 'time': '00:15', 'price': 129.93})
        self.assertEqual(q[95]['time'], '23:45')


class ClockChangeDays(unittest.TestCase):

    def test_spring_day_has_92_quarter_hours_and_no_two_o_clock(self):
        b = day_block('marginalpdbc_20260329.1')
        self.assertEqual((b['periods'], b['resolution_minutes']), (92, 15))
        times = [p['time'] for p in b['PT']['prices']]
        self.assertEqual(len(times), 23)
        self.assertNotIn('02:00', times)
        self.assertEqual(b['PT']['prices'][1], {'time': '01:00', 'price': 1.91})
        self.assertEqual(b['PT']['prices'][2], {'time': '03:00', 'price': 1.25})   # periods 9-12
        q = [p['time'] for p in b['PT']['prices_15min']]
        self.assertEqual(q[7:9], ['01:45', '03:00'])
        self.assertEqual(q[-1], '23:45')
        self.assertEqual(b['PT']['avg'], 0.18)

    def test_autumn_day_has_100_quarter_hours_and_two_o_clock_twice(self):
        b = day_block('marginalpdbc_20261025.1')
        self.assertEqual((b['periods'], b['resolution_minutes']), (100, 15))
        prices = b['PT']['prices']
        self.assertEqual([p['time'] for p in prices], [f'{h:02d}:00' for h in range(24)])
        self.assertEqual(prices[2], {'time': '02:00', 'price': 142.39})   # periods 9-16, both 02:00 hours
        self.assertEqual(prices[3], {'time': '03:00', 'price': 113.55})   # periods 17-20
        q = [p['time'] for p in b['PT']['prices_15min']]
        self.assertEqual(q[8:16], ['02:00', '02:15', '02:30', '02:45'] * 2)
        self.assertEqual(q[-1], '23:45')
        self.assertEqual(b['PT']['avg'], 111.88)


class HourlyFormat(unittest.TestCase):
    """Files from before 1 October 2025: one row per hour."""

    def test_24_hours(self):
        b = day_block('marginalpdbc_20250930.1')
        self.assertEqual((b['periods'], b['resolution_minutes']), (24, 60))
        self.assertEqual(len(b['PT']['prices']), 24)
        self.assertEqual(b['PT']['prices'][0], {'time': '00:00', 'price': 95.46})
        self.assertEqual(b['PT']['prices'][23], {'time': '23:00', 'price': 101.5})
        self.assertEqual(b['PT']['avg'], 82.8)
        self.assertNotIn('prices_15min', b['PT'])

    def test_23_hours(self):
        b = day_block('marginalpdbc_20250330.1')
        times = [p['time'] for p in b['PT']['prices']]
        self.assertEqual(len(times), 23)
        self.assertNotIn('02:00', times)
        self.assertEqual(b['PT']['prices'][2], {'time': '03:00', 'price': 0.65})   # period 3

    def test_25_hours(self):
        b = day_block('marginalpdbc_20241027.1')
        prices = b['PT']['prices']
        self.assertEqual(len(prices), 24)
        # periods 3 and 4 are both 02:00: (82.23 + 80.68) / 2 = 81.455
        self.assertEqual(prices[2], {'time': '02:00', 'price': 81.46})
        self.assertEqual(prices[23], {'time': '23:00', 'price': 102.99})          # period 25


class Columns(unittest.TestCase):
    """The first price column is Portugal, the second is Spain.

    On 2026-02-01 the two markets split in 66 of the 96 periods. ENTSO-E's
    day-ahead prices for that day (data/archive/spot-prices-2026-02-01.json)
    have 2.5 only in the PT zone and 3.78 in the ES zone."""

    def test_portugal_first(self):
        day, rows = fetch_omie.parse_marginalpdbc(load('marginalpdbc_20260201.1'))
        self.assertEqual(day, date(2026, 2, 1))
        self.assertEqual(rows[1], (2, Decimal('2.5'), Decimal('3.78')))
        b = fetch_omie.build_day(day, rows, 'u', 1)
        self.assertEqual(b['PT']['prices_15min'][1]['price'], 2.5)
        self.assertEqual(b['ES']['prices_15min'][1]['price'], 3.78)
        self.assertEqual((b['PT']['avg'], b['ES']['avg']), (1.04, 24.58))

    def test_old_format_too(self):
        day, rows = fetch_omie.parse_marginalpdbc(load('marginalpdbc_20250330.1'))
        self.assertEqual(rows[12], (13, Decimal('-3.43'), Decimal('-4')))


class BadFiles(unittest.TestCase):

    def test_a_truncated_day_is_rejected(self):
        text = load('marginalpdbc_20261116.1')
        lines = text.splitlines()
        truncated = '\r\n'.join(lines[:60] + ['*'])          # 59 periods
        day, rows = fetch_omie.parse_marginalpdbc(truncated)
        with self.assertRaises(fetch_omie.OmieFormatError):
            fetch_omie.build_day(day, rows, 'u', 1)

    def test_a_gap_in_the_periods_is_rejected(self):
        lines = load('marginalpdbc_20261116.1').splitlines()
        del lines[10]
        with self.assertRaises(fetch_omie.OmieFormatError):
            fetch_omie.parse_marginalpdbc('\r\n'.join(lines))

    def test_an_error_page_is_rejected(self):
        with self.assertRaises(fetch_omie.OmieFormatError):
            fetch_omie.parse_marginalpdbc('<html><body>Not found</body></html>')


class FakeResponse:
    def __init__(self, body=None, status=None):
        self.body = body or b''
        self.status = status if status is not None else (200 if body is not None else 404)
        self.ok = self.status == 200

    def text(self, encoding='utf-8'):
        return self.body.decode(encoding)

    def describe(self):
        return f'HTTP {self.status}'


class Versions(unittest.TestCase):
    """A republished day has a higher suffix and its earlier file is gone."""

    def fetch(self, day, available, **kwargs):
        asked = []

        def get(url, timeout=20):
            asked.append(url.rsplit('.', 1)[1])
            answer = available.get(asked[-1])
            return answer if isinstance(answer, FakeResponse) else FakeResponse(answer)

        block = fetch_omie.fetch_day(day, get=get, pause=0, **kwargs)
        return block, asked

    def test_ordinary_day(self):
        body = load('marginalpdbc_20261116.1').encode('latin-1')
        block, asked = self.fetch(date(2026, 11, 16), {'1': body})
        self.assertEqual(block['version'], 1)
        self.assertEqual(asked, ['1'])

    def test_republished_day(self):
        # 2025-11-27 only exists as .2 on OMIE's server: the real file.
        body = load('marginalpdbc_20251127.2').encode('latin-1')
        block, asked = self.fetch(date(2025, 11, 27), {'2': body})
        self.assertEqual(asked, ['1', '2'])
        self.assertEqual(block['version'], 2)
        self.assertTrue(block['url'].endswith('marginalpdbc_20251127.2'))
        self.assertEqual((block['date'], block['periods']), ('2025-11-27', 96))

    def test_the_version_already_known_is_asked_first(self):
        body = load('marginalpdbc_20251127.2').encode('latin-1')
        block, asked = self.fetch(date(2025, 11, 27), {'2': body}, known_version=2)
        self.assertEqual(asked, ['2'])
        self.assertEqual(block['version'], 2)

    def test_republished_again_since_the_last_run(self):
        v3 = load('marginalpdbc_20251127.2').replace('2025;11;27;1;', '2025;11;27;1;').encode('latin-1')
        block, asked = self.fetch(date(2025, 11, 27), {'3': v3}, known_version=2)
        self.assertEqual(asked, ['2', '3'])
        self.assertEqual(block['version'], 3)

    def test_no_version_at_all(self):
        block, asked = self.fetch(date(2026, 11, 17), {})
        self.assertIsNone(block)
        self.assertEqual(asked, ['1', '2', '3', '4'])

    def test_a_server_error_is_not_a_missing_version(self):
        block, asked = self.fetch(date(2026, 11, 16), {'1': FakeResponse(status=503)})
        self.assertIsNone(block)
        self.assertEqual(asked, ['1'])

    def test_a_file_for_another_day_is_not_used(self):
        body = load('marginalpdbc_20261025.1').encode('latin-1')
        block, _ = self.fetch(date(2026, 11, 16), {'1': body})
        self.assertIsNone(block)


class PreviousFile(unittest.TestCase):
    """When OMIE does not answer, a day already in the previous file is reused."""

    def test_next_day_becomes_today(self):
        today = day_block('marginalpdbc_20261025.1')
        tomorrow = day_block('marginalpdbc_20261116.1')
        previous = fetch_omie.assemble(today, tomorrow, '2026-11-15T12:00:00+00:00')
        block = fetch_omie.previous_day_block(previous, date(2026, 11, 16))
        self.assertEqual(block, tomorrow)
        self.assertEqual(fetch_omie.previous_day_block(previous, date(2026, 10, 25)), today)
        self.assertIsNone(fetch_omie.previous_day_block(previous, date(2026, 11, 17)))
        self.assertIsNone(fetch_omie.previous_day_block(None, date(2026, 11, 16)))


if __name__ == '__main__':
    unittest.main()
