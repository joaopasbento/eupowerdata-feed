"""Tests for the exchange rates (fx_rates.py) and for the conversion of the
day-ahead prices that ENTSO-E does not give in euros (fetch_entsoe.py).

Run from the repository root:  python -m unittest discover -s tests -v

No network: the ECB's files are written by hand in its own format.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest import mock

import feed_common as fc
import fetch_entsoe as fe
import fx_rates
from entsoe_xml import price_document, price_series


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def ecb(days):
    """One of the ECB's files: {day: {currency: rate}}."""
    cubes = ''.join(f"<Cube time='{day}'>" + ''.join(f"<Cube currency='{c}' rate='{r}'/>" for c, r in rates.items())
                    + '</Cube>' for day, rates in days.items())
    return (f'<?xml version="1.0" encoding="UTF-8"?><gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" '
            f'xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref"><Cube>{cubes}</Cube></gesmes:Envelope>').encode()


class TheRateOfADay(unittest.TestCase):

    RATES = fx_rates.Rates({'PLN': {'2026-11-13': 4.4113, '2026-11-16': 4.4200, '2026-11-18': 4.4118}})

    def test_the_lev_and_the_bosnian_mark_are_fixed(self):
        for currency in ('BGN', 'BAM'):
            self.assertEqual(self.RATES.rate(currency, date(2021, 12, 1)), (Decimal('1.95583'), None))

    def test_the_euro_is_one(self):
        self.assertEqual(self.RATES.rate('EUR', date(2026, 11, 18)), (Decimal(1), None))

    def test_the_rate_of_the_delivery_day_when_the_ecb_has_it(self):
        self.assertEqual(self.RATES.rate('PLN', date(2026, 11, 16)), (Decimal('4.42'), '2026-11-16'))

    def test_else_the_last_one_published_before(self):
        self.assertEqual(self.RATES.rate('PLN', date(2026, 11, 15)), (Decimal('4.4113'), '2026-11-13'))   # a Sunday
        self.assertEqual(self.RATES.rate('PLN', date(2026, 11, 19)), (Decimal('4.4118'), '2026-11-18'))   # not out yet

    def test_with_no_rate_up_to_that_day_there_is_none(self):
        self.assertIsNone(self.RATES.rate('PLN', date(2026, 11, 12)))
        self.assertIsNone(self.RATES.rate('RON', date(2026, 11, 18)))


class TheFile(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def update(self, when, answer):
        asked = []

        def get(url, timeout=None, max_seconds=None):
            asked.append(url)
            return answer if isinstance(answer, fc.Response) else fc.Response(200, {}, answer)

        with contextlib.redirect_stdout(io.StringIO()):
            return fx_rates.update(at(when), get=get), asked

    def test_the_first_time_it_asks_for_the_whole_history_and_keeps_the_currencies_of_europe(self):
        wrote, asked = self.update('2026-11-18 16:00', ecb({'2026-11-18': {'USD': 1.0968, 'PLN': 4.4118, 'RON': 5.3467},
                                                           '2026-11-17': {'USD': 1.09, 'PLN': 4.41}}))
        with open(fx_rates.PATH) as f:
            doc = json.load(f)
        self.assertEqual((wrote, asked), (True, [fx_rates.HISTORY_URL]))
        self.assertEqual(doc['rates'], {'PLN': {'2026-11-17': 4.41, '2026-11-18': 4.4118}, 'RON': {'2026-11-18': 5.3467}})
        self.assertEqual((doc['updated'], doc['fixed']), ('2026-11-18T16:00:00+00:00', {'BGN': 1.95583, 'BAM': 1.95583}))

    def test_with_the_days_rates_in_the_file_the_ecb_is_not_asked_again(self):
        self.update('2026-11-18 16:00', ecb({'2026-11-18': {'PLN': 4.4118}}))
        self.assertEqual(self.update('2026-11-18 20:00', ecb({})), (False, []))
        self.assertEqual(self.update('2026-11-19 09:00', ecb({})), (False, []))          # before the ECB publishes
        wrote, asked = self.update('2026-11-19 15:05', ecb({'2026-11-19': {'PLN': 4.40}}))
        self.assertEqual((wrote, asked), (True, [fx_rates.DAILY_URL]))
        self.assertEqual(fx_rates.load().rate('PLN', date(2026, 11, 19)), (Decimal('4.4'), '2026-11-19'))

    def test_a_weekend_asks_for_nothing_and_a_gap_asks_for_the_last_ninety_days(self):
        self.update('2026-11-20 16:00', ecb({'2026-11-20': {'PLN': 4.41}}))                 # a Friday
        self.assertEqual(self.update('2026-11-22 12:00', ecb({})), (False, []))
        _wrote, asked = self.update('2026-11-26 10:00', ecb({'2026-11-25': {'PLN': 4.39}}))
        self.assertEqual(asked, [fx_rates.RECENT_URL])

    def test_with_no_answer_the_file_stays_and_what_it_has_is_used(self):
        self.update('2026-11-18 16:00', ecb({'2026-11-18': {'PLN': 4.4118}}))
        before = os.stat(fx_rates.PATH).st_ino
        wrote, _asked = self.update('2026-11-19 16:00', fc.Response(error='timed out'))
        self.assertEqual((wrote, os.stat(fx_rates.PATH).st_ino), (False, before))
        self.assertEqual(fx_rates.load().rate('PLN', date(2026, 11, 19)), (Decimal('4.4118'), '2026-11-18'))

    def test_a_missing_file_has_no_rates_but_the_fixed_ones(self):
        rates = fx_rates.load()
        self.assertIsNone(rates.rate('PLN', date(2026, 11, 18)))
        self.assertEqual(rates.rate('BGN', date(2026, 11, 18))[0], Decimal('1.95583'))


class PricesThatAreNotInEuros(unittest.TestCase):

    DAY = date(2021, 12, 1)
    START = '2021-11-30 23:00'

    def read(self, currency, prices, rates=None):
        xml = price_document(price_series(self.START, prices, currency=currency))
        log = io.StringIO()
        with mock.patch.object(fe, '_rates', fx_rates.Rates(rates or {})), mock.patch.object(fe, '_no_rate', set()), \
                contextlib.redirect_stdout(log):
            return fe.read_day_prices(xml, self.DAY), log.getvalue()

    def test_levs_are_converted_at_the_fixed_rate(self):
        prices, _log = self.read('BGN', [195.583] * 23 + [391.166])
        self.assertEqual([float(price) for _hour, price in prices], [100.0] * 23 + [200.0])
        entry = fe.price_entry('x', prices, self.DAY)
        self.assertEqual((entry['avg'], entry['min'], entry['max']), (104.17, 100.0, 200.0))
        self.assertEqual(float(prices.intervals[0][2]), 100.0)       # the prices at the market's resolution too

    def test_euros_are_left_as_they_are(self):
        prices, _log = self.read('EUR', [50.0] * 24)
        self.assertEqual({float(price) for _hour, price in prices}, {50.0})

    def test_another_currency_goes_by_the_ecbs_rate_of_the_delivery_day(self):
        prices, _log = self.read('PLN', [440.0] * 24, {'PLN': {'2021-11-30': 4.5, '2021-12-01': 4.4}})
        self.assertEqual({float(price) for _hour, price in prices}, {100.0})

    def test_with_no_rate_the_zone_has_no_prices_that_day_and_the_log_says_why(self):
        prices, log = self.read('PLN', [440.0] * 24)
        self.assertEqual(list(prices), [])
        self.assertIn('its prices of 2021-12-01 are in PLN and there is no exchange rate for it', log)
        self.assertEqual(log.count('no exchange rate'), 1)             # once, not once for each hour

    def test_the_rate_is_that_of_each_delivery_day_of_a_long_answer(self):
        xml = price_document(price_series(self.START, [450.0] * 24 + [440.0] * 24, currency='PLN'))
        with mock.patch.object(fe, '_rates', fx_rates.Rates({'PLN': {'2021-12-01': 4.5, '2021-12-02': 4.4}})):
            days = fe.prices_by_day(fe.read_series(xml))
        self.assertEqual({day.isoformat(): {float(p) for _h, p in hours} for day, hours in days.items()},
                         {'2021-12-01': {100.0}, '2021-12-02': {100.0}})


if __name__ == '__main__':
    unittest.main()
