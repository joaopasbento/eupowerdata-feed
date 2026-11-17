"""Tests for the prices of backfill_archive.py, which fills the archive one
past day at a time: it reads them with the same code as the regular runs.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are A44 documents written by hand.
"""

import contextlib
import io
import unittest
from datetime import date
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

        def http_get(url, timeout=45):
            asked.append(url)
            return next((answer for eic, answer in answers.items() if f'in_Domain={eic}&' in url), NO_DATA)

        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), mock.patch.object(fe, 'http_get', http_get), \
                mock.patch.object(backfill_archive.time, 'sleep', lambda seconds: None), \
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
