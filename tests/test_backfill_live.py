"""Tests for the generation and the flows of backfill_archive.py, which fills
the archive one past day at a time: it reads them with the code of the
regular runs, given the end of the UTC day as the instant they are of.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are documents written by hand (tests/entsoe_xml.py).
"""

import contextlib
import io
import threading
import time
import unittest
import urllib.parse
from datetime import date
from unittest import mock

import backfill_archive
import fetch_entsoe as fe
from entsoe_xml import FLOW_NS, day_start, document, price_document, price_series, series

DAY = date(2026, 9, 1)


class APastDay(unittest.TestCase):

    def run_with(self, call, answers):
        """One of the two functions with ENTSO-E answering {zone or (from, to) link: XML}; returns (file, requests)."""
        asked = []

        def fetch_all(label, jobs, parser, describe, deadline):
            read = {}
            for key, _name, url in jobs:
                asked.append(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)))
                given = key[1] if label == 'Flow' else key
                if given in answers:
                    read[key] = parser(answers[given])
            return {key: value for key, value in read.items() if value}

        # The slowing down of the requests changes fetch_entsoe for the rest of the process: put it back after.
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), mock.patch.object(fe, 'fetch_all', fetch_all), \
                mock.patch.object(fe, 'MIN_INTERVAL_S', fe.MIN_INTERVAL_S), mock.patch.object(fe, '_throttle', fe._throttle), \
                mock.patch.object(fe, 'MAX_WORKERS', fe.MAX_WORKERS), contextlib.redirect_stdout(io.StringIO()):
            return call('https://entsoe.test/api', DAY), asked

    def generation(self, mw_at_23, psr='B04', out=False):
        """A75 by the hour for the whole of 1 Sep and the first two hours of the 2nd."""
        values = [1000.0] * 23 + [mw_at_23, 7777.0, 8888.0]
        return series('2026-09-01 00:00:00', values, psr=psr, out=out)

    def test_generation_is_the_last_value_of_the_utc_day(self):
        doc, asked = self.run_with(backfill_archive.fetch_gen_for_date,
                                   {'PT': document(self.generation(4200.0), self.generation(900.0, 'B16'))})
        self.assertEqual(list(doc), ['updated', 'date', 'source', 'zones'])
        self.assertEqual((doc['date'], doc['source']), ('2026-09-01', 'ENTSO-E Transparency Platform (backfilled)'))
        portugal = doc['zones']['PT']
        # 23:00 UTC is the last interval of the day: not the 00:00 of the day after.
        self.assertEqual((portugal['mix'], portugal['total_mw'], portugal['renewable_pct']),
                         ({'Fossil Gas': 4200.0, 'Solar': 900.0}, 5100.0, 17.6))
        self.assertEqual(portugal['data_time'], '2026-09-01T23:00:00+00:00')
        # The 24 hours before the end of the day, as the regular runs ask.
        self.assertEqual({(p['periodStart'], p['periodEnd']) for p in asked}, {('202608312300', '202609020000')})

    def test_consumption_is_not_generation(self):
        doc, _asked = self.run_with(backfill_archive.fetch_gen_for_date,
                                    {'PT': document(self.generation(4200.0), self.generation(500.0, 'B10', out=True))})
        self.assertEqual(doc['zones']['PT']['mix'], {'Fossil Gas': 4200.0})

    def test_italy_is_asked_for_by_the_country(self):
        _doc, asked = self.run_with(backfill_archive.fetch_gen_for_date, {})
        domains = [p['in_Domain'] for p in asked]
        self.assertIn('10YIT-GRTN-----B', domains)
        self.assertNotIn(fe.ZONES['IT'], domains)

    def flow(self, mw_at_23):
        """A11 by the hour for 1 Sep: the request ends at midnight, and so does the answer."""
        return document(series('2026-09-01 00:00:00', [100.0] * 23 + [mw_at_23]), ns=FLOW_NS)

    def test_flows_are_the_last_value_of_the_utc_day_with_the_days_points(self):
        doc, _asked = self.run_with(backfill_archive.fetch_flows_for_date,
                                    {('PT', 'ES'): self.flow(1500.0), ('ES', 'PT'): self.flow(200.0)})
        self.assertEqual(list(doc), ['updated', 'date', 'source', 'corridors', 'net'])
        corridor = doc['corridors']['PT→ES']
        self.assertEqual((corridor['latest'], len(corridor['values']), corridor['data_time']),
                         ({'time': '23:00', 'mw': 1500}, 24, '2026-09-01T23:00:00+00:00'))
        self.assertEqual(corridor['values'][0], {'time': '00:00', 'mw': 100})

    def test_the_net_block_has_only_the_pairs_that_have_a_corridor(self):
        doc, _asked = self.run_with(backfill_archive.fetch_flows_for_date,
                                    {('PT', 'ES'): self.flow(1500.0), ('ES', 'PT'): self.flow(200.0)})
        self.assertEqual(list(doc['net']), ['ES-PT'])
        self.assertEqual({k: doc['net']['ES-PT'][k] for k in ('from', 'to', 'net_mw')},
                         {'from': 'PT', 'to': 'ES', 'net_mw': 1300})

    def test_norway_sweden_is_the_sum_of_its_four_links(self):
        links = {link: self.flow(100.0 * (n + 1)) for n, link in enumerate(fe.FLOW_LINKS[('NO', 'SE')])}
        doc, asked = self.run_with(backfill_archive.fetch_flows_for_date, links)
        self.assertEqual(doc['corridors']['NO→SE']['latest'], {'time': '23:00', 'mw': 1000})
        self.assertIn((fe.LINK_ZONES['LT'], fe.LINK_ZONES['SE4']), {(p['out_Domain'], p['in_Domain']) for p in asked})

    def test_the_requests_go_out_slower_than_those_of_the_regular_runs(self):
        with mock.patch.object(fe, 'MIN_INTERVAL_S', fe.MIN_INTERVAL_S), mock.patch.object(fe, '_throttle', fe._throttle), \
                mock.patch.object(fe, 'MAX_WORKERS', fe.MAX_WORKERS):
            backfill_archive._gently()
            self.assertEqual((fe.MIN_INTERVAL_S, fe.MAX_WORKERS), (0.6, 6))


class ThePricesOfAPastDay(unittest.TestCase):
    """fetch_prices_for_date asks for the zones in parallel, by fetch_entsoe.fetch_all,
    and must write what asking for them one after the other wrote."""

    def answers(self):
        start = day_start(DAY)
        hourly = lambda base: [base + hour for hour in range(24)]
        return {
            fe.ZONES['PT']: price_document(price_series(start, hourly(80.0))),
            fe.ZONES['ES']: price_document(price_series(start, [None if hour % 3 else 70.0 + hour for hour in range(24)])),
            fe.ZONES['BG']: price_document(price_series(start, [195.583] * 24, currency='BGN')),
            fe.ZONES['NO1']: price_document(price_series(start, [10.0 + q % 4 for q in range(96)], minutes=15)),
            fe.ZONES['NO2']: price_document(price_series(start, [20.0] * 96, minutes=15)),
            fe.ZONES['MD']: price_document(price_series(start, [757.11])),                # one hour only
        }

    def run_it(self):
        answers, lock, flying, most = self.answers(), threading.Lock(), [0], [0]

        def get(url, deadline=None, **_more):
            with lock:
                flying[0] += 1
                most[0] = max(most[0], flying[0])
            time.sleep(0.01)
            with lock:
                flying[0] -= 1
            domain = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))['in_Domain']
            return answers.get(domain, ''), None

        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), mock.patch.object(fe, '_get', get), \
                mock.patch.object(fe, 'MIN_INTERVAL_S', 0), mock.patch.object(fe, '_throttle', fe._Throttle(0)), \
                mock.patch.object(fe, 'MAX_WORKERS', fe.MAX_WORKERS), \
                mock.patch.object(backfill_archive, 'BACKFILL_INTERVAL_S', 0.001), \
                contextlib.redirect_stdout(io.StringIO()):
            doc = backfill_archive.fetch_prices_for_date('https://entsoe.test/api', DAY)
        return doc, answers, most[0]

    def test_the_file_is_what_asking_one_zone_after_the_other_gave(self):
        doc, answers, _most = self.run_it()
        with contextlib.redirect_stdout(io.StringIO()):
            one_by_one = {code: fe.read_prices(answers[eic], DAY) for code, eic in fe.ZONES.items() if eic in answers}
            expected = fe.for_the_archive(fe.price_zones({code: hours for code, hours in one_by_one.items() if hours}, DAY))
        self.assertEqual(doc['zones'], expected)
        self.assertEqual(list(doc), ['updated', 'date', 'source', 'endpoint', 'zones'])
        self.assertEqual((doc['date'], doc['source']), ('2026-09-01', 'ENTSO-E Transparency Platform (backfilled)'))

    def test_the_rules_of_the_archive_hold(self):
        doc, _answers, _most = self.run_it()
        zones = doc['zones']
        self.assertEqual(list(zones), ['PT', 'ES', 'BG', 'NO'])          # the feed's order; NO1 and NO2 put together
        self.assertEqual({row['price'] for row in zones['BG']['prices']}, {100.0})   # levs to euros
        self.assertNotIn('MD', zones)                                     # a day of one hour is not archived
        self.assertEqual(zones['NO']['prices'][0]['price'], 15.75)        # (11.5 + 20) / 2, the hour's mean
        self.assertEqual(len(zones['ES']['prices']), 24)                  # A03: the hours left out put back

    def test_six_at_a_time_at_most(self):
        _doc, _answers, most = self.run_it()
        self.assertLessEqual(most, backfill_archive.BACKFILL_WORKERS)
        self.assertGreater(most, 1)

    def test_at_most_100_requests_a_minute(self):
        with mock.patch.object(fe, 'MIN_INTERVAL_S', fe.MIN_INTERVAL_S), mock.patch.object(fe, '_throttle', fe._throttle), \
                mock.patch.object(fe, 'MAX_WORKERS', fe.MAX_WORKERS):
            backfill_archive._gently()
            self.assertEqual((fe.MIN_INTERVAL_S, fe.MAX_WORKERS), (0.6, 6))
            self.assertEqual(fe._throttle._min, 0.6)


if __name__ == '__main__':
    unittest.main()
