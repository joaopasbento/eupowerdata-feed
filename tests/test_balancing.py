"""Tests for the balancing prices (fetch_balancing.py), on real answers of
ENTSO-E (tests/fixtures/entsoe, obtained on 18 Nov 2026 at 18:07 UTC).

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers were read from the answers by hand (the positions and
prices as they are written in the XML), not with the code under test.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import feed_common as fc
import fetch_balancing as fb
from entsoe_real import ASKED_AT, KEY, Entsoe, sample

PATH = fb.OUT_PATH
PT, ES = '10YPT-REN------W', '10YES-REE------0'


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def picasso(points_up, points_down=None, start='2026-11-18T17:00Z', end='2026-11-18T17:15Z'):
    """A PICASSO document written by hand: (position, price) points every 4
    seconds, None for a point with no price."""
    def series(direction, points):
        body = ''.join(f'<Point><position>{position}</position>'
                       + (f'<activation_Price.amount>{price}</activation_Price.amount>' if price is not None else '')
                       + '</Point>' for position, price in points)
        return (f'<TimeSeries><businessType>A96</businessType><flowDirection.direction>{direction}'
                f'</flowDirection.direction><currency_Unit.name>EUR</currency_Unit.name><curveType>A03</curveType>'
                f'<Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval>'
                f'<resolution>PT4S</resolution>{body}</Period></TimeSeries>')
    return ('<Balancing_MarketDocument xmlns="urn:x"><type>A84</type>' + series('A01', points_up)
            + (series('A02', points_down) if points_down is not None else '') + '</Balancing_MarketDocument>').encode()


class ThePriceOfEachSettlementPeriod(unittest.TestCase):
    """17.1.F, as Portugal publishes it (processType A16)."""

    def setUp(self):
        self.read = fb.read_activated(sample('balancing_A16_PT.xml'))

    def test_quarter_hours_in_euros_both_directions(self):
        self.assertEqual((self.read['step'], self.read['currency'], self.read['category']),
                         (timedelta(minutes=15), 'EUR', 'A06'))
        # The first upward period of the answer: 14 Nov 23:00 UTC, positions 1 to 4.
        self.assertEqual([self.read['up'][at('2026-11-14 23:00') + timedelta(minutes=15 * i)] for i in range(4)],
                         [158.81, 155.87, 146.89, 144.53])
        self.assertEqual(self.read['down'][at('2026-11-16 02:15')], -7.2)

    def test_a_period_with_no_activation_has_no_price(self):
        # The upward series stops at 01:45 UTC of the 15th and starts again at 02:00.
        self.assertIn(at('2026-11-15 01:30'), self.read['up'])
        self.assertNotIn(at('2026-11-15 01:45'), self.read['up'])
        self.assertIn(at('2026-11-15 02:00'), self.read['up'])

    def test_downward_prices_can_be_negative(self):
        self.assertEqual(min(self.read['down'].values()), -520.0)

    def test_zero_is_a_price(self):
        read = fb.read_activated(sample('balancing_A16_IT.xml'))
        self.assertEqual(sorted(read['up'].values()), [199.0, 303.0, 325.0])
        self.assertEqual(list(read['down'].values()), [0.0, 0.0, 0.0])

    def test_the_currency_is_the_one_the_operator_publishes(self):
        self.assertEqual(fb.read_activated(sample('balancing_A16_PL.xml'))['currency'], 'PLN')

    def test_an_answer_with_no_data_has_no_prices(self):
        self.assertIsNone(fb.read_activated(sample('no_data.xml')))
        self.assertIsNone(fb.read_activated(b'<html>not XML'))

    def test_when_a_direction_comes_twice_the_standard_product_stands(self):
        def series(product, price):
            return (f'<TimeSeries><businessType>A96</businessType>{product}<flowDirection.direction>A01'
                    f'</flowDirection.direction><currency_Unit.name>EUR</currency_Unit.name><curveType>A03</curveType>'
                    f'<Period><timeInterval><start>2026-11-18T17:00Z</start><end>2026-11-18T17:15Z</end></timeInterval>'
                    f'<resolution>PT15M</resolution><Point><position>1</position><activation_Price.amount>{price}'
                    f'</activation_Price.amount></Point></Period></TimeSeries>')
        standard = '<standard_MarketProduct.marketProductType>A01</standard_MarketProduct.marketProductType>'
        for order in ((series(standard, 50), series('', 70)), (series('', 70), series(standard, 50))):
            read = fb.read_activated(f'<Balancing_MarketDocument xmlns="urn:x">{"".join(order)}'
                                     f'</Balancing_MarketDocument>'.encode())
            self.assertEqual(read['up'], {at('2026-11-18 17:00'): 50.0})

    def test_an_hourly_price_among_quarter_hours_stands_for_its_four_periods(self):
        def series(resolution, end, price):
            return (f'<TimeSeries><businessType>A96</businessType><flowDirection.direction>A02'
                    f'</flowDirection.direction><curveType>A03</curveType><Period><timeInterval>'
                    f'<start>2026-11-18T{end - 1:02d}:00Z</start><end>2026-11-18T{end:02d}:00Z</end></timeInterval>'
                    f'<resolution>{resolution}</resolution><Point><position>1</position><activation_Price.amount>'
                    f'{price}</activation_Price.amount></Point></Period></TimeSeries>')
        read = fb.read_activated(('<Balancing_MarketDocument xmlns="urn:x">' + series('PT60M', 11, 30)
                                  + series('PT15M', 12, 40) + '</Balancing_MarketDocument>').encode())
        self.assertEqual(read['step'], timedelta(minutes=15))
        self.assertEqual([read['down'][at('2026-11-18 10:00') + timedelta(minutes=15 * i)] for i in range(4)], [30.0] * 4)
        self.assertEqual(len(read['down']), 8)


class TheDaysAZoneShows(unittest.TestCase):

    def test_portugal_two_days_late_shows_the_two_days_it_has(self):
        entry = fb.activated_entry(PT, 'A16', fb.read_activated(sample('balancing_A16_PT.xml')), date(2026, 11, 18))
        self.assertEqual(list(entry['days']), ['2026-11-16', '2026-11-15'])
        self.assertEqual(entry['data_time'], '2026-11-16T22:45:00+00:00')
        day = entry['days']['2026-11-16']
        self.assertEqual((day['start'], day['periods'], len(day['up']), len(day['down'])),
                         ('2026-11-15T23:00:00+00:00', 96, 96, 96))
        self.assertEqual(day['up'][20:23], [80.74, 98.74, 126.37])        # from 04:00 UTC of the 16th
        self.assertEqual(day['up'][95], 174.72)                           # 22:45 UTC, the last period of the day
        self.assertEqual(day['down'][13:17], [-7.2, -7.2, -7.2, 11.9])    # from 02:15 UTC

    def test_spain_shows_the_day_in_course_and_the_one_before(self):
        entry = fb.activated_entry(ES, 'A68', fb.read_activated(sample('balancing_A68_ES.xml')), date(2026, 11, 18))
        self.assertEqual(list(entry['days']), ['2026-11-18', '2026-11-17'])
        self.assertEqual(entry['data_time'], '2026-11-18T17:15:00+00:00')
        today = entry['days']['2026-11-18']
        self.assertEqual(today['up'][73], 110.79)                         # 17:15 UTC, position 362 of the answer
        self.assertEqual(today['up'][74:], [None] * 22)                   # not published yet
        self.assertNotIn(None, entry['days']['2026-11-17']['down'])

    def test_a_zone_with_nothing_in_the_four_days_has_no_entry(self):
        read = fb.read_activated(sample('balancing_A16_IT.xml'))
        self.assertIsNone(fb.activated_entry('x', 'A16', read, date(2026, 11, 25)))

    def test_the_day_the_clocks_go_back_has_a_hundred_periods(self):
        entry = fb.activated_entry('x', 'A16', {'up': {at('2026-10-25 00:15'): 1.0}, 'down': {},
                                                'step': timedelta(minutes=15), 'currency': 'EUR', 'category': 'A06'},
                                   date(2026, 10, 25))
        day = entry['days']['2026-10-25']
        self.assertEqual((day['start'], day['periods']), ('2026-10-24T22:00:00+00:00', 100))
        self.assertEqual(day['up'][9], 1.0)


class TheFourSecondSeries(unittest.TestCase):
    """PICASSO (processType A67): never kept, summarised by quarter-hour."""

    def test_spain_from_17_00_to_17_45(self):
        read = fb.read_cbmp(sample('picasso_ES_latest.xml'))
        self.assertEqual((read['currency'], read['step_seconds'], read['until']), ('EUR', 4, at('2026-11-18 17:45')))
        self.assertEqual(sorted(read['quarters']), [at('2026-11-18 17:00'), at('2026-11-18 17:15'), at('2026-11-18 17:30')])
        first = read['quarters'][at('2026-11-18 17:00')]
        # Upward: 37 of the 225 positions had a price, from 0.50 to 146.05, mean 95.0808.
        self.assertEqual(first['up'], (0.5, 95.08, 146.05, 37))
        self.assertEqual(first['down'], (56.94, 74.63, 291.6, 211))
        self.assertEqual(read['quarters'][at('2026-11-18 17:15')]['down'], (60.53, 69.38, 112.17, 225))
        self.assertEqual(read['quarters'][at('2026-11-18 17:30')]['up'], (95.25, 119.87, 153.88, 195))

    def test_a_point_with_no_price_means_no_price_from_there_on(self):
        read = fb.read_cbmp(picasso([(1, 10), (3, None), (5, 20)], [(1, None)]))
        # Positions 1-2 at 10, 3-4 with none, 5-225 at 20: (2 x 10 + 221 x 20) / 223 = 19.91.
        self.assertEqual(read['quarters'][at('2026-11-18 17:00')],
                         {'up': (10.0, 19.91, 20.0, 223), 'down': (None, None, None, 0)})

    def test_a_quarter_hour_that_is_not_whole_yet_is_left_for_the_next_run(self):
        read = fb.read_cbmp(picasso([(1, 10)], [(1, 12)], end='2026-11-18T17:20Z'))
        self.assertEqual(sorted(read['quarters']), [at('2026-11-18 17:00')])
        self.assertEqual(read['until'], at('2026-11-18 17:15'))

    def test_an_answer_with_no_series_has_no_summary(self):
        self.assertIsNone(fb.read_cbmp(sample('no_data.xml')))

    def test_only_the_new_quarter_hours_are_asked_for(self):
        today = date(2026, 11, 18)
        self.assertEqual(fb.cbmp_window(None, ASKED_AT, today), (at('2026-11-16 23:00'), at('2026-11-17 11:00')))
        self.assertEqual(fb.cbmp_window({'summarized_until': '2026-11-18T17:45:00+00:00'}, ASKED_AT, today),
                         (at('2026-11-18 17:45'), at('2026-11-18 19:00')))


class Feed(unittest.TestCase):
    """A temporary feed directory, and runs of fetch_balancing.py in it."""

    ANSWERS = [
        ({'processType': 'A16', 'controlArea_Domain': PT}, 'balancing_A16_PT.xml'),
        ({'processType': 'A68', 'controlArea_Domain': ES}, 'balancing_A68_ES.xml'),
        ({'processType': 'A16', 'controlArea_Domain': '10YPL-AREA-----S'}, 'balancing_A16_PL.xml'),
        ({'processType': 'A67', 'controlArea_Domain': ES}, 'picasso_ES_latest.xml'),
    ]

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def run_at(self, when, answers=None, budget=60):
        entsoe = Entsoe(self.ANSWERS if answers is None else answers)
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            code = fb.main(now=when, get=entsoe, budget=fc.Budget(budget), interval=0,
                           update_rates=lambda now: False)          # the ECB is not asked in the tests
        return code, entsoe, log.getvalue()

    def doc(self):
        with open(PATH, encoding='utf-8') as f:
            return json.load(f)


class ARun(Feed):

    def test_writes_the_two_blocks_each_with_its_licence(self):
        code, _entsoe, log = self.run_at(ASKED_AT)
        doc = self.doc()
        self.assertEqual(code, 0)
        self.assertEqual(list(doc), ['updated', 'data_time', 'timezone', 'afrr_activated', 'afrr_cbmp'])
        self.assertEqual((doc['updated'], doc['data_time']), ('2026-11-18T18:07:42+00:00', '2026-11-18T17:30:00+00:00'))
        self.assertEqual((doc['afrr_activated']['license'], doc['afrr_cbmp']['license']), ('cc-by-4.0', 'source-terms'))
        self.assertNotIn(KEY, log)
        self.assertNotIn('entsoe.eu', log)

    def test_portugal_has_the_price_of_two_days_before_and_no_picasso_series(self):
        self.run_at(ASKED_AT)
        doc = self.doc()
        portugal = doc['afrr_activated']['zones']['PT']
        self.assertEqual((portugal['process'], portugal['currency'], portugal['resolution_minutes']), ('A16', 'EUR', 15))
        self.assertEqual(list(portugal['days']), ['2026-11-16', '2026-11-15'])
        self.assertEqual(portugal['fetched_at'], '2026-11-18T18:07:42+00:00')
        self.assertNotIn('PT', doc['afrr_cbmp']['zones'])
        self.assertEqual(doc['afrr_cbmp']['not_published']['PT'], '2026-11-18T18:07:42+00:00')

    def test_spain_has_the_price_of_the_local_selection_and_the_picasso_summary(self):
        self.run_at(ASKED_AT)
        doc = self.doc()
        self.assertEqual(doc['afrr_activated']['zones']['ES']['process'], 'A68')
        self.assertIn('ES', doc['afrr_activated']['not_published']['A16'])
        summary = doc['afrr_cbmp']['zones']['ES']
        self.assertEqual((summary['summarized_until'], summary['data_time'], summary['resolution_seconds']),
                         ('2026-11-18T17:45:00+00:00', '2026-11-18T17:30:00+00:00', 4))
        day = summary['days']['2026-11-18']
        self.assertEqual((day['start'], day['quarters']), ('2026-11-17T23:00:00+00:00', 96))
        # 17:00 UTC is the 73rd quarter-hour of the delivery day.
        self.assertEqual([day['up'][name][72] for name in ('min', 'avg', 'max', 'count')], [0.5, 95.08, 146.05, 37])
        self.assertEqual(day['down']['count'][72:76], [211, 225, 68, None])
        self.assertEqual(day['up']['count'][:72], [None] * 72)

    def test_zones_are_in_the_feeds_order_and_keep_their_currency(self):
        self.run_at(ASKED_AT)
        zones = self.doc()['afrr_activated']['zones']
        self.assertEqual(list(zones), ['PT', 'ES', 'PL'])
        self.assertEqual(zones['PL']['currency'], 'PLN')

    def test_moldova_and_the_united_kingdom_are_never_asked_for(self):
        _code, entsoe, _log = self.run_at(ASKED_AT)
        asked = {params['controlArea_Domain'] for params in entsoe.asked}
        self.assertNotIn(fb.fe.ZONES['MD'], asked)
        self.assertNotIn(fb.fe.ZONES['GB'], asked)
        # Three requests for each zone (the two processes of 17.1.F and PICASSO), and two more for Italy,
        # which is asked for 17.1.F by two areas.
        self.assertEqual(len(entsoe.asked), 3 * len(fb.AREAS) + 2)


class NotInEuros(Feed):

    POLAND = '10YPL-AREA-----S'

    def rates(self, table):
        with open('data/fx-rates.json', 'w') as f:
            json.dump({'updated': 'u', 'rates': table}, f)

    def test_a_zone_in_another_currency_has_its_prices_in_euros_too_with_the_rate_and_its_day(self):
        self.rates({'PLN': {'2026-11-13': 4.4, '2026-11-17': 4.5, '2026-11-18': 4.4118}})
        self.run_at(ASKED_AT)
        poland = self.doc()['afrr_activated']['zones']['PL']
        day = poland['days']['2026-11-17']
        self.assertEqual((poland['currency'], day['fx_rate'], day['fx_date']), ('PLN', 4.5, '2026-11-17'))
        # The first upward period of 17 Nov: 1 568.62 zloty, 348.58 euros at 4.5.
        self.assertEqual((day['up'][0], day['up_eur'][0]), (1568.62, 348.58))
        self.assertEqual([value is None for value in day['down_eur']], [value is None for value in day['down']])
        self.assertEqual(poland['days']['2026-11-18']['fx_rate'], 4.4118)

    def test_while_the_days_rate_is_not_out_the_last_one_published_is_used(self):
        self.rates({'PLN': {'2026-11-13': 4.4}})
        self.run_at(ASKED_AT)
        day = self.doc()['afrr_activated']['zones']['PL']['days']['2026-11-18']
        self.assertEqual((day['fx_rate'], day['fx_date']), (4.4, '2026-11-13'))

    def test_with_no_rate_the_prices_stay_as_published_and_there_are_none_in_euros(self):
        self.run_at(ASKED_AT)
        day = self.doc()['afrr_activated']['zones']['PL']['days']['2026-11-18']
        self.assertEqual(sorted(day), ['down', 'periods', 'start', 'up'])

    def test_the_bosnian_mark_is_at_its_fixed_rate(self):
        read = dict(fb.read_activated(sample('balancing_A16_PL.xml')), currency='BAM')
        entry = fb.activated_entry('x', 'A16', read, date(2026, 11, 18), fb.fx_rates.Rates({}))
        day = entry['days']['2026-11-17']
        self.assertEqual((day['fx_rate'], day['fx_date'], day['up_eur'][0]), (1.95583, None, 802.02))

    def test_a_zone_in_euros_has_nothing_added(self):
        self.rates({'PLN': {'2026-11-18': 4.4118}})
        self.run_at(ASKED_AT)
        self.assertEqual(sorted(self.doc()['afrr_activated']['zones']['ES']['days']['2026-11-18']),
                         ['down', 'periods', 'start', 'up'])

    def test_the_rates_are_brought_up_to_date_before_they_are_used(self):
        called = []
        entsoe = Entsoe(self.ANSWERS)
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(io.StringIO()):
            fb.main(now=ASKED_AT, get=entsoe, budget=fc.Budget(60), interval=0, update_rates=called.append)
        self.assertEqual(called, [ASKED_AT])

    def test_the_ecb_failing_does_not_stop_the_step(self):
        def broken(now):
            raise OSError('no network')
        entsoe = Entsoe(self.ANSWERS)
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            code = fb.main(now=ASKED_AT, get=entsoe, budget=fc.Budget(60), interval=0, update_rates=broken)
        self.assertEqual(code, 0)
        self.assertIn('Exchange rates not brought up to date: OSError: no network', log.getvalue())


class ItalyByTwoAreas(Feed):

    COUNTRY, NORTH = '10YIT-GRTN-----B', fb.fe.ZONES['IT']

    def italy(self, answers):
        _code, entsoe, _log = self.run_at(ASKED_AT, answers + self.ANSWERS)
        return self.doc()['afrr_activated']['zones'].get('IT'), entsoe

    def test_is_asked_for_by_the_country_and_by_the_north_zone_in_both_processes(self):
        _entry, entsoe = self.italy([])
        asked = {(p['processType'], p['controlArea_Domain']) for p in entsoe.asked if p['processType'] in ('A16', 'A68')
                 and p['controlArea_Domain'] in (self.COUNTRY, self.NORTH)}
        self.assertEqual(asked, {('A16', self.COUNTRY), ('A16', self.NORTH), ('A68', self.COUNTRY), ('A68', self.NORTH)})

    def test_the_area_that_has_prices_stays_and_the_file_says_which(self):
        entry, _entsoe = self.italy([({'processType': 'A16', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml')])
        self.assertEqual((entry['eic'], entry['area'], entry['process']), (self.NORTH, 'North bidding zone', 'A16'))

    def test_the_country_stays_when_it_is_the_one_with_prices(self):
        entry, _entsoe = self.italy([({'processType': 'A68', 'controlArea_Domain': self.COUNTRY}, 'balancing_A68_ES.xml')])
        self.assertEqual((entry['eic'], entry['area'], entry['process']),
                         (self.COUNTRY, 'control area of the country', 'A68'))

    def test_with_both_the_one_whose_prices_go_furthest_stays(self):
        entry, _entsoe = self.italy([
            ({'processType': 'A16', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml'),         # up to 17 Nov 13:00
            ({'processType': 'A68', 'controlArea_Domain': self.COUNTRY}, 'balancing_A68_ES.xml')])      # up to 18 Nov 17:15
        self.assertEqual((entry['eic'], entry['data_time']), (self.COUNTRY, '2026-11-18T17:15:00+00:00'))

    def test_with_both_processes_the_one_with_more_periods_priced_stays(self):
        """In the North zone, A16 has a few hours and A68 whole days."""
        entry, _entsoe = self.italy([
            ({'processType': 'A16', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml'),
            ({'processType': 'A68', 'controlArea_Domain': self.NORTH}, 'balancing_A68_ES.xml')])
        self.assertEqual((entry['eic'], entry['process']), (self.NORTH, 'A68'))
        self.assertGreater(fb.priced_periods(entry), 100)

    def test_with_both_processes_a16_stays_when_it_has_more(self):
        entry, _entsoe = self.italy([
            ({'processType': 'A16', 'controlArea_Domain': self.NORTH}, 'balancing_A68_ES.xml'),
            ({'processType': 'A68', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml')])
        self.assertEqual(entry['process'], 'A16')

    def test_with_both_processes_a16_stays_when_they_tie(self):
        entry, _entsoe = self.italy([
            ({'processType': 'A16', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml'),
            ({'processType': 'A68', 'controlArea_Domain': self.NORTH}, 'balancing_A16_IT.xml')])
        self.assertEqual(entry['process'], 'A16')

    def test_the_rule_is_italys_alone(self):
        """Elsewhere A16 stays whenever it has prices, however few."""
        _code, _entsoe, _log = self.run_at(ASKED_AT, [
            ({'processType': 'A16', 'controlArea_Domain': ES}, 'balancing_A16_IT.xml'),
            ({'processType': 'A68', 'controlArea_Domain': ES}, 'balancing_A68_ES.xml')] + self.ANSWERS)
        self.assertEqual(self.doc()['afrr_activated']['zones']['ES']['process'], 'A16')

    def test_with_neither_italy_is_listed_as_not_published(self):
        entry, _entsoe = self.italy([])
        self.assertIsNone(entry)
        self.assertIn('IT', self.doc()['afrr_activated']['not_published']['A16'])

    def test_the_other_zones_have_no_area(self):
        self.run_at(ASKED_AT)
        self.assertNotIn('area', self.doc()['afrr_activated']['zones']['PT'])


class TheNextRuns(Feed):

    def test_a_quarter_of_an_hour_later_only_portugal_and_spain_are_asked_for(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=15))
        self.assertEqual([(p['processType'], p['controlArea_Domain']) for p in entsoe.asked],
                         [('A16', PT), ('A68', ES), ('A67', ES)])
        # The 4-second series is asked for from the last quarter-hour summarised.
        self.assertEqual(entsoe.asked[2]['periodStart'], '202611181745')
        self.assertEqual(entsoe.asked[0]['periodStart'], '202611142300')      # 17.1.F: three delivery days back

    def test_an_hour_later_the_other_zones_with_prices_are_asked_for_again(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=60))
        asked = [(p['processType'], p['controlArea_Domain']) for p in entsoe.asked]
        self.assertIn(('A16', '10YPL-AREA-----S'), asked)
        # What ENTSO-E had no data for is asked again once a day, but for Portugal and Spain: once an hour.
        self.assertNotIn(('A68', '10YPL-AREA-----S'), asked)
        self.assertNotIn(('A16', fb.AREAS['FR']), asked)
        self.assertIn(('A68', PT), asked)
        self.assertIn(('A67', PT), asked)
        self.assertIn(('A16', ES), asked)

    def test_a_day_later_everything_is_asked_for_again(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(hours=23, minutes=15))
        self.assertIn(('A16', fb.AREAS['FR']), [(p['processType'], p['controlArea_Domain']) for p in entsoe.asked])

    def test_a_zone_that_does_not_answer_keeps_its_prices_the_time_and_a_mark(self):
        self.run_at(ASKED_AT)
        answers = [({'controlArea_Domain': ES}, None)] + self.ANSWERS
        code, _entsoe, log = self.run_at(ASKED_AT + timedelta(minutes=15), answers)
        doc = self.doc()
        self.assertEqual(code, 0)
        for block in ('afrr_activated', 'afrr_cbmp'):
            spain = doc[block]['zones']['ES']
            self.assertEqual((spain['fetched_at'], spain.get('carried_over')), ('2026-11-18T18:07:42+00:00', True))
        portugal = doc['afrr_activated']['zones']['PT']
        self.assertEqual((portugal['fetched_at'], portugal.get('carried_over')), ('2026-11-18T18:22:42+00:00', None))
        self.assertEqual(doc['updated'], '2026-11-18T18:22:42+00:00')
        self.assertIn('Kept, not obtained in this run: ES', log)
        # ...and is asked for again in the next run, and loses the mark when it answers.
        self.run_at(ASKED_AT + timedelta(minutes=30))
        self.assertNotIn('carried_over', self.doc()['afrr_activated']['zones']['ES'])

    def test_a_zone_read_once_an_hour_stays_as_it_was_in_between(self):
        self.run_at(ASKED_AT)
        self.run_at(ASKED_AT + timedelta(minutes=15))
        poland = self.doc()['afrr_activated']['zones']['PL']
        self.assertEqual((poland['fetched_at'], poland.get('carried_over')), ('2026-11-18T18:07:42+00:00', None))

    def test_with_nothing_obtained_the_file_is_not_touched(self):
        self.run_at(ASKED_AT)
        with open(PATH, 'rb') as f:
            before = (f.read(), os.stat(PATH).st_ino)
        code, _entsoe, log = self.run_at(ASKED_AT + timedelta(minutes=15), [({}, None)])
        with open(PATH, 'rb') as f:
            self.assertEqual((f.read(), os.stat(PATH).st_ino), before)
        self.assertEqual(code, 1)
        self.assertIn('left exactly as it was', log)

    def test_a_first_run_with_nothing_writes_nothing(self):
        code, _entsoe, _log = self.run_at(ASKED_AT, [({}, fc.Response(503, {}, b'<html>', error='Service Unavailable'))])
        self.assertEqual(code, 1)
        self.assertFalse(os.path.exists(PATH))

    def test_out_of_time_what_was_not_asked_stays_for_the_next_run(self):
        code, entsoe, log = self.run_at(ASKED_AT, budget=0)
        self.assertEqual((code, entsoe.asked), (1, []))
        self.assertIn('left for the next run', log)

    def test_days_that_leave_the_window_leave_the_file(self):
        self.run_at(ASKED_AT)
        # The next delivery day, with Portugal not answering: of its two days (15 and 16) only the 16th is still
        # within the three days before the day in course.
        later = at('2026-11-19 10:00')                                    # delivery day 19 Nov
        self.run_at(later, [({'controlArea_Domain': PT}, None)] + self.ANSWERS)
        portugal = self.doc()['afrr_activated']['zones'].get('PT')
        self.assertEqual(list(portugal['days']), ['2026-11-16'])
        self.assertTrue(portugal['carried_over'])
        # The PICASSO summary keeps the day in course and the one before only.
        self.assertEqual(list(self.doc()['afrr_cbmp']['zones']['ES']['days']), ['2026-11-18'])
        # A day later still, nothing of Portugal's is left to keep.
        self.run_at(at('2026-11-20 10:00'), [({'controlArea_Domain': PT}, None)] + self.ANSWERS)
        self.assertNotIn('PT', self.doc()['afrr_activated']['zones'])


if __name__ == '__main__':
    unittest.main()
