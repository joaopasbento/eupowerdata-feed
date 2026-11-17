"""Tests for reading "the last value published" out of ENTSO-E's answers for
the last 24 hours (fetch_entsoe.latest_generation / latest_flow) and out of
Elexon's (fetch_gb.fetch_gb_generation).

Run from the repository root:  python -m unittest discover -s tests -v

No network: the answers are XML documents written by hand (tests/entsoe_xml.py)
with what ENTSO-E's have: intervals of 15, 30 or 60 minutes, curveType A03
(a point that repeats the one before is left out), a series cut in pieces.
"""

import contextlib
import io
import unittest
from datetime import timedelta
from unittest import mock

import fetch_entsoe as fe
import fetch_gb
from entsoe_xml import FLOW_NS, NO_DATA, at, document, iso, series
from test_carry_over import GENERATION, Feed, read

NOW = at('2026-11-16 10:20:00')


def flow(*all_series, now=NOW):
    return fe.latest_flow(document(*all_series, ns=FLOW_NS), now)


class TheWindow(unittest.TestCase):

    def test_is_the_last_24_hours_in_whole_hours(self):
        self.assertEqual(fe.last_day(NOW), ('202611151000', '202611161100'))
        self.assertEqual(fe.last_day(at('2026-11-17 00:59:00')), ('202611160000', '202611170100'))
        self.assertEqual(fe.last_day(at('2027-01-01 00:00:00')), ('202612310000', '202701010100'))

    def test_interval_lengths(self):
        self.assertEqual(fe._step('PT15M'), timedelta(minutes=15))
        self.assertEqual(fe._step('PT30M'), timedelta(minutes=30))
        self.assertEqual(fe._step('PT60M'), timedelta(hours=1))
        self.assertEqual(fe._step('PT1H'), timedelta(hours=1))
        self.assertEqual(fe._step(None), timedelta(hours=1))
        self.assertEqual(fe._step('nonsense'), timedelta(hours=1))


class Generation(unittest.TestCase):

    def test_is_the_last_value_of_each_technology_and_the_time_it_is_for(self):
        # Quarter-hours from 09:00: 09:00, 09:15, 09:30, 09:45 published.
        got = fe.latest_generation(document(
            series('2026-11-16 09:00', [100, 110, 120, 130], minutes=15, psr='B16'),
            series('2026-11-16 09:00', [900, 910, 920, 930], minutes=15, psr='B04')), NOW)
        self.assertEqual(got['mix'], {'Solar': 130.0, 'Fossil Gas': 930.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 09:45:00'))
        self.assertEqual(got['newest'], iso('2026-11-16 09:45:00'))
        self.assertEqual(got['behind'], [])
        self.assertEqual(fe._data_of(got, NOW), 'data of 16 Nov 09:45, 35 min before the run')

    def test_yesterdays_value_when_today_has_none_yet(self):
        got = fe.latest_generation(document(
            series('2026-11-16 00:00', [50] * 23 + [70], psr='B14')), at('2026-11-17 00:59:00'))
        self.assertEqual(got['mix'], {'Nuclear': 70.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 23:00:00'))

    def test_a_point_left_out_repeats_the_one_before(self):
        # curveType A03, up to 10:00 (the Period ends at 10:15): nuclear has not
        # moved since 09:00, so its last point written is the 09:00 one.
        got = fe.latest_generation(document(
            series('2026-11-16 09:00', [4000, None, None, None, None], minutes=15, psr='B14', curve='A03'),
            series('2026-11-16 09:00', [100, 110, None, 130, 140], minutes=15, psr='B16', curve='A03')), NOW)
        self.assertEqual(got['mix'], {'Nuclear': 4000.0, 'Solar': 140.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 10:00:00'))     # not 09:00
        self.assertEqual(got['newest'], iso('2026-11-16 10:00:00'))

    def test_a_point_left_out_does_not_reach_past_now(self):
        # The Period goes on to midnight; at 10:20 the interval in force is 10:15.
        got = fe.latest_generation(document(
            series('2026-11-16 09:00', [4000], minutes=15, psr='B14', curve='A03', end='2026-11-17 00:00')), NOW)
        self.assertEqual(got['data_time'], iso('2026-11-16 10:15:00'))

    def test_a_series_in_pieces_is_not_counted_twice(self):
        # Gas comes as two TimeSeries: up to 03:00, and from 05:00 on.
        answer = document(
            series('2026-11-15 22:00', [500, 510, 520, 530, 540, 550], psr='B04'),
            series('2026-11-16 05:00', [700, 710, 720, 730, 740], psr='B04'),
            series('2026-11-15 22:00', [10] * 12, psr='B16'))
        got = fe.latest_generation(answer, NOW)
        self.assertEqual(got['mix'], {'Fossil Gas': 740.0, 'Solar': 10.0})     # not 550 + 740
        self.assertEqual(got['data_time'], iso('2026-11-16 09:00:00'))

    def test_a_technology_published_later_than_the_others_shows_in_the_times(self):
        got = fe.latest_generation(document(
            series('2026-11-16 00:00', [900] * 10, psr='B04'),                # up to 09:00
            series('2026-11-16 00:00', [300] * 7, psr='B19')), NOW)           # wind only up to 06:00
        self.assertEqual(got['mix'], {'Fossil Gas': 900.0, 'Wind Onshore': 300.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 06:00:00'))     # the oldest part of the mix
        self.assertEqual(got['newest'], iso('2026-11-16 09:00:00'))
        self.assertEqual(got['behind'], [('Wind Onshore', iso('2026-11-16 06:00:00'), 300.0)])
        self.assertEqual(fe._data_of(got, NOW),
                         'data of 16 Nov 06:00 to 09:00, 260 min before the run; '
                         'behind the rest: Wind Onshore 16 Nov 06:00 (300 MW)')

    def test_consumption_is_not_added_to_the_generation_of_the_same_technology(self):
        # Portugal at 06:00 UTC on 17 Nov 2026, in round numbers: its pumped
        # storage plants generating and pumping at the same time.
        got = fe.latest_generation(document(
            series('2026-11-16 00:00', [600] * 10, psr='B10'),
            series('2026-11-16 00:00', [900] * 10, psr='B10', out=True),
            series('2026-11-16 00:00', [3800] * 10, psr='B19')), NOW)
        self.assertEqual(got['mix'], {'Hydro Pumped Storage': 600.0, 'Wind Onshore': 3800.0})    # not 1 500
        self.assertEqual(got['consumption'], [('Hydro Pumped Storage', iso('2026-11-16 09:00:00'), 900.0)])
        self.assertEqual(fe._data_of(got, NOW),
                         'data of 16 Nov 09:00, 80 min before the run; '
                         'consumption, not in the mix: Hydro Pumped Storage 900 MW (16 Nov 09:00)')

    def test_a_technology_that_only_consumes_is_not_in_the_mix(self):
        got = fe.latest_generation(document(
            series('2026-11-16 00:00', [12] * 10, psr='B20', out=True),
            series('2026-11-16 00:00', [100] * 10, psr='B16')), NOW)
        self.assertEqual(got['mix'], {'Solar': 100.0})
        self.assertEqual(got['consumption'], [('Other', iso('2026-11-16 09:00:00'), 12.0)])

    def test_a_consumption_series_that_stopped_does_not_date_the_mix(self):
        # Norway (NO2) on 16 Nov 2026: pumping was last published at 13:30 the
        # day before; everything it generates is published up to 07:45.
        got = fe.latest_generation(document(
            series('2026-11-15 10:00', [7000] * 88, minutes=15, psr='B12'),             # up to 07:45
            series('2026-11-15 10:00', [20] * 88, minutes=15, psr='B10'),
            series('2026-11-15 10:00', [13] * 15, minutes=15, psr='B10', out=True)), NOW)   # up to 13:30
        self.assertEqual(got['mix'], {'Hydro Water Reservoir': 7000.0, 'Hydro Pumped Storage': 20.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 07:45:00'))
        self.assertEqual(got['behind'], [])
        self.assertEqual(got['consumption'], [('Hydro Pumped Storage', iso('2026-11-15 13:30:00'), 13.0)])

    def test_a_country_with_consumption_only_has_no_mix(self):
        self.assertIsNone(fe.latest_generation(document(
            series('2026-11-16 00:00', [50] * 6, psr='B10', out=True)), NOW))

    def test_only_later_points_gives_the_earliest_of_them_as_before(self):
        got = fe.latest_generation(document(
            series('2026-11-16 12:00', [80, 90], psr='B16')), NOW)
        self.assertEqual(got['mix'], {'Solar': 80.0})
        self.assertEqual(got['data_time'], iso('2026-11-16 12:00:00'))

    def test_keeps_the_order_and_the_names(self):
        got = fe.latest_generation(document(
            series('2026-11-16 09:00', [1], psr='B19'), series('2026-11-16 09:00', [2], psr='B01'),
            series('2026-11-16 09:00', [3], psr='B99'), series('2026-11-16 09:00', [4], psr='B20')), NOW)
        self.assertEqual(list(got['mix'].items()), [('Wind Onshore', 1.0), ('Biomass', 2.0), ('Other', 7.0)])

    def test_gives_what_the_whole_day_parser_gives_for_a_whole_day(self):
        # A day long past, so that parse_generation_xml(), which goes by the
        # real clock, sees every point as published.
        generation = [
            series('2025-06-01 00:00', [float(v) for v in range(96)], minutes=15, psr='B16'),
            series('2025-06-01 00:00', [float(v) for v in range(500, 524)], psr='B04'),
            series('2025-06-01 00:00', [30.5] * 48, minutes=30, psr='B10')]
        consumption = series('2025-06-01 00:00', [9.5] * 48, minutes=30, psr='B10', out=True)
        with contextlib.redirect_stdout(io.StringIO()):
            before = fe.parse_generation_xml(document(*generation))
            with_consumption = fe.parse_generation_xml(document(*generation, consumption))
        got = fe.latest_generation(document(*generation, consumption), at('2025-06-02 00:30:00'))
        self.assertEqual(got['mix'], before)
        self.assertEqual(list(got['mix']), list(before))
        self.assertEqual((got['data_time'], got['newest']), (iso('2025-06-01 23:00:00'), iso('2025-06-01 23:45:00')))
        # The one difference: the whole-day parser, which backfill_archive.py
        # still uses, adds the consumption to the technology.
        self.assertEqual(with_consumption['Hydro Pumped Storage'], before['Hydro Pumped Storage'] + 9.5)

    def test_no_data(self):
        for answer in (NO_DATA, '<html><body>Service unavailable</body></html>', 'not xml at all', '',
                       document(), document(series('2026-11-16 09:00', [], psr='B16'))):
            self.assertIsNone(fe.latest_generation(answer, NOW), answer[:60])

    def test_an_unreadable_point_is_skipped(self):
        answer = document(series('2026-11-16 08:00', [10, 20], psr='B16')).replace(
            '<quantity>20</quantity>', '<quantity>n/a</quantity>')
        self.assertEqual(fe.latest_generation(answer, NOW)['mix'], {'Solar': 10.0})


class GenerationInTheFile(Feed):
    """What a run publishes in generation-mix.json."""

    def test_the_total_and_the_renewable_share_leave_consumption_out(self):
        self.entsoe(NOW, generation={'PT': document(
            series('2026-11-16 00:00', [1000] * 10, psr='B04'),              # gas
            series('2026-11-16 00:00', [400] * 10, psr='B12'),               # reservoir hydro, renewable
            series('2026-11-16 00:00', [600] * 10, psr='B10'),               # pumped storage, generating
            series('2026-11-16 00:00', [900] * 10, psr='B10', out=True))})   # and pumping
        zone = read(GENERATION)['zones']['PT']
        self.assertEqual(zone['mix'], {'Fossil Gas': 1000.0, 'Hydro Water Reservoir': 400.0,
                                       'Hydro Pumped Storage': 600.0})
        self.assertEqual(zone['total_mw'], 2000.0)                           # not 2 900
        self.assertEqual(zone['renewable_pct'], 20.0)                        # 400 of 2 000, not of 2 900
        self.assertEqual(list(zone), ['mix', 'total_mw', 'renewable_pct', 'data_time', 'fetched_at'])

    def test_the_run_log_still_says_what_was_consumed(self):
        _code, log = self.entsoe(NOW, generation={'PT': document(
            series('2026-11-16 00:00', [600] * 10, psr='B10'),
            series('2026-11-16 00:00', [900] * 10, psr='B10', out=True))})
        self.assertIn('consumption, not in the mix: Hydro Pumped Storage 900 MW (16 Nov 09:00)', log)


class Flows(unittest.TestCase):

    def test_the_days_series_starts_at_midnight(self):
        # Hourly from yesterday 10:00 to today 09:00.
        got = flow(series('2026-11-15 10:00', [100 + h for h in range(24)]))
        self.assertEqual(len(got['values']), 10)
        self.assertEqual(got['values'][0], {'time': '00:00', 'mw': 114})
        self.assertEqual(got['values'][-1], {'time': '09:00', 'mw': 123})
        self.assertEqual(got['latest'], {'time': '09:00', 'mw': 123})
        self.assertEqual(got['data_time'], iso('2026-11-16 09:00:00'))

    def test_quarter_hours_have_their_real_times(self):
        got = flow(series('2026-11-16 08:00', [1, 2, 3, 4, 5, 6], minutes=15))
        self.assertEqual([v['time'] for v in got['values']], ['08:00', '08:15', '08:30', '08:45', '09:00', '09:15'])
        self.assertEqual(got['latest'], {'time': '09:15', 'mw': 6})
        self.assertEqual(got['data_time'], iso('2026-11-16 09:15:00'))

    def test_with_nothing_for_today_yet_the_last_value_is_yesterdays(self):
        got = flow(series('2026-11-16 01:00', [300] * 21 + [320, 340]), now=at('2026-11-17 00:59:00'))
        self.assertEqual(got['values'], [])
        self.assertEqual(got['latest'], {'time': '23:00', 'mw': 340})
        self.assertEqual(got['data_time'], iso('2026-11-16 23:00:00'))

    def test_a_flow_that_has_not_changed_since_yesterday(self):
        # curveType A03: nothing has flowed since 18:00 yesterday, so that is
        # the last point written, in a Period that goes on to 09:00 today.
        got = flow(series('2026-11-15 10:00', [250] * 8 + [0] + [None] * 15, curve='A03'))
        self.assertEqual(got['values'], [{'time': '00:00', 'mw': 0}])      # what "since midnight" gave
        self.assertEqual(got['latest'], {'time': '00:00', 'mw': 0})
        self.assertEqual(got['data_time'], iso('2026-11-16 09:00:00'))     # not yesterday 18:00

    def test_points_left_out_today(self):
        # 15 minutes, A03, from 22:00 yesterday: changes at 23:30, 02:00 and 02:15.
        values = [40] + [None] * 5 + [55] + [None] * 9 + [70, 80] + [None] * 14
        got = flow(series('2026-11-15 22:00', values, minutes=15, curve='A03'))
        self.assertEqual(got['values'], [{'time': '00:00', 'mw': 55}, {'time': '02:00', 'mw': 70},
                                         {'time': '02:15', 'mw': 80}])
        self.assertEqual(got['latest'], {'time': '02:15', 'mw': 80})
        self.assertEqual(got['data_time'], iso('2026-11-16 05:45:00'))     # the Period ends at 06:00

    def test_a_series_in_pieces(self):
        got = flow(series('2026-11-15 20:00', [10, 20, 30]),               # 20:00 to 22:00
                   series('2026-11-16 00:00', [40, 50]),                   # 00:00 and 01:00
                   series('2026-11-16 06:00', [60, 70], minutes=30))       # 06:00 and 06:30
        self.assertEqual(got['values'], [{'time': '00:00', 'mw': 40}, {'time': '01:00', 'mw': 50},
                                         {'time': '06:00', 'mw': 60}, {'time': '06:30', 'mw': 70}])
        self.assertEqual(got['data_time'], iso('2026-11-16 06:30:00'))

    def test_megawatts_are_whole_numbers_as_before(self):
        got = flow(series('2026-11-16 08:00', [123.6, 99.4]))
        self.assertEqual(got['latest'], {'time': '09:00', 'mw': 99})
        self.assertIsInstance(got['latest']['mw'], int)

    def test_no_data(self):
        for answer in (NO_DATA, '<html></html>', '', document(ns=FLOW_NS)):
            self.assertIsNone(fe.latest_flow(answer, NOW), answer[:60])


class Elexon(unittest.TestCase):
    """fetch_gb.py: the mix is the last settlement period published in the last
    24 hours; the price is still asked for today only."""

    NIGHT = at('2026-11-17 00:08:00')

    AGPT = {'data': [
        {'startTime': '2026-11-16T23:00:00Z', 'settlementPeriod': 47, 'data': [
            {'psrType': 'Nuclear', 'quantity': 3900}, {'psrType': 'Fossil Gas', 'quantity': 9500},
            {'psrType': 'Wind Onshore', 'quantity': 90}]},
        {'startTime': '2026-11-16T23:30:00Z', 'settlementPeriod': 48, 'data': [
            {'psrType': 'Nuclear', 'quantity': 4000}, {'psrType': 'Fossil Gas', 'quantity': 9000},
            {'psrType': 'Wind Onshore', 'quantity': 100}]},
    ]}
    B1630 = {'data': [
        {'startTime': '2026-11-16T21:00:00Z', 'publishTime': '2026-11-16T22:10:00Z',
         'psrType': 'Wind Onshore', 'quantity': 2900},
        {'startTime': '2026-11-16T21:30:00Z', 'publishTime': '2026-11-16T22:40:00Z',
         'psrType': 'Wind Onshore', 'quantity': 3000},
        {'startTime': '2026-11-16T21:30:00Z', 'publishTime': '2026-11-16T22:40:00Z',
         'psrType': 'Wind Offshore', 'quantity': 6000},
        {'startTime': '2026-11-16T21:30:00Z', 'publishTime': '2026-11-16T22:40:00Z',
         'psrType': 'Solar', 'quantity': 0},
    ]}
    MID = {'data': [{'startTime': '2026-11-17T00:00:00Z', 'dataProvider': 'APXMIDP', 'price': 80.0, 'volume': 500.0}]}

    def run_with(self, function, answers):
        asked = []

        def http_get_json(url, timeout=30):
            asked.append(url)
            return next((answer for part, answer in answers.items() if part in url), None)

        with mock.patch.object(fetch_gb, 'utcnow', lambda: self.NIGHT), \
                mock.patch.object(fetch_gb, 'http_get_json', http_get_json), \
                contextlib.redirect_stdout(io.StringIO()):
            return function(), asked

    def test_the_mix_just_after_midnight_is_yesterdays_last_period(self):
        gen, asked = self.run_with(fetch_gb.fetch_gb_generation,
                                   {'wind-and-solar': self.B1630, 'per-type': self.AGPT})
        self.assertEqual(gen['mix'], {'Nuclear': 4000, 'Fossil Gas': 9000, 'Wind Onshore': 3000,
                                      'Wind Offshore': 6000})
        self.assertEqual(gen['total_mw'], 22000)
        # As old as its oldest part: wind and solar come out later than the rest.
        self.assertEqual(gen['data_time'], iso('2026-11-16 21:30:00'))
        self.assertEqual(list(gen), ['mix', 'total_mw', 'renewable_pct', 'source', 'data_time'])
        for url in asked:
            self.assertIn('from=2026-11-16T00%3A08Z&to=2026-11-17T01%3A08Z', url)

    def test_without_wind_and_solar_it_is_the_period_of_the_rest(self):
        gen, _asked = self.run_with(fetch_gb.fetch_gb_generation, {'per-type': self.AGPT, 'wind-and-solar': None})
        self.assertEqual(gen['data_time'], iso('2026-11-16 23:30:00'))

    def test_nothing_in_24_hours_is_no_mix(self):
        gen, _asked = self.run_with(fetch_gb.fetch_gb_generation, {'per-type': {'data': []}})
        self.assertIsNone(gen)

    def test_the_price_is_still_asked_for_today_only(self):
        price, asked = self.run_with(fetch_gb.fetch_gb_price,
                                     {'market-index': self.MID, 'frankfurter': {'rates': {'EUR': 1.2}}})
        self.assertIn('from=2026-11-17T00%3A00Z&to=2026-11-17T01%3A08Z', asked[0])
        self.assertEqual(price['prices'], [{'time': '00:00', 'hour': 0, 'price': 96.0}])
        self.assertNotIn('data_time', price)


if __name__ == '__main__':
    unittest.main()
