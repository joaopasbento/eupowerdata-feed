"""Tests for the forecast error of wind and solar (wind_solar_actual.py,
fetch_forecast.py and what fetch_entsoe.py keeps for them), on real answers of
ENTSO-E (tests/fixtures/entsoe, obtained on 18 Nov 2026 at 18:08 UTC).

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers were worked out from the answers by hand (the points as
they are written in the XML, the ones left out put back), not with the code
under test.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import backfill_archive
import feed_common as fc
import fetch_entsoe as fe
import fetch_forecast as ff
import wind_solar_actual as ws
from entsoe_real import KEY, Entsoe, sample
from entsoe_xml import NO_DATA, document, series
from test_carry_over import Feed as EntsoeFeed

ASKED_AT = datetime(2026, 11, 18, 18, 8, 34, tzinfo=timezone.utc)      # when the generation answers were obtained
PT, ES, BE = (fe.GEN_ZONES[code] for code in ('PT', 'ES', 'BE'))


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def generation(zone, now=ASKED_AT):
    """What fetch_entsoe.py reads of a zone's kept A75 answer for wind and solar."""
    return fe.read_generation(sample(f'generation_A75_{zone}.xml').decode('utf-8'), now)['wind_solar']


class TheSeriesOfTheDay(unittest.TestCase):
    """What fetch_entsoe.py takes from the generation answers it already gets."""

    def test_portugal_has_the_three_technologies_by_the_hour(self):
        found = generation('PT')
        self.assertEqual(sorted(found), ['solar', 'wind_offshore', 'wind_onshore'])
        step, solar = found['solar']
        self.assertEqual(step, timedelta(hours=1))
        # The answer covers 17 Nov 18:00 to 18 Nov 17:00 UTC: 23 hours.
        self.assertEqual((min(solar), max(solar), len(solar)), (at('2026-11-17 18:00'), at('2026-11-18 16:00'), 23))
        self.assertEqual([solar[at(f'2026-11-18 {hour}:00')] for hour in (14, 15, 16)], [402.6, 318.2, 104.7])
        self.assertEqual(found['wind_onshore'][1][at('2026-11-18 16:00')], 4614.9)

    def test_spain_has_them_by_the_quarter_hour_with_the_points_left_out_put_back(self):
        step, solar = generation('ES')['solar']
        self.assertEqual((step, len(solar)), (timedelta(minutes=15), 95))       # 53 points in the answer
        self.assertEqual(solar[at('2026-11-18 17:30')], 108.0)

    def test_consumption_is_not_generation(self):
        # A series with outBiddingZone_Domain is what a technology takes from the grid.
        xml = document(series('2026-11-18 10:00:00', [100, 110], psr='B16'),
                       series('2026-11-18 10:00:00', [7, 9], psr='B16', out=True))
        self.assertEqual(fe.read_generation(xml, ASKED_AT)['wind_solar']['solar'][1],
                         {at('2026-11-18 10:00'): 100.0, at('2026-11-18 11:00'): 110.0})

    def test_intervals_that_have_not_begun_are_left_out(self):
        xml = document(series('2026-11-18 17:00:00', [100, 110, 120], psr='B19'), series('2026-11-18 17:00:00', [5], psr='B04'))
        self.assertEqual(sorted(fe.read_generation(xml, ASKED_AT)['wind_solar']['wind_onshore'][1]),
                         [at('2026-11-18 17:00'), at('2026-11-18 18:00')])

    def test_the_generation_mix_is_what_it_was(self):
        xml = sample('generation_A75_PT.xml').decode('utf-8')
        read = fe.read_generation(xml, ASKED_AT)
        read.pop('wind_solar')
        self.assertEqual(read, fe.latest_generation(xml, ASKED_AT))


class InADirectory(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def doc(self, path):
        with open(path, encoding='utf-8') as f:
            return json.load(f)


class TheFileOfTheActuals(InADirectory):

    def test_has_the_day_before_and_the_day_in_course(self):
        ws.save({'PT': generation('PT'), 'ES': generation('ES')}, ASKED_AT)
        doc = self.doc(ws.PATH)
        block = doc['actual_generation']
        self.assertEqual(list(doc), ['updated', 'data_time', 'actual_generation'])
        self.assertEqual((doc['updated'], doc['data_time']), ('2026-11-18T18:08:34+00:00', '2026-11-18T17:30:00+00:00'))
        self.assertEqual(block['license'], 'source-terms')
        self.assertEqual(block['window'], {'start': '2026-11-16T23:00:00+00:00', 'end': '2026-11-18T23:00:00+00:00'})
        solar = block['zones']['PT']['solar']
        # One value for each hour from 16 Nov 23:00 UTC: 18 Nov 16:00 is the 42nd.
        self.assertEqual((solar['step_minutes'], len(solar['values'])), (60, 42))
        self.assertEqual(solar['values'][:19], [None] * 19)                     # before 17 Nov 18:00: not in the answer
        self.assertEqual(solar['values'][39:], [402.6, 318.2, 104.7])
        self.assertEqual(block['zones']['ES']['solar']['step_minutes'], 15)
        self.assertEqual((block['zones']['PT']['data_time'], block['zones']['PT']['fetched_at']),
                         ('2026-11-18T16:00:00+00:00', '2026-11-18T18:08:34+00:00'))

    def test_each_run_adds_to_what_was_kept_and_overwrites_what_was_revised(self):
        first = {'solar': (timedelta(hours=1), {at('2026-11-18 10:00'): 50.0, at('2026-11-18 11:00'): 60.0})}
        second = {'solar': (timedelta(hours=1), {at('2026-11-18 11:00'): 65.0, at('2026-11-18 12:00'): 70.0})}
        ws.save({'PT': first}, at('2026-11-18 12:10'))
        ws.save({'PT': second}, at('2026-11-18 13:10'))
        kept, _entries = ws.read()
        self.assertEqual(kept['PT']['solar'][1], {at('2026-11-18 10:00'): 50.0, at('2026-11-18 11:00'): 65.0,
                                                  at('2026-11-18 12:00'): 70.0})

    def test_a_zone_that_did_not_answer_keeps_its_series_and_is_marked(self):
        ws.save({'PT': generation('PT'), 'ES': generation('ES')}, ASKED_AT)
        ws.save({'PT': generation('PT')}, ASKED_AT + timedelta(minutes=15))
        zones = self.doc(ws.PATH)['actual_generation']['zones']
        self.assertEqual((zones['ES']['fetched_at'], zones['ES'].get('carried_over')), ('2026-11-18T18:08:34+00:00', True))
        self.assertEqual((zones['PT']['fetched_at'], zones['PT'].get('carried_over')), ('2026-11-18T18:23:34+00:00', None))

    def test_the_next_delivery_day_the_window_moves_on(self):
        ws.save({'PT': generation('PT')}, ASKED_AT)
        ws.save({'PT': generation('PT')}, at('2026-11-18 23:10:00'))            # delivery day 19 Nov
        block = self.doc(ws.PATH)['actual_generation']
        self.assertEqual(block['window'], {'start': '2026-11-17T23:00:00+00:00', 'end': '2026-11-19T23:00:00+00:00'})
        self.assertEqual(len(block['zones']['PT']['solar']['values']), 18)       # 17 Nov 23:00 to 18 Nov 16:00

    def test_with_nothing_obtained_the_file_is_left_as_it_was(self):
        ws.save({'PT': generation('PT')}, ASKED_AT)
        before = os.stat(ws.PATH).st_ino
        self.assertEqual(ws.save({}, ASKED_AT + timedelta(minutes=15)), 0)
        self.assertEqual(os.stat(ws.PATH).st_ino, before)


class ARunOfTheFeed(EntsoeFeed):
    """fetch_entsoe.py itself, with ENTSO-E answering as in the tests of carry_over."""

    def test_keeps_the_series_after_the_mix_and_the_mix_is_as_before(self):
        when = at('2026-11-16 10:00:05')
        code, log = self.entsoe(when, generation={'PT': 8000, 'ES': 30000})
        self.assertEqual(code, 0)
        self.assertIn('Kept the wind and solar series of the day for 2 zones', log)
        with open(ws.PATH, encoding='utf-8') as f:
            zones = json.load(f)['actual_generation']['zones']
        # The stand-in answers with a quarter of the generation as solar, by the hour, up to the hour before.
        self.assertEqual(zones['PT']['solar']['values'][-1], 2000.0)
        with open('data/generation-mix.json', encoding='utf-8') as f:
            mix = json.load(f)['zones']['PT']
        self.assertEqual(sorted(mix), ['data_time', 'fetched_at', 'mix', 'renewable_pct', 'total_mw'])

    def test_a_failure_to_keep_them_changes_nothing_else(self):
        with mock.patch.object(ws, 'save', side_effect=OSError('disk full')):
            code, log = self.entsoe(at('2026-11-16 10:00:05'), prices={'PT': 50}, generation={'PT': 8000},
                                    flows={('PT', 'ES'): 1000})
        self.assertEqual(code, 0)
        self.assertIn('Wind and solar series not kept: OSError: disk full', log)
        for path in ('data/spot-prices.json', 'data/generation-mix.json', 'data/cross-border-flows.json', 'data/status.json'):
            self.assertTrue(os.path.exists(path), path)


class ByTheHour(unittest.TestCase):

    START = at('2026-11-18 10:00')

    def test_an_hour_is_the_mean_of_its_intervals(self):
        quarters = {self.START + timedelta(minutes=15 * i): value for i, value in enumerate([10, 20, 30, 40, 100, 100, 100, 100])}
        self.assertEqual(ff.hourly((timedelta(minutes=15), quarters), self.START, 2), [25.0, 100.0])

    def test_an_hour_with_an_interval_missing_has_no_value(self):
        quarters = {self.START + timedelta(minutes=15 * i): 10 for i in (0, 1, 2, 4, 5, 6, 7)}
        self.assertEqual(ff.hourly((timedelta(minutes=15), quarters), self.START, 2), [None, 10.0])

    def test_the_error_is_actual_minus_forecast_and_the_summary_is_over_the_hours_with_both(self):
        block = ff.day_block(date(2026, 11, 18), {'solar': [100.0, 200.0, 300.0] + [None] * 21},
                             {'solar': [110.0, 170.0, None] + [None] * 21})
        self.assertEqual(block['solar']['error'][:3], [10.0, -30.0, None])
        self.assertEqual(block['summary']['solar'], {'hours': 2, 'forecast_mwh': 300.0, 'actual_mwh': 280.0,
                                                     'mean_error_mw': -10.0, 'mean_abs_error_mw': 20.0})

    def test_a_technology_with_no_forecast_is_not_there(self):
        block = ff.day_block(date(2026, 11, 18), {'solar': [1.0] * 24}, {'solar': [1.0] * 24, 'wind_offshore': [5.0] * 24})
        self.assertNotIn('wind_offshore', block)
        self.assertIsNone(ff.day_block(date(2026, 11, 18), {}, {'solar': [1.0] * 24}))

    def test_the_days_the_clocks_change_have_23_and_25_hours(self):
        self.assertEqual(ff.day_block(date(2026, 3, 29), {'solar': [1.0] * 23}, {})['hours'], 23)
        block = ff.day_block(date(2026, 10, 25), {'solar': [1.0] * 25}, {})
        self.assertEqual((block['hours'], block['start'], len(block['solar']['actual'])), (25, '2026-10-24T22:00:00+00:00', 25))


class Feed(InADirectory):
    """A temporary feed directory with the actuals kept, and runs of fetch_forecast.py in it."""

    ANSWERS = [
        ({'documentType': 'A69', 'in_Domain': PT}, 'forecast_A69_PT.xml'),
        ({'documentType': 'A69', 'in_Domain': ES}, 'forecast_A69_ES.xml'),
        ({'documentType': 'A69', 'in_Domain': BE}, 'forecast_A69_BE.xml'),
        ({'documentType': 'A69'}, 'no_data_forecast.xml'),
    ]

    def setUp(self):
        super().setUp()
        ws.save({zone: generation(zone) for zone in ('PT', 'ES', 'BE')}, ASKED_AT)

    def run_at(self, when, answers=None, budget=60):
        entsoe = Entsoe(self.ANSWERS if answers is None else answers)
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            code = ff.main(now=when, get=entsoe, budget=fc.Budget(budget), interval=0)
        return code, entsoe, log.getvalue()

    def zones(self):
        return self.doc(ff.OUT_PATH)['wind_solar']['zones']


class ARun(Feed):

    def test_writes_one_block_with_its_licence(self):
        code, _entsoe, log = self.run_at(ASKED_AT)
        doc = self.doc(ff.OUT_PATH)
        self.assertEqual(code, 0)
        self.assertEqual(list(doc), ['updated', 'data_time', 'timezone', 'wind_solar'])
        self.assertEqual(doc['wind_solar']['license'], 'source-terms')
        self.assertEqual((doc['updated'], doc['data_time']), ('2026-11-18T18:08:34+00:00', '2026-11-18T17:00:00+00:00'))
        self.assertEqual(list(doc['wind_solar']['zones']), ['PT', 'ES', 'BE'])
        self.assertNotIn(KEY, log)
        self.assertNotIn('entsoe.eu', log)

    def test_portugal_hour_by_hour(self):
        self.run_at(ASKED_AT)
        portugal = self.zones()['PT']
        day = portugal['days']['2026-11-18']
        self.assertEqual((day['start'], day['hours']), ('2026-11-17T23:00:00+00:00', 24))
        # 14:00 to 17:00 UTC are the hours 15 to 18 of the delivery day. The actuals end at 16:00.
        self.assertEqual(day['solar']['forecast'][15:19], [464.3, 270.1, 100.2, 0.0])
        self.assertEqual(day['solar']['actual'][15:19], [402.6, 318.2, 104.7, None])
        self.assertEqual(day['solar']['error'][15:19], [-61.7, 48.1, 4.5, None])
        self.assertEqual(day['wind_onshore']['error'][15:18], [149.5, 636.0, 593.9])
        self.assertEqual(day['wind_offshore']['forecast'][15:19], [20.1, 20.6, 20.5, 20.3])
        # Over the 18 hours of the day that have both: 3 964.4 MWh forecast, 4 069.9 generated.
        self.assertEqual(day['summary']['solar'], {'hours': 18, 'forecast_mwh': 3964.4, 'actual_mwh': 4069.9,
                                                   'mean_error_mw': 5.9, 'mean_abs_error_mw': 14.5})
        self.assertEqual((portugal['data_time'], portugal['fetched_at'], portugal['actual_updated']),
                         ('2026-11-18T16:00:00+00:00', '2026-11-18T18:08:34+00:00', '2026-11-18T18:08:34+00:00'))

    def test_spain_by_the_quarter_hour_is_compared_by_the_hour(self):
        self.run_at(ASKED_AT)
        day = self.zones()['ES']['days']['2026-11-18']
        # 14:00 UTC: the four quarter-hours of the forecast average 7 241.75, those of the actual 6 705.
        self.assertEqual((day['solar']['forecast'][15], day['solar']['actual'][15], day['solar']['error'][15]),
                         (7241.8, 6705.0, -536.8))
        # 17:00 UTC has three of its four quarter-hours: no value yet.
        self.assertIsNone(day['solar']['actual'][18])
        self.assertNotIn('wind_offshore', day)                     # Spain forecasts no offshore wind

    def test_a_forecast_by_the_quarter_hour_against_an_actual_by_the_hour(self):
        self.run_at(ASKED_AT)
        day = self.zones()['BE']['days']['2026-11-18']
        self.assertEqual((day['solar']['forecast'][15], day['solar']['actual'][15], day['solar']['error'][15]),
                         (454.6, 557.9, 103.3))
        self.assertEqual(day['wind_offshore']['error'][15:19], [174.2, -89.4, -444.2, None])

    def test_the_day_before_has_only_the_hours_the_actuals_cover(self):
        self.run_at(ASKED_AT)
        day = self.zones()['PT']['days']['2026-11-17']
        self.assertEqual(day['summary']['solar']['hours'], 5)       # the actuals kept begin at 18:00 UTC of the 17th
        self.assertEqual(day['solar']['actual'][:19], [None] * 19)

    def test_a_zone_with_no_forecast_is_listed_and_asked_again_a_day_later(self):
        self.run_at(ASKED_AT)
        silent = self.doc(ff.OUT_PATH)['wind_solar']['not_published']
        self.assertEqual(silent['MD'], '2026-11-18T18:08:34+00:00')
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(hours=1))
        self.assertEqual(entsoe.asked_for(in_Domain=fe.GEN_ZONES['MD']), [])
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(hours=23, minutes=15))
        self.assertEqual(len(entsoe.asked_for(in_Domain=fe.GEN_ZONES['MD'])), 1)

    def test_each_zone_is_asked_for_the_area_its_generation_is(self):
        _code, entsoe, _log = self.run_at(ASKED_AT)
        asked = [params['in_Domain'] for params in entsoe.asked]
        self.assertIn('10YIT-GRTN-----B', asked)                   # Italy as the country
        self.assertNotIn(fe.ZONES['IT'], asked)
        self.assertNotIn(fe.ZONES['GB'], asked)
        self.assertEqual({(p['periodStart'], p['periodEnd']) for p in entsoe.asked}, {('202611162300', '202611182300')})


class ACountryOfSeveralZones(Feed):

    NORWAY = (fe.GEN_ZONES['NO1'], fe.GEN_ZONES['NO2'])

    def forecast(self, mw):
        return document(series('2026-11-16 23:00:00', [mw] * 48, psr='B19')).encode()

    def setUp(self):
        super().setUp()
        hours = {at('2026-11-18 10:00') + timedelta(hours=i): value for i, value in enumerate([900.0, 950.0])}
        ws.save({'NO1': {'wind_onshore': (timedelta(hours=1), hours)},
                 'NO2': {'wind_onshore': (timedelta(hours=1), {moment: value / 2 for moment, value in hours.items()})}},
                ASKED_AT)

    def test_is_the_sum_of_its_zones_hour_by_hour(self):
        answers = [({'in_Domain': self.NORWAY[0]}, self.forecast(1000)), ({'in_Domain': self.NORWAY[1]}, self.forecast(400))]
        self.run_at(ASKED_AT, answers + self.ANSWERS)
        norway = self.zones()['NO']
        day = norway['days']['2026-11-18']
        self.assertEqual(norway['forecast_zones'], {'wind_onshore': ['NO1', 'NO2']})
        self.assertEqual(day['wind_onshore']['forecast'][11:13], [1400.0, 1400.0])
        self.assertEqual(day['wind_onshore']['actual'][11:13], [1350.0, 1425.0])
        self.assertEqual(day['wind_onshore']['error'][11:13], [-50.0, 25.0])
        self.assertNotIn('NO1', self.zones())

    def test_is_not_put_together_from_part_of_its_zones(self):
        answers = [({'in_Domain': self.NORWAY[0]}, self.forecast(1000))]
        _code, _entsoe, log = self.run_at(ASKED_AT, answers + self.ANSWERS)
        self.assertNotIn('NO', self.zones())
        self.assertIn('NO: no forecast for NO2', log)


class TheNextRuns(Feed):

    def test_a_quarter_of_an_hour_later_only_portugal_and_spain_are_asked_for(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=15))
        self.assertEqual([params['in_Domain'] for params in entsoe.asked], [PT, ES])

    def test_the_actuals_of_every_zone_are_brought_up_to_date_in_every_run(self):
        self.run_at(ASKED_AT)
        later = ASKED_AT + timedelta(minutes=15)
        ws.save({'BE': {'wind_offshore': (timedelta(hours=1), {at('2026-11-18 17:00'): 1000.0})}}, later)
        self.run_at(later)
        belgium = self.zones()['BE']
        # Belgium was not asked for (it is read once an hour): its forecast is the one it had, the actual is new.
        self.assertEqual(belgium['fetched_at'], '2026-11-18T18:08:34+00:00')
        self.assertEqual(belgium['actual_updated'], '2026-11-18T18:23:34+00:00')
        day = belgium['days']['2026-11-18']
        self.assertEqual((day['wind_offshore']['forecast'][18], day['wind_offshore']['actual'][18],
                          day['wind_offshore']['error'][18]), (1553.3, 1000.0, -553.3))
        self.assertEqual(day['summary']['wind_offshore']['hours'], 19)

    def test_a_zone_that_does_not_answer_keeps_its_forecast_the_time_and_a_mark(self):
        self.run_at(ASKED_AT)
        code, _entsoe, log = self.run_at(ASKED_AT + timedelta(minutes=15), [({'in_Domain': ES}, None)] + self.ANSWERS)
        zones = self.zones()
        self.assertEqual(code, 0)
        self.assertEqual((zones['ES']['fetched_at'], zones['ES'].get('carried_over')), ('2026-11-18T18:08:34+00:00', True))
        self.assertEqual((zones['PT']['fetched_at'], zones['PT'].get('carried_over')), ('2026-11-18T18:23:34+00:00', None))
        self.assertIn('Kept, not obtained in this run: ES', log)
        self.run_at(ASKED_AT + timedelta(minutes=30))
        self.assertNotIn('carried_over', self.zones()['ES'])

    def test_with_entsoe_not_answering_the_file_still_has_the_new_actuals(self):
        self.run_at(ASKED_AT)
        later = ASKED_AT + timedelta(minutes=15)
        ws.save({'PT': {'solar': (timedelta(hours=1), {at('2026-11-18 17:00'): 3.0})}}, later)
        code, _entsoe, _log = self.run_at(later, [({}, None)])
        portugal = self.zones()['PT']
        self.assertEqual(code, 1)
        self.assertEqual(portugal['days']['2026-11-18']['solar']['actual'][18], 3.0)
        self.assertEqual((portugal['fetched_at'], portugal.get('carried_over')), ('2026-11-18T18:08:34+00:00', True))

    def test_a_first_run_with_nothing_writes_nothing(self):
        code, _entsoe, _log = self.run_at(ASKED_AT, [({}, None)])
        self.assertEqual(code, 1)
        self.assertFalse(os.path.exists(ff.OUT_PATH))

    def test_the_next_delivery_day_the_day_before_yesterday_leaves(self):
        self.run_at(ASKED_AT)
        self.run_at(at('2026-11-18 23:10:00'), [({}, None)])        # delivery day 19 Nov, ENTSO-E not answering
        self.assertEqual(list(self.zones()['PT']['days']), ['2026-11-18'])


class TheArchive(Feed):

    def test_has_one_record_a_day_with_no_time_of_its_own(self):
        self.run_at(ASKED_AT)
        record = self.doc(ff.ARCHIVE_PATH.format(day='2026-11-18'))
        self.assertEqual(list(record), ['date', 'wind_solar'])
        self.assertEqual((record['date'], record['wind_solar']['license']), ('2026-11-18', 'source-terms'))
        self.assertEqual(list(record['wind_solar']['zones']), ['BE', 'ES', 'PT'])
        self.assertEqual(record['wind_solar']['zones']['PT'], self.zones()['PT']['days']['2026-11-18'])
        self.assertTrue(os.path.exists(ff.ARCHIVE_PATH.format(day='2026-11-17')))

    def test_is_not_rewritten_when_nothing_changed(self):
        self.run_at(ASKED_AT)
        path = ff.ARCHIVE_PATH.format(day='2026-11-18')
        before = os.stat(path).st_ino
        self.run_at(ASKED_AT + timedelta(minutes=15))
        self.assertEqual(os.stat(path).st_ino, before)

    def test_a_zone_the_file_no_longer_has_keeps_its_record(self):
        self.run_at(ASKED_AT)
        doc = self.doc(ff.OUT_PATH)
        del doc['wind_solar']['zones']['BE']
        ff.archive(doc, ASKED_AT)
        self.assertIn('BE', self.doc(ff.ARCHIVE_PATH.format(day='2026-11-18'))['wind_solar']['zones'])

    def test_records_older_than_five_years_are_taken_out(self):
        os.makedirs('data/archive', exist_ok=True)
        for name in ('wind-solar-forecast-2021-11-01.json', 'wind-solar-forecast-2021-11-30.json', 'outages-2021-11-01.json'):
            with open(os.path.join('data/archive', name), 'w') as f:
                f.write('{}')
        self.run_at(ASKED_AT)
        left = os.listdir('data/archive')
        self.assertNotIn('wind-solar-forecast-2021-11-01.json', left)
        self.assertIn('wind-solar-forecast-2021-11-30.json', left)
        self.assertIn('outages-2021-11-01.json', left)


class TheFilesOfTheZones(Feed):
    """The last 30 days and the day in course, one file for each zone, from the records of the archive."""

    def zone(self, code):
        return self.doc(ff.ZONE_PATH.format(zone=code))

    def put_record(self, day, zones):
        os.makedirs('data/archive', exist_ok=True)
        with open(ff.ARCHIVE_PATH.format(day=day), 'w', encoding='utf-8') as f:
            json.dump({'date': day, 'wind_solar': {'license': 'source-terms', 'source': 's', 'zones': zones}}, f)

    def test_each_zone_with_a_forecast_has_a_file_with_its_days(self):
        self.run_at(ASKED_AT)
        self.assertEqual(sorted(os.listdir(ff.OUT_DIR)), ['BE.json', 'ES.json', 'PT.json', 'index.json'])
        portugal = self.zone('PT')
        self.assertEqual(list(portugal), ['zone', 'timezone', 'data_time', 'wind_solar'])
        block = portugal['wind_solar']
        self.assertEqual(list(block), ['license', 'source', 'error', 'eic', 'forecast_zones', 'days'])
        self.assertEqual((block['license'], block['eic'], portugal['data_time']),
                         ('source-terms', PT, '2026-11-18T16:00:00+00:00'))
        self.assertEqual(list(block['days']), ['2026-11-17', '2026-11-18'])           # oldest first
        self.assertEqual(block['days'], self.zones()['PT']['days'])                   # each day as it is published
        self.assertNotIn('fetched_at', json.dumps(portugal))                          # the times of the run: in the index

    def test_the_index_has_the_zones_the_days_and_the_times(self):
        self.run_at(ASKED_AT)
        index = self.doc(ff.INDEX_PATH)
        self.assertEqual(list(index), ['updated', 'data_time', 'timezone', 'days', 'zones', 'wind_solar'])
        self.assertEqual(index['days'], {'from': '2026-10-19', 'to': '2026-11-18'})   # 30 days and the day in course
        self.assertEqual(index['zones'], ['PT', 'ES', 'BE'])
        self.assertEqual(index['wind_solar']['license'], 'source-terms')
        self.assertEqual(index['wind_solar']['zones']['PT'],
                         {'days': 2, 'first': '2026-11-17', 'last': '2026-11-18', 'data_time': '2026-11-18T16:00:00+00:00',
                          'fetched_at': '2026-11-18T18:08:34+00:00', 'actual_updated': '2026-11-18T18:08:34+00:00'})
        self.assertIn('MD', index['wind_solar']['not_published'])

    def test_the_day_that_closes_stays_and_the_oldest_leaves(self):
        oldest = {'start': '2026-10-18T22:00:00+00:00', 'hours': 24, 'summary': {}}
        self.put_record('2026-10-19', {'PT': oldest})
        self.put_record('2026-10-18', {'PT': dict(oldest, start='2026-10-17T22:00:00+00:00')})   # out of the window
        self.run_at(ASKED_AT)
        self.assertEqual(list(self.zone('PT')['wind_solar']['days']), ['2026-10-19', '2026-11-17', '2026-11-18'])
        # The next delivery day (19 Nov), with ENTSO-E not answering: 17 Nov is no longer one of the two days a run
        # asks for, and stays as it closed; 19 Oct leaves the window.
        self.run_at(at('2026-11-18 23:10:00'), [({}, None)])
        days = self.zone('PT')['wind_solar']['days']
        self.assertEqual(list(days), ['2026-11-17', '2026-11-18'])
        self.assertEqual(days['2026-11-17']['summary']['solar']['hours'], 5)
        self.assertEqual(self.doc(ff.INDEX_PATH)['days'], {'from': '2026-10-20', 'to': '2026-11-19'})

    def test_a_failed_run_takes_no_day_out(self):
        self.run_at(ASKED_AT)
        before = self.zone('ES')['wind_solar']['days']
        code, _entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=15), [({}, None)])
        self.assertEqual(code, 1)
        self.assertEqual(self.zone('ES')['wind_solar']['days'], before)

    def test_only_the_files_that_changed_are_written(self):
        self.run_at(ASKED_AT)
        inodes = {code: os.stat(ff.ZONE_PATH.format(zone=code)).st_ino for code in ('PT', 'ES', 'BE')}
        index = os.stat(ff.INDEX_PATH).st_ino
        later = ASKED_AT + timedelta(minutes=15)
        ws.save({'BE': {'wind_offshore': (timedelta(hours=1), {at('2026-11-18 17:00'): 1000.0})}}, later)
        self.run_at(later)
        self.assertEqual(os.stat(ff.ZONE_PATH.format(zone='PT')).st_ino, inodes['PT'])     # asked again, the same
        self.assertEqual(os.stat(ff.ZONE_PATH.format(zone='ES')).st_ino, inodes['ES'])
        self.assertNotEqual(os.stat(ff.ZONE_PATH.format(zone='BE')).st_ino, inodes['BE'])  # a new actual
        self.assertNotEqual(os.stat(ff.INDEX_PATH).st_ino, index)


class FillingInTheDaysBefore(InADirectory):
    """fill_days: the 30 days before the files existed, from ENTSO-E, once."""

    NOW = at('2026-11-20 10:00:00')                     # delivery day 20 Nov: 21 Oct to 18 Nov are filled in
    START = at('2026-10-19 22:00:00')                   # the period asked for begins a day early

    def answer(self, url):
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        self.asked.append(params)
        hours = int((at('2026-11-19 23:00:00') - self.START) / timedelta(hours=1))
        if params['in_Domain'] == ES:
            return None, 'HTTP 400 Bad Request'
        if params['in_Domain'] != PT:
            return NO_DATA, None
        if params['documentType'] == 'A69':
            return document(series(self.START, [100] * hours, psr='B16')), None
        return document(series(self.START, [110] * hours, psr='B16'), series(self.START, [500] * hours, psr='B19')), None

    def fill(self, commit=None):
        self.asked = []
        log = io.StringIO()
        with mock.patch.object(backfill_archive, '_gently'), contextlib.redirect_stdout(log):
            code = ff.fill_days('https://example.invalid/api', now=self.NOW, ask=self.answer, sleep=lambda s: None,
                                commit=commit, previous={'wind_solar': {'not_published': {'MD': 'x', 'XK': 'x'}}})
        return code, log.getvalue()

    def record(self, day):
        return self.doc(ff.ARCHIVE_PATH.format(day=day))['wind_solar']

    def test_each_day_before_yesterday_gets_its_record_worked_out_as_a_run_would(self):
        code, log = self.fill()
        days = sorted(name[len('wind-solar-forecast-'):-len('.json')] for name in os.listdir('data/archive'))
        self.assertEqual((days[0], days[-1], len(days)), ('2026-10-21', '2026-11-18', 29))
        day = self.record('2026-11-02')['zones']['PT']
        self.assertEqual((day['start'], day['hours']), ('2026-11-01T23:00:00+00:00', 24))
        self.assertEqual((day['solar']['forecast'], day['solar']['actual'], day['solar']['error']),
                         ([100.0] * 24, [110.0] * 24, [10.0] * 24))
        self.assertNotIn('wind_onshore', day)                     # no forecast of it: not there, as in a run
        self.assertEqual(day['summary']['solar'], {'hours': 24, 'forecast_mwh': 2400.0, 'actual_mwh': 2640.0,
                                                   'mean_error_mw': 10.0, 'mean_abs_error_mw': 10.0})
        self.assertEqual(self.record('2026-10-25')['zones']['PT']['hours'], 25)     # the clocks go back
        self.assertEqual(self.record('2026-10-21')['license'], 'source-terms')
        self.assertIn('PT: 29 days', log)

    def test_one_request_for_the_forecast_and_one_for_the_actuals_of_each_zone(self):
        self.fill()
        kinds = {(p['documentType'], p.get('processType')) for p in self.asked}
        self.assertEqual(kinds, {('A69', 'A01'), ('A75', 'A16')})
        self.assertEqual({(p['periodStart'], p['periodEnd']) for p in self.asked}, {('202610192200', '202611192300')})
        zones = [code for name in ff.ORDER if name not in ('MD', 'XK') for code in ff.PARTS[name]]
        asked = [p['in_Domain'] for p in self.asked]
        self.assertEqual(len(asked), 2 * len(zones) - 1)          # Spain's first request is refused: no second
        self.assertNotIn(fe.GEN_ZONES['MD'], asked)               # publishes no forecast

    def test_a_zone_that_cannot_be_had_is_left_out_and_the_run_says_so(self):
        code, log = self.fill()
        self.assertEqual(code, 1)
        self.assertIn('ES: NOT filled in', log)
        self.assertNotIn('ES', self.record('2026-11-02')['zones'])

    def test_the_day_before_and_the_day_in_course_are_left_to_the_runs_and_other_zones_stay(self):
        os.makedirs('data/archive', exist_ok=True)
        with open(ff.ARCHIVE_PATH.format(day='2026-11-02'), 'w', encoding='utf-8') as f:
            json.dump({'date': '2026-11-02', 'wind_solar': {'license': 'source-terms', 'source': ff.SOURCE,
                                                            'zones': {'BE': {'hours': 24}}}}, f)
        self.fill()
        self.assertEqual(list(self.record('2026-11-02')['zones']), ['BE', 'PT'])
        self.assertFalse(os.path.exists(ff.ARCHIVE_PATH.format(day='2026-11-19')))
        self.assertFalse(os.path.exists(ff.ARCHIVE_PATH.format(day='2026-11-20')))

    def test_records_not_published_end_in_failure(self):
        code, log = self.fill(commit=lambda message: False)
        self.assertEqual(code, 1)
        self.assertIn('NOT published', log)


if __name__ == '__main__':
    unittest.main()
