"""Tests for the records (fetch_records.py), on real answers, without network:

  - ENTSO-E's actual generation per production type (A75) for whole days, taken
    on GitHub on 24 Nov 2026 (tests/fixtures/records): Portugal on 26 Oct 2025
    (25 hours) and on 29 Mar 2026 (23 hours), Germany on 20 Nov 2026 (by the
    quarter-hour), and Norway's two zones of the feed, NO1 and NO2, that day;
    and the 24-hour answer of Belgium of tests/fixtures/entsoe, whose pumped
    storage reports only now and then; and, taken the same way, Belgium and
    Hungary on 20 Nov 2026 and Italy on 19 Nov 2026 (whose data stopped at 09:45);
  - the feed's archive of the hourly prices on those days and its count of the
    low prices of 20 Nov 2026;
  - Ember's Monthly Electricity Data, the rows of Portugal and Latvia;
  - ENTSO-E's day-ahead prices (A44): Finland from 5 to 7 Oct 2026, a run of
    prices below or at zero across two midnights (tests/fixtures/records), and
    Spain on 18 Nov 2026, with quarter-hours at zero (tests/fixtures/entsoe).

The expected numbers are read from the raw answers apart from the code under test.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import gzip
import io
import json
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import carry_over
import feed_common as fc
import fetch_entsoe as fe
import fetch_low_prices as flp
import fetch_records as fr

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, 'fixtures', 'records')


def raw(name):
    with open(os.path.join(FIX, name), 'rb') as f:
        return gzip.decompress(f.read())


def techs(name):
    return fr.technologies([raw(name)])


def by_psr(name, day):
    """{psr: {start: MW}} of a day, read with fetch_entsoe's own parsing, generation only."""
    start, end = carry_over.day_bounds(day)
    out = {}
    for series in fe.read_series(raw(name)):
        if series['out']:
            continue
        for period in series['periods']:
            for at, value in fe._every_interval(period):
                if start <= at < end:
                    out.setdefault(series['psr'], {})[at] = value
    return out


NO_LIMIT = {'solar_mw': 10 ** 6, 'wind_mw': 10 ** 6}


class TheGenerationOfADay(unittest.TestCase):

    def test_the_day_of_25_hours_and_the_day_of_23(self):
        for name, day, hours in (('generation_A75_PT_2025-10-26.xml.gz', date(2025, 10, 26), 25),
                                 ('generation_A75_PT_2026-03-29.xml.gz', date(2026, 3, 29), 23)):
            values = by_psr(name, day)
            self.assertEqual({len(v) for v in values.values()}, {hours})
            numbers = fr.day_numbers(techs(name), day, NO_LIMIT)
            renewable = sum(sum(v.values()) for psr, v in values.items() if psr in fr.RENEWABLE)
            total = sum(sum(v.values()) for v in values.values())
            self.assertTrue(numbers['complete'])
            self.assertEqual(numbers['step'], 60)
            self.assertEqual(numbers['ren_day'], round(100 * renewable / total, 1))
            self.assertEqual(numbers['solar'][0], int(round(max(values['B16'].values()))))

    def test_the_highest_quarter_hour_of_germany_is_the_raw_one(self):
        day = date(2026, 11, 20)
        values = by_psr('generation_A75_DE_2026-11-20.xml.gz', day)
        numbers = fr.day_numbers(techs('generation_A75_DE_2026-11-20.xml.gz'), day, NO_LIMIT)
        peak = max(values['B16'], key=values['B16'].get)
        self.assertEqual(numbers['step'], 15)
        self.assertEqual(numbers['solar'], [int(round(values['B16'][peak])), fc.iso(peak)])
        wind = {at: values.get('B18', {}).get(at, 0) + values['B19'][at] for at in values['B19']}
        self.assertEqual(numbers['wind'][0], int(round(max(wind.values()))))

    def test_norway_is_the_sum_of_its_two_zones_interval_by_interval(self):
        day = date(2026, 11, 20)
        parts = [techs(f'generation_A75_{z}_2026-11-20.xml.gz') for z in ('NO1', 'NO2')]
        numbers = fr.day_numbers(fr.add_zones(parts), day, NO_LIMIT)
        one, two = by_psr('generation_A75_NO1_2026-11-20.xml.gz', day), by_psr('generation_A75_NO2_2026-11-20.xml.gz', day)
        wind = {at: one['B19'].get(at, 0) + two['B19'].get(at, 0) for at in set(one['B19']) | set(two['B19'])}
        self.assertEqual(numbers['wind'][0], int(round(max(wind.values()))))
        self.assertIsNone(fr.add_zones([parts[0], None]))          # a zone that did not answer: no country

    def test_a_value_above_the_installed_capacity_does_not_count(self):
        day = date(2026, 11, 20)
        name = 'generation_A75_DE_2026-11-20.xml.gz'
        solar = sorted(by_psr(name, day)['B16'].values(), reverse=True)
        limit = {'solar_mw': solar[0] / fr.CAPACITY_MARGIN - 1, 'wind_mw': 10 ** 6}
        numbers = fr.day_numbers(techs(name), day, limit)
        self.assertLess(numbers['solar'][0], int(round(solar[0])))

    def test_an_isolated_peak_does_not_count(self):
        day = date(2026, 11, 20)
        found = techs('generation_A75_DE_2026-11-20.xml.gz')
        step, values = found['B16']
        noon = datetime(2026, 11, 20, 11, 0, tzinfo=timezone.utc)
        values[noon] = 3 * max(values.values())                    # a quarter-hour far above both around it
        numbers = fr.day_numbers(found, day, NO_LIMIT)
        self.assertNotEqual(numbers['solar'][1], fc.iso(noon))

    def test_a_day_with_an_interval_missing_is_not_a_record_of_the_day(self):
        day = date(2026, 11, 20)
        found = techs('generation_A75_DE_2026-11-20.xml.gz')
        del found['B04'][1][datetime(2026, 11, 20, 3, 0, tzinfo=timezone.utc)]
        numbers = fr.day_numbers(found, day, NO_LIMIT)
        self.assertFalse(numbers['complete'])
        self.assertNotIn('ren_day', numbers)
        self.assertIn('solar', numbers)                            # the records of an interval still count

    def test_when_the_data_stop_in_the_middle_of_the_day_the_rest_is_missing_not_zero(self):
        # Italy on 19 Nov 2026, as ENTSO-E had it on the 24th: every technology up to 09:45 only.
        numbers = fr.day_numbers(techs('generation_A75_IT_2026-11-19.xml.gz'), date(2026, 11, 19), NO_LIMIT)
        self.assertFalse(numbers['complete'])
        self.assertNotIn('ren_day', numbers)
        self.assertLess(numbers['ren'][1], '2026-11-19T09:45:00+00:00')

    def test_a_small_technology_that_leaves_out_some_quarter_hours_does_not_spoil_the_day(self):
        # Hungary's oil (B06) on 20 Nov 2026: 84 of the 96 quarter-hours.
        day = date(2026, 11, 20)
        self.assertEqual(len(by_psr('generation_A75_HU_2026-11-20.xml.gz', day)['B06']), 84)
        self.assertTrue(fr.day_numbers(techs('generation_A75_HU_2026-11-20.xml.gz'), day, NO_LIMIT)['complete'])

    def test_a_large_technology_with_hours_missing_spoils_the_day(self):
        # Belgium's offshore wind (B18) on 20 Nov 2026: 21 of the 24 hours.
        day = date(2026, 11, 20)
        self.assertEqual(len(by_psr('generation_A75_BE_2026-11-20.xml.gz', day)['B18']), 21)
        self.assertFalse(fr.day_numbers(techs('generation_A75_BE_2026-11-20.xml.gz'), day, NO_LIMIT)['complete'])

    def test_a_technology_that_reports_now_and_then_does_not_spoil_the_day(self):
        with open(os.path.join(HERE, 'fixtures', 'entsoe', 'generation_A75_BE.xml.gz'), 'rb') as f:
            found = fr.technologies([gzip.decompress(f.read())])
        self.assertLess(len(found['B10'][1]), 12)
        numbers = fr.day_numbers(found, date(2026, 11, 18), NO_LIMIT)
        self.assertIn('ren', numbers)


class TheProductionOfADay(unittest.TestCase):

    def test_the_day_of_25_hours_and_the_quarter_hours_of_germany(self):
        for name, day, hours in (('generation_A75_PT_2025-10-26.xml.gz', date(2025, 10, 26), 1.0),
                                 ('generation_A75_DE_2026-11-20.xml.gz', date(2026, 11, 20), 0.25)):
            values = by_psr(name, day)
            numbers = fr.day_numbers(techs(name), day, NO_LIMIT)
            self.assertEqual(numbers['solar_gwh'], round(sum(values['B16'].values()) * hours / 1000, 2), name)
            wind = sum(values.get('B18', {}).values()) + sum(values['B19'].values())
            self.assertEqual(numbers['wind_gwh'], round(wind * hours / 1000, 2), name)

    def test_an_incomplete_day_has_no_production(self):
        numbers = fr.day_numbers(techs('generation_A75_IT_2026-11-19.xml.gz'), date(2026, 11, 19), NO_LIMIT)
        self.assertNotIn('solar_gwh', numbers)
        self.assertNotIn('wind_gwh', numbers)

    def test_a_peak_or_a_value_above_the_capacity_keeps_the_day_out(self):
        day = date(2026, 11, 20)
        found = techs('generation_A75_DE_2026-11-20.xml.gz')
        found['B16'][1][datetime(2026, 11, 20, 11, 0, tzinfo=timezone.utc)] = 3 * max(found['B16'][1].values())
        self.assertNotIn('solar_gwh', fr.day_numbers(found, day, NO_LIMIT))
        solar = max(by_psr('generation_A75_DE_2026-11-20.xml.gz', day)['B16'].values())
        numbers = fr.day_numbers(techs('generation_A75_DE_2026-11-20.xml.gz'), day,
                                 {'solar_mw': solar / fr.CAPACITY_MARGIN - 1, 'wind_mw': 10 ** 6})
        self.assertNotIn('solar_gwh', numbers)
        self.assertIn('wind_gwh', numbers)


def a44(path):
    """A fake ENTSO-E that answers every request for prices with the saved answer at `path`."""
    with open(path, 'rb') as f:
        body = f.read()

    def get(url, timeout=40, **_kwargs):
        return fc.Response(200, {}, gzip.decompress(body))
    return get


def raw_runs(path):
    """[(start, end)] of the runs below or at zero of an A44 answer, read apart from the code under test."""
    with open(path, 'rb') as f:
        all_series = [s for s in fe.read_series(gzip.decompress(f.read())) if s['contract'] in (None, 'A01')]
    first = min(s['sequence'] or 0 for s in all_series)          # one auction, as the feed reads the day
    points = {}
    for series in all_series:
        if (series['sequence'] or 0) != first:
            continue
        for period in series['periods']:
            for at, value in fe._every_interval(period):
                points[at] = (at, period['step'], value)
    points = list(points.values())
    runs = []
    for at, step, value in sorted(points):
        if value <= 0:
            if runs and runs[-1][1] == at:
                runs[-1][1] = at + step
            else:
                runs.append([at, at + step])
    return runs


class TheRunsOfPricesBelowOrAtZero(unittest.TestCase):

    def test_quarter_hours_at_zero_are_a_run_of_quarter_hours(self):
        path = os.path.join(HERE, 'fixtures', 'entsoe', 'prices_A44_ES.xml.gz')
        found, asked = fr.interval_runs(['ES'], date(2026, 11, 18), date(2026, 11, 18), fc.Budget(30), a44(path))
        runs = found['ES']['2026-11-18']
        expected = raw_runs(path)
        self.assertEqual(asked, 1)
        self.assertEqual(runs['basis'], 'market-resolution')
        self.assertEqual([[r[0], r[1]] for r in runs['runs']], [[fc.iso(a), fc.iso(b)] for a, b in expected])
        self.assertEqual(sum(r[2] for r in runs['runs']), sum((b - a).total_seconds() / 3600 for a, b in expected))

    def test_a_run_across_midnight_counts_whole_with_its_start_and_end(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')
        path = os.path.join(FIX, 'prices_A44_FI_2026-10-05_2026-10-07.xml.gz')
        found, _asked = fr.interval_runs(['FI'], date(2026, 10, 5), date(2026, 10, 7), fc.Budget(30), a44(path))
        fr.store('FI', 'runs', found['FI'])
        longest = max(raw_runs(path), key=lambda r: r[1] - r[0])
        self.assertGreater(longest[1].date(), longest[0].date())        # it does cross midnight
        record = fr.build(datetime(2026, 11, 25, tzinfo=timezone.utc), {})['zones']['FI']['negative_zero_run_hours_max']
        self.assertEqual((record['at'], record['end']), (fc.iso(longest[0]), fc.iso(longest[1])))
        self.assertEqual(record['value'], (longest[1] - longest[0]).total_seconds() / 3600)
        self.assertEqual((record['unit'], record['basis']), ('h', 'market-resolution'))

    def test_the_hourly_runs_of_the_archive_up_to_2025(self):
        starts = [datetime(2025, 5, 11, 10 + i, tzinfo=timezone.utc) for i in range(6)]
        runs = fr.runs_of(starts, [1] * 6, [5.0, 0.0, -1.0, None, -2.0, -3.0])
        self.assertEqual(runs, [['2025-05-11T11:00:00+00:00', '2025-05-11T13:00:00+00:00', 2],
                                ['2025-05-11T14:00:00+00:00', '2025-05-11T16:00:00+00:00', 2]])


class ThePricesOfADay(unittest.TestCase):

    def entry(self, day, zone):
        return json.loads(raw(f'spot-prices-{day}.json.gz'))['zones'][zone]

    def test_the_day_of_25_hours_weighs_its_02_00_twice(self):
        day = date(2025, 10, 26)
        rows = self.entry('2025-10-26', 'PT')['prices']
        numbers = fr.price_numbers(day, self.entry('2025-10-26', 'PT'))
        weights = [2 if r['hour'] == 2 else 1 for r in rows]
        self.assertEqual(numbers['avg'], round(sum(r['price'] * w for r, w in zip(rows, weights)) / 25, 2))
        low = min(range(len(rows)), key=lambda i: rows[i]['price'])
        start = carry_over.day_bounds(day)[0] + timedelta(hours=low + (1 if low > 2 else 0))
        self.assertEqual(numbers['min'], [rows[low]['price'], fc.iso(start)])
        count = flp.zone_count([r['price'] for r in rows], 60, 'hourly-means', weights)
        self.assertEqual(numbers['neg_zero_h'], float(count['negative_h']) + float(count['zero_h']))

    def test_the_day_of_23_hours_is_complete_with_23(self):
        numbers = fr.price_numbers(date(2026, 3, 29), self.entry('2026-03-29', 'PT'))
        self.assertEqual(len(self.entry('2026-03-29', 'PT')['prices']), 23)
        self.assertIn('avg', numbers)

    def test_an_hour_missing_leaves_out_the_records_of_the_day(self):
        entry = self.entry('2026-11-20', 'DE')
        entry = {**entry, 'prices': entry['prices'][:-1]}
        numbers = fr.price_numbers(date(2026, 11, 20), entry)
        self.assertNotIn('avg', numbers)
        self.assertNotIn('neg_zero_h', numbers)
        self.assertIn('min', numbers)

    def test_the_hours_below_or_at_zero_come_from_the_count_of_the_low_prices_where_there_is_one(self):
        low = json.loads(raw('low-prices-2026-11-20.json.gz'))['entsoe']['zones']['DE']
        numbers = fr.price_numbers(date(2026, 11, 20), self.entry('2026-11-20', 'DE'), low)
        self.assertEqual((numbers['neg_zero_h'], numbers['basis']), (0.0, 'market-resolution'))

    def test_what_the_archive_leaves_out_stays_out(self):
        self.assertTrue(fr.left_out('MD', date(2026, 6, 30)))
        self.assertFalse(fr.left_out('MD', date(2026, 7, 1)))
        self.assertTrue(fr.left_out('BG', date(2022, 1, 6)))
        self.assertFalse(fr.left_out('BG', date(2022, 1, 7)))


class InAFolder(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def quiet(self, function, *args, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()) as log:
            result = function(*args, **kwargs)
        return result, log.getvalue()


class TheRecords(InAFolder):

    NOW = datetime(2026, 11, 24, 10, 0, tzinfo=timezone.utc)

    def days(self, values):
        """Generation numbers of PT with the solar peaks `values` ({day: MW})."""
        return {d: {'complete': True, 'step': 60, 'solar': [mw, f'{d}T12:00:00+00:00'], 'ren_day': 50.0}
                for d, mw in values.items()}

    def test_the_current_record_the_one_it_beat_and_the_ones_beaten_in_the_last_12_months(self):
        fr.store('PT', 'gen', self.days({'2021-06-01': 1000, '2022-06-01': 1500, '2025-06-01': 1400,
                                         '2026-03-01': 1600, '2026-06-01': 1700}))
        doc = fr.build(self.NOW, {})
        record = doc['zones']['PT']['solar_mw_max']
        self.assertEqual((record['value'], record['unit'], record['at'], record['source']),
                         (1700, 'MW', '2026-06-01T12:00:00+00:00', 'ENTSO-E Transparency Platform'))
        self.assertEqual(record['previous'], {'value': 1600, 'at': '2026-03-01T12:00:00+00:00'})
        beaten = [(r['area'], r['type'], r['value']) for r in doc['recent']]
        self.assertEqual(beaten, [('PT', 'solar_mw_max', 1700), ('PT', 'solar_mw_max', 1600)])
        self.assertEqual(doc['types']['solar_mw_max']['license'], 'source-terms')
        self.assertEqual(set(doc['types']), set(fr.NAMES))
        self.assertEqual(doc['types']['solar_mw_max']['name'], 'Solar generation record (MW)')
        self.assertEqual(doc['types']['negative_zero_run_hours_max']['name'], 'Longest run of zero or negative prices')
        for kind, unit in (('solar_gwh_day_max', 'GWh'), ('wind_gwh_day_max', 'GWh'), ('negative_zero_run_hours_max', 'h')):
            self.assertEqual((doc['types'][kind]['unit'], doc['types'][kind]['license']), (unit, 'source-terms'))

    def test_a_zone_with_less_than_a_year_of_data_lists_no_record_beaten(self):
        fr.store('MD', 'gen', self.days({'2026-07-01': 10, '2026-08-01': 20, '2026-09-01': 30}))
        doc = fr.build(self.NOW, {})
        self.assertEqual(doc['zones']['MD']['solar_mw_max']['value'], 30)
        self.assertEqual(doc['recent'], [])

    def test_a_complete_day_is_not_replaced_by_an_incomplete_one_but_by_the_fill(self):
        fr.store('PT', 'gen', {'2026-11-20': {'complete': True, 'ren_day': 70.0}})
        fr.store('PT', 'gen', {'2026-11-20': {'complete': False}})
        self.assertEqual(fr.load_days('PT', 2026)['2026-11-20']['gen']['ren_day'], 70.0)
        fr.store('PT', 'gen', {'2026-11-20': {'complete': False}}, keep_complete=False)
        self.assertEqual(fr.load_days('PT', 2026)['2026-11-20']['gen'], {'complete': False})

    def test_the_months_of_ember(self):
        rows = raw('ember_monthly_PT_LV.csv.gz')
        answer = fc.Response(200, {'last-modified': 'Tue, 17 Nov 2026 18:34:54 GMT'}, rows)
        doc, _log = self.quiet(fr.ember_monthly, get=lambda url, timeout=120, headers=None: answer)
        lines = [line.split(',') for line in rows.decode().splitlines()[1:]]
        solar = {l[2][:7]: float(l[9]) for l in lines if l[1] == 'PRT' and l[4] == 'Solar'}
        self.assertEqual(doc['countries']['PT']['2026-07'][0], round(solar['2026-07'], 2))
        records = fr.build(self.NOW, doc)['countries']['PT']['solar_share_month_max']
        best = max(solar, key=solar.get)
        self.assertEqual((records['at'], records['value'], records['source']),
                         (best, round(solar[best], 2), 'Ember, Monthly Electricity Data'))
        unchanged, _log = self.quiet(fr.ember_monthly, get=lambda url, timeout=120, headers=None: fc.Response(304, {}, b''))
        self.assertEqual(unchanged['edition'], 'Tue, 17 Nov 2026 18:34:54 GMT')   # kept, not downloaded again


class TheRuns(InAFolder):

    def test_the_daily_update_asks_for_each_zone_of_the_feed_and_counts_norway_as_its_two_zones(self):
        asked = []

        with open(os.path.join(HERE, 'fixtures', 'entsoe', 'no_data.xml'), 'rb') as f:
            no_data = f.read()

        def entsoe(url, timeout=40, **_kwargs):
            asked.append(url)
            if url == fr.EMBER_URL:
                return fc.Response(304, {}, b'')
            return fc.Response(200, {}, no_data)
        with mock.patch.dict(os.environ, {'ENTSOE_API_KEY': 'k'}):
            self.quiet(fr.update, now=datetime(2026, 11, 24, 9, 40, tzinfo=timezone.utc), get=entsoe,
                       budget=fc.Budget(60))
        domains = [u.split('in_Domain=')[1].split('&')[0] for u in asked if 'A75' in u]
        self.assertEqual(len(domains), len(fr.GEN_ZONES) - len(fr.SUBZONES) + sum(map(len, fr.SUBZONES.values())))
        self.assertIn(fe.ZONES['NO1'], domains)
        self.assertNotIn('GB', fr.GEN_ZONES)                       # GB is Elexon's, not ENTSO-E's
        self.assertTrue(os.path.exists(fr.OUT_PATH))


if __name__ == '__main__':
    unittest.main()
