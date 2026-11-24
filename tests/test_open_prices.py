"""Tests for the prices of open sources (fetch_open_prices.py): SMARD for 15
zones, Elexon's MID for GB and OMIE's file for PT and ES. On real answers of
SMARD (tests/fixtures/smard, taken on 23 Nov 2026 at about 17:00 UTC, when
only Switzerland had the prices of 24 Nov), a real answer of Elexon
(tests/fixtures/elexon) and the feed's own OMIE file of that day
(tests/fixtures/open_prices). The stand-in for SMARD, Energy-Charts, on its real
answers of 24 Nov 2026 at about 08:15 UTC, when SMARD still lacked that day in
14 zones (tests/fixtures/energy_charts): Germany-Luxembourg under CC BY 4.0,
Spain under its licence for private and internal use only, and Poland asked
only up to 11:45, an incomplete day. No network.

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers are read from the raw files apart from the code under
test.
"""

import contextlib
import gzip
import io
import json
import os
import re
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import carry_over
import feed_common as fc
import fetch_open_prices as op

HERE = os.path.dirname(os.path.abspath(__file__))
SMARD = os.path.join(HERE, 'fixtures', 'smard')
ELEXON = os.path.join(HERE, 'fixtures', 'elexon', 'market_index_2026-11-23.json')
OMIE = os.path.join(HERE, 'fixtures', 'open_prices', 'omie-prices-2026-11-23.json')
EC = os.path.join(HERE, 'fixtures', 'energy_charts')
EC_OPEN = 'price_DE-LU_2026-11-24.json'
EC_RESTRICTED = 'price_ES_2026-11-24_restricted.json'
EC_INCOMPLETE = 'price_PL_2026-11-24_until-11h45.json'
NOW = datetime(2026, 11, 23, 17, 0, tzinfo=timezone.utc)
LATEST_WEEK = 1795388400000                       # Monday 23 Nov 2026, 00:00 Brussels time
ZONE_OF_FILTER = {filter_id: zone for zone, (filter_id, _region) in op.SMARD_ZONES.items()}


def raw(name):
    with open(os.path.join(SMARD, name + '.gz'), 'rb') as f:
        return json.loads(gzip.decompress(f.read()))


def raw_day(name, first_ms, end_ms):
    """The prices of a weekly file of SMARD from `first_ms` to `end_ms`, read as plain data."""
    return [value for at, value in raw(name)['series'] if first_ms <= at < end_ms]


def ec_raw(name):
    with open(os.path.join(EC, name), encoding='utf-8') as f:
        return json.load(f)


class Source:
    """SMARD, Elexon and Energy-Charts, from the saved answers. `down` makes some URLs answer an error. Energy-Charts
    gives, for any zone, the answer of Germany-Luxembourg (or the one named in `ec` for the zone), cut to the days
    asked for, and 404 when nothing is left; `ec_status` makes it answer that status instead."""

    def __init__(self, down=(), missing=(), ec=None, ec_status=None):
        self.down, self.missing, self.asked = down, missing, []
        self.ec, self.ec_status = ec or {}, ec_status

    def energy_charts(self, url):
        if self.ec_status:
            return fc.Response(self.ec_status, {'retry-after': '30'}, b'{"detail":"Too Many Requests"}')
        found = re.fullmatch(r'https://api\.energy-charts\.info/price\?bzn=([\w-]+)&start=([\d-]+)&end=([\d-]+)', url)
        doc = ec_raw(self.ec.get(found.group(1), EC_OPEN))
        first = carry_over.day_bounds(datetime.fromisoformat(found.group(2)).date())[0]
        end = carry_over.day_bounds(datetime.fromisoformat(found.group(3)).date())[1]
        kept = [(s, p) for s, p in zip(doc['unix_seconds'], doc['price']) if first.timestamp() <= s < end.timestamp()]
        if not kept:
            return fc.Response(404, {}, b'{"detail":"No data"}')
        doc = {**doc, 'unix_seconds': [s for s, _ in kept], 'price': [p for _, p in kept]}
        return fc.Response(200, {}, json.dumps(doc).encode())

    def __call__(self, url, timeout=30, **_kwargs):
        self.asked.append(url)
        if any(part in url for part in self.down):
            return fc.Response(500, {}, b'error')
        if url.startswith('https://data.elexon.co.uk/'):
            with open(ELEXON, 'rb') as f:
                return fc.Response(200, {}, f.read())
        if url.startswith('https://api.energy-charts.info/'):
            return self.energy_charts(url)
        found = re.fullmatch(r'https://www\.smard\.de/app/chart_data/(\d+)/DE/(.+)\.json', url)
        if not found or any(part in url for part in self.missing):
            return fc.Response(404, {}, b'not found')
        zone, rest = ZONE_OF_FILTER[int(found.group(1))], found.group(2)
        path = os.path.join(SMARD, f'{zone}_{rest}.json.gz')
        if not os.path.isfile(path):
            return fc.Response(404, {}, b'not found')
        with open(path, 'rb') as f:
            return fc.Response(200, {}, gzip.decompress(f.read()))


class InADirectory(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')
        shutil.copy(OMIE, op.OMIE_PATH)
        with open('data/fx-rates.json', 'w', encoding='utf-8') as f:
            json.dump({'updated': '2026-11-23T15:12:45+00:00', 'source': 's', 'fixed': {},
                       'rates': {'GBP': {'2026-11-20': 0.83735, '2026-11-23': 0.83673}}}, f)

    def run_at(self, now, source=None):
        source = source or Source()
        log = io.StringIO()
        with contextlib.redirect_stdout(log):
            code = op.main(now=now, get=source, budget=fc.Budget(60), pause=0)
        return code, source, log.getvalue()

    def doc(self):
        with open(op.OUT_PATH, encoding='utf-8') as f:
            return json.load(f)


class ARun(InADirectory):

    def test_has_the_18_zones_each_with_its_source_and_licence(self):
        code, source, _log = self.run_at(NOW)
        doc = self.doc()
        self.assertEqual(code, 0)
        self.assertEqual(list(doc), ['updated', 'data_time', 'timezone', 'unit', 'zones'])
        self.assertEqual((doc['updated'], doc['timezone'], doc['unit']),
                         ('2026-11-23T17:00:00+00:00', 'Europe/Brussels', 'EUR/MWh'))
        self.assertEqual(list(doc['zones']), list(op.SMARD_ZONES) + ['GB', 'PT', 'ES'])
        self.assertEqual(len(doc['zones']), 18)
        for zone in op.SMARD_ZONES:
            entry = doc['zones'][zone]
            through = any(day['source'] == 'energy-charts' for day in entry['days'].values())
            self.assertEqual((entry['license'], entry['attribution'], entry['currency']),
                             ('cc-by-4.0', 'Bundesnetzagentur | SMARD.de' + (', energy-charts.info' if through else ''),
                              'EUR'))
            self.assertEqual(entry['fetched_at'], '2026-11-23T17:00:00+00:00')
            self.assertNotIn('carried_over', entry)
        self.assertEqual(doc['zones']['GB']['license'], 'elexon')
        self.assertEqual({doc['zones'][z]['license'] for z in ('PT', 'ES')}, {'omie'})
        self.assertFalse([url for url in source.asked if 'entsoe' in url])         # no ENTSO-E, ever

    def test_each_smard_zone_has_today_by_the_quarter_hour_as_smard_gives_it(self):
        self.run_at(NOW)
        start = int(datetime(2026, 11, 22, 23, tzinfo=timezone.utc).timestamp() * 1000)
        for zone, (filter_id, region) in op.SMARD_ZONES.items():
            day = self.doc()['zones'][zone]['days']['2026-11-23']
            self.assertEqual((day['start'], day['step_minutes'], len(day['prices'])),
                             ('2026-11-22T23:00:00+00:00', 15, 96), zone)
            self.assertEqual(day['prices'], raw_day(f'{zone}_{filter_id}_{region}_quarterhour_{LATEST_WEEK}.json',
                                                    start, start + 96 * 900_000), zone)
            self.assertEqual((day['source'], day['license']), ('smard', 'cc-by-4.0'), zone)
            self.assertNotIn('license_info', day)

    def test_tomorrow_is_there_where_smard_has_it_and_for_two_zones_from_energy_charts(self):
        _code, source, log = self.run_at(NOW)
        zones = self.doc()['zones']
        with_tomorrow = sorted(zone for zone, entry in zones.items() if '2026-11-24' in entry['days'])
        # The coupled zones of SMARD: not yet at 17:00 UTC; Energy-Charts had them, and is asked for two a run.
        self.assertEqual(with_tomorrow, ['AT', 'CH', 'DE-LU', 'ES', 'PT'])
        self.assertEqual(zones['CH']['days']['2026-11-24']['source'], 'smard')
        self.assertEqual(len(zones['CH']['days']['2026-11-24']['prices']), 96)
        for zone in ('DE-LU', 'AT'):
            self.assertEqual(zones[zone]['days']['2026-11-24']['source'], 'energy-charts')
            self.assertEqual(zones[zone]['days']['2026-11-23']['source'], 'smard')
        self.assertEqual([url for url in source.asked if 'energy-charts' in url],
                         ['https://api.energy-charts.info/price?bzn=DE-LU&start=2026-11-24&end=2026-11-24',
                          'https://api.energy-charts.info/price?bzn=AT&start=2026-11-24&end=2026-11-24'])
        self.assertIn('BE: Energy-Charts not asked for 2026-11-24 in this run (2 zones a run at most)', log)

    def test_gb_has_the_half_hours_so_far_in_euros_with_the_pounds_beside(self):
        self.run_at(NOW)
        gb = self.doc()['zones']['GB']
        day = gb['days']['2026-11-23']
        self.assertEqual((gb['currency'], gb['currency_original'], gb['fx_rate'], gb['fx_date']),
                         ('EUR', 'GBP', 0.83673, '2026-11-23'))
        self.assertEqual((day['start'], day['step_minutes'], len(day['prices'])), ('2026-11-22T23:00:00+00:00', 30, 48))
        with open(ELEXON, encoding='utf-8') as f:
            records = json.load(f)['data']
        first = [r for r in records if r['startTime'] == '2026-11-23T00:00:00Z']   # the third half-hour of the day
        pounds = sum(r['price'] * r['volume'] for r in first if r['price']) / sum(r['volume'] for r in first if r['price'])
        self.assertEqual(day['prices_gbp'][2], round(pounds, 2))
        self.assertAlmostEqual(day['prices'][2], round(round(pounds, 2) / 0.83673, 2), places=2)
        priced = sorted({r['startTime'] for r in records if r['price']})
        self.assertEqual(sum(v is not None for v in day['prices']), len(priced))   # the periods still to come: null
        self.assertIsNone(day['prices'][-1])
        self.assertNotIn('2026-11-24', gb['days'])

    def test_portugal_and_spain_are_omie_s_quarter_hours_of_today_and_tomorrow(self):
        self.run_at(NOW)
        with open(OMIE, encoding='utf-8') as f:
            omie = json.load(f)
        for zone in ('PT', 'ES'):
            days = self.doc()['zones'][zone]['days']
            self.assertEqual(days['2026-11-23']['prices'], [p['price'] for p in omie[zone]['prices_15min']])
            self.assertEqual(days['2026-11-24']['prices'], [p['price'] for p in omie['next_day'][zone]['prices_15min']])
            self.assertEqual(days['2026-11-24']['start'], '2026-11-23T23:00:00+00:00')


class TheDaysTheClocksChange(unittest.TestCase):

    def zone(self, zone, now):
        return op.smard_zone(zone, op.window(now), Source(), fc.Budget(30), pause=0)

    def test_the_spring_day_has_92_quarter_hours(self):
        for zone in ('DE-LU', 'PL'):
            filter_id = op.SMARD_ZONES[zone][0]
            day = self.zone(zone, datetime(2026, 3, 29, 10, tzinfo=timezone.utc))['days']['2026-03-29']
            start = int(datetime(2026, 3, 28, 23, tzinfo=timezone.utc).timestamp() * 1000)
            self.assertEqual((day['start'], len(day['prices'])), ('2026-03-28T23:00:00+00:00', 92), zone)
            self.assertEqual(day['prices'], raw_day(f'{zone}_{filter_id}_DE_quarterhour_1774220400000.json',
                                                    start, start + 92 * 900_000), zone)

    def test_the_autumn_day_has_100_quarter_hours(self):
        for zone in ('DE-LU', 'PL'):
            filter_id = op.SMARD_ZONES[zone][0]
            day = self.zone(zone, datetime(2026, 10, 25, 12, tzinfo=timezone.utc))['days']['2026-10-25']
            start = int(datetime(2026, 10, 24, 22, tzinfo=timezone.utc).timestamp() * 1000)
            self.assertEqual((day['start'], len(day['prices'])), ('2026-10-24T22:00:00+00:00', 100), zone)
            self.assertEqual(day['prices'], raw_day(f'{zone}_{filter_id}_DE_quarterhour_1792360800000.json',
                                                    start, start + 100 * 900_000), zone)


class ByTheHour(unittest.TestCase):

    def test_without_the_quarter_hours_the_hours_are_taken_and_are_their_mean(self):
        days = op.window(NOW)
        hours = op.smard_zone('DE-LU', days, Source(missing=('index_quarterhour',)), fc.Budget(30), pause=0)['days']['2026-11-23']
        quarters = op.smard_zone('DE-LU', days, Source(), fc.Budget(30), pause=0)['days']['2026-11-23']['prices']
        self.assertEqual((hours['step_minutes'], len(hours['prices'])), (60, 24))
        for hour, price in enumerate(hours['prices']):
            self.assertAlmostEqual(price, sum(quarters[4 * hour:4 * hour + 4]) / 4, delta=0.0051)


class AZoneThatFails(InADirectory):

    def test_the_others_are_published_and_it_keeps_its_last_prices_and_their_time(self):
        self.run_at(NOW)
        before = self.doc()['zones']['FR']
        code, _source, log = self.run_at(NOW + timedelta(hours=1), Source(down=('/254/',)))
        zones = self.doc()['zones']
        self.assertEqual(code, 0)
        self.assertEqual(len(zones), 18)
        self.assertTrue(zones['FR']['carried_over'])
        self.assertEqual((zones['FR']['fetched_at'], zones['FR']['days']), (before['fetched_at'], before['days']))
        self.assertEqual(zones['NL']['fetched_at'], '2026-11-23T18:00:00+00:00')
        self.assertNotIn('carried_over', zones['NL'])
        self.assertIn('FR: SMARD did not give it', log)

    def test_after_24_hours_it_is_left_out(self):
        self.run_at(NOW)
        later = NOW + timedelta(hours=24, minutes=15)
        self.run_at(later, Source(down=('/254/',)))
        self.assertNotIn('FR', self.doc()['zones'])

    def test_a_day_with_no_price_is_not_taken(self):
        # In SMARD's week of 16 Nov 2026, Germany-Luxembourg has null for the whole of 20 and 21 Nov.
        days = op.window(datetime(2026, 11, 21, 10, tzinfo=timezone.utc))
        name = 'DE-LU_4169_DE_quarterhour_1794783600000_gap-20-21nov.json'

        def week_of_the_gap(url, timeout=30, **_kwargs):
            if url.endswith('index_quarterhour.json') or url.endswith('index_hour.json'):
                return fc.Response(200, {}, json.dumps({'timestamps': [1794783600000]}).encode())
            if '1794783600000' in url and 'quarterhour' in url:
                return fc.Response(200, {}, json.dumps(raw(name)).encode())
            return fc.Response(404, {}, b'')
        start = int(datetime(2026, 11, 20, 23, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertTrue(all(v is None for v in raw_day(name, start, start + 96 * 900_000)))
        with self.assertRaises(op.SourceDown):
            op.smard_zone('DE-LU', days, week_of_the_gap, fc.Budget(30), pause=0)

    def test_with_nothing_at_all_the_file_is_left_as_it_was(self):
        os.remove(op.OMIE_PATH)
        code, _source, log = self.run_at(NOW, Source(down=('smard.de', 'elexon', 'energy-charts')))
        self.assertEqual(code, 1)
        self.assertFalse(os.path.exists(op.OUT_PATH))
        self.assertIn('Nothing to publish', log)


class TomorrowNotYetOut(InADirectory):

    def smard_asked(self, source):
        return sorted({ZONE_OF_FILTER[int(m.group(1))] for url in source.asked
                       if (m := re.search(r'chart_data/(\d+)/', url))})

    def test_before_12_30_a_zone_with_today_is_not_asked_again_within_the_hour(self):
        morning = datetime(2026, 11, 23, 9, 0, tzinfo=timezone.utc)        # 10:00 in Brussels
        self.run_at(morning)
        _code, source, _log = self.run_at(morning + timedelta(minutes=15))
        self.assertEqual(self.smard_asked(source), [])
        self.assertTrue(any('elexon' in url for url in source.asked))       # GB in every run

    def test_before_12_30_tomorrow_is_not_asked_of_energy_charts(self):
        _code, source, _log = self.run_at(datetime(2026, 11, 23, 9, 0, tzinfo=timezone.utc))
        self.assertFalse([url for url in source.asked if 'energy-charts' in url])

    def test_from_12_30_a_zone_without_tomorrow_is_asked_in_every_run_and_one_with_it_is_not(self):
        nothing_at_energy_charts = Source(ec_status=404)
        self.run_at(NOW, nothing_at_energy_charts)
        _code, source, _log = self.run_at(NOW + timedelta(minutes=15), Source(ec_status=404))
        self.assertEqual(self.smard_asked(source), sorted(set(op.SMARD_ZONES) - {'CH'}))

    def test_the_next_day_the_day_before_leaves_and_tomorrow_becomes_today(self):
        self.run_at(NOW, Source(ec_status=404))                          # Energy-Charts with nothing either
        _code, _source, _log = self.run_at(datetime(2026, 11, 24, 0, 30, tzinfo=timezone.utc),
                                           Source(down=('smard.de', 'elexon', 'energy-charts')))
        zones = self.doc()['zones']
        self.assertEqual(list(zones['CH']['days']), ['2026-11-24'])
        self.assertTrue(zones['CH']['carried_over'])
        self.assertNotIn('DE-LU', zones)                                 # had only 23 Nov: nothing left to show
        self.assertEqual(list(zones['PT']['days']), ['2026-11-24'])      # OMIE's file still has it, as next_day


class EnergyChartsStandsInForSMARD(InADirectory):
    """24 Nov 2026, 09:00 in Brussels: SMARD's latest week stops at 23 Nov in the coupled zones (only Switzerland has
    24 Nov), as it did that day."""

    MORNING = datetime(2026, 11, 24, 8, 0, tzinfo=timezone.utc)

    def ec_asked(self, source):
        return [re.search(r'bzn=([\w-]+)', url).group(1) for url in source.asked if 'energy-charts' in url]

    def test_the_first_two_zones_without_today_get_it_from_energy_charts_as_it_gives_it(self):
        _code, source, log = self.run_at(self.MORNING)
        zones = self.doc()['zones']
        raw_doc = ec_raw(EC_OPEN)
        self.assertEqual(self.ec_asked(source), ['DE-LU', 'AT'])
        for zone in ('DE-LU', 'AT'):
            day = zones[zone]['days']['2026-11-24']
            self.assertEqual((day['start'], day['step_minutes']), ('2026-11-23T23:00:00+00:00', 15))
            self.assertEqual(day['prices'], raw_doc['price'])
            self.assertEqual((day['source'], day['license'], day['license_info']),
                             ('energy-charts', 'cc-by-4.0', raw_doc['license_info']))
            self.assertTrue(day['license_info'].startswith('CC BY 4.0'))
            self.assertEqual(zones[zone]['attribution'], 'Bundesnetzagentur | SMARD.de, energy-charts.info')
            self.assertEqual((zones[zone]['license'], zones[zone]['fetched_at']), ('cc-by-4.0', '2026-11-24T08:00:00+00:00'))
        self.assertEqual(zones['CH']['attribution'], 'Bundesnetzagentur | SMARD.de')     # from SMARD itself
        self.assertEqual(zones['CH']['days']['2026-11-24']['source'], 'smard')
        self.assertNotIn('BE', zones)                                    # not asked yet: 2 zones a run
        self.assertIn('BE: Energy-Charts not asked for 2026-11-24 in this run (2 zones a run at most)', log)
        self.assertIn('DE-LU: Energy-Charts (CC BY 4.0), 2026-11-24', log)

    def test_the_next_run_asks_for_the_next_two_and_not_again_for_those_it_has(self):
        self.run_at(self.MORNING)
        _code, source, _log = self.run_at(self.MORNING + timedelta(minutes=15))
        self.assertEqual(self.ec_asked(source), ['BE', 'CZ'])
        self.assertFalse([url for url in source.asked if '/4169/' in url or '/4170/' in url])   # DE-LU, AT: done
        self.assertEqual(list(self.doc()['zones']), ['DE-LU', 'AT', 'BE', 'CH', 'CZ', 'PT', 'ES'])   # GB: not of 24 Nov

    def test_an_hour_later_a_day_from_energy_charts_is_kept_with_no_new_request(self):
        self.run_at(self.MORNING)
        first = self.doc()['zones']['DE-LU']['days']['2026-11-24']
        _code, source, log = self.run_at(self.MORNING + timedelta(hours=1))
        entry = self.doc()['zones']['DE-LU']
        self.assertTrue([url for url in source.asked if '/4169/' in url])                # SMARD asked again
        self.assertNotIn('DE-LU', self.ec_asked(source))                                # Energy-Charts not
        self.assertEqual(entry['days']['2026-11-24'], first)
        self.assertNotIn('carried_over', entry)
        self.assertIn('DE-LU: 2026-11-24 as Energy-Charts gave it before (SMARD does not have it yet)', log)

    def test_under_another_licence_the_day_is_not_taken_and_the_log_says_so(self):
        code, _source, log = self.run_at(self.MORNING, Source(ec={'DE-LU': EC_RESTRICTED}))
        zones = self.doc()['zones']
        self.assertEqual(code, 0)
        self.assertNotIn('DE-LU', zones)
        self.assertEqual(zones['AT']['days']['2026-11-24']['source'], 'energy-charts')
        self.assertIn('DE-LU: Energy-Charts did not give 2026-11-24 (under another licence: '
                      '"The data provided herein is for private and internal use only.', log)

    def test_another_licence_keeps_what_the_zone_had(self):
        self.run_at(datetime(2026, 11, 23, 17, 0, tzinfo=timezone.utc), Source(ec={'DE-LU': EC_RESTRICTED}))
        entry = self.doc()['zones']['DE-LU']
        self.assertEqual(list(entry['days']), ['2026-11-23'])            # SMARD's today, and no tomorrow
        self.assertEqual(entry['attribution'], 'Bundesnetzagentur | SMARD.de')

    def test_an_incomplete_day_is_not_taken(self):
        self.assertEqual(len(ec_raw(EC_INCOMPLETE)['price']), 48)        # up to 11:45 of 24 Nov
        _code, _source, log = self.run_at(self.MORNING, Source(ec={'DE-LU': EC_INCOMPLETE}))
        self.assertNotIn('DE-LU', self.doc()['zones'])
        self.assertIn('DE-LU: Energy-Charts (CC BY 4.0), nothing; no whole day of 2026-11-24 there yet', log)

    def test_when_it_asks_to_wait_no_more_requests_are_made_in_the_run(self):
        _code, source, log = self.run_at(self.MORNING, Source(ec_status=429))
        self.assertEqual(self.ec_asked(source), ['DE-LU'])
        self.assertIn('DE-LU: Energy-Charts did not give 2026-11-24 (HTTP 429, retry after 30 s); '
                      'no more requests to it in this run', log)
        self.assertIn('AT: Energy-Charts not asked for 2026-11-24 in this run (Energy-Charts asked to wait', log)

    def test_with_smard_whole_energy_charts_is_not_asked(self):
        self.run_at(NOW)
        _code, source, _log = self.run_at(datetime(2026, 11, 23, 10, 0, tzinfo=timezone.utc))
        self.assertFalse([url for url in source.asked if 'energy-charts' in url])

    def test_a_file_written_before_the_days_said_their_source_is_read_as_smard_s(self):
        self.run_at(NOW)
        doc = self.doc()
        for entry in doc['zones'].values():
            for day in entry['days'].values():
                day.pop('source', None), day.pop('license', None), day.pop('license_info', None)
        with open(op.OUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(doc, f)
        self.run_at(NOW + timedelta(minutes=15), Source(down=('smard.de', 'energy-charts')))
        day = self.doc()['zones']['FR']['days']['2026-11-23']
        self.assertEqual((day['source'], day['license']), ('smard', 'cc-by-4.0'))


if __name__ == '__main__':
    unittest.main()
