"""Tests for the history of the day-ahead prices (fetch_price_history.py), on
real answers of the sources, without network:

  - SMARD (tests/fixtures/price_history and tests/fixtures/smard): the week of
    5 Jan 2015 of Belgium (its first), the week of the spring clock change of
    2018 of DE-AT-LU (Germany-Luxembourg and Austria until 30 Sep 2018), the
    week of the autumn change of 2017 of France, by the hour; and the week of
    23 Nov 2026 of the 15 zones, by the quarter-hour, when the coupled zones
    lacked 24 Nov;
  - Energy-Charts (tests/fixtures/energy_charts): Germany-Luxembourg on 24 Nov
    2026 under CC BY 4.0, and Spain under a licence for private use only;
  - OMIE (tests/fixtures/price_history): the zip of 2018 (a day of which has a
    second version) and the files of 30 Sep 2025 (hours), 1 Oct 2025 (the first
    by the quarter-hour) and 26 Oct 2025 (the autumn change, 100 quarter-hours).

The expected numbers are read from the raw files apart from the code under
test.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import gzip
import io
import json
import os
import re
import tempfile
import unittest
import zipfile
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import feed_common as fc
import fetch_open_prices as op
import fetch_price_history as ph

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, 'fixtures', 'price_history')
SMARD = os.path.join(HERE, 'fixtures', 'smard')
EC = os.path.join(HERE, 'fixtures', 'energy_charts')
ZONE_OF_FILTER = {filter_id: zone for zone, (filter_id, _region) in op.SMARD_ZONES.items()}


def gz(path):
    with open(path, 'rb') as f:
        return gzip.decompress(f.read())


def smard_week(filter_id, resolution, stamp):
    return json.loads(gz(os.path.join(FIX, f'smard_{filter_id}_DE_{resolution}_{stamp}.json.gz')))['series']


def omie_rows(text):
    return [line.split(';') for line in text.splitlines() if line[:1].isdigit()]


class Sources:
    """SMARD, Energy-Charts and OMIE from the saved answers; any other URL is 404. `ec` names the answer of
    Energy-Charts for a zone (Germany-Luxembourg's by default), cut to the days asked for."""

    def __init__(self, ec=None):
        self.asked, self.ec = [], ec or {}

    def __call__(self, url, timeout=30, **_kwargs):
        self.asked.append(url)
        found = re.fullmatch(r'https://www\.smard\.de/app/chart_data/(\d+)/DE/(.+)\.json', url)
        if found:
            return self.smard(int(found.group(1)), found.group(2))
        if url.startswith('https://api.energy-charts.info/'):
            return self.energy_charts(url)
        found = re.search(r'filename=(marginalpdbc_[\d.]+(?:zip|\d))$', url)
        if found and os.path.isfile(os.path.join(FIX, 'omie_' + found.group(1))):
            with open(os.path.join(FIX, 'omie_' + found.group(1)), 'rb') as f:
                return fc.Response(200, {}, f.read())
        return fc.Response(404, {}, b'not found')

    def smard(self, filter_id, rest):
        for path in (os.path.join(FIX, f'smard_{rest}.json.gz'),                   # a week: <filter>_DE_<res>_<ms>
                     os.path.join(FIX, f'smard_{filter_id}_{rest}.json.gz'),       # an index: index_<res>
                     os.path.join(SMARD, f'{ZONE_OF_FILTER.get(filter_id)}_{rest}.json.gz')):
            if os.path.isfile(path):
                return fc.Response(200, {}, gz(path))
        return fc.Response(404, {}, b'not found')

    def energy_charts(self, url):
        found = re.fullmatch(r'https://api\.energy-charts\.info/price\?bzn=([\w-]+)&start=([\d-]+)&end=([\d-]+)', url)
        with open(os.path.join(EC, self.ec.get(found.group(1), 'price_DE-LU_2026-11-24.json')), encoding='utf-8') as f:
            doc = json.load(f)
        first = ph.carry_over.day_bounds(date.fromisoformat(found.group(2)))[0].timestamp()
        end = ph.carry_over.day_bounds(date.fromisoformat(found.group(3)))[1].timestamp()
        kept = [(s, p) for s, p in zip(doc['unix_seconds'], doc['price']) if first <= s < end]
        if not kept:
            return fc.Response(404, {}, b'{"detail":"No data"}')
        return fc.Response(200, {}, json.dumps({**doc, 'unix_seconds': [s for s, _ in kept],
                                                'price': [p for _, p in kept]}).encode())


class InAFolder(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = os.path.join(folder.name, 'price-history')
        ph._omie_read.clear()
        patcher = mock.patch.object(ph, 'OMIE_PAUSE_S', 0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def zone(self, zone, days, sources=None, ec=0):
        found, _asked = ph.fetch_zone(zone, days, sources or Sources(), fc.Budget(60), ec, pause=0)
        return found

    def doc(self, *parts):
        with open(os.path.join(self.folder, *parts), encoding='utf-8') as f:
            return json.load(f)

    def quiet(self, function, *args, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()) as log:
            result = function(*args, **kwargs)
        return result, log.getvalue()


class SMARDByTheHour(InAFolder):

    def test_until_30_sep_2018_germany_and_austria_have_the_series_of_their_joint_zone(self):
        stamp = 1521414000000                                    # Monday 19 Mar 2018, 00:00 Brussels time
        start = int(datetime(2018, 3, 24, 23, tzinfo=timezone.utc).timestamp() * 1000)
        raw = [v for at, v in smard_week(251, 'hour', stamp) if start <= at < start + 23 * 3_600_000]
        for zone in ('DE-LU', 'AT'):
            day = self.zone(zone, [date(2018, 3, 25)])['2018-03-25']
            self.assertEqual((day['start'], day['step_minutes'], len(day['prices'])), ('2018-03-24T23:00:00+00:00', 60, 23))
            self.assertEqual(day['prices'], raw)
            self.assertEqual((day['source'], day['series']), ('smard', 'DE-AT-LU'))

    def test_the_autumn_day_has_25_hours(self):
        stamp = 1508709600000                                    # Monday 23 Oct 2017
        start = int(datetime(2017, 10, 28, 22, tzinfo=timezone.utc).timestamp() * 1000)
        raw = [v for at, v in smard_week(254, 'hour', stamp) if start <= at < start + 25 * 3_600_000]
        day = self.zone('FR', [date(2017, 10, 29)])['2017-10-29']
        self.assertEqual((day['start'], len(day['prices']), day['prices']), ('2017-10-28T22:00:00+00:00', 25, raw))
        self.assertNotIn('series', day)

    def test_belgium_starts_on_the_first_week_smard_has(self):
        found = self.zone('BE', ph.days_between(date(2015, 1, 1), date(2015, 1, 6)))
        self.assertEqual(sorted(found), ['2015-01-05', '2015-01-06'])


class SMARDByTheQuarterHour(InAFolder):

    def test_a_day_of_quarter_hours_is_kept_by_the_quarter_hour(self):
        day = self.zone('DE-LU', [date(2026, 11, 23)])['2026-11-23']
        self.assertEqual((day['step_minutes'], len(day['prices']), day['source']), (15, 96, 'smard'))

    def test_four_equal_quarters_are_one_hour(self):
        block = {'start': 's', 'step_minutes': 15, 'prices': [1, 1, 1, 1, 2, 2, 2, 2], 'source': 'smard'}
        self.assertEqual(ph.hourly_if_repeated(block)['prices'], [1, 2])
        self.assertEqual(ph.hourly_if_repeated({**block, 'prices': [1, 1, 1, 2, 2, 2, 2, 2]})['step_minutes'], 15)

    def test_a_day_smard_lacks_is_taken_from_energy_charts_under_cc_by(self):
        found = self.zone('DE-LU', [date(2026, 11, 23), date(2026, 11, 24)], ec=2)
        with open(os.path.join(EC, 'price_DE-LU_2026-11-24.json'), encoding='utf-8') as f:
            raw = json.load(f)
        self.assertEqual(found['2026-11-23']['source'], 'smard')
        day = found['2026-11-24']
        self.assertEqual((day['source'], day['prices'], day['license_info']), ('energy-charts', raw['price'], raw['license_info']))

    def test_under_another_licence_it_is_not_taken(self):
        found, log = self.quiet(self.zone, 'DE-LU', [date(2026, 11, 24)],
                                Sources(ec={'DE-LU': 'price_ES_2026-11-24_restricted.json'}), 2)
        self.assertEqual(found, {})
        self.assertIn('under another licence', log)


class OMIE(InAFolder):

    def test_the_zip_of_2018_gives_both_zones_and_the_last_version_of_a_day(self):
        found = self.zone('PT', [date(2018, 3, 25), date(2018, 7, 4)])
        with zipfile.ZipFile(os.path.join(FIX, 'omie_marginalpdbc_2018.zip')) as archive:
            second = omie_rows(archive.read('marginalpdbc_20180704.2').decode('latin-1'))
        self.assertEqual(len(found['2018-03-25']['prices']), 23)
        self.assertEqual(found['2018-07-04']['prices'], [round(float(r[4]), 2) for r in second])
        self.assertEqual(found['2018-07-04']['source'], 'omie')

    def test_files_of_a_day_by_the_hour_and_by_the_quarter_hour(self):
        found = self.zone('ES', [date(2025, 9, 30), date(2025, 10, 1), date(2025, 10, 26)])
        with open(os.path.join(FIX, 'omie_marginalpdbc_20251001.1'), encoding='latin-1') as f:
            quarters = omie_rows(f.read())
        self.assertEqual([found[d]['step_minutes'] for d in ('2025-09-30', '2025-10-01', '2025-10-26')], [60, 15, 15])
        self.assertEqual(found['2025-10-01']['prices'], [round(float(r[5]), 2) for r in quarters])
        self.assertEqual((found['2025-10-26']['start'], len(found['2025-10-26']['prices'])),
                         ('2025-10-25T22:00:00+00:00', 100))

    def test_portugal_and_spain_read_each_file_once(self):
        sources = Sources()
        self.zone('PT', [date(2025, 10, 1)], sources)
        self.zone('ES', [date(2025, 10, 1)], sources)
        self.assertEqual(len([u for u in sources.asked if '20251001.1' in u]), 1)


class TheFiles(InAFolder):

    NOW = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)

    def build(self):
        sources = Sources()
        for zone, days in (('DE-LU', [date(2018, 3, 25), date(2026, 11, 23), date(2026, 11, 24)]),
                           ('FR', [date(2017, 10, 29)]), ('PT', [date(2018, 3, 25), date(2025, 10, 1)])):
            found, _ = ph.fetch_zone(zone, days, sources, fc.Budget(60), 2, pause=0)
            ph.store(zone, found, self.NOW, self.folder)
        return ph.build_summaries(self.NOW, self.folder)

    def test_a_file_per_zone_and_year_with_its_licence_attribution_and_sources(self):
        self.quiet(self.build)
        doc = self.doc('DE-LU', '2026.json')
        self.assertEqual((doc['zone'], doc['year'], doc['unit'], doc['license']), ('DE-LU', 2026, 'EUR/MWh', 'cc-by-4.0'))
        self.assertEqual(doc['attribution'], 'Bundesnetzagentur | SMARD.de, energy-charts.info')
        self.assertEqual(list(doc['days']), ['2026-11-23', '2026-11-24'])
        self.assertEqual(self.doc('DE-LU', '2018.json')['attribution'], 'Bundesnetzagentur | SMARD.de')
        portugal = self.doc('PT', '2025.json')
        self.assertEqual((portugal['license'], portugal['attribution']), ('omie', 'OMIE'))

    def test_the_daily_means_are_the_means_of_the_intervals(self):
        self.quiet(self.build)
        daily = self.doc('daily', '2026.json')['zones']['DE-LU']
        day = self.doc('DE-LU', '2026.json')['days']['2026-11-24']
        self.assertEqual((daily['first'], len(daily['values'])), ('2026-11-23', 2))
        self.assertEqual(daily['values'][1], round(sum(day['prices']) / len(day['prices']), 2))
        self.assertEqual([s['source'] for s in daily['sources']], ['smard', 'energy-charts'])
        self.assertEqual(self.doc('daily', '2017.json')['zones']['FR']['first'], '2017-10-29')

    def test_the_monthly_means_weigh_each_interval_by_its_length_and_say_how_many_days(self):
        self.quiet(self.build)
        monthly = self.doc('monthly.json')['zones']['DE-LU']
        days = self.doc('DE-LU', '2026.json')['days']
        minutes = sum(len(d['prices']) * d['step_minutes'] for d in days.values())
        expected = sum(sum(d['prices']) * d['step_minutes'] for d in days.values()) / minutes
        self.assertEqual(monthly['first'], '2018-03')
        self.assertEqual(monthly['values'][-1], round(expected, 2))
        self.assertEqual((monthly['days'][0], monthly['days'][-1], monthly['values'][1]), (1, 2, None))

    def test_the_index_says_from_when_until_when_and_from_where(self):
        self.quiet(self.build)
        index = self.doc('index.json')
        self.assertEqual(list(index['zones']), ['DE-LU', 'FR', 'PT'])
        de = index['zones']['DE-LU']
        self.assertEqual((de['first'], de['last'], de['years']), ('2018-03-25', '2026-11-24', [2018, 2026]))
        self.assertEqual([(s['source'], s.get('series'), s['days']) for s in de['sources']],
                         [('smard', 'DE-AT-LU', 1), ('smard', None, 1), ('energy-charts', None, 1)])
        self.assertEqual(de['missing_days'], len(ph.days_between(date(2018, 3, 25), date(2026, 11, 24))) - 3)

    def test_nothing_new_writes_nothing(self):
        self.quiet(self.build)
        written, _log = self.quiet(self.build)
        self.assertEqual(written, 0)


class TheRuns(InAFolder):

    def test_the_daily_update_asks_only_for_the_days_missing(self):
        now = datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc)
        today = date(2026, 11, 24)
        window = ph.days_between(today - timedelta(days=ph.UPDATE_DAYS), today + timedelta(days=1))
        whole = {day.isoformat(): {'start': 's', 'step_minutes': 60, 'prices': [1.0] * 24, 'source': 'omie'}
                 for day in window}
        ph.store('PT', whole, now, self.folder)                  # Portugal has every day of the window
        sources = Sources()
        self.quiet(ph.update, now=now, get=sources, budget=fc.Budget(60), folder=self.folder, pause=0)
        self.assertIn('2026-11-23', self.doc('DE-LU', '2026.json')['days'])
        self.assertEqual(len(ph.missing_days('ES', window, self.folder)), len(window))   # OMIE: no file then
        asked_for_pt = [u for u in sources.asked if 'marginalpdbc' in u]
        self.assertEqual(len(asked_for_pt), len(window))         # one 404 per day, asked for Spain only

    def test_energy_charts_is_asked_ec_per_run_times_at_most_in_a_daily_update(self):
        sources = Sources()
        self.quiet(ph.update, now=datetime(2026, 11, 24, 15, 0, tzinfo=timezone.utc), get=sources,
                   budget=fc.Budget(60), folder=self.folder, pause=0)
        self.assertEqual(len([u for u in sources.asked if 'energy-charts' in u]), ph.EC_PER_RUN)

    def test_the_fill_publishes_a_zone_at_a_time_and_fails_when_a_push_fails(self):
        now = datetime(2025, 10, 2, 12, 0, tzinfo=timezone.utc)
        messages = []
        code, _log = self.quiet(ph.fill, ['PT'], now=now, get=Sources(), folder=self.folder,
                                publish=lambda m: messages.append(m) or True, ec_gap_s=0, pause=0)
        self.assertEqual(code, 0)
        self.assertEqual(len(messages), 1)
        self.assertTrue(messages[0].startswith('Price history of PT:'))
        self.assertEqual(sorted(self.doc('PT', '2018.json')['days'])[0], '2018-01-01')
        code, _log = self.quiet(ph.fill, ['ES'], now=now, get=Sources(), folder=self.folder,
                                publish=lambda m: False, ec_gap_s=0, pause=0)
        self.assertEqual(code, 1)

    def test_the_fill_that_finds_nothing_ends_in_error_and_one_with_nothing_missing_does_not(self):
        nothing = lambda url, timeout=30, **_k: fc.Response(500, {}, b'')   # noqa: E731
        now = datetime(2018, 1, 3, 12, 0, tzinfo=timezone.utc)
        code, _log = self.quiet(ph.fill, ['PT'], now=now, get=nothing, folder=self.folder,
                                publish=lambda m: True, ec_gap_s=0, pause=0)
        self.assertEqual(code, 1)
        self.quiet(ph.fill, ['PT'], now=now, get=Sources(), folder=self.folder, publish=lambda m: True,
                   ec_gap_s=0, pause=0)
        code, log = self.quiet(ph.fill, ['PT'], now=now, get=Sources(), folder=self.folder, publish=lambda m: True,
                               ec_gap_s=0, pause=0)
        self.assertEqual(code, 0)
        self.assertIn('nothing missing', log)


if __name__ == '__main__':
    unittest.main()
