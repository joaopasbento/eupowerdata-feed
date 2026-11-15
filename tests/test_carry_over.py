"""Tests for keeping, within the same UTC day, what a source did not answer for
(carry_over.py, fetch_entsoe.py, fetch_gb.py, fetch_ree.py, fetch_ren.py).

Run from the repository root:  python -m unittest discover -s tests -v

No network: each test runs the real main() of fetch_entsoe.py (and of
fetch_gb.py) in a temporary directory, with the clock set and with ENTSO-E and
Elexon answering only for the zones the test says.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

import fetch_entsoe as fe
import fetch_gb
import fetch_ree
import fetch_ren

PRICES = 'data/spot-prices.json'
GENERATION = 'data/generation-mix.json'
FLOWS = 'data/cross-border-flows.json'
STATUS = 'data/status.json'


def at(text):
    """'2026-11-16 10:00:05' as a UTC time."""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def iso(text):
    return at(text).isoformat()


def read(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def fingerprint(path):
    """Content and identity of a file: a rewrite changes the inode."""
    stat = os.stat(path)
    with open(path, 'rb') as f:
        return f.read(), stat.st_ino, stat.st_mtime_ns


class Feed(unittest.TestCase):
    """A temporary feed directory and ways to run the scripts in it."""

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def entsoe(self, when, prices=None, generation=None, flows=None):
        """One run of fetch_entsoe.py at `when`. ENTSO-E answers only for what
        is given: prices {zone: EUR/MWh}, generation {zone: MW}, flows
        {(from, to): MW}. Returns (exit code, log)."""
        answers = {
            'Prices': {zone: [{'time': f'{h:02d}:00', 'hour': h, 'price': price + h} for h in range(24)]
                       for zone, price in (prices or {}).items()},
            'Generation': {zone: {'Solar': mw * 0.25, 'Fossil Gas': mw * 0.75}
                           for zone, mw in (generation or {}).items()},
            'Flow': {pair: [{'time': '09:00', 'mw': mw - 10}, {'time': '10:00', 'mw': mw}]
                     for pair, mw in (flows or {}).items()},
        }

        def fetch_all(label, jobs, parser, describe, deadline):
            asked = {key for key, _name, _url in jobs}
            return {key: value for key, value in answers[label].items() if key in asked}

        log = io.StringIO()
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), \
                mock.patch.object(fe, 'utcnow', lambda: when), \
                mock.patch.object(fe, 'find_working_endpoint', lambda deadline=None: 'https://entsoe.test/api'), \
                mock.patch.object(fe, 'fetch_all', fetch_all), \
                mock.patch.object(fe, 'update_forward_curves', lambda deadline=None: None), \
                contextlib.redirect_stdout(log):
            code = fe.main()
        return code, log.getvalue()

    def elexon(self, when, answers=True):
        """One run of fetch_gb.py at `when`, with Elexon answering or not."""
        generation = {'mix': {'Wind Onshore': 9000.0, 'Fossil Gas': 21000.0}, 'total_mw': 30000.0,
                      'renewable_pct': 30.0, 'source': 'Elexon (AGPT + wind/solar)'}
        price = {'eic': fetch_gb.GB_EIC, 'prices': [{'time': '10:00', 'hour': 10, 'price': 250.0}],
                 'latest': {'time': '10:00', 'hour': 10, 'price': 250.0}, 'avg': 250.0, 'min': 250.0,
                 'max': 250.0, 'source': 'Elexon MID (N2EX, market index)', 'currency': 'EUR'}
        log = io.StringIO()
        with mock.patch.object(fetch_gb, 'utcnow', lambda: when), \
                mock.patch.object(fetch_gb, 'fetch_gb_generation', lambda: dict(generation) if answers else None), \
                mock.patch.object(fetch_gb, 'fetch_gb_price', lambda: dict(price) if answers else None), \
                contextlib.redirect_stdout(log):
            code = fetch_gb.main()
        return code, log.getvalue()


MORNING = at('2026-11-16 10:00:05')
LATER = at('2026-11-16 10:15:05')
LATER_STILL = at('2026-11-16 10:30:05')


class AZoneThatDoesNotAnswer(Feed):

    def test_keeps_its_value_and_the_time_it_was_obtained(self):
        self.entsoe(MORNING, prices={'PT': 50, 'ES': 60, 'FR': 70})
        first = read(PRICES)
        _code, log = self.entsoe(LATER, prices={'PT': 55, 'ES': 65})
        doc = read(PRICES)

        self.assertEqual(doc['updated'], LATER.isoformat())
        self.assertEqual(list(doc['zones']), ['PT', 'ES', 'FR'])           # FR back in its place

        fr = doc['zones']['FR']
        self.assertEqual(fr['fetched_at'], MORNING.isoformat())            # not the time of this run
        self.assertIs(fr['carried_over'], True)
        for field in ('eic', 'prices', 'latest', 'avg', 'min', 'max'):     # the value itself, untouched
            self.assertEqual(fr[field], first['zones']['FR'][field])

        pt = doc['zones']['PT']
        self.assertEqual(pt['fetched_at'], LATER.isoformat())
        self.assertNotIn('carried_over', pt)
        self.assertEqual(pt['avg'], 66.5)                                  # 55 + mean(0..23)

        # The log says which was kept and from when.
        self.assertIn(f'FR (from {MORNING.isoformat()})', log)

    def test_in_two_runs_running_still_has_the_first_time(self):
        self.entsoe(MORNING, prices={'PT': 50, 'FR': 70})
        self.entsoe(LATER, prices={'PT': 55})
        self.entsoe(LATER_STILL, prices={'PT': 58})
        doc = read(PRICES)
        self.assertEqual(doc['updated'], LATER_STILL.isoformat())
        self.assertEqual(doc['zones']['FR']['fetched_at'], MORNING.isoformat())
        self.assertIs(doc['zones']['FR']['carried_over'], True)
        self.assertEqual(doc['zones']['FR']['avg'], 81.5)                  # still the morning's 70

    def test_and_then_answers_again_replaces_the_kept_value(self):
        self.entsoe(MORNING, prices={'PT': 50, 'FR': 70})
        self.entsoe(LATER, prices={'PT': 55})
        self.entsoe(LATER_STILL, prices={'PT': 58, 'FR': 90})
        fr = read(PRICES)['zones']['FR']
        self.assertEqual(fr['fetched_at'], LATER_STILL.isoformat())
        self.assertNotIn('carried_over', fr)
        self.assertEqual(fr['avg'], 101.5)

    def test_the_same_goes_for_generation_and_flows(self):
        self.entsoe(MORNING, generation={'PT': 4000, 'FR': 50000}, flows={('PT', 'ES'): 1000, ('ES', 'FR'): 300})
        self.entsoe(LATER, generation={'PT': 4400}, flows={('PT', 'ES'): 1200})
        gen, flows = read(GENERATION), read(FLOWS)

        self.assertEqual(gen['zones']['FR']['total_mw'], 50000)
        self.assertEqual(gen['zones']['FR']['fetched_at'], MORNING.isoformat())
        self.assertIs(gen['zones']['FR']['carried_over'], True)
        self.assertEqual(gen['zones']['PT']['fetched_at'], LATER.isoformat())
        self.assertNotIn('carried_over', gen['zones']['PT'])

        kept = flows['corridors']['ES→FR']
        self.assertEqual((kept['latest']['mw'], kept['fetched_at'], kept['carried_over']),
                         (300, MORNING.isoformat(), True))
        self.assertEqual(flows['corridors']['PT→ES']['fetched_at'], LATER.isoformat())
        self.assertEqual(list(flows['corridors']), ['PT→ES', 'ES→FR'])

    def test_a_zone_entsoe_never_has_data_for_stays_absent(self):
        self.entsoe(MORNING, prices={'PT': 50})
        self.entsoe(LATER, prices={'PT': 55})
        self.assertEqual(list(read(PRICES)['zones']), ['PT'])


class ThePreviousDay(Feed):

    def test_is_not_kept_in_the_first_run_after_midnight(self):
        self.entsoe(at('2026-11-16 23:50:00'), prices={'PT': 50, 'FR': 70},
                    generation={'PT': 4000, 'FR': 50000}, flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200})
        _code, log = self.entsoe(at('2026-11-17 00:05:00'), prices={'PT': 55},
                                 generation={'PT': 4400}, flows={('PT', 'ES'): 1200})
        self.assertEqual(list(read(PRICES)['zones']), ['PT'])
        self.assertEqual(list(read(GENERATION)['zones']), ['PT'])
        self.assertEqual(list(read(FLOWS)['corridors']), ['PT→ES'])
        self.assertEqual(read(PRICES)['zones']['PT']['fetched_at'], iso('2026-11-17 00:05:00'))
        self.assertIn('Left out, last obtained on an earlier day: FR', log)

    def test_is_not_kept_even_if_it_was_being_kept_the_day_before(self):
        self.entsoe(at('2026-11-16 10:00:00'), prices={'PT': 50, 'FR': 70})
        self.entsoe(at('2026-11-16 23:50:00'), prices={'PT': 52})
        self.assertEqual(read(PRICES)['zones']['FR']['fetched_at'], iso('2026-11-16 10:00:00'))
        self.entsoe(at('2026-11-17 00:05:00'), prices={'PT': 55})
        self.assertNotIn('FR', read(PRICES)['zones'])

    def test_the_day_is_the_utc_day_of_the_value_not_of_the_file(self):
        # Obtained yesterday at 23:50, but kept by a run of today: the file is
        # of today, the value is not.
        self.entsoe(at('2026-11-16 23:50:00'), prices={'PT': 50, 'FR': 70})
        self.entsoe(at('2026-11-17 00:05:00'), prices={'PT': 55, 'FR': 75})
        self.entsoe(at('2026-11-17 00:20:00'), prices={'PT': 56})
        fr = read(PRICES)['zones']['FR']
        self.assertEqual((fr['fetched_at'], fr['carried_over']), (iso('2026-11-17 00:05:00'), True))


class WhenNothingAnswers(Feed):

    def test_the_file_is_not_touched(self):
        self.entsoe(MORNING, prices={'PT': 50, 'FR': 70}, generation={'PT': 4000}, flows={('PT', 'ES'): 1000})
        archive = 'data/archive/spot-prices-2026-11-16.json'
        before = fingerprint(PRICES), fingerprint(archive)

        _code, log = self.entsoe(LATER, generation={'PT': 4400}, flows={('PT', 'ES'): 1200})

        self.assertEqual((fingerprint(PRICES), fingerprint(archive)), before)
        self.assertEqual(read(PRICES)['updated'], MORNING.isoformat())       # did not move
        self.assertNotIn('carried_over', read(PRICES)['zones']['FR'])
        self.assertEqual(read(GENERATION)['updated'], LATER.isoformat())     # the others did
        self.assertIn('spot-prices.json is left exactly as it was', log)

        status = read(STATUS)
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (2, 0, 2))
        self.assertEqual((status['generation_zones'], status['generation_zones_fetched'],
                          status['generation_zones_carried_over']), (1, 1, 0))

    def test_for_any_of_the_three_files_none_is_touched(self):
        self.entsoe(MORNING, prices={'PT': 50}, generation={'PT': 4000}, flows={('PT', 'ES'): 1000})
        before = [fingerprint(path) for path in (PRICES, GENERATION, FLOWS)]
        code, _log = self.entsoe(LATER)
        self.assertEqual([fingerprint(path) for path in (PRICES, GENERATION, FLOWS)], before)
        self.assertEqual(code, 1)
        status = read(STATUS)
        self.assertEqual(status['status'], 'ok')
        for prefix in ('prices_zones', 'generation_zones', 'flow_corridors'):
            self.assertEqual((status[prefix], status[prefix + '_fetched'], status[prefix + '_carried_over']),
                             (1, 0, 1), prefix)

    def test_a_first_run_with_nothing_writes_nothing(self):
        self.entsoe(MORNING)
        for path in (PRICES, GENERATION, FLOWS):
            self.assertFalse(os.path.exists(path), path)


class AFileFromBeforeTheseFields(Feed):
    """The published files have no fetched_at yet: their zones are taken to be
    as old as the file's own `updated`."""

    def legacy(self, updated):
        zone = {'eic': 'x', 'prices': [{'time': '00:00', 'hour': 0, 'price': 40.0}],
                'latest': {'time': '00:00', 'hour': 0, 'price': 40.0}, 'avg': 40.0, 'min': 40.0, 'max': 40.0}
        with open(PRICES, 'w', encoding='utf-8') as f:
            json.dump({'updated': updated, 'source': 'ENTSO-E Transparency Platform',
                       'endpoint': 'https://web-api.tp.entsoe.eu/api',
                       'zones': {'PT': dict(zone), 'FR': dict(zone)}}, f)

    def test_of_the_same_day(self):
        self.legacy(iso('2026-11-16 08:00:00'))
        self.entsoe(MORNING, prices={'PT': 50})
        fr = read(PRICES)['zones']['FR']
        self.assertEqual((fr['fetched_at'], fr['carried_over'], fr['avg']),
                         (iso('2026-11-16 08:00:00'), True, 40.0))

    def test_of_a_previous_day(self):
        self.legacy(iso('2026-10-29 03:02:01'))        # the feed as it stood for 17 days
        self.entsoe(MORNING, prices={'PT': 50})
        self.assertEqual(list(read(PRICES)['zones']), ['PT'])

    def test_with_no_usable_time_at_all(self):
        self.legacy('not a time')
        self.entsoe(MORNING, prices={'PT': 50})
        self.assertEqual(list(read(PRICES)['zones']), ['PT'])


class ACountryOfSeveralZones(Feed):
    """NO is NO1 + NO2 (CONSOLIDATE). With only one of them, there is no NO to
    publish from this run."""

    def test_is_not_put_together_from_part_of_its_zones(self):
        self.entsoe(MORNING, prices={'PT': 50, 'NO1': 40, 'NO2': 60}, generation={'PT': 4000, 'NO1': 2000, 'NO2': 6000})
        first_prices, first_gen = read(PRICES), read(GENERATION)
        self.assertEqual(first_gen['zones']['NO']['total_mw'], 8000)
        self.assertEqual(first_prices['zones']['NO']['avg'], 61.5)          # mean of 40 and 60, + mean(0..23)

        _code, log = self.entsoe(LATER, prices={'PT': 55, 'NO1': 44}, generation={'PT': 4400, 'NO1': 2200})
        prices, gen = read(PRICES), read(GENERATION)

        no = gen['zones']['NO']
        self.assertEqual(no['total_mw'], 8000)                              # not 2200: that is half of Norway
        self.assertEqual(no['mix'], first_gen['zones']['NO']['mix'])
        self.assertEqual((no['fetched_at'], no['carried_over']), (MORNING.isoformat(), True))

        no = prices['zones']['NO']
        self.assertEqual(no['avg'], 61.5)                                   # not NO1's 55.5
        self.assertEqual((no['fetched_at'], no['carried_over']), (MORNING.isoformat(), True))

        for doc in (prices, gen):
            self.assertNotIn('NO1', doc['zones'])
            self.assertNotIn('NO2', doc['zones'])
            self.assertEqual(list(doc['zones']), ['PT', 'NO'])
        self.assertIn('NO: NO1 answered but NO2 did not', log)

    def test_and_is_left_out_when_there_is_nothing_from_today_to_keep(self):
        self.entsoe(MORNING, prices={'PT': 50, 'NO1': 40}, generation={'PT': 4000, 'NO2': 6000})
        self.assertEqual(list(read(PRICES)['zones']), ['PT'])
        self.assertEqual(list(read(GENERATION)['zones']), ['PT'])

    def test_with_all_its_zones_it_is_fresh(self):
        self.entsoe(MORNING, generation={'NO1': 2000, 'NO2': 6000})
        self.entsoe(LATER, generation={'NO1': 2100, 'NO2': 6300})
        no = read(GENERATION)['zones']['NO']
        self.assertEqual(no['total_mw'], 8400)
        self.assertEqual(no['fetched_at'], LATER.isoformat())
        self.assertNotIn('carried_over', no)

    def test_part_of_a_country_alone_does_not_count_as_an_answer(self):
        self.entsoe(MORNING, generation={'NO1': 2000, 'NO2': 6000})
        before = fingerprint(GENERATION)
        self.entsoe(LATER, generation={'NO1': 2100})
        self.assertEqual(fingerprint(GENERATION), before)


class TheNetBlock(Feed):

    def test_is_worked_out_from_the_corridors_in_the_file_kept_ones_included(self):
        self.entsoe(MORNING, flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200, ('ES', 'FR'): 500, ('FR', 'ES'): 100})
        self.assertEqual(read(FLOWS)['net']['ES-PT'],
                         {'from': 'PT', 'to': 'ES', 'net_mw': 800, 'fetched_at': MORNING.isoformat()})

        self.entsoe(LATER, flows={('PT', 'ES'): 1500, ('ES', 'FR'): 550, ('FR', 'ES'): 150})
        flows = read(FLOWS)

        self.assertIs(flows['corridors']['ES→PT']['carried_over'], True)
        # 1500 of this run against the 200 kept from the morning: what the file shows.
        self.assertEqual(flows['net']['ES-PT'],
                         {'from': 'PT', 'to': 'ES', 'net_mw': 1300,
                          'fetched_at': MORNING.isoformat(), 'carried_over': True})
        # A pair with both corridors from this run has this run's time and no mark.
        self.assertEqual(flows['net']['ES-FR'],
                         {'from': 'ES', 'to': 'FR', 'net_mw': 400, 'fetched_at': LATER.isoformat()})
        # A pair with no corridor in the file is as it always was.
        self.assertEqual(flows['net']['DE-FR'], {'from': 'FR', 'to': 'DE', 'net_mw': 0})

        # Every pair adds up with the corridors of the same file.
        corridors = flows['corridors']
        self.assertEqual(len(flows['net']), 47)
        for pair, net in flows['net'].items():
            a, b = net['from'], net['to']
            forward = corridors.get(f'{a}→{b}', {}).get('latest', {}).get('mw', 0)
            back = corridors.get(f'{b}→{a}', {}).get('latest', {}).get('mw', 0)
            self.assertEqual(net['net_mw'], forward - back, pair)

    def test_a_kept_corridor_of_the_previous_day_leaves_the_net_too(self):
        self.entsoe(at('2026-11-16 23:50:00'), flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200})
        self.entsoe(at('2026-11-17 00:05:00'), flows={('PT', 'ES'): 1500})
        flows = read(FLOWS)
        self.assertNotIn('ES→PT', flows['corridors'])
        self.assertEqual(flows['net']['ES-PT'],
                         {'from': 'PT', 'to': 'ES', 'net_mw': 1500, 'fetched_at': iso('2026-11-17 00:05:00')})


class TheUnitedKingdom(Feed):
    """GB comes from Elexon, through fetch_gb.py, after fetch_entsoe.py."""

    GB_MORNING = at('2026-11-16 10:03:00')
    GB_LATER = at('2026-11-16 10:18:00')

    def test_with_elexon_failing_keeps_todays_value_and_its_time(self):
        self.entsoe(MORNING, prices={'PT': 50, 'NO1': 40, 'NO2': 60}, generation={'PT': 4000})
        self.elexon(self.GB_MORNING)
        self.assertEqual(read(PRICES)['zones']['GB']['fetched_at'], self.GB_MORNING.isoformat())
        self.assertEqual(list(read(PRICES)['zones']), ['PT', 'NO', 'GB'])

        self.entsoe(LATER, prices={'PT': 55, 'NO1': 44, 'NO2': 66}, generation={'PT': 4400})
        _code, log = self.elexon(self.GB_LATER, answers=False)

        for path in (PRICES, GENERATION):
            doc = read(path)
            gb = doc['zones']['GB']
            self.assertEqual((gb['fetched_at'], gb['carried_over']), (self.GB_MORNING.isoformat(), True), path)
            self.assertEqual(gb['source'][:6], 'Elexon')
            self.assertEqual(list(doc['zones'])[-1], 'GB')                 # still last
            self.assertEqual(doc['updated'], LATER.isoformat())
        self.assertEqual(read(PRICES)['zones']['GB']['avg'], 250.0)
        self.assertIn(f'obtained at {self.GB_MORNING.isoformat()} (kept from an earlier run today)', log)

        status = read(STATUS)
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (3, 2, 1))
        self.assertEqual((status['generation_zones'], status['generation_zones_fetched'],
                          status['generation_zones_carried_over']), (2, 1, 1))

    def test_with_elexon_answering_gets_the_new_data_and_no_mark(self):
        self.entsoe(MORNING, prices={'PT': 50}, generation={'PT': 4000})
        self.elexon(self.GB_MORNING)
        self.entsoe(LATER, prices={'PT': 55}, generation={'PT': 4400})
        self.assertIs(read(PRICES)['zones']['GB']['carried_over'], True)   # as fetch_entsoe.py left it
        self.assertEqual(read(STATUS)['prices_zones_carried_over'], 1)

        self.elexon(self.GB_LATER)

        for path in (PRICES, GENERATION):
            gb = read(path)['zones']['GB']
            self.assertEqual(gb['fetched_at'], self.GB_LATER.isoformat(), path)
            self.assertNotIn('carried_over', gb)
        status = read(STATUS)
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (2, 2, 0))
        self.assertEqual((status['generation_zones'], status['generation_zones_fetched'],
                          status['generation_zones_carried_over']), (2, 2, 0))

    def test_of_the_previous_day_is_not_kept(self):
        self.entsoe(at('2026-11-16 23:50:00'), prices={'PT': 50}, generation={'PT': 4000})
        self.elexon(at('2026-11-16 23:53:00'))
        self.entsoe(at('2026-11-17 00:05:00'), prices={'PT': 55}, generation={'PT': 4400})
        self.elexon(at('2026-11-17 00:08:00'), answers=False)
        for path in (PRICES, GENERATION):
            self.assertEqual(list(read(path)['zones']), ['PT'], path)
        status = read(STATUS)
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (1, 1, 0))

    def test_when_entsoe_gave_nothing_only_gb_is_new_and_the_file_time_does_not_move(self):
        self.entsoe(MORNING, prices={'PT': 50}, generation={'PT': 4000})
        self.elexon(self.GB_MORNING)
        self.entsoe(LATER)
        self.elexon(self.GB_LATER)
        doc = read(PRICES)
        self.assertEqual(doc['updated'], MORNING.isoformat())
        self.assertEqual(doc['zones']['PT']['fetched_at'], MORNING.isoformat())
        self.assertEqual(doc['zones']['GB']['fetched_at'], self.GB_LATER.isoformat())
        status = read(STATUS)
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (2, 1, 1))

    def test_stays_out_of_the_archive_as_before(self):
        self.entsoe(MORNING, prices={'PT': 50}, generation={'PT': 4000})
        self.elexon(self.GB_MORNING)
        self.entsoe(LATER, prices={'PT': 55, 'FR': 70}, generation={'PT': 4400})
        published = read(PRICES)
        archived = read('data/archive/spot-prices-2026-11-16.json')
        self.assertIn('GB', published['zones'])
        self.assertNotIn('GB', archived['zones'])
        published['zones'].pop('GB')
        self.assertEqual(archived, published)                              # otherwise the same document

    def test_a_status_file_from_another_run_is_left_alone(self):
        self.entsoe(MORNING, prices={'PT': 50}, generation={'PT': 4000})
        before = read(STATUS)
        self.elexon(at('2026-11-16 12:00:00'))
        self.assertEqual(read(STATUS), before)


class TheArchive(Feed):

    def test_follows_what_is_published(self):
        self.entsoe(MORNING, prices={'PT': 50, 'FR': 70}, generation={'PT': 4000, 'FR': 50000},
                    flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200})
        self.entsoe(LATER, prices={'PT': 55}, generation={'PT': 4400}, flows={('PT', 'ES'): 1500})
        for name, path in (('spot-prices', PRICES), ('generation-mix', GENERATION), ('cross-border-flows', FLOWS)):
            self.assertEqual(read(f'data/archive/{name}-2026-11-16.json'), read(path), name)
        self.assertIs(read('data/archive/spot-prices-2026-11-16.json')['zones']['FR']['carried_over'], True)


class TheFormat(Feed):
    """The site reads these files as they are: fields may only be added."""

    def test_only_gains_fields_and_keeps_the_order(self):
        self.entsoe(MORNING, prices={'XK': 90, 'PT': 50, 'DK1': 30, 'DK2': 34, 'ES': 60, 'NO1': 40, 'NO2': 60},
                    generation={'XK': 300, 'PT': 4000, 'ES': 30000, 'SE1': 1500, 'SE3': 7000},
                    flows={('ES', 'PT'): 200, ('PT', 'ES'): 1000})
        self.elexon(at('2026-11-16 10:03:00'))
        self.entsoe(LATER, prices={'ES': 61, 'NO1': 41, 'NO2': 61}, generation={'ES': 31000},
                    flows={('ES', 'PT'): 210})
        prices, gen, flows = read(PRICES), read(GENERATION), read(FLOWS)

        self.assertEqual(list(prices), ['updated', 'source', 'endpoint', 'zones'])
        self.assertEqual(list(gen), ['updated', 'source', 'zones'])
        self.assertEqual(list(flows), ['updated', 'source', 'corridors', 'net'])
        # ZONES order, then the consolidated countries, then GB: as in a run where all answer.
        self.assertEqual(list(prices['zones']), ['PT', 'ES', 'XK', 'NO', 'DK', 'GB'])
        self.assertEqual(list(gen['zones']), ['PT', 'ES', 'XK', 'SE', 'GB'])
        self.assertEqual(list(flows['corridors']), ['PT→ES', 'ES→PT'])

        self.assertEqual(list(prices['zones']['ES']), ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'fetched_at'])
        self.assertEqual(list(prices['zones']['PT']),
                         ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'fetched_at', 'carried_over'])
        self.assertEqual(list(prices['zones']['GB']),
                         ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'source', 'currency',
                          'fetched_at', 'carried_over'])
        self.assertEqual(list(gen['zones']['ES']), ['mix', 'total_mw', 'renewable_pct', 'fetched_at'])
        self.assertEqual(list(gen['zones']['SE']), ['mix', 'total_mw', 'renewable_pct', 'fetched_at', 'carried_over'])
        self.assertEqual(list(flows['corridors']['ES→PT']), ['from', 'to', 'latest', 'values', 'fetched_at'])
        self.assertEqual(list(flows['corridors']['PT→ES']),
                         ['from', 'to', 'latest', 'values', 'fetched_at', 'carried_over'])
        self.assertEqual(list(flows['net']['ES-PT']), ['from', 'to', 'net_mw', 'fetched_at', 'carried_over'])

    def test_status_keeps_its_fields_and_gains_the_counts(self):
        self.entsoe(MORNING, prices={'PT': 50, 'FR': 70}, generation={'PT': 4000}, flows={('PT', 'ES'): 1000})
        self.entsoe(LATER, prices={'PT': 55}, generation={'PT': 4400}, flows={('PT', 'ES'): 1200})
        status = read(STATUS)
        self.assertEqual(list(status)[:6],
                         ['status', 'endpoint', 'time', 'prices_zones', 'generation_zones', 'flow_corridors'])
        self.assertEqual((status['prices_zones'], status['prices_zones_fetched'],
                          status['prices_zones_carried_over']), (2, 1, 1))
        self.assertEqual((status['flow_corridors'], status['flow_corridors_fetched'],
                          status['flow_corridors_carried_over']), (1, 1, 0))
        self.assertEqual(status['run_started'], LATER.isoformat())


class TheCopiesOfPortugalAndSpain(Feed):
    """ree-generation.json and ren-generation.json take the time of the zone
    they copy, not of the file they copy it from."""

    def test_take_the_time_of_a_kept_zone(self):
        self.entsoe(MORNING, generation={'PT': 4000, 'ES': 30000, 'FR': 50000})
        self.entsoe(LATER, generation={'FR': 51000})                       # PT and ES did not answer
        gen = read(GENERATION)
        self.assertEqual(gen['updated'], LATER.isoformat())

        spain = fetch_ree.from_generation_mix(gen)
        portugal = fetch_ren.from_generation_mix(gen)
        self.assertEqual(spain['updated'], MORNING.isoformat())
        self.assertEqual(portugal['updated'], MORNING.isoformat())
        self.assertEqual((spain['total_mw'], portugal['total_mw']), (30000, 4000))

    def test_take_the_time_of_a_zone_of_this_run(self):
        self.entsoe(MORNING, generation={'PT': 4000, 'ES': 30000})
        self.entsoe(LATER, generation={'PT': 4400, 'ES': 31000})
        gen = read(GENERATION)
        self.assertEqual(fetch_ree.from_generation_mix(gen)['updated'], LATER.isoformat())
        self.assertEqual(fetch_ren.from_generation_mix(gen)['updated'], LATER.isoformat())

    def test_take_the_time_of_the_file_when_the_zone_has_none(self):
        gen = {'updated': iso('2026-11-15 22:07:02'), 'source': 'ENTSO-E Transparency Platform',
               'zones': {'PT': {'mix': {'Solar': 10.0}, 'total_mw': 10.0, 'renewable_pct': 100.0},
                         'ES': {'mix': {'Solar': 20.0}, 'total_mw': 20.0, 'renewable_pct': 100.0}}}
        self.assertEqual(fetch_ree.from_generation_mix(gen)['updated'], iso('2026-11-15 22:07:02'))
        self.assertEqual(fetch_ren.from_generation_mix(gen)['updated'], iso('2026-11-15 22:07:02'))

    def test_the_spanish_file_does_not_change_while_spain_is_being_kept(self):
        self.entsoe(MORNING, generation={'ES': 30000, 'FR': 50000})
        with contextlib.redirect_stdout(io.StringIO()):
            fetch_ree.main()
        before = fingerprint('data/ree-generation.json')
        self.assertEqual(read('data/ree-generation.json')['updated'], MORNING.isoformat())

        self.entsoe(LATER, generation={'FR': 51000})
        with contextlib.redirect_stdout(io.StringIO()):
            fetch_ree.main()
        self.assertEqual(fingerprint('data/ree-generation.json'), before)


if __name__ == '__main__':
    unittest.main()
