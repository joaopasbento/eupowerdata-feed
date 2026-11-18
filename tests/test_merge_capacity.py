"""Tests for the capacity merge (merge_capacity.py).

Run from the repository root:  python -m unittest discover -s tests -v

tests/fixtures/capacity holds capacity-installed.json as it was on 15 November
2026 and what the site served that day (from it and from RenewableNodes): the
file must keep those countries and fields, in that order.
tests/fixtures/ember/ember-yearly-2026-11-17.json is what fetch_ember.py made
of Ember's file of the edition of 17 November 2026. The numbers the tests
expect were taken from Ember's file by hand (grep), not through that code.
"""

import json
import os
import tempfile
import unittest

import merge_capacity as mc

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures')


def load(*name):
    with open(os.path.join(FIXTURES, *name), encoding='utf-8') as f:
        return json.load(f)


class SameFile(unittest.TestCase):

    def setUp(self):
        self.entsoe = load('capacity', 'capacity-installed.json')
        self.ember = load('ember', 'ember-yearly-2026-11-17.json')
        self.served = load('capacity', 'renewablenodes-capacity.site-2026-11-15.json')
        self.result = mc.build(self.entsoe, self.ember)

    def test_same_countries_and_fields_in_the_same_order(self):
        self.assertEqual(list(self.result), list(self.served))
        self.assertEqual(list(self.result['countries']), list(self.served['countries']))
        self.assertEqual(list(self.result['countries']), mc.EU_CODES)
        self.assertEqual(self.result['count'], 32)
        for code, country in self.result['countries'].items():
            self.assertEqual(list(country), list(self.served['countries'][code]), code)

    def test_what_does_not_come_from_ember_is_what_the_site_served(self):
        """ENTSO-E's capacity, and the table of names, population and targets."""
        fields = ('name', 'capacity_mw', 'solar_pct', 'wind_pct', 'hydro_pct', 'solar_mw', 'wind_mw', 'hydro_mw',
                  'target', 'target_year', 'population_m', 'cap_source', 'cap_year')
        for code, served in self.served['countries'].items():
            if code == 'GB':
                continue
            country = self.result['countries'][code]
            self.assertEqual([country[f] for f in fields], [served[f] for f in fields], code)
        gb, served = self.result['countries']['GB'], self.served['countries']['GB']
        for field in ('name', 'target', 'target_year', 'population_m'):
            self.assertEqual(gb[field], served[field], field)

    def test_the_source_says_where_the_numbers_come_from(self):
        source = self.result['source']
        self.assertNotIn('RenewableNodes', source)
        for part in ('ENTSO-E', 'Ember, Yearly Electricity Data', 'edition of 2026-11-17', 'CC BY 4.0'):
            self.assertIn(part, source)
        self.assertEqual({c['cap_source'] for c in self.result['countries'].values()}, {'entsoe', 'ember'})
        self.assertEqual([code for code, c in self.result['countries'].items() if c['cap_source'] == 'ember'], ['GB'])


class NumbersOfEmber(unittest.TestCase):
    """Against Ember's file of 17 November 2026, read by hand."""

    def setUp(self):
        self.countries = mc.build(load('capacity', 'capacity-installed.json'),
                                  load('ember', 'ember-yearly-2026-11-17.json'))['countries']

    def test_shares_of_the_generation_of_2025(self):
        # country: Renewables, Bioenergy and Nuclear, "Share of generation (%)" of 2025
        by_hand = {'PT': (80.882, 6.618, 0.0), 'FR': (26.636, 1.785, 68.257), 'DK': (91.153, 21.372, 0.0),
                   'MK': (45.389, 0.842, 0.0), 'GB': (51.824, 13.939, 12.388)}
        expected = {'PT': (80.9, 6.6, 0), 'FR': (26.6, 1.8, 68.3), 'DK': (91.2, 21.4, 0), 'MK': (45.4, 0.8, 0),
                    'GB': (51.8, 13.9, 12.4)}
        for code, shares in by_hand.items():
            country = self.countries[code]
            got = (country['renewable_pct'], country['bio_pct'], country['nuclear_pct'])
            self.assertEqual(got, expected[code], code)
            for value, exact in zip(got, shares):
                self.assertAlmostEqual(value, exact, delta=0.05)

    def test_the_trend_is_the_seven_years_to_2025(self):
        # North Macedonia's Renewables, 2019 to 2025: 22.921 27.919 29.9 27.566 33.24 40.482 45.389
        self.assertEqual(self.countries['MK']['trend'], [22.9, 27.9, 29.9, 27.6, 33.2, 40.5, 45.4])
        # The United Kingdom's: 36.596 43.065 39.782 41.611 46.49 50.705 51.824
        self.assertEqual(self.countries['GB']['trend'], [36.6, 43.1, 39.8, 41.6, 46.5, 50.7, 51.8])
        for code, country in self.countries.items():
            self.assertEqual(len(country['trend']), 7, code)
            self.assertEqual(country['trend'][-1], country['renewable_pct'], code)

    def test_a_share_is_never_over_100(self):
        """Albania's come as 100.002, 99.992, 99.997, 99.997, 100.005, 100.005, 99.998."""
        self.assertEqual(self.countries['AL']['trend'], [100] * 7)
        self.assertEqual(self.countries['AL']['renewable_pct'], 100)

    def test_the_united_kingdom_has_embers_capacity_by_source(self):
        """Capacity (GW) of 2025: Solar 21.66, Wind 33.4, Hydro 2.19, Bioenergy
        7.01, no row of other renewables. 64.26 GW of renewables in all."""
        gb = self.countries['GB']
        self.assertEqual((gb['solar_mw'], gb['wind_mw'], gb['hydro_mw']), (21660, 33400, 2190))
        self.assertEqual(gb['capacity_mw'], 21660 + 33400 + 2190 + 7010)
        self.assertEqual((gb['solar_pct'], gb['wind_pct'], gb['hydro_pct']), (33.7, 52, 3.4))
        self.assertEqual((gb['cap_source'], gb['cap_year']), ('ember', 2025))


class Sources(unittest.TestCase):

    EMBER = {'iso3': 'PRT', 'year': 2025,
             'share_of_generation_pct': {'renewables': 80.882, 'bioenergy': 6.618, 'nuclear': 0.0},
             'renewables_trend': {'years': [2019, 2020], 'share_of_generation_pct': [53.12, None]},
             'capacity_gw': {'year': 2024, 'solar': 5.0, 'wind': 6.1, 'hydro': 8.0, 'bioenergy': 0.7,
                             'other_renewables': 0.2}}

    def test_entsoe_capacity_comes_first(self):
        entsoe = {'PT': {'year': 2025, 'capacity_mw': 13311, 'solar_mw': 2589, 'wind_mw': 5391, 'hydro_mw': 4644}}
        pt = mc.merge(entsoe, {'PT': self.EMBER})['PT']
        self.assertEqual((pt['capacity_mw'], pt['solar_mw'], pt['wind_mw'], pt['hydro_mw']),
                         (13311, 2589, 5391, 4644))
        self.assertEqual((pt['solar_pct'], pt['wind_pct'], pt['hydro_pct']), (19.5, 40.5, 34.9))
        self.assertEqual((pt['cap_source'], pt['cap_year']), ('entsoe', 2025))
        self.assertEqual((pt['renewable_pct'], pt['bio_pct'], pt['nuclear_pct']), (80.9, 6.6, 0))
        self.assertEqual((pt['name'], pt['population_m'], pt['target'], pt['target_year']),
                         ('Portugal', 10.3, 85, 2030))

    def test_embers_capacity_when_entsoe_has_none(self):
        """The five renewable sources make the total; the year is Ember's."""
        for entsoe in ({}, {'PT': {'year': 2025, 'capacity_mw': 0}}):
            pt = mc.merge(entsoe, {'PT': self.EMBER})['PT']
            self.assertEqual(pt['capacity_mw'], 20000)
            self.assertEqual((pt['solar_mw'], pt['wind_mw'], pt['hydro_mw']), (5000, 6100, 8000))
            self.assertEqual((pt['solar_pct'], pt['wind_pct'], pt['hydro_pct']), (25, 30.5, 40))
            self.assertEqual((pt['cap_source'], pt['cap_year']), ('ember', 2024))

    def test_a_year_of_the_trend_without_a_share_is_null(self):
        self.assertEqual(mc.merge({}, {'PT': self.EMBER})['PT']['trend'], [53.1, None])

    def test_a_country_with_no_capacity_anywhere_is_left_out(self):
        ember = {key: value for key, value in self.EMBER.items() if key != 'capacity_gw'}
        self.assertEqual(mc.merge({}, {'PT': ember}), {})
        self.assertEqual(mc.merge({}, {'PT': dict(ember, capacity_gw={'year': 2025, 'solar': 0.0})}), {})
        self.assertEqual(mc.merge({}, {}), {})

    def test_entsoe_only(self):
        """A country Ember does not have keeps its capacity and the table's
        values, with no share and no trend."""
        entsoe = {'XK': {'year': 2023, 'capacity_mw': 204, 'solar_mw': 0, 'wind_mw': 136, 'hydro_mw': 68}}
        xk = mc.merge(entsoe, {})['XK']
        self.assertEqual((xk['name'], xk['population_m'], xk['target'], xk['target_year']), ('Kosovo', 1.8, 25, 2030))
        self.assertEqual((xk['renewable_pct'], xk['bio_pct'], xk['nuclear_pct'], xk['trend']), (0, 0, 0, []))
        self.assertEqual((xk['wind_pct'], xk['hydro_pct']), (66.7, 33.3))

    def test_the_table_has_the_countries_ember_is_read_for(self):
        import fetch_ember
        self.assertEqual(list(mc.COUNTRIES), list(fetch_ember.ISO3))


class NoRequest(unittest.TestCase):
    """The merge reads the repository's files and nothing else."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.paths = (mc.ENTSOE_PATH, mc.EMBER_PATH, mc.OUT_PATH)
        self.addCleanup(lambda: setattr(mc, 'ENTSOE_PATH', self.paths[0]))
        self.addCleanup(lambda: setattr(mc, 'EMBER_PATH', self.paths[1]))
        self.addCleanup(lambda: setattr(mc, 'OUT_PATH', self.paths[2]))
        mc.ENTSOE_PATH = os.path.join(FIXTURES, 'capacity', 'capacity-installed.json')
        mc.EMBER_PATH = os.path.join(FIXTURES, 'ember', 'ember-yearly-2026-11-17.json')
        mc.OUT_PATH = os.path.join(self.dir.name, 'renewablenodes-capacity.json')

    def test_it_has_nothing_to_ask_with(self):
        self.assertFalse(hasattr(mc, 'http_get'))
        self.assertFalse(hasattr(mc, 'RN_URL'))

    def test_the_file_is_written_from_the_two_files(self):
        self.assertEqual(mc.main(['--force']), 0)
        with open(mc.OUT_PATH, encoding='utf-8') as f:
            written = json.load(f)
        self.assertEqual(written['count'], 32)
        self.assertEqual(written['countries']['GB']['capacity_mw'], 64260)
        self.assertEqual(mc.main([]), 0)                     # less than 12 hours old: left as it is

    def test_without_embers_file_the_previous_one_is_kept(self):
        mc.main(['--force'])
        with open(mc.OUT_PATH, encoding='utf-8') as f:
            before = f.read()
        mc.EMBER_PATH = os.path.join(self.dir.name, 'missing.json')
        self.assertEqual(mc.main(['--force']), 1)
        with open(mc.OUT_PATH, encoding='utf-8') as f:
            self.assertEqual(f.read(), before)


class Rounding(unittest.TestCase):
    """PHP round() sends halves away from zero; Python's round() does not."""

    def test_halves(self):
        self.assertEqual(mc.php_round(0.5), 1)
        self.assertEqual(mc.php_round(2.5), 3)
        self.assertEqual(mc.php_round(-2.5), -3)
        self.assertEqual(mc.php_round(1.25, 1), 1.3)
        self.assertEqual(mc.php_round(2.675, 2), 2.68)

    def test_whole_numbers_are_ints(self):
        self.assertIsInstance(mc.php_round(13311.0), int)
        self.assertIsInstance(mc.php_round(40.0, 1), int)
        self.assertEqual(mc.php_round(19.45007, 1), 19.5)


if __name__ == '__main__':
    unittest.main()
