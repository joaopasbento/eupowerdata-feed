"""Tests for the capacity merge (merge_capacity.py), the Python port of
eew_fetch_renewablenodes_capacity() in the site's eew-engine.php.

Run from the repository root:  python -m unittest discover -s tests -v

tests/fixtures/capacity holds the two inputs as they were on 15 November 2026
(capacity-installed.json as the site mirrored it; an excerpt of RenewableNodes'
api-data.json) and what the site served that day from them.
"""

import json
import os
import unittest

import merge_capacity as mc

FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'capacity')


def load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return json.load(f)


class SameAsTheSite(unittest.TestCase):

    def setUp(self):
        self.entsoe = load('capacity-installed.json')
        self.rn = load('renewablenodes-api-data.json')
        self.served = load('renewablenodes-capacity.site-2026-11-15.json')

    def test_same_inputs_give_what_the_site_served(self):
        result = mc.build(self.entsoe, self.rn, updated=self.served['updated'])
        self.assertEqual(result, self.served)

    def test_same_countries_and_fields_in_the_same_order(self):
        result = mc.build(self.entsoe, self.rn)
        self.assertEqual(list(result), list(self.served))
        self.assertEqual(list(result['countries']), list(self.served['countries']))
        self.assertEqual(list(result['countries']), mc.EU_CODES)
        self.assertEqual(result['count'], 32)
        for code, country in result['countries'].items():
            self.assertEqual(list(country), list(self.served['countries'][code]), code)

    def test_countries_outside_the_list_are_ignored(self):
        ids = {c['id'] for c in self.rn['countries']}
        self.assertTrue({'US', 'BR'} <= ids)
        result = mc.build(self.entsoe, self.rn)
        self.assertNotIn('US', result['countries'])
        self.assertNotIn('BR', result['countries'])


class Sources(unittest.TestCase):

    RN = {'id': 'PT', 'name': 'Portugal', 'capacity': 20.0, 'solar': 25, 'wind': 30.5, 'hydro': 40,
          'renewable': 80.8, 'bio': 6.6, 'nuclear': 0, 'target': 85, 'targetYear': 2030,
          'trend': [53.1, 58.6], 'pop': 10.3}

    def test_entsoe_capacity_comes_first(self):
        entsoe = {'PT': {'year': 2025, 'capacity_mw': 13311, 'solar_mw': 2589, 'wind_mw': 5391, 'hydro_mw': 4644}}
        pt = mc.merge(entsoe, {'PT': self.RN})['PT']
        self.assertEqual((pt['capacity_mw'], pt['solar_mw'], pt['wind_mw'], pt['hydro_mw']),
                         (13311, 2589, 5391, 4644))
        self.assertEqual((pt['solar_pct'], pt['wind_pct'], pt['hydro_pct']), (19.5, 40.5, 34.9))
        self.assertEqual((pt['cap_source'], pt['cap_year']), ('entsoe', 2025))
        self.assertEqual((pt['renewable_pct'], pt['target'], pt['target_year']), (80.8, 85, 2030))

    def test_renewablenodes_capacity_when_entsoe_has_none(self):
        for entsoe in ({}, {'PT': {'year': 2025, 'capacity_mw': 0}}):
            pt = mc.merge(entsoe, {'PT': self.RN})['PT']
            self.assertEqual(pt['capacity_mw'], 20000)                 # 20 GW
            self.assertEqual((pt['solar_mw'], pt['wind_mw'], pt['hydro_mw']), (5000, 6100, 8000))
            self.assertEqual((pt['solar_pct'], pt['wind_pct'], pt['hydro_pct']), (25, 30.5, 40))
            self.assertEqual((pt['cap_source'], pt['cap_year']), ('rn', None))

    def test_a_country_with_no_capacity_anywhere_is_left_out(self):
        rn = dict(self.RN, capacity=0)
        self.assertEqual(mc.merge({}, {'PT': rn}), {})
        self.assertEqual(mc.merge({}, {}), {})

    def test_entsoe_only(self):
        entsoe = {'XK': {'year': 2023, 'capacity_mw': 204, 'solar_mw': 0, 'wind_mw': 136, 'hydro_mw': 68}}
        xk = mc.merge(entsoe, {})['XK']
        self.assertEqual(xk['name'], 'XK')
        self.assertEqual((xk['renewable_pct'], xk['bio_pct'], xk['nuclear_pct'], xk['population_m']), (0, 0, 0, 0))
        self.assertEqual((xk['target'], xk['target_year'], xk['trend']), (0, 2030, []))
        self.assertEqual((xk['wind_pct'], xk['hydro_pct']), (66.7, 33.3))


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
