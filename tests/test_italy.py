"""Tests for the zone Italy's generation is asked for: the country, not the
North bidding zone that ZONES has for its price and its northern borders.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import fetch_entsoe as fe
from entsoe_xml import at
from test_carry_over import GENERATION, PRICES, Feed, read

NOW = at('2026-11-18 15:50:00')
COUNTRY, NORTH = '10YIT-GRTN-----B', '10Y1001A1001A73I'


class Italy(Feed):

    def domains(self, label, field):
        return [url.split(f'{field}=')[1].split('&')[0] for url in self.asked[label]]

    def test_generation_is_asked_for_the_whole_country(self):
        self.entsoe(NOW, prices={'IT': 100}, generation={'IT': 31866, 'PT': 4000}, flows={('FR', 'IT'): 500})
        self.assertEqual(fe.ZONES['IT'], NORTH)
        generation = self.domains('Generation', 'in_Domain')
        self.assertIn(COUNTRY, generation)
        self.assertNotIn(NORTH, generation)
        self.assertEqual(len(generation), len(set(generation)))               # one request for Italy, as before
        zone = read(GENERATION)['zones']['IT']                                 # under the same name, in the same place
        self.assertEqual(zone['total_mw'], 31866)
        self.assertEqual(list(read(GENERATION)['zones']), ['PT', 'IT'])

    def test_every_other_country_is_asked_for_as_before(self):
        self.entsoe(NOW, generation={'PT': 4000})
        self.assertEqual({code: eic for code, eic in fe.GEN_ZONES.items() if eic != fe.ZONES[code]}, {'IT': COUNTRY})
        self.assertIn(fe.ZONES['PT'], self.domains('Generation', 'in_Domain'))

    def test_the_price_and_the_northern_borders_are_still_those_of_the_north(self):
        self.entsoe(NOW, prices={'IT': 100}, flows={('FR', 'IT'): 500})
        self.assertIn(NORTH, self.domains('Prices', 'in_Domain'))
        self.assertNotIn(COUNTRY, self.domains('Prices', 'in_Domain'))
        self.assertEqual(read(PRICES)['zones']['IT']['eic'], NORTH)
        flow_zones = set(self.domains('Flow', 'in_Domain')) | set(self.domains('Flow', 'out_Domain'))
        self.assertIn(NORTH, flow_zones)
        self.assertNotIn(COUNTRY, flow_zones)
