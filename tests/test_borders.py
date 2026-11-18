"""Tests for the three borders whose flows are asked for between the zones
that really touch (fetch_entsoe.FLOW_LINKS): Lithuania-Sweden through SE4,
Italy-Montenegro through Italy's Centre-South, and Norway-Sweden as the sum
of its four borders between zones.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are A11 documents written by hand.
"""

import unittest

import fetch_entsoe as fe
from entsoe_xml import FLOW_NS, at, document, iso, series
from test_carry_over import FLOWS, Feed, read

NOW = at('2026-11-17 08:26:00')
NORWAY_TO_SWEDEN = [('NO1', 'SE3'), ('NO3', 'SE2'), ('NO4', 'SE1'), ('NO4', 'SE2')]
SWEDEN_TO_NORWAY = [('SE3', 'NO1'), ('SE2', 'NO3'), ('SE1', 'NO4'), ('SE2', 'NO4')]


def answer(start, values, **more):
    return document(series(start, values, **more), ns=FLOW_NS)


def link(start, values, now=NOW, **more):
    return fe.latest_flow(answer(start, values, **more), now)


class TheZonesAskedFor(Feed):

    def test_each_border_is_asked_for_between_the_zones_that_touch(self):
        self.entsoe(NOW, flows={('PT', 'ES'): 1000})
        asked = {}
        for url in self.asked['Flow']:
            pair = (url.split('out_Domain=')[1].split('&')[0], url.split('in_Domain=')[1].split('&')[0])
            asked[pair] = asked.get(pair, 0) + 1
        eic = fe.LINK_ZONES
        for frm, to in NORWAY_TO_SWEDEN + SWEDEN_TO_NORWAY + [('LT', 'SE4'), ('SE4', 'LT'), ('IT-CSUD', 'ME'),
                                                             ('ME', 'IT-CSUD')]:
            self.assertEqual(asked.get((eic[frm], eic[to])), 1, (frm, to))
        # ... and no longer between the zones that do not.
        for frm, to in [('NO2', 'SE3'), ('SE3', 'NO2'), ('LT', 'SE3'), ('SE3', 'LT'), ('IT', 'ME'), ('ME', 'IT')]:
            self.assertNotIn((fe.ZONES[frm], fe.ZONES[to]), asked, (frm, to))
        self.assertEqual(eic['SE4'], '10Y1001A1001A47J')
        self.assertEqual(eic['IT-CSUD'], '10Y1001A1001A71M')
        self.assertEqual(len(self.asked['Flow']), len(fe.FLOW_CORRIDORS) + 6)    # 100: Norway-Sweden is 8, not 2
        # Every other corridor is asked for as it was.
        self.assertEqual(asked[(fe.ZONES['PT'], fe.ZONES['ES'])], 1)
        self.assertEqual(asked[(fe.ZONES['IT'], fe.ZONES['FR'])], 1)             # Italy's other borders: the North

    def test_lithuania_sweden_and_italy_montenegro_are_published_under_their_own_names(self):
        self.entsoe(NOW, flows={('LT', 'SE4'): 456, ('SE4', 'LT'): 0, ('ME', 'IT-CSUD'): 150, ('IT-CSUD', 'ME'): 0})
        flows = read(FLOWS)
        self.assertEqual(list(flows['corridors']), ['LT→SE', 'SE→LT', 'ME→IT', 'IT→ME'])       # their old places
        corridor = flows['corridors']['LT→SE']
        self.assertEqual(list(corridor), ['from', 'to', 'latest', 'values', 'data_time', 'fetched_at'])
        self.assertEqual((corridor['from'], corridor['to'], corridor['latest']['mw']), ('LT', 'SE', 456))
        self.assertEqual(flows['net']['LT-SE'], {'from': 'LT', 'to': 'SE', 'net_mw': 456,
                                                 'data_time': iso('2026-11-17 07:00:00'), 'fetched_at': NOW.isoformat()})
        self.assertEqual((flows['net']['IT-ME']['from'], flows['net']['IT-ME']['net_mw']), ('ME', 150))


class NorwaySweden(Feed):
    """The corridor is the sum of the four borders between zones."""

    def test_is_the_sum_of_its_four_links(self):
        self.entsoe(NOW, flows={**dict(zip(SWEDEN_TO_NORWAY, [1300, 273, 0, 0])),
                                **dict(zip(NORWAY_TO_SWEDEN, [0, 0, 1, 47]))})
        flows = read(FLOWS)
        south, north = flows['corridors']['SE→NO'], flows['corridors']['NO→SE']
        self.assertEqual(list(south), ['from', 'to', 'latest', 'values', 'data_time', 'fetched_at'])
        self.assertEqual((south['from'], south['to']), ('SE', 'NO'))
        self.assertEqual(south['latest'], {'time': '07:00', 'mw': 1573})          # 1 300 + 273 + 0 + 0
        self.assertEqual(north['latest'], {'time': '07:00', 'mw': 48})
        # The day's series: every hour of today, each the sum of the four.
        self.assertEqual([v['time'] for v in south['values']], [f'{h:02d}:00' for h in range(8)])
        self.assertEqual(south['values'][0], {'time': '00:00', 'mw': 1533})       # each link 10 less before its last hour
        self.assertEqual(south['values'][-1], south['latest'])
        self.assertEqual(flows['net']['NO-SE'], {'from': 'SE', 'to': 'NO', 'net_mw': 1525,
                                                 'data_time': iso('2026-11-17 07:00:00'), 'fetched_at': NOW.isoformat()})

    def test_is_not_put_together_from_some_of_its_links(self):
        self.entsoe(at('2026-11-17 08:11:00'), flows=dict(zip(SWEDEN_TO_NORWAY, [1300, 273, 0, 0])))
        _code, log = self.entsoe(NOW, flows={**dict(zip(SWEDEN_TO_NORWAY[:3], [1400, 300, 0])),   # SE2→NO4 did not answer
                                             ('PT', 'ES'): 1000})
        kept = read(FLOWS)['corridors']['SE→NO']
        self.assertEqual((kept['latest']['mw'], kept['fetched_at'], kept['carried_over']),
                         (1573, iso('2026-11-17 08:11:00'), True))                # not 1 700: that is part of the border
        self.assertIn('SE→NO: only some of its links answered', log)

    def test_and_is_left_out_when_there_is_nothing_to_keep(self):
        self.entsoe(NOW, flows={('SE3', 'NO1'): 1300, ('PT', 'ES'): 1000})
        flows = read(FLOWS)
        self.assertEqual(list(flows['corridors']), ['PT→ES'])
        self.assertNotIn('NO-SE', flows['net'])

    def test_is_as_old_as_the_oldest_of_its_links(self):
        # NO4→SE2 is only published up to 05:00; the others up to 07:00.
        self.entsoe(NOW, flows={**dict(zip(NORWAY_TO_SWEDEN[:3], [100, 200, 300])),
                                ('NO4', 'SE2'): (50, '2026-11-17 05:00')})
        corridor = read(FLOWS)['corridors']['NO→SE']
        self.assertEqual(corridor['data_time'], iso('2026-11-17 05:00:00'))
        self.assertEqual(corridor['latest'], {'time': '05:00', 'mw': 620})        # 90 + 190 + 290 + 50 at 05:00


class TheSum(unittest.TestCase):
    """sum_flows() on answers as ENTSO-E gives them: quarter-hours, with the
    points that repeat left out (curveType A03)."""

    def test_puts_back_the_points_left_out_before_adding(self):
        # 17 Nov 2026: NO1→SE3 is one point, 0 MW, for the whole 24 hours;
        # SE3→NO1 has all its 96.
        idle = link('2026-11-16 08:00', [0] + [None] * 95, minutes=15, curve='A03')
        busy = link('2026-11-16 08:00', [100.0 + q for q in range(96)], minutes=15, curve='A03')
        total = fe.sum_flows([idle, busy], NOW)
        self.assertEqual(len(total['values']), 32)                                # 00:00 to 07:45 today
        self.assertEqual(total['values'][0], {'time': '00:00', 'mw': 164})
        self.assertEqual(total['latest'], {'time': '07:45', 'mw': 195})
        self.assertEqual(total['data_time'], iso('2026-11-17 07:45:00'))

    def test_only_adds_the_intervals_every_link_has(self):
        a = link('2026-11-17 00:00', [10.4] * 32, minutes=15)                     # up to 07:45
        b = link('2026-11-17 00:00', [20.4] * 28, minutes=15)                     # up to 06:45
        total = fe.sum_flows([a, b], NOW)
        self.assertEqual(total['latest'], {'time': '06:45', 'mw': 31})            # 30.8, rounded after adding
        self.assertEqual(total['data_time'], iso('2026-11-17 06:45:00'))
        self.assertEqual(len(total['values']), 28)

    def test_with_nothing_of_today_the_last_value_is_yesterdays(self):
        night = at('2026-11-17 00:20:00')
        a = link('2026-11-16 20:00', [5.0] * 12, now=night, minutes=15)           # up to 22:45
        b = link('2026-11-16 20:00', [7.0] * 16, now=night, minutes=15)           # up to 23:45
        total = fe.sum_flows([a, b], night)
        self.assertEqual((total['values'], total['latest']), ([], {'time': '22:45', 'mw': 12}))

    def test_links_with_no_interval_in_common_give_nothing(self):
        a = link('2026-11-17 00:00', [5.0] * 4, minutes=15)
        b = link('2026-11-17 03:00', [7.0] * 4, minutes=15)
        self.assertIsNone(fe.sum_flows([a, b], NOW))

    def test_a_single_answer_is_read_as_before(self):
        got = link('2026-11-17 00:00', [5.0, 6.0, 7.0])
        self.assertEqual((got['latest'], got['data_time']), ({'time': '02:00', 'mw': 7}, iso('2026-11-17 02:00:00')))
        self.assertEqual(sorted(got), ['data_time', 'intervals', 'latest', 'values'])


if __name__ == '__main__':
    unittest.main()
