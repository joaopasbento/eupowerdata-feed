"""Tests for the corrections made once to the archive (archive_fixes.py).

Run from the repository root:  python -m unittest discover -s tests -v

Each runs on files written for the test, in a temporary directory.
"""

import json
import os
import tempfile
import unittest

import archive_fixes
import fetch_entsoe as fe
from entsoe_xml import at, price_document, price_series
from test_carry_over import Feed as EntsoeFeed


def zone(prices, **more):
    rows = [{'time': f'{hour:02d}:00', 'hour': hour, 'price': price} for hour, price in enumerate(prices)]
    return {'eic': 'x', 'prices': rows, 'latest': rows[-1], 'avg': round(sum(prices) / len(prices), 2),
            'min': min(prices), 'max': max(prices), **more}


class InADirectory(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data/archive')

    def write(self, day, zones):
        with open(archive_fixes.ARCHIVE.format(day=day), 'w') as f:
            json.dump({'updated': 'u', 'zones': zones}, f)

    def read(self, day):
        with open(archive_fixes.ARCHIVE.format(day=day)) as f:
            return json.load(f)['zones']


class BulgariaInLevs(InADirectory):

    def test_every_price_of_the_range_is_divided_by_the_rate_of_the_lev(self):
        self.write('2021-12-01', {'BG': zone([195.583, 391.166, 97.79]), 'RO': zone([100.0, 200.0, 50.0])})
        converted, _left = archive_fixes.convert_bgn()
        bulgaria = self.read('2021-12-01')['BG']
        self.assertEqual([str(day) for day in converted], ['2021-12-01'])
        self.assertEqual([row['price'] for row in bulgaria['prices']], [100.0, 200.0, 50.0])
        self.assertEqual((bulgaria['latest'], bulgaria['avg'], bulgaria['min'], bulgaria['max']),
                         ({'time': '02:00', 'hour': 2, 'price': 50.0}, 116.67, 50.0, 200.0))
        self.assertEqual(bulgaria['converted_from'], 'BGN')
        self.assertEqual(self.read('2021-12-01')['RO'], zone([100.0, 200.0, 50.0]))

    def test_a_day_already_converted_is_left_alone(self):
        self.write('2021-12-01', {'BG': zone([195.583, 391.166])})
        archive_fixes.convert_bgn()
        once = self.read('2021-12-01')
        converted, left = archive_fixes.convert_bgn()
        self.assertEqual((converted, [why for _day, why in left]), ([], ['already converted']))
        self.assertEqual(self.read('2021-12-01'), once)

    def test_the_days_after_21_jan_2022_are_already_in_euros(self):
        self.write('2022-01-22', {'BG': zone([195.583, 391.166])})
        self.assertEqual(archive_fixes.convert_bgn(), ([], []))
        self.assertEqual(self.read('2022-01-22')['BG'], zone([195.583, 391.166]))


class BulgariaOf6Jan2022(InADirectory):
    """ENTSO-E's price for Bulgaria that day is Romania's divided by 1.95583."""

    def test_only_bulgaria_of_that_day_leaves(self):
        others = {'RO': zone([199.9] * 24), 'HU': zone([199.9] * 24)}
        self.write('2022-01-06', {'RO': others['RO'], 'BG': zone([52.25] * 24, converted_from='BGN'),
                                  'HU': others['HU']})
        self.write('2022-01-05', {'BG': zone([90.0] * 24), 'RO': zone([90.0] * 24)})
        self.write('2022-01-07', {'BG': zone([120.0] * 24)})
        removed = archive_fixes.remove_wrong_at_the_source()
        self.assertEqual([(str(day), name) for day, name in removed], [('2022-01-06', 'BG')])
        self.assertEqual(self.read('2022-01-06'), others)
        self.assertEqual(list(self.read('2022-01-06')), ['RO', 'HU'])        # the order of the others kept
        self.assertEqual(self.read('2022-01-05')['BG'], zone([90.0] * 24))
        self.assertEqual(self.read('2022-01-07')['BG'], zone([120.0] * 24))
        self.assertEqual(archive_fixes.remove_wrong_at_the_source(), [])     # done once, nothing left to do
        self.assertEqual(self.read('2022-01-06'), others)

    def test_the_rest_of_the_file_is_kept(self):
        self.write('2022-01-06', {'BG': zone([52.25]), 'RO': zone([199.9])})
        path = archive_fixes.ARCHIVE.format(day='2022-01-06')
        with open(path) as f:
            before = json.load(f)
        archive_fixes.remove_wrong_at_the_source()
        with open(path) as f:
            after = json.load(f)
        self.assertEqual({k: v for k, v in after.items() if k != 'zones'}, {k: v for k, v in before.items() if k != 'zones'})


class MoldovaOutOfTheArchive(InADirectory):

    def test_leaves_every_day_up_to_30_jun_2026_and_the_later_days_of_one_hour(self):
        whole, one_hour = zone([200.0] * 24), zone([757.11])
        self.write('2025-12-11', {'MD': whole, 'RO': zone([100.0] * 24)})
        self.write('2026-06-30', {'MD': whole})
        self.write('2026-07-01', {'MD': whole, 'RO': zone([100.0] * 24)})
        self.write('2026-08-26', {'MD': one_hour, 'RO': zone([100.0] * 24)})
        early, thin = archive_fixes.remove_moldova()
        self.assertEqual(([str(day) for day in early], [str(day) for day in thin]),
                         (['2025-12-11', '2026-06-30'], ['2026-08-26']))
        self.assertEqual(list(self.read('2025-12-11')), ['RO'])
        self.assertEqual(self.read('2026-06-30'), {})
        self.assertEqual(self.read('2026-07-01')['MD'], whole)
        self.assertEqual(list(self.read('2026-08-26')), ['RO'])
        self.assertEqual(archive_fixes.remove_moldova(), ([], []))          # done once, nothing left to do


class TheMeanOfTheDaysOf25Hours(InADirectory):

    def test_counts_the_entry_of_two_o_clock_twice(self):
        # 24 entries for 25 hours: 10, 20, 40 (the mean of 30 and 50) and 21 at 100.
        prices = [10.0, 20.0, 40.0] + [100.0] * 21
        self.write('2026-10-25', {'PT': zone(prices, delivery_date='2026-10-25'),
                                  'XX': zone(prices, delivery_date='2026-10-24')})
        changed = archive_fixes.fix_long_days()
        self.assertEqual({str(day): rows for day, rows in changed.items()}, {'2026-10-25': [('PT', 90.42, 88.4)]})
        portugal = self.read('2026-10-25')['PT']
        self.assertEqual((portugal['avg'], portugal['min'], portugal['max'], len(portugal['prices'])), (88.4, 10.0, 100.0, 24))
        self.assertEqual(self.read('2026-10-25')['XX']['avg'], 90.42)       # not of that delivery day: not touched
        self.assertEqual(archive_fixes.fix_long_days(), {})                 # done once, nothing left to do

    def test_a_day_of_24_hours_is_not_one_of_them(self):
        self.write('2026-10-24', {'PT': zone([10.0, 20.0, 40.0], delivery_date='2026-10-24')})
        self.assertEqual(archive_fixes.fix_long_days(), {})


class TheArchiveFromNowOn(EntsoeFeed):
    """fetch_entsoe.py itself: a day Moldova has one hour of is not written to the archive for it."""

    def moldova(self, hours):
        # Moldova's day begins at 22:00 UTC: of the market's day of 16 Nov it then has the last hours.
        return price_document(price_series('2026-11-16 22:00:00', [180.0] * hours))

    def run_with(self, hours):
        self.entsoe(at('2026-11-16 23:30:05'), prices={'MD': self.moldova(hours), 'RO': 100})
        with open('data/spot-prices.json', encoding='utf-8') as f:
            live = json.load(f)['zones']
        with open('data/archive/spot-prices-2026-11-17.json', encoding='utf-8') as f:
            return live, json.load(f)['zones']

    def test_with_one_hour_it_is_in_the_file_of_the_day_and_not_in_the_archive(self):
        live, archived = self.run_with(2)             # 22:00 is of the day before: one hour of the 17th
        self.assertEqual(len(live['MD']['prices']), 1)
        self.assertEqual(list(archived), ['RO'])

    def test_with_more_it_is_in_both(self):
        live, archived = self.run_with(5)
        self.assertEqual((len(live['MD']['prices']), len(archived['MD']['prices'])), (4, 4))

    def test_the_rule_is_moldovas_alone(self):
        zones = {'MD': {'prices': [1]}, 'RO': {'prices': [1]}}
        self.assertEqual(list(fe.for_the_archive(zones)), ['RO'])


if __name__ == '__main__':
    unittest.main()
