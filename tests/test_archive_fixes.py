"""Tests for the corrections made once to the archive (archive_fixes.py).

Run from the repository root:  python -m unittest discover -s tests -v

Each runs on files written for the test, in a temporary directory.
"""

import json
import os
import tempfile
import unittest

import archive_fixes


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


if __name__ == '__main__':
    unittest.main()
