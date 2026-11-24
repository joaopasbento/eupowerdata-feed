"""Tests for the PPA price index (fetch_ppa_index.py), on real pages of OMIP's
market data (tests/fixtures/ppa, taken on 24 Nov 2026): the Spanish futures
(FTB) on 1 and 2 Jan 2026 (the first a holiday, with no reference price) and on
1 Oct 2026, and the Portuguese futures (FPB) on 1 Oct 2026, which have no PPA.
No network. The expected prices are those the site had written by hand.

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
from datetime import date, datetime, timezone

import feed_common as fc
import fetch_ppa_index as ppa

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'ppa')


class OMIP:
    """The saved pages; any other day is a page with no price (as a weekend's)."""

    def __init__(self):
        self.asked = []

    def __call__(self, url, timeout=60, **_kwargs):
        self.asked.append(url)
        found = re.search(r'date=([\d-]+)&product=EL&zone=(\w+)&instrument=(\w+)', url)
        path = os.path.join(FIX, f'omip_{found.group(1)}_{found.group(2)}_{found.group(3)}.html.gz')
        if not os.path.isfile(path):
            return fc.Response(200, {}, b'<html>no data</html>')
        with open(path, 'rb') as f:
            return fc.Response(200, {}, gzip.decompress(f.read()))


def page(name):
    with open(os.path.join(FIX, name), 'rb') as f:
        return gzip.decompress(f.read()).decode('utf-8')


class TheContractAndItsPrice(unittest.TestCase):

    def test_the_contract_is_the_five_years_from_the_next(self):
        self.assertEqual(ppa.contract(date(2026, 10, 1)), 'FTB PPA 27/31')
        self.assertEqual(ppa.contract(date(2023, 1, 2)), 'FTB PPA 24/28')
        self.assertEqual(ppa.contract(date(2096, 1, 2), 'FPB'), 'FPB PPA 97/01')

    def test_the_reference_price_of_the_day_is_the_first_of_the_two(self):
        self.assertEqual(ppa.reference_price(page('omip_2026-10-01_ES_FTB.html.gz'), 'FTB PPA 27/31'), 64.32)
        self.assertEqual(ppa.reference_price(page('omip_2026-01-02_ES_FTB.html.gz'), 'FTB PPA 27/31'), 53.86)

    def test_a_holiday_has_no_price_and_portugal_has_no_ppa(self):
        self.assertIsNone(ppa.reference_price(page('omip_2026-01-01_ES_FTB.html.gz'), 'FTB PPA 27/31'))
        self.assertIsNone(ppa.reference_price(page('omip_2026-10-01_PT_FPB.html.gz'), 'FPB PPA 27/31'))


class TheFile(unittest.TestCase):

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def run_at(self, now, omip):
        with contextlib.redirect_stdout(io.StringIO()):
            return ppa.main(now=now, get=omip)

    def test_the_first_trading_day_of_the_quarter_and_portugal_without_a_point(self):
        kept = [{'date': d.strftime('%Y-%m'), 'trading_day': d.isoformat(), 'contract': 'x', 'es': 1.0, 'pt': None}
                for d in ppa.quarters(date(2025, 10, 1))]
        with open(ppa.OUT_PATH, 'w', encoding='utf-8') as f:
            json.dump({'points': kept}, f)
        omip = OMIP()
        self.run_at(datetime(2026, 1, 5, 12, tzinfo=timezone.utc), omip)
        with open(ppa.OUT_PATH, encoding='utf-8') as f:
            point = json.load(f)['points'][-1]
        self.assertEqual(point, {'date': '2026-01', 'trading_day': '2026-01-02', 'contract': 'FTB PPA 27/31',
                                 'es': 53.86, 'pt': None})
        self.assertEqual(len([u for u in omip.asked if 'zone=ES' in u]), 2)   # 1 Jan (holiday), then 2 Jan

    def test_a_quarter_with_its_point_is_not_asked_again(self):
        kept = [{'date': d.strftime('%Y-%m'), 'trading_day': d.isoformat(), 'contract': 'x', 'es': 1.0, 'pt': None}
                for d in ppa.quarters(date(2026, 7, 1))]
        with open(ppa.OUT_PATH, 'w', encoding='utf-8') as f:
            json.dump({'points': kept}, f)
        self.run_at(datetime(2026, 10, 2, 12, tzinfo=timezone.utc), OMIP())
        again = OMIP()
        self.run_at(datetime(2026, 11, 24, 12, tzinfo=timezone.utc), again)
        self.assertEqual(again.asked, [])
        with open(ppa.OUT_PATH, encoding='utf-8') as f:
            points = {p['date']: p['es'] for p in json.load(f)['points']}
        self.assertEqual(points['2026-10'], 64.32)

    def test_the_quarters_since_january_2023(self):
        self.assertEqual([d.isoformat() for d in ppa.quarters(date(2023, 11, 24))],
                         ['2023-01-01', '2023-04-01', '2023-07-01', '2023-10-01'])


if __name__ == '__main__':
    unittest.main()
