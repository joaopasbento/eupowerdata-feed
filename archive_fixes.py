#!/usr/bin/env python3
"""archive_fixes.py - corrections made once to files already in the archive.

Each one puts right, in the files themselves and without asking ENTSO-E for
anything, something the archive of the prices had wrong. They are kept here,
with their tests, so that what was done to the files can be read and done
again: every one of them leaves alone a file it has already put right.

    python archive_fixes.py bgn        Bulgaria's prices in levs, to euros
    python archive_fixes.py moldova    Moldova out of the days before 1 Jul 2026 and of its one-hour days
    python archive_fixes.py avg25      the daily mean of the days of 25 hours

bgn. ENTSO-E gave Bulgaria's day-ahead prices in levs until January 2022 and
the feed took them for euros: from 14 Nov 2021 (the first day of the archive)
to 21 Jan 2022 Bulgaria's daily mean is 1.96 times Romania's, with which it is
coupled, on 67 of the 69 days (on 18 Dec and 6 Jan the two markets were apart,
and the currency was the same). Each price of those days is divided by
1.95583, the fixed rate of the lev, in `prices`, `latest`, `avg`, `min` and
`max`, and the zone gains `converted_from`: "BGN", by which a day already
converted is known and left alone. (The reading of the prices converts by
itself from now on: see fetch_entsoe.py and fx_rates.py.)

moldova. Moldova's exchange opened on 10 Dec 2025 and until June 2026 it
published prices of a market with hardly any trade: on 79 of the 85 days the
zone has in the archive up to 30 Jun 2026 its daily mean is 2.5 to 6 times
Romania's. The zone MD is taken out of the price files up to 30 Jun 2026,
that day included, and out of the later days on which it has one hour only
(its delivery day begins an hour before the market's: see ONE_HOUR_IS_NO_DAY
in fetch_entsoe.py, which keeps such days out of the archive from now on).
"""

import glob
import json
import os
import sys
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

from feed_common import write_json

ARCHIVE = 'data/archive/spot-prices-{day}.json'
LEV = Decimal('1.95583')
LEV_DAYS = (date(2021, 11, 14), date(2022, 1, 21))


def _cents(value):
    return float(Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def price_files(first=None, last=None):
    """(day, path) of the price files of the archive, in order, between two days."""
    found = []
    for path in sorted(glob.glob(ARCHIVE.format(day='*'))):
        try:
            day = date.fromisoformat(os.path.basename(path)[len('spot-prices-'):-len('.json')])
        except ValueError:
            continue
        if (first is None or day >= first) and (last is None or day <= last):
            found.append((day, path))
    return found


def _load(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def to_euros(zone, rate=LEV):
    """A zone of a price file with every price divided by `rate`."""
    originals = [Decimal(str(row['price'])) for row in zone['prices']]
    prices = [dict(row, price=_cents(price / rate)) for row, price in zip(zone['prices'], originals)]
    published = [row['price'] for row in prices]
    return dict(zone, prices=prices, latest=prices[-1], avg=_cents(Decimal(str(zone['avg'])) / rate),
                min=min(published), max=max(published), converted_from='BGN')


def convert_bgn():
    """Returns (files converted, files of the range left alone, why)."""
    converted, left = [], []
    for day, path in price_files(*LEV_DAYS):
        doc = _load(path)
        zones = doc.get('zones') or {}
        bulgaria = zones.get('BG')
        if not bulgaria or not bulgaria.get('prices'):
            left.append((day, 'no Bulgaria in the file'))
            continue
        if bulgaria.get('converted_from'):
            left.append((day, 'already converted'))
            continue
        zones['BG'] = to_euros(bulgaria)
        write_json(path, doc, separators=(',', ':'))
        converted.append(day)
    return converted, left


MOLDOVA_FROM = date(2026, 7, 1)          # the first day Moldova's prices are kept


def remove_moldova():
    """Returns (days Moldova left for being before MOLDOVA_FROM, later days it left for having one hour)."""
    early, thin = [], []
    for day, path in price_files():
        doc = _load(path)
        zones = doc.get('zones') or {}
        moldova = zones.get('MD')
        if moldova is None:
            continue
        if day < MOLDOVA_FROM:
            early.append(day)
        elif len(moldova.get('prices') or []) <= 1:
            thin.append(day)
        else:
            continue
        del zones['MD']
        write_json(path, doc, separators=(',', ':'))
    return early, thin


def main(argv):
    if argv[1:] == ['bgn']:
        converted, left = convert_bgn()
        print(f'Bulgaria, levs to euros: {len(converted)} files converted'
              + (f', {converted[0]} to {converted[-1]}' if converted else ''))
        for day, why in left:
            print(f'  {day}: left alone - {why}')
        return 0
    if argv[1:] == ['moldova']:
        early, thin = remove_moldova()
        print(f'Moldova: taken out of {len(early)} files up to 30 Jun 2026'
              + (f' ({early[0]} to {early[-1]})' if early else '')
              + f' and of {len(thin)} later files in which it had one hour'
              + (f' ({", ".join(str(day) for day in thin)})' if thin else ''))
        return 0
    print(__doc__)
    return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv))
