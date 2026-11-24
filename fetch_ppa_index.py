#!/usr/bin/env python3
"""fetch_ppa_index.py - data/ppa-index.json: the PPA price index of the site's Insights, from OMIP.

The site wrote by hand, each quarter, the price of OMIP's 5-year baseload PPA
future of Spain: the reference (settlement) price of the first trading day of
the quarter, for the delivery that starts the year after (traded in 2026: FTB
PPA 27/31), since January 2023. The feed now keeps it, a new point each quarter
by itself, from the same pages of OMIP the feed reads its forward curves from
(omip.pt, market data: /en/dados-mercado, the futures of the Spanish zone, FTB,
maturity PPA). Portugal's futures at OMIP (FPB) have no PPA contract: it has no
point (checked on 24 Nov 2026, each quarter it is looked for again).

    {updated, source, url, license, unit, product,
     points: [{date: "2026-10", trading_day: "2026-10-01", contract: "FTB PPA 27/31", es: 64.32, pt: null}]}

`date` is the quarter's first month, as the site had it; `trading_day` the
first day of the quarter OMIP has a reference price for the contract (a day of
the first ten of the quarter: weekends and holidays have none). A point already
in the file is not asked for again; a quarter without one is looked for in each
run until it is found. The step runs once a day and makes no request when the
quarter has its point.
"""

import html
import re
import sys
from datetime import date, datetime, timedelta, timezone

from feed_common import SOURCE_TERMS, http_get, iso, load_json, write_json_if_changed

OUT_PATH = 'data/ppa-index.json'
URL = 'https://www.omip.pt/en/dados-mercado?date={day}&product=EL&zone={zone}&instrument={instrument}'
FIRST_QUARTER = date(2023, 1, 1)
DAYS_TO_TRY = 10                       # the first days of a quarter a first trading day is looked for in
MARKETS = (('es', 'ES', 'FTB'), ('pt', 'PT', 'FPB'))
ROW = re.compile(r'</span>\s*(F[A-Z]B PPA \d\d/\d\d)\s*</strong>\s*</td>(.*?)</tr>', re.S)
CELL = re.compile(r'<td[^>]*>(.*?)</td>', re.S)


def contract(day, instrument='FTB'):
    """The 5-year PPA traded on `day`: delivery from the next year, for five years."""
    first = (day.year + 1) % 100
    return f'{instrument} PPA {first:02d}/{(first + 4) % 100:02d}'


def reference_price(page, name):
    """The reference price of the day of a contract in a page of OMIP's market data, or None (n.a., or no row).
    The row ends with the two reference prices, of the day and of the day before: the first is the one."""
    for found, cells in ROW.findall(page):
        if found != name:
            continue
        values = [html.unescape(re.sub(r'<[^>]+>', '', cell)).strip() for cell in CELL.findall(cells)]
        values = [v for v in values if v]
        numbers = [v for v in values if re.fullmatch(r'-?\d+(?:\.\d+)?|n\.a\.', v)]
        if len(numbers) >= 2:
            try:
                return float(numbers[-2])
            except ValueError:
                return None
    return None


def quarters(today):
    """The first days of the quarters from FIRST_QUARTER up to the one of `today`."""
    out, day = [], FIRST_QUARTER
    while day <= today:
        out.append(day)
        day = date(day.year + (day.month + 3 > 12), (day.month + 2) % 12 + 1, 1)
    return out


def find_point(first, today, get=http_get):
    """The point of the quarter that starts on `first`, or None when no day of it has a price yet."""
    for offset in range(DAYS_TO_TRY):
        day = first + timedelta(days=offset)
        if day > today:
            return None
        if day.weekday() >= 5:
            continue
        response = get(URL.format(day=day.isoformat(), zone='ES', instrument='FTB'), timeout=60)
        if not response.ok:
            print(f'  {day}: {response.describe()}')
            continue
        price = reference_price(response.text(), contract(day))
        if price is None:
            print(f'  {day}: no reference price of {contract(day)} (not a trading day?)')
            continue
        point = {'date': first.strftime('%Y-%m'), 'trading_day': day.isoformat(), 'contract': contract(day),
                 'es': price, 'pt': None}
        portugal = get(URL.format(day=day.isoformat(), zone='PT', instrument='FPB'), timeout=60)
        if portugal.ok:
            point['pt'] = reference_price(portugal.text(), contract(day, 'FPB'))
        return point
    return None


def main(now=None, get=http_get):
    now = now or datetime.now(timezone.utc)
    today = now.date()
    kept = {p['date']: p for p in ((load_json(OUT_PATH) or {}).get('points') or []) if isinstance(p, dict)}
    points = dict(kept)
    for first in quarters(today):
        name = first.strftime('%Y-%m')
        if name in points:
            continue
        point = find_point(first, today, get)
        if point:
            points[name] = point
            print(f"  {name}: {point['contract']} {point['es']} on {point['trading_day']}"
                  + (f", Portugal {point['pt']}" if point['pt'] is not None else ', no price for Portugal'))
    doc = {'source': 'OMIP', 'url': 'https://www.omip.pt/en/dados-mercado', 'license': SOURCE_TERMS,
           'unit': 'EUR/MWh',
           'product': '5-year baseload PPA futures: the reference price of the first trading day of each quarter, '
                      'for the delivery that starts the year after (Spain: FTB PPA; Portugal has none at OMIP)',
           'points': [points[k] for k in sorted(points)]}
    before = load_json(OUT_PATH) or {}
    if {k: v for k, v in before.items() if k != 'updated'} != doc:
        write_json_if_changed(OUT_PATH, {'updated': iso(now), **doc}, indent=1)
        print(f"Wrote {OUT_PATH}: {len(doc['points'])} quarters")
    else:
        print(f'{OUT_PATH}: nothing new')
    return 0 if doc['points'] else 1


if __name__ == '__main__':
    sys.exit(main())
