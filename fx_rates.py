"""fx_rates.py - exchange rates, to put in euros what ENTSO-E does not give in euros.

ENTSO-E publishes each price in the currency its operator sends it in. The
day-ahead prices of Bulgaria came in levs until January 2022 and the feed took
them for euros (389 "EUR/MWh" on 1 Dec 2021, with Romania at 199); the prices
of balancing energy come in zloty for Poland, lei for Romania and convertible
marks for Bosnia. This module says how many units of a currency one euro is:

    fixed    the Bulgarian lev (BGN) and the Bosnian convertible mark (BAM)
             are pegged to the euro at 1.95583, the rate of the German mark:
             always that, whatever the day.
    ECB      every other currency, by the euro foreign exchange reference
             rates of the European Central Bank: the rate of the delivery day
             or, while that is not published (the ECB publishes at about
             16:00 CET on working days), the last one published before it.

The reference rates are kept in data/fx-rates.json, so that a run the ECB does
not answer for uses the last ones known:

    {updated, source, fixed: {BGN, BAM}, rates: {PLN: {"2026-11-18": 4.4118}}}

`updated` is when the ECB last gave a new day. The file has the days of the
last 400 or so, for the currencies of Europe. update() brings it up to date:
it asks the ECB only when a new day's rates may be out (once they are in the
file, not again until the next working day at 15:00 UTC), with the small file
of the day, the one of the last 90 days after a gap, or the whole history the
first time. It is called by the steps that run after the files the site
already reads are published (fetch_balancing.py), never by fetch_entsoe.py,
which only reads the file: the reading of the prices makes no request of its
own for this.

With no rate at all for a currency (a new one, or a file that was lost and an
ECB that does not answer), there is no price in euros: the caller leaves the
zone out for that day and says why.
"""

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from feed_common import http_get, iso, load_json, write_json

PATH = 'data/fx-rates.json'
SOURCE = 'European Central Bank, euro foreign exchange reference rates'
DAILY_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml'
RECENT_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml'
HISTORY_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml'

FIXED = {'BGN': Decimal('1.95583'), 'BAM': Decimal('1.95583')}
CURRENCIES = ('PLN', 'RON', 'CZK', 'HUF', 'SEK', 'NOK', 'DKK', 'CHF', 'GBP', 'ISK', 'TRY')
KEEP_DAYS = 400
PUBLICATION_HOUR_UTC = 15


def parse(xml):
    """{day as text: {currency: rate as text}} of one of the ECB's files."""
    text = xml.decode('utf-8', errors='replace') if isinstance(xml, bytes) else xml
    days = {}
    for day, body in re.findall(r"<Cube time=['\"](\d{4}-\d{2}-\d{2})['\"]>(.*?)</Cube>", text, re.S):
        rates = dict(re.findall(r"<Cube currency=['\"]([A-Z]{3})['\"] rate=['\"]([0-9.]+)['\"]", body))
        if rates:
            days[day] = rates
    return days


class Rates:
    """The rates of a table {currency: {day as text: rate}}."""

    def __init__(self, table=None):
        self.table = {currency: dict(days) for currency, days in (table or {}).items() if isinstance(days, dict)}

    def rate(self, currency, day):
        """(units of `currency` one euro is, the day that rate is of) for a
        delivery day: the day's own rate or the last one published before it.
        The day is None for the euro itself and for a fixed rate. None when
        there is no rate for the currency up to that day."""
        if currency in (None, '', 'EUR'):
            return Decimal(1), None
        if currency in FIXED:
            return FIXED[currency], None
        days = self.table.get(currency) or {}
        wanted = day.isoformat() if isinstance(day, date) else str(day)
        known = [d for d in days if d <= wanted]
        if not known:
            return None
        last = max(known)
        return Decimal(str(days[last])), last

    def latest_day(self):
        days = [d for rates in self.table.values() for d in rates]
        return max(days) if days else None


def load(path=PATH):
    """The rates the repository has; empty when the file is missing or cannot be read."""
    doc = load_json(path)
    return Rates((doc or {}).get('rates') if isinstance(doc, dict) else None)


def _due(latest, now):
    """Which of the ECB's files to ask for, or None when the file is up to date."""
    today = now.astimezone(timezone.utc).date()
    if latest is None:
        return HISTORY_URL
    last = date.fromisoformat(latest)
    if last >= today:
        return None
    working_day = today.weekday() < 5
    out_today = working_day and now.astimezone(timezone.utc).hour >= PUBLICATION_HOUR_UTC
    # The last day the ECB should have published by now.
    expected = today if out_today else today - timedelta(days=1)
    while expected.weekday() >= 5:
        expected -= timedelta(days=1)
    if last >= expected:
        return None
    # One working day behind: the file of the day has it. More: the file of the last 90 days.
    return DAILY_URL if last >= _working_day_before(expected) else RECENT_URL


def _working_day_before(day):
    day -= timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def update(now=None, get=http_get, path=PATH):
    """Bring the file up to date with the ECB when a new day's rates may be
    out. Returns True when it wrote new rates. Never raises: with no answer
    the file stays as it is, and what it has is what is used."""
    now = now or datetime.now(timezone.utc)
    doc = load_json(path)
    rates = Rates((doc or {}).get('rates') if isinstance(doc, dict) else None)
    url = _due(rates.latest_day(), now)
    if url is None:
        return False
    response = get(url, timeout=20, max_seconds=60)
    if not response.ok:
        print(f'  Exchange rates: the ECB did not answer ({response.describe()}) - using the last ones known '
              f'({rates.latest_day() or "none"})')
        return False
    found = parse(response.body)
    before = rates.latest_day()
    cutoff = (now.astimezone(timezone.utc).date() - timedelta(days=KEEP_DAYS)).isoformat()
    table = {currency: {d: r for d, r in rates.table.get(currency, {}).items() if d >= cutoff} for currency in CURRENCIES}
    for day, values in found.items():
        if day < cutoff:
            continue
        for currency in CURRENCIES:
            if currency in values:
                table[currency][day] = float(values[currency])
    table = {currency: dict(sorted(days.items())) for currency, days in table.items() if days}
    after = Rates(table).latest_day()
    if after is None or (before is not None and after <= before):
        return False                                   # nothing newer than what the file has
    write_json(path, {
        'updated': iso(now),
        'source': SOURCE,
        'fixed': {currency: float(rate) for currency, rate in FIXED.items()},
        'rates': table,
    }, separators=(',', ':'))
    print(f'  Exchange rates: the ECB has those of {after} ({path})')
    return True
