#!/usr/bin/env python3
"""diagnose_entsoe.py - TEMPORARY: what ENTSO-E's answers are made of.

Run by .github/workflows/diagnose.yml, by hand, while the readers of prices
are being put right (17 Nov 2026). To be removed with the workflow when that
work is done.

It asks ENTSO-E a few questions and writes to the log only the STRUCTURE of
each answer: for each TimeSeries the fields that tell it from the others, the
length of its intervals, the kind of curve, the period it covers, how many
points it has and what they average. It never writes the API key, nor an
address with it: the requests go through fetch_entsoe._get(), whose failure
reasons never include the address, and nothing here prints one.

It keeps well under ENTSO-E's limit of 400 requests a minute even if a run of
the feed is going at the same time: at most 100 requests a minute here, on top
of the feed's 240.

DIAGNOSE=structure (the default) or prices (the feed's own fetch_prices(), as
it is in the branch the workflow runs on, without writing any file).
"""

import os
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import fetch_entsoe as fe

MARKET = ZoneInfo('Europe/Brussels')
BASE = fe.ALT_URLS[0]

# Zones the feed does not ask for, by their EIC codes.
EXTRA = {
    'NO3': '10YNO-3--------J', 'NO4': '10YNO-4--------9', 'NO5': '10Y1001A1001A48H',
    'SE2': '10Y1001A1001A45N', 'SE4': '10Y1001A1001A47J',
    'IT-CSUD': '10Y1001A1001A71M', 'IT-CNOR': '10Y1001A1001A70O', 'IT-SUD': '10Y1001A1001A788',
    'IT-country': '10YIT-GRTN-----B',
}
EIC = {**fe.ZONES, **EXTRA}


def local(tag):
    return tag.split('}', 1)[-1]


def stamp(dt):
    return dt.strftime('%Y%m%d%H%M')


def market_day(now):
    """(start, end) in UTC of the delivery day `now` is in, on the market's clock."""
    day = now.astimezone(MARKET).date()
    start = datetime(day.year, day.month, day.day, tzinfo=MARKET).astimezone(timezone.utc)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=MARKET).astimezone(timezone.utc)
    return start, end


def url(**params):
    return f"{BASE}?{urllib.parse.urlencode({'securityToken': fe.API_KEY, **params})}"


def minutes(resolution):
    return int(fe._step(resolution).total_seconds() // 60)


def series_structure(text, value_tag):
    """One line for each TimeSeries of an answer: its own fields, then each of
    its Periods. The average is over every interval of the Period, with the
    points that curveType A03 leaves out put back."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return [f'    not XML ({len(text)} characters)']
    lines = [f'    document {local(root.tag)}, type {root.findtext("{*}type")}, '
             f'{len(root.findall("{*}TimeSeries"))} TimeSeries']
    reason = root.findtext('{*}Reason/{*}text')
    if reason:
        lines.append(f'    reason: {reason[:120]}')
    for number, ts in enumerate(root.findall('{*}TimeSeries'), 1):
        fields = []
        for child in ts:
            name = local(child.tag)
            if name == 'Period' or name == 'mRID':
                continue
            if len(child):                                   # e.g. MktPSRType/psrType
                fields.append(' '.join(f'{name}/{local(sub.tag)}={sub.text}' for sub in child))
            else:
                fields.append(f'{name}={child.text}')
        lines.append(f'    series {number}: ' + ' '.join(fields))
        for period in ts.findall('{*}Period'):
            start = period.findtext('{*}timeInterval/{*}start')
            end = period.findtext('{*}timeInterval/{*}end')
            resolution = period.findtext('{*}resolution')
            points = []
            for pt in period.findall('{*}Point'):
                try:
                    points.append((int(pt.findtext('{*}position')), float(pt.findtext('{*}' + value_tag))))
                except (TypeError, ValueError):
                    continue
            points.sort()
            detail = f'      {start} -> {end} {resolution}: {len(points)} points'
            if points:
                try:
                    span = (datetime.fromisoformat(end.replace('Z', '+00:00'))
                            - datetime.fromisoformat(start.replace('Z', '+00:00')))
                    count = int(span.total_seconds() // 60 // minutes(resolution))
                except (AttributeError, ValueError, ZeroDivisionError):
                    count = points[-1][0]
                filled, current, at = [], None, dict(points)
                for position in range(1, max(count, points[-1][0]) + 1):
                    current = at.get(position, current)
                    if current is not None:
                        filled.append(current)
                detail += (f' (positions {points[0][0]} to {points[-1][0]} of {count}); '
                           f'all {len(filled)} intervals: mean {sum(filled) / len(filled):.2f}, '
                           f'min {min(filled):.2f}, max {max(filled):.2f}')
            lines.append(detail)
    return lines


def ask(label, jobs, parser):
    """Send the jobs through the feed's own machinery and print each answer's
    structure, in the order the jobs were given."""
    print(f'\n=== {label}: {len(jobs)} requests ===', flush=True)
    found = fe.fetch_all(label, jobs, parser, lambda lines: f'{len(lines)} lines', time.monotonic() + 240)
    for key, name, _url in jobs:
        print(f'  {name}:')
        print('\n'.join(found.get(key, ['    (no answer, or no data)'])))


def prices(now):
    start, end = market_day(now)
    utc_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    windows = {
        'market day': (start, end),
        'UTC day, as asked for today': (utc_start, utc_start + timedelta(days=1)),
        'day before to day after': (start - timedelta(days=1), end + timedelta(days=1)),
    }
    print(f'Market day: {start:%Y-%m-%d %H:%M}Z to {end:%Y-%m-%d %H:%M}Z')

    def job(zone, window, **more):
        begin, finish = windows[window]
        name = f'{zone} [{window}' + ''.join(f', {k}={v}' for k, v in more.items()) + ']'
        return ((zone, window, tuple(more.items())), name,
                url(documentType='A44', in_Domain=EIC[zone], out_Domain=EIC[zone],
                    periodStart=stamp(begin), periodEnd=stamp(finish), **more))

    parser = lambda text: series_structure(text, 'price.amount')    # noqa: E731
    ask('Prices, market day, every zone', [job(zone, 'market day') for zone in fe.ZONES], parser)
    some = ['ES', 'DE', 'AT', 'PT', 'RS', 'MD', 'NO1', 'IE']
    ask('Prices, UTC day', [job(zone, 'UTC day, as asked for today') for zone in some], parser)
    ask('Prices, market day, day-ahead contracts only',
        [job(zone, 'market day', **{'contract_MarketAgreement.type': 'A01'}) for zone in ['ES', 'DE', 'AT', 'PT', 'MD']],
        parser)
    ask('Prices, three days', [job(zone, 'day before to day after') for zone in ['PT', 'MD', 'ES']], parser)


def flows(now):
    start, end = fe.last_day(now)
    pairs = [('NO', 'SE'), ('NO1', 'SE3'), ('NO3', 'SE2'), ('NO4', 'SE1'), ('NO4', 'SE2'),
             ('SE', 'LT'), ('SE4', 'LT'),
             ('IT', 'ME'), ('IT-CSUD', 'ME'), ('IT-CNOR', 'ME'), ('IT-SUD', 'ME'), ('IT-country', 'ME'),
             ('AL', 'MK')]
    codes = {**EIC, **{k: fe.FLOW_ZONES[k] for k in ('NO', 'SE')}}

    def parser(text):
        got = fe.latest_flow(text, now)
        lines = series_structure(text, 'quantity')
        if got:
            lines.insert(0, f"    read as: {got['latest']['mw']} MW, data of {got[fe.DATA_TIME]}, "
                            f"{len(got['values'])} points today")
        return lines

    jobs = []
    for a, b in pairs:
        for frm, to in ((a, b), (b, a)):
            jobs.append(((frm, to), f'{frm} -> {to}',
                         url(documentType='A11', in_Domain=codes[to], out_Domain=codes[frm],
                             periodStart=start, periodEnd=end)))
    ask('Flows, last 24 hours, the four borders with no data', jobs, parser)


def generation(now):
    start, end = fe.last_day(now)
    # The moments the published file of 07:53 UTC on 17 Nov was read for.
    moments = {
        'PT': ['2026-11-17T06:00+00:00'], 'NO1': ['2026-11-17T06:45+00:00'], 'NO2': ['2026-11-17T06:45+00:00'],
        'IT': ['2026-11-17T07:00+00:00'], 'ME': [],
    }

    def parser_for(zone):
        def parser(text):
            lines = []
            grid = set()
            everything = fe.read_series(text)
            for series in everything:
                name = fe.PSR_MAP.get(series['psr'], 'Other') + (' CONSUMPTION' if series['out'] else '')
                for period in series['periods']:
                    step = period['step']
                    for position, _value in period['points']:
                        grid.add(period['start'] + step * (position - 1))
                    values = []
                    for moment in moments[zone] + [now.isoformat()]:
                        at = datetime.fromisoformat(moment)
                        got = fe._in_force(period, at)
                        values.append(f"at {at:%d %H:%M}: " + (f'{got[1]:.1f} MW (interval {got[0]:%d %H:%M})'
                                                             if got else 'nothing'))
                    lines.append(f"    {series['psr']} {name}: {period['start']:%d %H:%M} -> "
                                 f"{period['end']:%d %H:%M} step {int(step.total_seconds() // 60)} min, "
                                 f"{'A03' if period['variable'] else 'A01'}, {len(period['points'])} points; "
                                 + '; '.join(values))
            # The total of the generation series (not consumption) at each of the last intervals.
            for at in sorted(grid)[-8:]:
                total = 0.0
                for series in everything:
                    if series['out']:
                        continue
                    got = [v for v in (fe._in_force(p, at) for p in series['periods']) if v]
                    if got:
                        total += max(got, key=lambda v: v[0])[1]
                lines.append(f'    generation in force at {at:%d %H:%M}: {total:.1f} MW')
            return lines
        return parser

    print(f'\n=== Generation, last 24 hours ===', flush=True)
    for zone in moments:
        job = ((zone,), zone, url(documentType='A75', processType='A16', in_Domain=EIC[zone],
                                  periodStart=start, periodEnd=end))
        found = fe.fetch_all('Generation', [job], parser_for(zone), lambda lines: f'{len(lines)} lines',
                             time.monotonic() + 120)
        print(f'  {zone}:')
        print('\n'.join(found.get((zone,), ['    (no answer, or no data)'])))


def new_prices(now):
    """The feed's own reading of the prices, as it is in this branch."""
    result = fe.fetch_prices(BASE, time.monotonic() + 240)
    print(f"\n=== fetch_prices(): {len(result['zones'])} zones, updated {result['updated']} ===")
    for code, zone in result['zones'].items():
        hours = zone.get('prices') or []
        labels = [p.get('time') for p in hours]
        print(f"  {code:3s} {len(hours):3d} values, {labels[0] if labels else '-'} to {labels[-1] if labels else '-'}, "
              f"hours {sorted({p.get('hour') for p in hours})[:1]}..{sorted({p.get('hour') for p in hours})[-1:]}, "
              f"avg {zone.get('avg')}, min {zone.get('min')}, max {zone.get('max')}, "
              f"day {zone.get('delivery_date')}, latest {zone.get('latest')}")


def main():
    if not fe.API_KEY:
        print('ENTSOE_API_KEY is not set')
        return 1
    fe._throttle = fe._Throttle(0.6)          # 100 requests a minute at most
    now = datetime.now(timezone.utc)
    print(f'Diagnosis at {now:%Y-%m-%d %H:%M:%S} UTC, mode {os.environ.get("DIAGNOSE") or "structure"}')
    try:
        if (os.environ.get('DIAGNOSE') or 'structure') == 'prices':
            new_prices(now)
        else:
            prices(now)
            flows(now)
            generation(now)
    except Exception as e:  # noqa: BLE001 - say what failed, never an address
        print(f'Diagnosis stopped: {type(e).__name__}')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
