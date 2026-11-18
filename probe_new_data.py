#!/usr/bin/env python3
"""probe_new_data.py - TEMPORARY: what ENTSO-E answers for four new datasets.

Run by .github/workflows/diagnose.yml, by hand, before the feed starts to
publish balancing prices, outages and the wind and solar forecast (18 Nov
2026). To be removed with the workflow when that work is done.

It asks ENTSO-E a few questions and writes to the log only the STRUCTURE of
each answer (the fields of each series, the length of its intervals, the
period it covers, how many points it has, how long the request took and how
big the answer was). It never writes the API key, nor an address with it:
nothing here prints an address, and every line goes through clean(), which
blanks the key should an answer ever echo it. It writes no file and commits
nothing.

At most 120 requests a minute, on top of the feed's 240 when a run of
fetch.yml is going at the same time: under ENTSO-E's limit of 400.

PROBE chooses the parts, separated by commas: balancing, picasso, forecast,
outages, generation. Default: all of them.
"""

import collections
import io
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import carry_over
import fetch_entsoe as fe
from feed_common import USER_AGENT

KEY = fe.API_KEY
BASE = fe.ALT_URLS[0]
PARTS = [p.strip() for p in (os.environ.get('PROBE') or 'all').split(',') if p.strip()]
STAMP = '%Y%m%d%H%M'

# Areas the feed does not ask prices for, by their EIC codes: control areas
# and countries, which is what balancing data are published by.
EXTRA = {
    'DE-50Hertz': '10YDE-VE-------2', 'DE-Amprion': '10YDE-RWENET---I', 'DE-TenneT': '10YDE-EON------1',
    'DE-TransnetBW': '10YDE-ENBW-----N', 'DE-LFC': '10YCB-GERMANY--8', 'DE-country': '10Y1001A1001A83F',
    'IT-country': '10YIT-GRTN-----B', 'NO-country': '10YNO-0--------C', 'SE-country': '10YSE-1--------K',
    'DK-country': '10Y1001A1001A65H', 'SK': '10YSK-SEPS-----K', 'LU': '10YLU-CEGEDEL-NQ',
}
EIC = {**{code: eic for code, eic in fe.ZONES.items() if code != 'GB'}, **EXTRA}


def clean(text):
    return text.replace(KEY, '***') if KEY else text


def local(tag):
    return tag.rsplit('}', 1)[-1]


# --- Asking ---

_slot_lock = threading.Lock()
_next_slot = [0.0]
TIMINGS = collections.defaultdict(list)      # kind -> [(seconds, bytes, status)]


def get(kind, gzip_ok=False, **params):
    """One request: (status or None, bytes, seconds, headers, failure)."""
    url = BASE + '?' + urllib.parse.urlencode({'securityToken': KEY, **params})
    with _slot_lock:
        slot = max(time.monotonic(), _next_slot[0])
        _next_slot[0] = slot + 0.5
    time.sleep(max(0.0, slot - time.monotonic()))
    headers = {'User-Agent': USER_AGENT, 'Accept': '*/*'}
    if gzip_ok:
        headers['Accept-Encoding'] = 'gzip'
    request = urllib.request.Request(url, headers=headers)
    started = time.monotonic()
    status, body, got, failure = None, b'', {}, None
    try:
        with urllib.request.urlopen(request, timeout=120) as answer:
            body, status = answer.read(), answer.status
            got = {k.lower(): v for k, v in answer.headers.items()}
    except urllib.error.HTTPError as e:
        status = e.code
        got = {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}
        try:
            body = e.read()
        except Exception:  # noqa: BLE001
            body = b''
    except Exception as e:  # noqa: BLE001 - the name of the failure only: never the address
        failure = type(e).__name__
    seconds = time.monotonic() - started
    TIMINGS[kind].append((seconds, len(body), status))
    return status, body, seconds, got, failure


# --- Describing ---

def fields_of(element, skip=()):
    """The fields of an element on one line, children in braces."""
    parts = []
    for child in element:
        name = local(child.tag)
        if name in skip:
            continue
        if len(child):
            parts.append(f'{name}{{{fields_of(child)}}}')
        else:
            parts.append(f'{name}={(child.text or "").strip()[:60]}')
    return ' '.join(parts)


def leaves(element, prefix=''):
    for child in element:
        name = prefix + local(child.tag)
        if len(child):
            yield from leaves(child, name + '/')
        else:
            yield name, (child.text or '').strip()


def describe_series(root, max_groups=10):
    """The TimeSeries of a document, grouped by what tells them apart."""
    lines = []
    series = root.findall('{*}TimeSeries')
    groups = collections.OrderedDict()
    for ts in series:
        groups.setdefault(fields_of(ts, skip=('Period', 'mRID')), []).append(ts)
    lines.append(f'{len(series)} TimeSeries in {len(groups)} kinds')
    for number, (signature, group) in enumerate(groups.items()):
        if number >= max_groups:
            lines.append(f'  ... and {len(groups) - max_groups} kinds more')
            break
        lines.append(f'  [{len(group)} series] {signature[:900]}')
        periods = [p for ts in group for p in ts.findall('{*}Period')]
        if not periods:
            continue
        resolutions = collections.Counter(p.findtext('{*}resolution') for p in periods)
        starts = sorted(p.findtext('{*}timeInterval/{*}start') or '' for p in periods)
        ends = sorted(p.findtext('{*}timeInterval/{*}end') or '' for p in periods)
        points = sum(len(p.findall('{*}Point')) for p in periods)
        lines.append(f'    {len(periods)} periods, resolutions {dict(resolutions)}, from {starts[0]} to {ends[-1]}, '
                     f'{points} points')
        for which, period in (('first', min(periods, key=lambda p: p.findtext('{*}timeInterval/{*}start') or '')),
                              ('last', max(periods, key=lambda p: p.findtext('{*}timeInterval/{*}end') or ''))):
            pts = period.findall('{*}Point')
            if not pts:
                continue
            shown = ' '.join('{' + fields_of(pt) + '}' for pt in pts[:3])
            lines.append(f'    {which} period {period.findtext("{*}timeInterval/{*}start")} -> '
                         f'{period.findtext("{*}timeInterval/{*}end")} {period.findtext("{*}resolution")}: '
                         f'{len(pts)} points; {shown[:420]} ... {{{fields_of(pts[-1])[:140]}}}')
            if len(periods) == 1:
                break
    return lines


def describe_xml(data, max_groups=10):
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return [f'not XML: {data[:160]!r}']
    lines = [f'{local(root.tag)}: {fields_of(root, skip=("TimeSeries", "Reason"))[:500]}']
    for reason in root.findall('{*}Reason'):
        lines.append('reason: ' + fields_of(reason)[:400])
    if root.findall('{*}TimeSeries'):
        lines += describe_series(root, max_groups)
    return lines


def describe_outages(members):
    """What a zip of unavailability documents has: how many, of what kinds,
    and every field they carry (with the value of the first document that has it)."""
    lines = []
    kinds = collections.Counter()
    seen = collections.OrderedDict()            # leaf path -> [example, how many documents have it]
    revisions, created, ids, resolutions, points = collections.Counter(), [], collections.Counter(), collections.Counter(), []
    for data in members:
        try:
            root = ET.fromstring(data)
        except ET.ParseError:
            kinds['not XML'] += 1
            continue
        status = root.findtext('{*}docStatus/{*}value') or 'none'
        ids[root.findtext('{*}mRID')] += 1
        revisions[root.findtext('{*}revisionNumber')] += 1
        created.append(root.findtext('{*}createdDateTime') or '')
        series = root.findall('{*}TimeSeries')
        for ts in series or [None]:
            business = ts.findtext('{*}businessType') if ts is not None else None
            psr = (ts.findtext('{*}production_RegisteredResource.pSRType.psrType') if ts is not None else None)
            kinds[f'{local(root.tag)} type={root.findtext("{*}type")} docStatus={status} business={business} '
                  f'psr={psr} series={len(series)}'] += 1
            if ts is not None:
                for period in ts.findall('{*}Available_Period'):
                    resolutions[period.findtext('{*}resolution')] += 1
                    points.append(len(period.findall('{*}Point')))
        in_this = set()
        for path, value in leaves(root):
            if path not in seen:
                seen[path] = [value[:70], 0]
            if path not in in_this:
                in_this.add(path)
                seen[path][1] += 1
    lines.append(f'{len(members)} documents, {len(ids)} different mRID; revisions {dict(revisions.most_common(6))}; '
                 f'createdDateTime {min(created) if created else None} to {max(created) if created else None}')
    for kind, count in kinds.most_common(14):
        lines.append(f'  {count:4d} x {kind}')
    if points:
        lines.append(f'  Available_Period: resolutions {dict(resolutions)}, points per period '
                     f'min {min(points)} median {statistics.median(points)} max {max(points)}')
    lines.append('  fields (example, documents that have it):')
    for path, (example, count) in seen.items():
        lines.append(f'    {path} = {example!r} ({count})')
    return lines


def describe(status, body, seconds, headers, failure, outages=False, max_groups=10):
    head = (f'HTTP {status}, {len(body)} bytes, {seconds:.1f}s, type {headers.get("content-type")}'
            + (f', encoding {headers.get("content-encoding")}' if headers.get('content-encoding') else '')
            + (f', disposition {headers.get("content-disposition")}' if headers.get('content-disposition') else ''))
    if failure:
        return [f'no answer ({failure}) after {seconds:.1f}s']
    lines = [head]
    if headers.get('content-encoding') == 'gzip':
        import gzip
        try:
            body = gzip.decompress(body)
            lines.append(f'  {len(body)} bytes once decompressed')
        except OSError:
            pass
    if body[:2] == b'PK':
        try:
            archive = zipfile.ZipFile(io.BytesIO(body))
        except zipfile.BadZipFile:
            return lines + ['a broken zip']
        names = archive.namelist()
        members = [archive.read(name) for name in names]
        lines.append(f'zip of {len(names)} files, {sum(len(m) for m in members)} bytes unzipped; '
                     f'first names {names[:3]}')
        if outages:
            lines += describe_outages(members)
        else:
            for name, data in list(zip(names, members))[:2]:
                lines.append(f'  file {name}:')
                lines += ['  ' + line for line in describe_xml(data, max_groups)]
    elif body:
        lines += describe_xml(body, max_groups)
    return lines


def run(title, queries, workers=6, **how):
    """Send the (name, kind, params) queries, a few at a time, and print each
    answer in the order the queries were given."""
    print(f'\n=== {title}: {len(queries)} requests ===', flush=True)

    def one(query):
        name, kind, params = query
        gzip_ok = params.pop('_gzip', False)
        return name, describe(*get(kind, gzip_ok=gzip_ok, **params), **how)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for name, lines in pool.map(one, queries):
            print(clean(f'  {name}:'))
            print(clean('\n'.join('    ' + line for line in lines)), flush=True)


# --- The questions ---

NOW = datetime.now(timezone.utc)
HOUR = NOW.replace(minute=0, second=0, microsecond=0)
TODAY = carry_over.delivery_day(NOW)
DAY_START, DAY_END = carry_over.day_bounds(TODAY)
YESTERDAY_START = carry_over.day_bounds(TODAY - timedelta(days=1))[0]
TOMORROW_END = carry_over.day_bounds(TODAY + timedelta(days=1))[1]


def span(start, end):
    return {'periodStart': start.strftime(STAMP), 'periodEnd': end.strftime(STAMP)}


def balancing():
    """17.1.F, prices of activated balancing energy: documentType A84, processType A16."""
    window = span(YESTERDAY_START, HOUR + timedelta(hours=1))
    queries = []
    for code in ('PT', 'ES'):
        queries.append((f'{code} aFRR (businessType A96), yesterday and today', '17.1.F',
                        dict(documentType='A84', processType='A16', businessType='A96', controlArea_Domain=EIC[code], **window)))
        queries.append((f'{code} every kind of reserve (no businessType)', '17.1.F',
                        dict(documentType='A84', processType='A16', controlArea_Domain=EIC[code], **window)))
        queries.append((f'{code} aFRR, the last 3 hours only', '17.1.F',
                        dict(documentType='A84', processType='A16', businessType='A96', controlArea_Domain=EIC[code],
                             **span(HOUR - timedelta(hours=3), HOUR + timedelta(hours=1)))))
    run('17.1.F Portugal and Spain', queries, max_groups=14)
    others = [(f'{code} aFRR, yesterday and today', '17.1.F',
               dict(documentType='A84', processType='A16', businessType='A96', controlArea_Domain=eic, **window))
              for code, eic in EIC.items() if code not in ('PT', 'ES')]
    run('17.1.F the other areas (which ones answer)', others, max_groups=4)


def picasso():
    """CBMPs for aFRR central selection: documentType A84, processType A67."""
    hour = span(HOUR - timedelta(hours=2), HOUR - timedelta(hours=1))
    base = dict(documentType='A84', processType='A67', businessType='A96')
    queries = []
    for code in ('PT', 'ES', 'AT', 'DE-TenneT', 'DE-LFC', 'CZ', 'NL', 'BE', 'IT-country', 'IT', 'DK1', 'FI', 'FR', 'SK',
                 'CH', 'PL', 'HU', 'SI', 'HR', 'RO', 'BG', 'GR', 'DE-50Hertz', 'DE-Amprion', 'DE-TransnetBW', 'DK2',
                 'NO1', 'SE3', 'EE', 'LV', 'LT'):
        queries.append((f'{code} one hour, two hours ago', 'PICASSO 1 h',
                        dict(base, controlArea_Domain=EIC[code], **hour)))
    run('CBMPs for aFRR CS (A84, A67, A96): one hour for each area', queries, max_groups=6)

    queries = []
    for code in ('PT', 'ES', 'AT', 'DE-TenneT'):
        area = dict(base, controlArea_Domain=EIC[code])
        queries += [
            (f'{code} the last hour up to now (how late is the last value)', 'PICASSO 1 h',
             dict(area, **span(HOUR - timedelta(hours=1), HOUR + timedelta(hours=1)))),
            (f'{code} 15 minutes, two hours ago', 'PICASSO 15 min',
             dict(area, **span(HOUR - timedelta(hours=2), HOUR - timedelta(hours=2) + timedelta(minutes=15)))),
            (f'{code} 6 hours', 'PICASSO 6 h', dict(area, **span(HOUR - timedelta(hours=8), HOUR - timedelta(hours=2)))),
            (f'{code} 24 hours (yesterday)', 'PICASSO 24 h', dict(area, **span(YESTERDAY_START, DAY_START))),
            (f'{code} 24 hours, compressed if the server does it', 'PICASSO 24 h gzip',
             dict(area, _gzip=True, **span(YESTERDAY_START, DAY_START))),
            (f'{code} 48 hours', 'PICASSO 48 h', dict(area, **span(YESTERDAY_START - timedelta(days=1), DAY_START))),
            (f'{code} one hour with Standard_MarketProduct A01', 'PICASSO 1 h',
             dict(area, Standard_MarketProduct='A01', **hour)),
            (f'{code} one hour without businessType', 'PICASSO 1 h',
             dict(documentType='A84', processType='A67', controlArea_Domain=EIC[code], **hour)),
            (f'{code} one hour, local selection (processType A68)', 'PICASSO 1 h',
             dict(documentType='A84', processType='A68', businessType='A96', controlArea_Domain=EIC[code], **hour)),
        ]
    run('CBMPs for aFRR CS: how much one request may ask for, and how late the data are', queries, workers=4,
        max_groups=6)


def forecast():
    """14.1.D, day-ahead forecast of wind and solar generation: documentType A69, processType A01."""
    window = span(YESTERDAY_START, TOMORROW_END)
    print(f'\nForecast asked at {NOW:%Y-%m-%d %H:%M} UTC for {YESTERDAY_START:%Y-%m-%d %H:%M}Z to '
          f'{TOMORROW_END:%Y-%m-%d %H:%M}Z: yesterday, today and tomorrow on the market\'s clock')
    print(f'\n=== A69 A01, one line for each area: {len(EIC)} requests ===', flush=True)

    def one(item):
        code, eic = item
        status, body, seconds, headers, failure = get('A69 forecast', documentType='A69', processType='A01',
                                                      in_Domain=eic, **window)
        if failure:
            return f'  {code}: no answer ({failure}) [{seconds:.1f}s]'
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return f'  {code}: HTTP {status}, not XML [{seconds:.1f}s]'
        reason = root.findtext('{*}Reason/{*}text')
        if reason:
            return f'  {code}: HTTP {status}, {reason[:110]} [{seconds:.1f}s]'
        by_psr = collections.OrderedDict()
        for ts in root.findall('{*}TimeSeries'):
            psr = ts.findtext('{*}MktPSRType/{*}psrType')
            entry = by_psr.setdefault(psr, {'res': collections.Counter(), 'start': [], 'end': [], 'points': 0,
                                            'fields': fields_of(ts, skip=('Period', 'mRID', 'MktPSRType'))})
            for period in ts.findall('{*}Period'):
                entry['res'][period.findtext('{*}resolution')] += 1
                entry['start'].append(period.findtext('{*}timeInterval/{*}start'))
                entry['end'].append(period.findtext('{*}timeInterval/{*}end'))
                entry['points'] += len(period.findall('{*}Point'))
        parts = [f'{psr} {"/".join(e["res"])} {min(e["start"])}->{max(e["end"])} {e["points"]} points'
                 for psr, e in by_psr.items()]
        first = next(iter(by_psr.values()), None)
        return (f'  {code}: {len(body)} bytes [{seconds:.1f}s] ' + '; '.join(parts)
                + (f' | fields: {first["fields"][:260]}' if first else ''))

    with ThreadPoolExecutor(max_workers=6) as pool:
        for line in pool.map(one, list(EIC.items())):
            print(clean(line), flush=True)
    run('A69 A01 Portugal, the whole answer', [
        ('PT', 'A69 forecast', dict(documentType='A69', processType='A01', in_Domain=EIC['PT'], **window)),
        ('PT, intraday forecast (processType A40)', 'A69 forecast',
         dict(documentType='A69', processType='A40', in_Domain=EIC['PT'], **window)),
        ('PT, current forecast (processType A18)', 'A69 forecast',
         dict(documentType='A69', processType='A18', in_Domain=EIC['PT'], **window)),
    ])


def outages():
    """Unavailability of generation units (A80), of production units (A77) and of transmission (A78)."""
    nine_days = span(NOW - timedelta(hours=48), NOW + timedelta(days=7))
    today = span(DAY_START, DAY_END)
    yesterday = span(YESTERDAY_START, DAY_START)
    updated = {'periodStartUpdate': (NOW - timedelta(hours=24)).strftime(STAMP), 'periodEndUpdate': NOW.strftime(STAMP)}
    queries = []
    for code in ('PT', 'ES'):
        for doc, what in (('A80', 'generation units'), ('A77', 'production units')):
            base = dict(documentType=doc, biddingZone_Domain=EIC[code])
            queries += [
                (f'{code} {doc} {what}: 48 hours back to 7 days ahead, no docStatus', f'{doc} 9 days',
                 dict(base, **nine_days)),
                (f'{code} {doc}: the same, second page (offset 200)', f'{doc} 9 days', dict(base, offset=200, **nine_days)),
                (f'{code} {doc}: the same, docStatus A05 (active)', f'{doc} 9 days', dict(base, docStatus='A05', **nine_days)),
                (f'{code} {doc}: the same, docStatus A09 (cancelled)', f'{doc} 9 days',
                 dict(base, docStatus='A09', **nine_days)),
                (f'{code} {doc}: the same, docStatus A13 (withdrawn)', f'{doc} 9 days',
                 dict(base, docStatus='A13', **nine_days)),
                (f'{code} {doc}: today', f'{doc} 1 day', dict(base, **today)),
                (f'{code} {doc}: yesterday', f'{doc} 1 day', dict(base, **yesterday)),
                (f'{code} {doc}: updated in the last 24 hours, 9 days', f'{doc} updated', dict(base, **nine_days, **updated)),
                (f'{code} {doc}: updated in the last 24 hours, no period', f'{doc} updated', dict(base, **updated)),
            ]
    run('Unavailability of generation and production units', queries, workers=5, outages=True)
    queries = []
    for frm, to in (('PT', 'ES'), ('ES', 'PT'), ('ES', 'FR'), ('FR', 'ES')):
        base = dict(documentType='A78', in_Domain=EIC[to], out_Domain=EIC[frm])
        queries += [
            (f'{frm}->{to} A78: 48 hours back to 7 days ahead', 'A78 9 days', dict(base, **nine_days)),
            (f'{frm}->{to} A78: the same, docStatus A09', 'A78 9 days', dict(base, docStatus='A09', **nine_days)),
            (f'{frm}->{to} A78: the same, docStatus A13', 'A78 9 days', dict(base, docStatus='A13', **nine_days)),
            (f'{frm}->{to} A78: today', 'A78 1 day', dict(base, **today)),
            (f'{frm}->{to} A78: the last 30 days', 'A78 30 days', dict(base, **span(NOW - timedelta(days=30), NOW))),
        ]
    run('Unavailability of transmission', queries, workers=5, outages=True)


def generation():
    """A75 for one technology at a time: what a day's series of wind and solar costs."""
    window = span(YESTERDAY_START, HOUR + timedelta(hours=1))
    queries = [(f'{code} A75 {psr}, yesterday and today', 'A75 one technology',
                dict(documentType='A75', processType='A16', in_Domain=EIC[code], psrType=psr, **window))
               for code in ('PT', 'ES', 'DE') for psr in ('B16', 'B18', 'B19')]
    queries.append(('PT A75 every technology, yesterday and today', 'A75 two days',
                    dict(documentType='A75', processType='A16', in_Domain=EIC['PT'], **window)))
    queries.append(('DE A75 every technology, yesterday and today', 'A75 two days',
                    dict(documentType='A75', processType='A16', in_Domain=EIC['DE'], **window)))
    run('A75 by technology', queries, max_groups=4)


def main():
    if not KEY:
        sys.exit('ENTSOE_API_KEY is not set')
    print(f'Probe at {NOW:%Y-%m-%d %H:%M:%S} UTC; delivery day {TODAY}, {DAY_START:%H:%M}Z to {DAY_END:%H:%M}Z; '
          f'parts: {PARTS}')
    every = {'balancing': balancing, 'picasso': picasso, 'forecast': forecast, 'outages': outages,
             'generation': generation}
    for name, part in every.items():
        if 'all' in PARTS or name in PARTS:
            try:
                part()
            except Exception as e:  # noqa: BLE001 - one part must not hide the others
                print(clean(f'  part {name} stopped: {type(e).__name__}: {e}'), flush=True)
    print('\n=== How long the requests took ===')
    for kind, rows in TIMINGS.items():
        seconds = sorted(s for s, _size, _status in rows)
        sizes = sorted(size for _s, size, _status in rows)
        statuses = collections.Counter(status for _s, _size, status in rows)
        print(f'  {kind}: {len(rows)} requests; seconds min {seconds[0]:.1f} median {statistics.median(seconds):.1f} '
              f'max {seconds[-1]:.1f}; bytes median {int(statistics.median(sizes))} max {sizes[-1]}; '
              f'HTTP {dict(statuses)}')


if __name__ == '__main__':
    main()
