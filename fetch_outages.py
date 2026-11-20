#!/usr/bin/env python3
"""fetch_outages.py - outages of power plants and of lines, by country.

Writes one file for each zone, data/outages/<ZONE>.json, and an index,
data/outages/index.json: the outages in course, those that start in the next
seven days and those that ended in the last 48 hours, in two blocks that are
kept apart because they are under different licences (see feed_common.py):

    generation     source-terms   ENTSO-E items 15.1.A to D: unavailability of
                                  generation units (document A80) and of
                                  production units (A77), by zone.
    transmission   cc-by-4.0      ENTSO-E items 10.1.A and B: unavailability
                                  of transmission infrastructure (A78), by
                                  border.

and, for history, one record a day in data/archive/outages-YYYY-MM-DD.json:
the outages that were in force on that delivery day.

What ENTSO-E has, as its answers of 18 Nov 2026 showed:

  - An answer is a zip with one XML document for each outage, in its latest
    revision, 200 at most: with more, the request must say which page it
    wants (offset 0, 200, 400...), or it is refused. A request selects the
    outages whose period overlaps the one asked for.
  - With no docStatus the answer has the active outages and the cancelled
    ones (docStatus A09 in the document). The withdrawn ones (A13: published
    by mistake) only come when asked for, in a request of their own.
  - A generation or production unit has its nominal power and the power that
    stays available during the outage: what is unavailable is the difference
    (never below zero: some documents have more available than nominal).
    Portugal publishes generation units only; Spain both.
  - A line, a transformer or a substation has no nominal power. What its
    document has is the exchange capacity left on the border while it is out,
    hour by hour, in one direction: the same outage is two documents, one for
    each direction, with identifiers of their own. Here they are one outage,
    with the capacity of each direction (`available_mw` at the time of the
    run, or at the start of an outage to come and at the end of one that is
    over; `min_available_mw` the lowest of the whole outage). There is no
    unavailable power to give, and `nominal_mw` and `unavailable_mw` are not
    there.
  - Since 22 Oct 2026 ENTSO-E warns that the unavailability of production and
    generation units "may be partially or completely incorrect" because of a
    technical issue on its platform (notice of 17 Nov 2026, still open). The
    generation block repeats the warning in `known_issue`. Transmission is
    not affected.
  - Some documents are not well-formed XML (a known issue of the platform):
    such a document is left out and counted in the log.

Portugal and Spain (and their borders) are asked for in every run; the other
zones and borders of the feed once an hour, and what ENTSO-E has no outages
for every six hours. The withdrawn outages are only asked for where there are
others: a zone or a border with none costs one request, not two.

The requests go out in batches, each one finished before the next begins:
Portugal, Spain and their borders first, then the rest, a few zones at a time,
for as long as the budget lasts. A zone only counts when every request of it
has come back, so sending the first request of every zone at once would, on a
slow day, use up the budget and finish none of them: that is what the first
real run did (run 2610, 18 Nov 2026: 108 requests sent in 90 seconds, no zone
complete, nothing written). What a run does not get to is first in line in the
next one.

The step has its own time budget, runs after the files the site
already reads have been published, and never fails the run. A zone or border
that was asked for and did not answer, or whose pages could not all be had,
keeps what was published, with the time it was obtained and the mark
carried_over, for under three hours (the feed's rule for generation and
flows); a run that obtains nothing leaves the files exactly as they were.

What is published (the "Radar" page, decided on 20 Nov 2026, asks only for
the file of the country shown):

  - Only the outages in force: the cancelled and the withdrawn ones are still
    asked for and read, because the archive needs them to take an outage out
    of a day's record, but they are not published. On 20 Nov 2026 they were
    877 of the 2 375 outages of the single file there was until then.
  - A zone's file has its units and the borders that touch it: those of the
    zone and, for a zone of Norway, Sweden or Denmark, those of its country,
    whose borders are by the country (DE-DK goes into DK1 and DK2). Every zone
    of the feed has a file, with no outages when there are none.
  - `unavailable_now` is what is out at the time of the run, each unit counted
    once: a unit (its `asset_eic`, or its name when it has none) with several
    outages in force at once (two notices of Ascó 2 that overlap) counts with
    the largest of their unavailable powers, not their sum. `units` counts the
    units whose largest unavailable power in force is above 0 MW: one whose
    outages say 0 MW, or give no power at all, is not counted (decided on
    20 Nov 2026; at 15:01 UTC that day Spain had one, 18WABO1-12345-D7, with
    no name, technology nor nominal power, reason A95, from 21 Jan to 31 Dec
    2026). Its outages stay in the file as they are. On 20 Nov 2026 that was
    9 000 MW in 30 units in Spain, where adding up the outages gave 11 435 MW.
  - A zone's file has no time of the run in it: it is only written when what
    it says changes, so that the commits do not grow with files that did not.
    The times of the run (`updated`, and `fetched_at` and `carried_over` of
    each zone and border) are in the index, which is written by every run
    that obtains something.

    data/outages/ES.json:
    {zone, data_time,
     generation: {license, source, known_issue, eic, outages:
         [{id, revision, asset_type, name, plant, asset_eic, location, zone,
           technology, psr_type, nominal_mw, available_mw, unavailable_mw,
           start, end, planned, status, published, reason, phase}], counts,
         data_time, unavailable_now: {unavailable_mw, units}},
     transmission: {license, source, data_time, borders: {"ES-PT": {zones,
         outages: [{id, ids, revision, asset_type, name, assets, border,
           start, end, planned, status, published, reason, available_mw,
           min_available_mw, phase}], counts, data_time}}}}

    data/outages/index.json:
    {updated, data_time, window: {from, to}, zones: [every zone with a file],
     generation: {license, source, zones: {ES: {outages, counts, data_time,
         unavailable_now, fetched_at[, carried_over]}},
         not_published: {A80: {zone: time}, A77: {...}}},
     transmission: {license, source, borders: {"ES-PT": {zones, outages,
         counts, data_time, fetched_at[, carried_over]}},
         not_published: {border: time}}}

`data_time` is the time of the most recent publication among the outages (a
cancellation is a publication too); null in a zone's block when nothing is
published for it. `outages` in the index is how many the zone's file has.

The single data/outages.json of before is no longer written: nothing read it
(neither the site's Worker nor its pages, checked on 20 Nov 2026). The first
run after the change takes what it needs from it (when each zone was obtained,
what has nothing published) and then removes it.
"""

import collections
import os
import sys
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import carry_over
import fetch_entsoe as fe
from feed_common import (CARRIED_OVER, CC_BY, DATA_TIME, FETCHED_AT, HOURLY, SOURCE_TERMS, Answer, Budget,
                         asked_recently, carry, due, entsoe_answer, entsoe_ask, http_get, iso, latest, load_json,
                         parse_time, step_of, write_json, write_json_if_changed)

OUT_DIR = 'data/outages'
ZONE_PATH = OUT_DIR + '/{zone}.json'
INDEX_PATH = OUT_DIR + '/index.json'
OLD_PATH = 'data/outages.json'   # the single file of before 20 Nov 2026: read once, then removed
ARCHIVE_PATH = 'data/archive/outages-{day}.json'
BUDGET_S = float(os.environ.get('OUTAGES_BUDGET_SECONDS', '110'))

GENERATION = 'generation'
TRANSMISSION = 'transmission'
BACK = timedelta(hours=48)       # outages that ended this long ago are still shown
AHEAD = timedelta(days=7)        # outages that start within this are already shown
MAX_AGE = timedelta(hours=3)     # how long a zone that does not answer keeps what was published
QUIET = timedelta(hours=6)       # how often a zone or border with no outages is asked again
PAGE = 200                       # documents in one answer, at most
MAX_PAGES = 10
BATCH = 8                        # first requests sent at a time for the zones and borders read once an hour:
                                 # one for each request in flight, so that a batch is two waves and no more
ENOUGH_S = 30                    # a batch is only started with this much of the budget left
KEEP_DAYS = 1830                 # the archive goes back five years, as the others
STAMP = '%Y%m%d%H%M'

ALWAYS_ZONES = ('PT', 'ES')
ALWAYS_BORDERS = ('ES-PT', 'ES-FR')
UNITS = {'A80': 'generation_unit', 'A77': 'production_unit'}
STATUS = {None: 'active', 'A05': 'active', 'A09': 'cancelled', 'A13': 'withdrawn'}
PHASES = ('ongoing', 'upcoming', 'ended')
ASSETS = {'B21': 'AC link', 'B22': 'DC link', 'B23': 'Substation', 'B24': 'Transformer'}
TECHNOLOGY = {**fe.PSR_MAP, 'B25': 'Energy storage'}

KNOWN_ISSUE = ('ENTSO-E notice of 17 Nov 2026: because of a technical issue on the Transparency Platform, the '
               'unavailability of production and generation units (15.1.A-D) may be partially or completely '
               'incorrect since 22 Oct 2026.')

# The zones asked for: the feed's own, with Italy as the country (as its
# generation is). The United Kingdom is not on ENTSO-E.
ZONES = {code: eic for code, eic in fe.GEN_ZONES.items() if code not in fe.ELEXON_ZONES}
ZONE_ORDER = list(ALWAYS_ZONES) + [code for code in ZONES if code not in ALWAYS_ZONES]


def _borders():
    """{border: [(from, to)]}: each border of the feed's corridors, by the name
    its pair has in cross-border-flows.json, with the two directions of each of
    the links it is asked for (see FLOW_LINKS in fetch_entsoe.py). The borders
    with the United Kingdom are left out: its cables' data are not ENTSO-E's
    to license (its list of data free to reuse excludes IFA, Nemo Link and
    BritNed), so they could not go in the cc-by-4.0 block."""
    borders = {}
    for frm, to in fe.FLOW_CORRIDORS:
        if frm in fe.ELEXON_ZONES or to in fe.ELEXON_ZONES:
            continue
        name = '-'.join(sorted([frm, to]))
        for link in fe.flow_links(frm, to):
            if link not in borders.setdefault(name, []):
                borders[name].append(link)
    return borders


BORDERS = _borders()
BORDER_ORDER = list(ALWAYS_BORDERS) + [name for name in BORDERS if name not in ALWAYS_BORDERS]
COUNTRY_OF = {zone: country for country, zones in fe.CONSOLIDATE.items() for zone in zones}


def touches(border, zone):
    """Whether a border goes into a zone's file: it is of the zone, or of its
    country for a zone of Norway, Sweden or Denmark (whose borders are named
    by the country)."""
    sides = border.split('-')
    return zone in sides or COUNTRY_OF.get(zone) in sides


# --- Reading a document ---

def _number(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _mw(value):
    if value is None:
        return None
    return int(value) if float(value).is_integer() else round(value, 1)


def _moment(date_text, time_text):
    """start_DateAndOrTime.date and .time ('2026-04-06', '07:00:00Z') as a time."""
    if not date_text or not time_text:
        return None
    return parse_time(f'{date_text.strip()}T{time_text.strip()}')


def availability(series):
    """[(time, MW)] of the capacity a TimeSeries says stays available, in
    order: one entry for each time it changes."""
    changes = []
    for period in series.findall('{*}Available_Period'):
        start = parse_time(period.findtext('{*}timeInterval/{*}start'))
        step = step_of(period.findtext('{*}resolution'))
        if start is None or step is None:
            continue
        for point in period.findall('{*}Point'):
            quantity = _number(point.findtext('{*}quantity'))
            try:
                position = int(point.findtext('{*}position'))
            except (TypeError, ValueError):
                continue
            if quantity is not None:
                changes.append((start + step * (position - 1), quantity))
    return sorted(changes)


def available_at(changes, moment):
    """The capacity in force at `moment`: that of the last change up to it,
    or of the first one when they all come later. None with no changes."""
    if not changes:
        return None
    value = changes[0][1]
    for at, quantity in changes:
        if at > moment:
            break
        value = quantity
    return value


def phase_of(start, end, now):
    """'ongoing', 'upcoming' or 'ended' for an outage the file shows, else None."""
    if start is None or end is None:
        return None
    if start <= now < end:
        return 'ongoing'
    if now < start <= now + AHEAD:
        return 'upcoming'
    if now - BACK <= end <= now:
        return 'ended'
    return None


def read_outage(xml, now, withdrawn=False):
    """One outage document as a plain record, or None when it cannot be read
    or is not within what the file shows. A record of a unit is as the file
    has it; one of transmission has `direction` and `available`, for
    merge_directions()."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return None
    series = root.find('{*}TimeSeries')
    if series is None:
        return None
    start = _moment(series.findtext('{*}start_DateAndOrTime.date'), series.findtext('{*}start_DateAndOrTime.time'))
    end = _moment(series.findtext('{*}end_DateAndOrTime.date'), series.findtext('{*}end_DateAndOrTime.time'))
    if start is None or end is None:
        start = parse_time(root.findtext('{*}unavailability_Time_Period.timeInterval/{*}start'))
        end = parse_time(root.findtext('{*}unavailability_Time_Period.timeInterval/{*}end'))
    phase = phase_of(start, end, now)
    if phase is None:
        return None
    kind = (root.findtext('{*}type') or '').strip()
    status = 'withdrawn' if withdrawn else STATUS.get((root.findtext('{*}docStatus/{*}value') or '').strip() or None, 'active')
    try:
        revision = int(root.findtext('{*}revisionNumber'))
    except (TypeError, ValueError):
        revision = None
    published = parse_time(root.findtext('{*}createdDateTime'))
    reasons = [(reason.findtext('{*}code') or '').strip() for reason in root.findall('{*}Reason')]
    changes = availability(series)
    moment = now if phase == 'ongoing' else start if phase == 'upcoming' else end - timedelta(minutes=1)
    record = {
        'id': (root.findtext('{*}mRID') or '').strip(),
        'revision': revision,
        'start': iso(start),
        'end': iso(end),
        'planned': (series.findtext('{*}businessType') or '').strip() == 'A53',
        'status': status,
        'published': iso(published) if published else None,
        'reason': next((code for code in reasons if code), None),
        'phase': phase,
    }
    if kind in UNITS:
        resource = 'production_RegisteredResource.'
        unit = series.findtext('{*}' + resource + 'pSRType.powerSystemResources.name')
        plant = series.findtext('{*}' + resource + 'name')
        psr = (series.findtext('{*}' + resource + 'pSRType.psrType') or '').strip() or None
        nominal = _number(series.findtext('{*}' + resource + 'pSRType.powerSystemResources.nominalP'))
        available = min((quantity for _at, quantity in changes), default=None)
        unavailable = max(0.0, nominal - available) if nominal is not None and available is not None else None
        record.update({
            'asset_type': UNITS[kind],
            'name': (unit or plant or '').strip() or None,
            'plant': (plant or '').strip() or None,
            'location': (series.findtext('{*}' + resource + 'location.name') or '').strip() or None,
            'asset_eic': (series.findtext('{*}' + resource + 'pSRType.powerSystemResources.mRID')
                          or series.findtext('{*}' + resource + 'mRID') or '').strip() or None,
            'technology': TECHNOLOGY.get(psr),
            'psr_type': psr,
            'nominal_mw': _mw(nominal),
            'available_mw': _mw(available),
            'unavailable_mw': _mw(unavailable),
        })
    elif kind == 'A78':
        assets = [{'name': (asset.findtext('{*}name') or '').strip() or None,
                   'type': ASSETS.get((asset.findtext('{*}asset_PSRType.psrType') or '').strip()),
                   'location': (asset.findtext('{*}location.name') or '').strip() or None}
                  for asset in series.findall('{*}Asset_RegisteredResource')]
        record.update({
            'asset_type': 'transmission',
            'name': ' + '.join(asset['name'] for asset in assets if asset['name']) or None,
            'assets': assets,
            'direction': ((series.findtext('{*}out_Domain.mRID') or '').strip(),
                          (series.findtext('{*}in_Domain.mRID') or '').strip()),
            'available': (_mw(available_at(changes, moment)), _mw(min((q for _at, q in changes), default=None))),
        })
    else:
        return None
    return record


ORDER_OF_FIELDS = ('id', 'ids', 'revision', 'asset_type', 'name', 'plant', 'asset_eic', 'assets', 'location', 'zone', 'border',
                   'technology', 'psr_type', 'nominal_mw', 'available_mw', 'min_available_mw', 'unavailable_mw',
                   'start', 'end', 'planned', 'status', 'published', 'reason', 'phase')


def _tidy(record):
    return {name: record[name] for name in ORDER_OF_FIELDS if name in record}


def _sorted(outages):
    return sorted(outages, key=lambda o: (PHASES.index(o['phase']), o['start'], o['name'] or '', o['id']))


def _counts(outages):
    """How many outages in force (not cancelled nor withdrawn) each phase has."""
    tally = collections.Counter(o['phase'] for o in outages if o['status'] == 'active')
    return {phase: tally.get(phase, 0) for phase in PHASES}


def unavailable_now(outages, now):
    """What is out at `now`, each unit counted once: {unavailable_mw, units}.
    A unit is its asset_eic or, with none, its name; with several outages in
    force at once it counts with the largest of their unavailable powers.
    A unit whose largest unavailable power is 0 MW, or not given, is not
    counted in `units`."""
    largest = {}
    for outage in outages:
        if outage.get('status') != 'active':
            continue
        start, end = parse_time(outage.get('start')), parse_time(outage.get('end'))
        if start is None or end is None or not start <= now < end:
            continue
        unit = outage.get('asset_eic') or outage.get('name') or outage.get('id')
        largest[unit] = max(largest.get(unit, 0), outage.get('unavailable_mw') or 0)
    return {'unavailable_mw': _mw(round(sum(largest.values()), 1)), 'units': sum(1 for mw in largest.values() if mw > 0)}


def merge_directions(records, names):
    """The outages of a border from the records of its documents: the two
    documents of one outage (one for each direction) become one outage, with
    the capacity of each direction. `names` gives the zone code of each EIC."""
    merged, seen = {}, collections.Counter()
    for record in records:
        frm, to = (names.get(eic, eic) for eic in record['direction'])
        direction = f'{frm}→{to}'
        key = (record['name'], tuple(asset['name'] for asset in record['assets']), record['start'], record['end'],
               record['planned'], record['status'])
        # Two outages can be alike in all of that (documents with no asset, for the same days): the first
        # of one direction goes with the first of the other, the second with the second.
        seen[(direction, key)] += 1
        key += (seen[(direction, key)],)
        outage = merged.setdefault(key, dict(
            {k: v for k, v in record.items() if k not in ('direction', 'available', 'id', 'revision', 'published')},
            ids={}, revisions=[], publications=[], available_mw={}, min_available_mw={}))
        outage['ids'][direction] = record['id']
        outage['revisions'].append(record['revision'])
        outage['publications'].append(record['published'])
        outage['available_mw'][direction], outage['min_available_mw'][direction] = record['available']
    outages = []
    for outage in merged.values():
        ids = dict(sorted(outage['ids'].items()))
        revisions = [r for r in outage.pop('revisions') if r is not None]
        outages.append(dict(outage, id=next(iter(ids.values())), ids=ids,
                            revision=max(revisions) if revisions else None,
                            published=latest(outage.pop('publications')),
                            available_mw=dict(sorted(outage['available_mw'].items())),
                            min_available_mw=dict(sorted(outage['min_available_mw'].items()))))
    return outages


# --- Asking ---

def ask_paged(requests, followers, budget, get, interval):
    """Send the requests {key: (name, params)} and every further page of the
    answers that are full. A key of `followers` ({key: the key it follows}) is
    only sent once the request it follows has come back with documents: with
    none, it is taken to have none either, and is not asked for.

    Returns {key: Answer} with the documents of all the pages, or with the
    problem when a page could not be had; a key there was no time to ask for
    is absent."""
    results = {}
    waiting = {key: after for key, after in followers.items() if key in requests}
    pending = {key: (name, params, 0, []) for key, (name, params) in requests.items() if key not in waiting}
    while pending:
        jobs = [((key, offset), name + (f', page {offset // PAGE + 1}' if offset else ''), dict(params, offset=offset))
                for key, (name, params, offset, _documents) in pending.items()]
        answers = entsoe_ask(jobs, budget, get=get, interval=interval, label='Outages')
        following = {}
        for key, (name, params, offset, documents) in pending.items():
            response = answers.get((key, offset))
            if response is None:
                if offset:
                    results[key] = Answer([], 'out of time before its last page')
                continue
            answer = entsoe_answer(response)
            if answer.problem is not None:
                results[key] = answer
                continue
            documents = documents + answer.documents
            if len(answer.documents) >= PAGE and offset + PAGE < PAGE * MAX_PAGES:
                following[key] = (name, params, offset + PAGE, documents)
            else:
                results[key] = Answer(documents, None)
        for key, after in list(waiting.items()):
            if after not in results:
                continue
            del waiting[key]
            if results[after].problem is not None:
                continue                                  # its zone has failed already
            if results[after].documents:
                following[key] = (*requests[key], 0, [])
            else:
                results[key] = Answer([], None)
        pending = following
    return results


def _entries(doc, block, key):
    entries = ((doc or {}).get(block) or {}).get(key)
    return {name: entry for name, entry in entries.items() if isinstance(entry, dict)} if isinstance(entries, dict) else {}


def _memory(doc, block):
    memory = ((doc or {}).get(block) or {}).get('not_published')
    return memory if isinstance(memory, dict) else {}


def plan(previous, now):
    """The requests of this run, Portugal and Spain first: (requests, followers)
    with requests {(block, name, document or direction, withdrawn?): (name for
    the log, params)} and followers {the request for the withdrawn outages:
    the request it is only sent after}."""
    window = {'periodStart': (now - BACK).strftime(STAMP), 'periodEnd': (now + AHEAD).strftime(STAMP)}
    requests, followers = {}, {}
    zones, no_units = _entries(previous, GENERATION, 'zones'), _memory(previous, GENERATION)
    borders, no_lines = _entries(previous, TRANSMISSION, 'borders'), _memory(previous, TRANSMISSION)

    def add(key, name, params):
        requests[key] = (name, params)
        withdrawn = key[:3] + (True,)
        requests[withdrawn] = (name + ' withdrawn', dict(params, docStatus='A13'))
        followers[withdrawn] = key

    def units(zone):
        again = HOURLY if zone in ALWAYS_ZONES else QUIET
        for document in UNITS:
            if not asked_recently((no_units.get(document) or {}).get(zone), now, again):
                add((GENERATION, zone, document, False), f'{zone} {document}',
                    dict(documentType=document, biddingZone_Domain=ZONES[zone], **window))

    def lines(border):
        if asked_recently(no_lines.get(border), now, HOURLY if border in ALWAYS_BORDERS else QUIET):
            return
        for frm, to in BORDERS[border]:
            for out_zone, in_zone in ((frm, to), (to, frm)):
                add((TRANSMISSION, border, (out_zone, in_zone), False), f'{out_zone}→{in_zone} A78',
                    dict(documentType='A78', in_Domain=fe.LINK_ZONES[in_zone], out_Domain=fe.LINK_ZONES[out_zone], **window))

    for zone in ALWAYS_ZONES:
        units(zone)
    for border in ALWAYS_BORDERS:
        lines(border)
    for zone in ZONE_ORDER:
        if zone not in ALWAYS_ZONES and due(zones.get(zone), now):
            units(zone)
    for border in BORDER_ORDER:
        if border not in ALWAYS_BORDERS and due(borders.get(border), now):
            lines(border)
    return requests, followers


def batches(requests, followers):
    """The requests of a run in the order they are sent, a batch at a time:
    first those of Portugal, Spain and their borders, then the rest in groups
    of about BATCH first requests, a zone or border never split between two.
    Each batch has the requests for the withdrawn outages of its own zones."""
    def always(key):
        return key[1] in (ALWAYS_ZONES if key[0] == GENERATION else ALWAYS_BORDERS)

    units = {}
    for key in requests:
        units.setdefault(key[:2], []).append(key)
    first = [key for unit, keys in units.items() if always(unit) for key in keys]
    groups, group, size = ([first] if first else []), [], 0
    for unit, keys in units.items():
        if always(unit):
            continue
        group += keys
        size += sum(key not in followers for key in keys)
        if size >= BATCH:
            groups.append(group)
            group, size = [], 0
    if group:
        groups.append(group)
    return [{key: requests[key] for key in keys} for keys in groups]


def _fresh_enough(now):
    def may_keep(_name, entry):
        obtained = parse_time(entry.get(FETCHED_AT))
        return obtained is not None and now - obtained < MAX_AGE
    return may_keep


def _collect(results, block, name):
    """What the answers say of one zone or border: ('obtained', {part: Answer}),
    ('failed', None) when a request of it had a problem, or ('not asked', None)
    when there was no time for all of them."""
    parts = {key[2:]: answer for key, answer in results['asked'].items() if key[:2] == (block, name)}
    wanted = [key for key in results['wanted'] if key[:2] == (block, name)]
    if not wanted:
        return 'not due', None
    if any(answer.problem is not None for answer in parts.values()):
        return 'failed', None            # the requests that follow a failed one are never sent
    if any(key not in results['asked'] for key in wanted):
        return 'not asked', None
    return 'obtained', parts


def main(now=None, get=http_get, budget=None, interval=0.25):
    now = now or datetime.now(timezone.utc)
    stamp = iso(now)
    previous = read_published()
    requests, followers = plan(previous, now)
    print(f'Outages: {len(requests) - len(followers)} requests (and as many for the withdrawn ones, where there are '
          f'others), for the outages from {now - BACK:%d %b %H:%M} to {now + AHEAD:%d %b %H:%M} UTC')
    if not requests:
        print('  Nothing due in this run')
        return 0
    budget = budget or Budget(BUDGET_S)
    results = {'wanted': list(requests), 'asked': {}}
    for number, batch in enumerate(batches(requests, followers)):
        if number and budget.left() < ENOUGH_S:
            print(f'  Outages: {len(requests) - len(results["asked"])} requests of zones and borders read once an '
                  f'hour left for the next run')
            break
        results['asked'].update(ask_paged(batch, followers, budget, get, interval))
    unreadable = 0

    def records(answer, withdrawn):
        nonlocal unreadable
        found = []
        for document in answer.documents:
            record = read_outage(document, now, withdrawn)
            if record is None:
                try:
                    ET.fromstring(document)
                except ET.ParseError:
                    unreadable += 1
                continue
            found.append(record)
        return found

    # Generation and production units, by zone
    had_zones = _entries(previous, GENERATION, 'zones')
    no_units = {document: dict(_memory(previous, GENERATION).get(document) or {}) for document in UNITS}
    obtained, failed = {}, set()
    for zone in ZONE_ORDER:
        state, parts = _collect(results, GENERATION, zone)
        if state == 'failed':
            failed.add(zone)
        if state != 'obtained':
            continue
        outages = []
        for document in UNITS:
            if (document, False) not in parts:
                continue
            if not parts[(document, False)].documents:
                no_units[document][zone] = stamp
                continue
            no_units[document].pop(zone, None)
            for withdrawn in (False, True):
                outages += [dict(record, zone=zone) for record in records(parts[(document, withdrawn)], withdrawn)]
        if not outages and zone not in had_zones:
            continue                                  # ENTSO-E has nothing for this zone: listed in not_published
        outages = [_tidy(o) for o in _sorted(outages)]
        obtained[zone] = {'eic': ZONES[zone], 'outages': outages, 'counts': _counts(outages),
                          DATA_TIME: latest(o['published'] for o in outages)}
        print(f"  {zone}: {len(outages)} outages of units ({obtained[zone]['counts']})")
    zones, kept, dropped = carry(had_zones, obtained, failed, stamp, _fresh_enough(now))
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(kept))
    if dropped:
        print('  Left out, last obtained 3 hours ago or more: ' + ', '.join(dropped))

    # Transmission, by border
    names = {eic: code for code, eic in fe.LINK_ZONES.items()}
    had_borders = _entries(previous, TRANSMISSION, 'borders')
    no_lines = dict(_memory(previous, TRANSMISSION))
    crossing, missed = {}, set()
    for border in BORDER_ORDER:
        state, parts = _collect(results, TRANSMISSION, border)
        if state == 'failed':
            missed.add(border)
        if state != 'obtained':
            continue
        if not any(answer.documents for (_direction, withdrawn), answer in parts.items() if not withdrawn):
            no_lines[border] = stamp
            if border not in had_borders:
                continue
        else:
            no_lines.pop(border, None)
        found = [record for (_direction, withdrawn), answer in parts.items() for record in records(answer, withdrawn)]
        outages = [_tidy(dict(o, border=border)) for o in _sorted(merge_directions(found, names))]
        crossing[border] = {'zones': border.split('-'), 'outages': outages, 'counts': _counts(outages),
                            DATA_TIME: latest(o['published'] for o in outages)}
        print(f"  {border}: {len(outages)} outages of transmission ({crossing[border]['counts']})")
    borders, kept, dropped = carry(had_borders, crossing, missed, stamp, _fresh_enough(now))
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(kept))
    if dropped:
        print('  Left out, last obtained 3 hours ago or more: ' + ', '.join(dropped))
    if unreadable:
        print(f'  {unreadable} documents were not well-formed XML and were left out')

    if not obtained and not crossing:
        print(f'Nothing obtained in this run: {OUT_DIR}/ is left exactly as it was')
        return 1

    doc = {
        'updated': stamp,
        DATA_TIME: latest(entry.get(DATA_TIME) for entries in (zones, borders) for entry in entries.values()),
        'window': {'from': iso(now - BACK), 'to': iso(now + AHEAD)},
        GENERATION: {
            'license': SOURCE_TERMS,
            'source': 'ENTSO-E Transparency Platform, items 15.1.A-D: unavailability of generation and production units',
            'known_issue': KNOWN_ISSUE,
            'zones': {zone: zones[zone] for zone in ZONES if zone in zones},
            'not_published': {document: {zone: no_units[document][zone] for zone in ZONES if zone in no_units[document]}
                              for document in UNITS},
        },
        TRANSMISSION: {
            'license': CC_BY,
            'source': 'ENTSO-E Transparency Platform, items 10.1.A-B: unavailability of transmission infrastructure',
            'borders': {border: borders[border] for border in BORDERS if border in borders},
            'not_published': {border: no_lines[border] for border in BORDERS if border in no_lines},
        },
    }
    written = publish(doc, now)
    print(f'Wrote {INDEX_PATH} and {written} of the {len(ZONE_ORDER)} files of the zones (the others had not '
          f'changed): {len(zones)} zones with outages of units ({len(obtained)} from this run), '
          f'{len(borders)} borders ({len(crossing)} from this run)')
    for zone in ALWAYS_ZONES:
        now_out = unavailable_now((zones.get(zone) or {}).get('outages', []), now)
        print(f"  {zone}: {now_out['unavailable_mw']} MW out now, in {now_out['units']} units, each counted once")
    archive(doc, now)
    return 0


# --- What is published: a file for each zone, and the index ---

def _published(outages):
    """The outages that are published: those in force, not the cancelled nor the withdrawn ones."""
    return [o for o in outages if o.get('status') == 'active']


def _zero_counts():
    return {phase: 0 for phase in PHASES}


def zone_file(doc, zone, now):
    """The file of one zone: its units and the borders that touch it."""
    entry = doc[GENERATION]['zones'].get(zone)
    units = _published(entry['outages']) if entry else []
    generation = {
        'license': doc[GENERATION]['license'],
        'source': doc[GENERATION]['source'],
        'known_issue': doc[GENERATION]['known_issue'],
        'eic': ZONES[zone],
        'outages': units,
        'counts': entry['counts'] if entry else _zero_counts(),
        DATA_TIME: entry.get(DATA_TIME) if entry else None,
        'unavailable_now': unavailable_now(units, now),
    }
    borders = {name: {'zones': entry['zones'], 'outages': _published(entry['outages']), 'counts': entry['counts'],
                      DATA_TIME: entry.get(DATA_TIME)}
               for name, entry in doc[TRANSMISSION]['borders'].items() if touches(name, zone)}
    transmission = {
        'license': doc[TRANSMISSION]['license'],
        'source': doc[TRANSMISSION]['source'],
        DATA_TIME: latest(entry[DATA_TIME] for entry in borders.values()),
        'borders': borders,
    }
    return {'zone': zone, DATA_TIME: latest([generation[DATA_TIME], transmission[DATA_TIME]]),
            GENERATION: generation, TRANSMISSION: transmission}


def _run_marks(entry):
    return {name: entry[name] for name in (FETCHED_AT, CARRIED_OVER) if name in entry}


def index_file(doc, now):
    """The index: the zones, how many outages each file has, the times of the data and of the run."""
    zones = {}
    for zone, entry in doc[GENERATION]['zones'].items():
        units = _published(entry['outages'])
        zones[zone] = {'outages': len(units), 'counts': entry['counts'], DATA_TIME: entry.get(DATA_TIME),
                       'unavailable_now': unavailable_now(units, now), **_run_marks(entry)}
    borders = {name: {'zones': entry['zones'], 'outages': len(_published(entry['outages'])), 'counts': entry['counts'],
                      DATA_TIME: entry.get(DATA_TIME), **_run_marks(entry)}
               for name, entry in doc[TRANSMISSION]['borders'].items()}
    return {
        'updated': doc['updated'],
        DATA_TIME: doc[DATA_TIME],
        'window': doc['window'],
        'zones': list(ZONE_ORDER),
        GENERATION: {'license': doc[GENERATION]['license'], 'source': doc[GENERATION]['source'], 'zones': zones,
                     'not_published': doc[GENERATION]['not_published']},
        TRANSMISSION: {'license': doc[TRANSMISSION]['license'], 'source': doc[TRANSMISSION]['source'],
                       'borders': borders, 'not_published': doc[TRANSMISSION]['not_published']},
    }


def publish(doc, now):
    """Write the files of the zones that changed and the index; take out the
    single file of before and the file of a zone the feed no longer has.
    Returns how many files of zones were written."""
    written = sum(write_json_if_changed(ZONE_PATH.format(zone=zone), zone_file(doc, zone, now), separators=(',', ':'))
                  for zone in ZONE_ORDER)
    write_json(INDEX_PATH, index_file(doc, now), separators=(',', ':'))
    for name in os.listdir(OUT_DIR):
        if name.endswith('.json') and name != os.path.basename(INDEX_PATH) and name[:-len('.json')] not in ZONES:
            os.remove(os.path.join(OUT_DIR, name))
    if os.path.exists(OLD_PATH):
        os.remove(OLD_PATH)
        print(f'Took out {OLD_PATH}, replaced by {OUT_DIR}/')
    return written


def read_published():
    """What was published, in the shape of one document ({updated, data_time,
    window, generation: {..., zones: {zone: entry}, not_published},
    transmission: {..., borders: {border: entry}, not_published}}), each
    entry with its outages and the times of the run the index keeps. None
    when nothing was published; the single file of before when it is all
    there is (the first run after the change)."""
    index = load_json(INDEX_PATH)
    if not isinstance(index, dict):
        return load_json(OLD_PATH)
    files = {}

    def block_of(zone, block):
        if zone not in files:
            files[zone] = load_json(ZONE_PATH.format(zone=zone)) or {}
        return files[zone].get(block) or {}

    generation, transmission = index.get(GENERATION) or {}, index.get(TRANSMISSION) or {}
    zones = {}
    for zone, info in (generation.get('zones') or {}).items():
        block = block_of(zone, GENERATION) if zone in ZONES else {}
        if isinstance(info, dict) and isinstance(block.get('outages'), list):
            zones[zone] = {'eic': block.get('eic'), 'outages': block['outages'], 'counts': block.get('counts'),
                           DATA_TIME: block.get(DATA_TIME), 'unavailable_now': block.get('unavailable_now'),
                           **_run_marks(info)}
    borders = {}
    for border, info in (transmission.get('borders') or {}).items():
        entry = next((found for zone in ZONE_ORDER if touches(border, zone)
                      if isinstance(found := (block_of(zone, TRANSMISSION).get('borders') or {}).get(border), dict)),
                     None)
        if isinstance(info, dict) and entry is not None:
            borders[border] = {**entry, **_run_marks(info)}
    return {
        'updated': index.get('updated'),
        DATA_TIME: index.get(DATA_TIME),
        'window': index.get('window'),
        GENERATION: {**{k: v for k, v in generation.items() if k != 'zones'},
                     'known_issue': block_of(ZONE_ORDER[0], GENERATION).get('known_issue'), 'zones': zones},
        TRANSMISSION: {**{k: v for k, v in transmission.items() if k != 'borders'}, 'borders': borders},
    }


# --- The archive: what was in force on each day ---

ARCHIVED = ('id', 'ids', 'revision', 'asset_type', 'name', 'plant', 'technology', 'nominal_mw', 'unavailable_mw',
            'min_available_mw', 'start', 'end', 'planned', 'published')


def _in_force(outage, start, end):
    """True for an outage that was not cancelled nor withdrawn and whose period overlaps [start, end)."""
    begins, ends = parse_time(outage.get('start')), parse_time(outage.get('end'))
    return outage.get('status') == 'active' and begins is not None and ends is not None and begins < end and ends > start


def archive_day(doc, day, before=None):
    """The record of a delivery day: the outages of the file that were in
    force on it, with the ones the record already had (`before`) unless the
    file now says they were cancelled or withdrawn. It has no time of its own:
    it only changes when the outages do."""
    start, end = carry_over.day_bounds(day)
    record = {'date': day.isoformat()}
    for block, key in ((GENERATION, 'zones'), (TRANSMISSION, 'borders')):
        entries = {}
        had = ((before or {}).get(block) or {}).get(key) or {}
        for name, entry in ((doc.get(block) or {}).get(key) or {}).items():
            gone = {o['id'] for o in entry.get('outages', []) if o.get('status') != 'active'}
            now_in_force = {o['id']: {field: o[field] for field in ARCHIVED if field in o}
                            for o in entry.get('outages', []) if _in_force(o, start, end)}
            kept = {o['id']: o for o in had.get(name, []) if isinstance(o, dict) and o.get('id') not in gone}
            merged = {**kept, **now_in_force}
            if merged:
                entries[name] = sorted(merged.values(), key=lambda o: (o.get('start') or '', o.get('name') or '', o['id']))
        for name, outages in had.items():
            if name not in entries and name not in ((doc.get(block) or {}).get(key) or {}) and outages:
                entries[name] = outages                  # a zone the file no longer has keeps its record
        record[block] = {'license': doc[block]['license'], key: dict(sorted(entries.items()))}
    return record


def archive(doc, now):
    """Write the records of the delivery day in course and of the one before,
    and take out the records older than five years."""
    os.makedirs(os.path.dirname(ARCHIVE_PATH), exist_ok=True)
    today = carry_over.delivery_day(now)
    for day in (today, today - timedelta(days=1)):
        path = ARCHIVE_PATH.format(day=day.isoformat())
        before = load_json(path)
        record = archive_day(doc, day, before)
        if record != before:
            write_json(path, record, separators=(',', ':'))
            print(f'Archived to {path}')
    cutoff = (today - timedelta(days=KEEP_DAYS)).isoformat()
    folder, prefix = os.path.dirname(ARCHIVE_PATH), 'outages-'
    for name in os.listdir(folder):
        if name.startswith(prefix) and name.endswith('.json') and name[len(prefix):-len('.json')] < cutoff:
            os.remove(os.path.join(folder, name))


if __name__ == '__main__':
    sys.exit(main())
