"""Shared helpers for the feed's newer fetchers (standard library only).

Every request made through http_get() identifies the feed honestly: a real
User-Agent with the feed's name and https://eupowerdata.com as the contact,
no browser User-Agent and no Referer. A source that only answers to a
disguised client is reported as failing, never worked around.
"""

import gzip
import http.client
import io
import json
import os
import queue
import re
import tempfile
import threading
import time
import zipfile
import zlib
import urllib.error
import urllib.parse
import urllib.request
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

USER_AGENT = 'EUPowerData-Feed/1.0 (+https://eupowerdata.com)'


class Response:
    """Outcome of http_get(): it never raises, so callers branch on .ok / .status."""

    def __init__(self, status=None, headers=None, body=b'', error=None):
        self.status = status            # HTTP status, or None when there was no answer
        self.headers = headers or {}    # header names in lower case
        self.body = body                # bytes
        self.error = error              # short reason when there is no usable body

    @property
    def ok(self):
        return self.status == 200

    def text(self, encoding='utf-8'):
        return self.body.decode(encoding, errors='replace')

    def json(self):
        return json.loads(self.body.decode('utf-8'))

    def describe(self):
        """One line for the log: what the source answered."""
        if self.status is None:
            return f'no answer ({self.error})'
        return f'HTTP {self.status}, {len(self.body)} bytes' + (f' ({self.error})' if self.error else '')


def http_get(url, timeout=30, headers=None, max_seconds=None):
    """GET `url`. `timeout` bounds connecting and each read; `max_seconds`
    (default: twice the timeout) bounds the whole request."""
    request_headers = {'User-Agent': USER_AGENT, 'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate'}
    request_headers.update(headers or {})
    req = urllib.request.Request(url, headers=request_headers)
    limit = time.monotonic() + (max_seconds if max_seconds is not None else 2 * timeout)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            chunks = []
            while True:
                chunk = r.read1(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                if time.monotonic() > limit:
                    return Response(error='response too slow')
            headers = _lower(r.headers)
            return Response(r.status, headers, _decode(b''.join(chunks), headers))
    except urllib.error.HTTPError as e:
        # 304, 4xx and 5xx arrive here; keep the status, the headers and the body.
        headers = _lower(e.headers)
        try:
            body = _decode(e.read(), headers)
        except Exception:  # noqa: BLE001
            body = b''
        return Response(e.code, headers, body, error=str(e.reason))
    except urllib.error.URLError as e:
        return Response(error=str(e.reason))
    except (OSError, EOFError, zlib.error, http.client.HTTPException) as e:
        return Response(error=f'{type(e).__name__}: {e}')


def _decode(body, headers):
    """Undo the Content-Encoding of a compressed answer."""
    encoding = (headers.get('content-encoding') or '').lower()
    if not body:
        return body
    if encoding == 'gzip':
        return gzip.decompress(body)
    if encoding == 'deflate':
        try:
            return zlib.decompress(body)
        except zlib.error:
            return zlib.decompress(body, -zlib.MAX_WBITS)      # raw deflate, sent by some servers
    return body


def _lower(headers):
    return {k.lower(): v for k, v in (headers.items() if headers else [])}


def now_iso():
    """Current UTC time, to the second: 2026-11-15T21:12:38+00:00."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path, default=None):
    """Parsed JSON file, or `default` when it is missing or unreadable."""
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def write_json(path, payload, **dump_args):
    """Write JSON through a temp file + rename, so an interrupted step can
    never leave a truncated file for the commit step to pick up."""
    dump_args.setdefault('ensure_ascii', False)
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix='.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, **dump_args)
            f.write('\n')
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_json_if_changed(path, payload, **dump_args):
    """Write the file only when what it holds would change, so that a file of
    which a run brings nothing new is not rewritten and does not go into the
    commit. Returns whether it was written."""
    if load_json(path) == payload:
        return False
    write_json(path, payload, **dump_args)
    return True


# --- What the newer datasets share ---
#
# balancing-prices.json, the files of outages/, wind-solar-forecast.json and
# low-prices.json (fetch_balancing.py, fetch_outages.py, fetch_forecast.py,
# fetch_low_prices.py) are made by steps of their own, after the files the
# site already reads have been published, each with its own time budget. What
# they have in common is here.
#
# Licences. Each block of those files says under which terms its data may be
# reused, because only part of them may go into paid plans. A block never
# mixes data of different licences:
#
#   cc-by-4.0      on ENTSO-E's "List of data available for free re-use"
#                  (version of 18 October 2023): the prices of activated
#                  balancing energy (17.1.F) and the unavailability of
#                  transmission (10.1.A and B);
#   omie           everything that comes from OMIE's files;
#   source-terms   ENTSO-E data that are not on that list (the PICASSO
#                  series, the unavailability of generation and production
#                  units, the forecasts, actual generation, day-ahead prices):
#                  the terms of the source apply.
CC_BY = 'cc-by-4.0'
OMIE_TERMS = 'omie'
SOURCE_TERMS = 'source-terms'

FETCHED_AT = 'fetched_at'          # as in carry_over.py: when an entry's data were obtained
CARRIED_OVER = 'carried_over'      # true on an entry the source did not answer for in the last run
DATA_TIME = 'data_time'            # the start of the last interval an entry has data for

ENTSOE_API = 'https://web-api.tp.entsoe.eu/api'
ENTSOE_REFUSED = (401, 429)        # the key was rejected, or the token is suspended
HOURLY = timedelta(minutes=50)     # "once an hour", for runs 15 minutes apart: every fourth run
DAILY = timedelta(hours=23)        # how often an area ENTSO-E has no data for is asked again


def parse_time(value):
    """An ISO 8601 time as an aware datetime, or None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def iso(moment):
    """A time as the newer files write it: UTC, to the second."""
    return moment.astimezone(timezone.utc).replace(microsecond=0).isoformat()


class Budget:
    """The time a step may spend on its requests, counted from when it starts."""

    def __init__(self, seconds, clock=time.monotonic):
        self._clock = clock
        self.seconds = seconds
        self._end = clock() + seconds

    def left(self):
        return self._end - self._clock()

    def spent(self):
        return self.seconds - self.left()


def due(entry, now, every=HOURLY):
    """True when an entry is to be asked for again: it is not there, the source
    did not answer for it last time, or it was obtained `every` ago or more."""
    if not isinstance(entry, dict) or entry.get(CARRIED_OVER):
        return True
    obtained = parse_time(entry.get(FETCHED_AT))
    return obtained is None or now - obtained >= every


def asked_recently(checked, now, every=DAILY):
    """True when `checked` (the time ENTSO-E last said it had no data for
    something) is less than `every` ago: not worth asking again yet."""
    when = parse_time(checked)
    return when is not None and timedelta(0) <= now - when < every


def entsoe_ask(jobs, budget, get=http_get, key=None, workers=8, interval=0.25, timeout=40, label='ENTSO-E'):
    """Send the (key, name, params) jobs to ENTSO-E, a few at a time, within
    `budget`. Returns {key: Response} for the jobs that were sent; a job there
    was no time left for is absent, and so are those not sent once ENTSO-E has
    refused the API key (HTTP 401) or suspended it (HTTP 429).

    A request that was still waiting for its answer when the budget ended is
    not a failure of ENTSO-E's: it is left out too, as one that was not sent.

    Each request writes one line to the log: its name, what came back and how
    long it took. Never the address, which carries the API key."""
    api_key = key if key is not None else os.environ.get('ENTSOE_API_KEY', '')
    answers, lock = {}, threading.Lock()
    todo = queue.SimpleQueue()
    for job in jobs:
        todo.put(job)
    refused = threading.Event()
    next_slot = [time.monotonic()]

    def work():
        while not refused.is_set():
            try:
                job_key, name, params = todo.get_nowait()
            except queue.Empty:
                return
            with lock:
                slot = max(time.monotonic(), next_slot[0])
                next_slot[0] = slot + interval
            wait = slot - time.monotonic()
            if budget.left() - wait < 3:
                return                              # no time for an answer: left for the next run
            if wait > 0:
                time.sleep(wait)
            url = ENTSOE_API + '?' + urllib.parse.urlencode({'securityToken': api_key, **params})
            started = time.monotonic()
            left = budget.left()
            response = get(url, timeout=max(1.0, min(timeout, left)), max_seconds=max(1.0, min(timeout + 15, left)))
            if response.status is None and budget.left() < 1:
                # Cut short by the end of the budget, not by ENTSO-E: it counts as
                # not asked, and is left for the next run.
                with lock:
                    print(f'  {label}: {name}... cut short by the end of the time budget '
                          f'[{time.monotonic() - started:.1f}s]', flush=True)
                continue
            if response.status in ENTSOE_REFUSED:
                refused.set()
            with lock:
                answers[job_key] = response
                print(f'  {label}: {name}... {response.describe()} [{time.monotonic() - started:.1f}s]', flush=True)

    # Daemon threads: one stuck in a socket cannot keep the script from exiting.
    threads = [threading.Thread(target=work, daemon=True) for _ in range(max(1, min(workers, len(jobs))))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(max(0.0, budget.left() + 5))
    with lock:
        done = dict(answers)
    unsent = len(jobs) - len(done)
    if refused.is_set():
        print(f'  {label}: ENTSO-E is refusing the API key - the rest was not asked for', flush=True)
    elif unsent:
        print(f'  {label}: {unsent} of {len(jobs)} requests left for the next run (out of time)', flush=True)
    return done


# What an answer of ENTSO-E has: `documents` are its XML documents (the answer
# itself, or the files of the zip the outage documents come in); `problem` is
# None, or why there are none. No documents and no problem: ENTSO-E answered
# that it has no data for what was asked.
Answer = namedtuple('Answer', 'documents problem')


def entsoe_answer(response):
    """Read a Response of ENTSO-E into an Answer."""
    if response is None:
        return Answer([], 'not asked')
    if response.status is None:
        return Answer([], f'no answer ({response.error})')
    body = response.body
    if body[:2] == b'PK':
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                return Answer([archive.read(name) for name in archive.namelist()], None)
        except (zipfile.BadZipFile, OSError, zlib.error) as e:
            return Answer([], f'a zip that cannot be read ({type(e).__name__})')
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return Answer([], f'HTTP {response.status}, not XML')
    if local_name(root.tag) == 'Acknowledgement_MarketDocument':
        reason = ' '.join((root.findtext('{*}Reason/{*}text') or '').split())
        if 'No matching data' in reason:
            return Answer([], None)
        return Answer([], f'HTTP {response.status}: {reason[:160]}')
    if response.status != 200:
        return Answer([], f'HTTP {response.status}')
    return Answer([body], None)


def local_name(tag):
    """An element's name without its namespace, which changes with the document."""
    return tag.rsplit('}', 1)[-1]


def step_of(text):
    """The length of a Period's intervals: 'PT15M' -> 15 minutes, 'PT4S' -> 4
    seconds. None when it cannot be read."""
    found = re.fullmatch(r'PT(\d+)([SMH])', (text or '').strip())
    if not found or int(found.group(1)) == 0:
        return None
    return timedelta(**{{'S': 'seconds', 'M': 'minutes', 'H': 'hours'}[found.group(2)]: int(found.group(1))})


def period_intervals(period, value_tag, variable=True):
    """(start, value) of every interval of a Period element, in order: the
    value of each as a float, or None where the document has none.

    With curveType A03 (`variable`) a position that is left out repeats the
    point before it: its value or, for a point written with a position and
    nothing else, its lack of one. With a fixed curve a position left out is
    an interval with no value. Yields nothing for a Period that cannot be read."""
    start = parse_time(period.findtext('{*}timeInterval/{*}start'))
    end = parse_time(period.findtext('{*}timeInterval/{*}end'))
    step = step_of(period.findtext('{*}resolution'))
    if start is None or step is None:
        return
    points = {}
    for point in period.findall('{*}Point'):
        try:
            position = int(point.findtext('{*}position'))
        except (TypeError, ValueError):
            continue
        try:
            points[position] = float(point.findtext('{*}' + value_tag))
        except (TypeError, ValueError):
            points[position] = None
    if not points:
        return
    count = max(points)
    if end is not None and end > start:
        count = max(count, int((end - start) / step)) if variable else int((end - start) / step)
    value = None
    for position in range(1, count + 1):
        if position in points:
            value = points[position]
        elif not variable:
            value = None
        yield start + step * (position - 1), value


def carry(previous, obtained, failed, fetched_at, may_keep):
    """The entries of a block after a run, by the feed's rule for kept values
    (carry_over.py), for blocks whose entries are not all asked for in every run.

    previous    {name: entry} as published before the run
    obtained    {name: entry} obtained in this run
    failed      names asked for in this run that the source did not answer for
    fetched_at  the time of this run
    may_keep    (name, entry) -> whether a published entry may still be shown

    An entry obtained now is stamped with `fetched_at`. One that was asked for
    and not answered keeps what was published, with the time it was obtained
    and the mark carried_over. One that was not asked for in this run (it is
    read once an hour) stays exactly as it was. Either stays only while
    `may_keep` allows it.

    Returns (entries, kept, dropped): `kept` the names marked carried_over,
    `dropped` those left out."""
    entries, kept, dropped = {}, [], []
    for name in list(dict.fromkeys(list(previous) + list(obtained))):
        if name in obtained:
            entry = dict(obtained[name])
            entry.pop(CARRIED_OVER, None)
            entry[FETCHED_AT] = fetched_at
            entries[name] = entry
            continue
        before = previous[name]
        if not isinstance(before, dict) or not may_keep(name, before):
            dropped.append(name)
            continue
        entry = dict(before)
        if name in failed:
            entry[CARRIED_OVER] = True
            kept.append(name)
        entries[name] = entry
    return entries, kept, dropped


def latest(times):
    """The latest of some ISO 8601 times, as it was written, or None."""
    known = [t for t in times if parse_time(t) is not None]
    return max(known, key=parse_time) if known else None
