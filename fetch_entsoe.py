"""
EU Power Data — ENTSO-E data fetcher
Runs via GitHub Actions every 15 minutes.
Outputs JSON files consumed by eupowerdata.com

One run makes about 150 requests (36 price zones, 36 generation zones, 76 flow
corridors, plus OMIP). They are sent in parallel, under a total time budget,
and each file is written as soon as its dataset is done — see "Run limits".

Prices are those of the delivery day in course on the market's clock (Central
European Time), one value for each hour of that day — see "The prices of the
day". Generation and flows are asked for the last 24 hours, and each zone and
corridor shows the last value ENTSO-E has published for it, with the time that
value is for (data_time): just after midnight most countries have nothing for
the new day yet, and a request "since midnight" came back empty for them — see
"The last value".

A zone or a corridor that does not answer in a run keeps what was published
for it before, with the time it was obtained (fetched_at) and a mark
(carried_over): a price while it is of the delivery day in course, generation
and flows if they were obtained less than three hours before, and never with
the time of the run that kept it — see carry_over.py and publish().
"""

import os
import re
import sys
import json
import queue
import tempfile
import threading
import time
import http.client
import urllib.error
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from xml.etree import ElementTree as ET

import carry_over
from carry_over import CARRIED_OVER, DATA_TIME, DELIVERY_DATE, FETCHED_AT, MARKET_TZ
from feed_common import load_json

API_KEY = os.environ.get('ENTSOE_API_KEY', '')
BASE_URL = 'https://web-api.tp.entsoe.eu/api'
ALT_URLS = [
    'https://web-api.tp.entsoe.eu/api',
    'https://transparency.entsoe.eu/api',
    'https://webportal.tp.entsoe.eu/api',
]

ZONES = {
    'PT': '10YPT-REN------W',
    'ES': '10YES-REE------0',
    'DE': '10Y1001A1001A82H',
    'FR': '10YFR-RTE------C',
    'IT': '10Y1001A1001A73I',
    'NL': '10YNL----------L',
    'BE': '10YBE----------2',
    'AT': '10YAT-APG------L',
    'CH': '10YCH-SWISSGRIDZ',
    'PL': '10YPL-AREA-----S',
    'NO1': '10YNO-1--------2',
    'NO2': '10YNO-2--------T',
    'SE1': '10Y1001A1001A44P',
    'SE3': '10Y1001A1001A46L',
    'DK1': '10YDK-1--------W',
    'DK2': '10YDK-2--------M',
    'FI': '10YFI-1--------U',
    'GR': '10YGR-HTSO-----Y',
    'IE': '10Y1001A1001A59C',
    'RO': '10YRO-TEL------P',
    'BG': '10YCA-BULGARIA-R',
    'HU': '10YHU-MAVIR----U',
    'CZ': '10YCZ-CEPS-----N',
    'EE': '10Y1001A1001A39I',
    'LV': '10YLV-1001A00074',
    'LT': '10YLT-1001A0008Q',
    'GB': '10YGB----------A',
    # Energy Community / South-East Europe (Phase 3 — ENTSO-E members/associates)
    'SI': '10YSI-ELES-----O',
    'HR': '10YHR-HEP------M',
    'RS': '10YCS-SERBIATSOV',
    'BA': '10YBA-JPCC-----D',
    'ME': '10YCS-CG-TSO---S',
    'MK': '10YMK-MEPSO----8',
    'AL': '10YAL-KESH-----5',
    'MD': '10Y1001A1001A990',
    'XK': '10Y1001C--00100H',
}

TIER1 = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE']

CONSOLIDATE = {
    'NO': ['NO1', 'NO2'],
    'SE': ['SE1', 'SE3'],
    'DK': ['DK1', 'DK2'],
}

GEN_COUNTRIES = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE', 'AT', 'CH', 'PL', 'FI', 'GR', 'IE', 'RO', 'BG', 'HU', 'CZ', 'EE', 'LV', 'LT', 'GB', 'SI', 'HR', 'RS', 'BA', 'ME', 'MK', 'AL', 'MD', 'XK']

CORRIDORS = [
    ('PT', 'ES'), ('ES', 'PT'), ('ES', 'FR'), ('FR', 'ES'),
    ('FR', 'DE'), ('DE', 'FR'), ('FR', 'GB'), ('GB', 'FR'),
    ('DE', 'NL'), ('NL', 'DE'), ('DE', 'AT'), ('AT', 'DE'),
    ('FR', 'IT'), ('IT', 'FR'), ('NL', 'BE'), ('BE', 'NL'),
    ('DE', 'PL'), ('PL', 'DE'), ('DE', 'DK1'), ('DK1', 'DE'),
]

# Simplified zone codes for cross-border flow fetching
# Maps aggregated codes to representative EIC zone
FLOW_ZONES = {
    **ZONES,
    'NO': ZONES['NO2'],   # Southern Norway (main exchange zone)
    'SE': ZONES['SE3'],   # Southern Sweden (most liquid)
    'DK': ZONES['DK1'],   # Western Denmark (mainland)
}

# Flow corridors use simplified codes (DK not DK1, NO/SE/FI included)
# These must match the pair keys expected by eew-grid.js
FLOW_CORRIDORS = [
    ('PT', 'ES'), ('ES', 'PT'), ('ES', 'FR'), ('FR', 'ES'),
    ('FR', 'DE'), ('DE', 'FR'), ('FR', 'GB'), ('GB', 'FR'),
    ('DE', 'NL'), ('NL', 'DE'), ('DE', 'AT'), ('AT', 'DE'),
    ('FR', 'IT'), ('IT', 'FR'), ('NL', 'BE'), ('BE', 'NL'),
    ('DE', 'PL'), ('PL', 'DE'), ('DE', 'DK'), ('DK', 'DE'),
    ('NO', 'SE'), ('SE', 'NO'), ('NO', 'DK'), ('DK', 'NO'),
    ('SE', 'FI'), ('FI', 'SE'), ('SE', 'DK'), ('DK', 'SE'),
    ('NO', 'GB'), ('GB', 'NO'),
    ('AT', 'HU'), ('HU', 'AT'),
    ('HU', 'RO'), ('RO', 'HU'),
    ('RO', 'BG'), ('BG', 'RO'),
    ('BG', 'GR'), ('GR', 'BG'),
    # Baltic states — synchronised with continental Europe since Feb 2025
    # via LitPol Link (LT↔PL); Nordic links via NordBalt (LT↔SE) and EstLink (EE↔FI).
    ('EE', 'FI'), ('FI', 'EE'),
    ('EE', 'LV'), ('LV', 'EE'),
    ('LV', 'LT'), ('LT', 'LV'),
    ('LT', 'PL'), ('PL', 'LT'),
    ('LT', 'SE'), ('SE', 'LT'),
    # Phase A inc. 3 — Energy Community / SE Europe corridors.
    # Endpoints already in ZONES (inc.1). Corridors the ENTSO-E feed does not
    # serve are skipped server-side (no data) and never render anywhere; the
    # display subset is wired into eew-grid.js only for corridors verified via
    # a fetch.yml run.
    # Anchors to core-EU nodes
    ('SI', 'IT'), ('IT', 'SI'), ('SI', 'AT'), ('AT', 'SI'),
    ('SI', 'HU'), ('HU', 'SI'), ('HR', 'HU'), ('HU', 'HR'),
    ('RS', 'HU'), ('HU', 'RS'), ('RS', 'RO'), ('RO', 'RS'),
    ('RS', 'BG'), ('BG', 'RS'), ('ME', 'IT'), ('IT', 'ME'),
    ('MK', 'BG'), ('BG', 'MK'), ('MK', 'GR'), ('GR', 'MK'),
    ('AL', 'GR'), ('GR', 'AL'),
    # Internal SE-Europe mesh
    ('SI', 'HR'), ('HR', 'SI'), ('HR', 'RS'), ('RS', 'HR'),
    ('HR', 'BA'), ('BA', 'HR'), ('RS', 'BA'), ('BA', 'RS'),
    ('RS', 'ME'), ('ME', 'RS'), ('RS', 'MK'), ('MK', 'RS'),
    ('RS', 'XK'), ('XK', 'RS'), ('BA', 'ME'), ('ME', 'BA'),
    ('ME', 'AL'), ('AL', 'ME'), ('MK', 'AL'), ('AL', 'MK'),
    ('MK', 'XK'), ('XK', 'MK'), ('AL', 'XK'), ('XK', 'AL'),
]

PSR_MAP = {
    'B01': 'Biomass', 'B02': 'Fossil Brown coal/Lignite',
    'B03': 'Fossil Coal-derived gas', 'B04': 'Fossil Gas',
    'B05': 'Fossil Hard coal', 'B06': 'Fossil Oil',
    'B09': 'Geothermal', 'B10': 'Hydro Pumped Storage',
    'B11': 'Hydro Run-of-river', 'B12': 'Hydro Water Reservoir',
    'B13': 'Marine', 'B14': 'Nuclear',
    'B15': 'Other renewable', 'B16': 'Solar',
    'B17': 'Waste', 'B18': 'Wind Offshore',
    'B19': 'Wind Onshore', 'B20': 'Other',
}

RENEWABLE_TYPES = [
    'Biomass', 'Geothermal', 'Hydro Run-of-river', 'Hydro Water Reservoir',
    'Marine', 'Solar', 'Wind Offshore', 'Wind Onshore', 'Other renewable',
]

# The order of the zones and corridors in the published files: the ENTSO-E
# zones as listed above, then the countries put together from several zones,
# then the zones that fetch_gb.py fills in from Elexon after this script. A
# zone kept from an earlier run (see carry_over.py) goes back to its place.
ELEXON_ZONES = ('GB',)
SUBZONES = {zone for zones in CONSOLIDATE.values() for zone in zones}
PRICE_ORDER = ([code for code in ZONES if code not in SUBZONES and code not in ELEXON_ZONES]
               + list(CONSOLIDATE) + list(ELEXON_ZONES))
GEN_ORDER = ([code for code in GEN_COUNTRIES if code not in ELEXON_ZONES]
             + list(CONSOLIDATE) + list(ELEXON_ZONES))
FLOW_ORDER = [f'{frm}→{to}' for frm, to in FLOW_CORRIDORS]


def utcnow():
    """The current time, in UTC. The run's day and the files' `updated` come from here."""
    return datetime.now(timezone.utc)


# --- Run limits ---
#
# The requests used to go out one after another and the files were only written
# at the very end of main(), so on a slow ENTSO-E day the run hit the job's time
# limit and everything it had fetched was lost. Now:
#   - requests run in parallel, throttled well below ENTSO-E's limit of 400
#     requests per minute per API token (a breach suspends the token for about
#     10 minutes): https://transparencyplatform.zendesk.com/hc/en-us/articles/12783148966036
#   - every request has a hard time limit, so a slow or dead zone only costs
#     its own slot and never holds the others back;
#   - the run has a total time budget, shared by the three datasets;
#   - each file is written as soon as its dataset is done.
#
# The time limits are sized for how ENTSO-E has been answering since late
# October 2026: roughly half of the requests, even ones it rejects outright,
# sit for about 15 seconds before the first byte arrives, and the rest answer
# in under a second. A request therefore needs well over 15 s, and it takes
# many requests in flight to finish a run in a couple of minutes.
TOTAL_BUDGET_S = float(os.environ.get('FETCH_BUDGET_SECONDS', '300'))
MAX_WORKERS = 16         # requests in flight at the same time
MIN_INTERVAL_S = 0.25    # between request starts: 240 requests per minute at most
SOCKET_TIMEOUT_S = 45    # connecting, and each read
REQUEST_MAX_S = 60       # one attempt, however slowly the bytes arrive
ATTEMPTS = 2             # one retry after a connection error or an HTTP 5xx (never after a timeout)
GRACE_S = 5              # how long a dataset waits past its deadline for stragglers
PROBE_MAX_S = 5          # how long the test of the endpoint waits for its answer (see find_working_endpoint)
OUT_OF_TIME = 'time budget exhausted'   # why a request was dropped, as shown in the log


# --- HTTP ---

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

_print_lock = threading.Lock()


def log(msg):
    """print() that stays one whole line when several threads are writing."""
    with _print_lock:
        print(msg, flush=True)


class _Throttle:
    """Spaces out request starts across all threads."""

    def __init__(self, min_interval):
        self._lock = threading.Lock()
        self._min = min_interval
        self._next = 0.0

    def wait(self, deadline=None):
        """Sleep until this request's slot. False if the slot is past `deadline`."""
        with self._lock:
            slot = max(time.monotonic(), self._next)
            if deadline is not None and slot >= deadline:
                return False
            self._next = slot + self._min
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        return True


_throttle = _Throttle(MIN_INTERVAL_S)

# Set when ENTSO-E answers 401 (key rejected) or 429 (token suspended): sending
# the rest of the run's requests would only fail too, or prolong the suspension.
_stop = threading.Event()


def _http_once(req, limit, socket_timeout, context=None):
    """One attempt. Raises on any failure, including running past `limit`."""
    remaining = limit - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(OUT_OF_TIME)
    with urllib.request.urlopen(req, timeout=min(socket_timeout, remaining), context=context) as r:
        chunks = []
        while True:
            chunk = r.read1(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if time.monotonic() > limit:
                raise TimeoutError('response too slow')
        return b''.join(chunks).decode('utf-8')


def _timeout_reason(reason, deadline):
    """Tell a request cut short by the run's budget from a zone that is not answering."""
    if deadline is not None and time.monotonic() >= deadline - 0.1:
        return OUT_OF_TIME
    return reason


def _get(url, deadline=None, socket_timeout=SOCKET_TIMEOUT_S, max_s=REQUEST_MAX_S):
    """GET with a hard time limit. Returns (body, None) or (None, reason).

    The reason never includes the URL, which carries the API key.
    """
    headers = {
        'Accept': 'application/xml',
        'User-Agent': 'EUPowerData-GitHubAction/1.0',
    }
    req = urllib.request.Request(url, headers=headers)
    reason = 'not attempted'
    context = None
    for _ in range(ATTEMPTS):
        if not _throttle.wait(deadline):
            return None, OUT_OF_TIME
        limit = time.monotonic() + max_s
        if deadline is not None:
            limit = min(limit, deadline)
        try:
            return _http_once(req, limit, socket_timeout, context), None
        except urllib.error.HTTPError as e:
            reason = f'HTTP {e.code}'
            if e.code < 500:
                return None, reason      # the answer will not change on a retry
        except urllib.error.URLError as e:
            reason = str(e.reason)
            if isinstance(e.reason, TimeoutError):
                return None, _timeout_reason(reason, deadline)
            if isinstance(e.reason, ssl.SSLCertVerificationError):
                context = ctx            # as before: retry without certificate verification
        except TimeoutError as e:
            # A zone that is not answering: do not wait for it twice.
            return None, _timeout_reason(f'timeout: {e}', deadline)
        except (OSError, http.client.HTTPException, UnicodeDecodeError) as e:
            reason = f'{type(e).__name__}: {e}'
    return None, reason


def http_get(url, timeout=45):
    """Single GET, returning the body or None. Used by backfill_archive.py."""
    text, reason = _get(url, socket_timeout=timeout, max_s=2 * timeout)
    if text is None:
        print(f'  HTTP failed: {reason}')
    return text


def fetch_all(label, jobs, parser, describe, deadline):
    """Send every (key, name, url) job in parallel and parse each answer.

    Returns {key: parsed} for the jobs that produced data. A job that fails,
    stalls or is still running at `deadline` is simply left out.
    """
    started = time.monotonic()
    results = {}
    counts = {'ok': 0, 'no data': 0, 'failed': 0, 'out of time': 0}
    lock = threading.Lock()
    todo = queue.SimpleQueue()
    for job in jobs:
        todo.put(job)

    def work():
        while True:
            try:
                key, name, url = todo.get_nowait()
            except queue.Empty:
                return
            t0 = time.monotonic()
            if _stop.is_set():
                text, reason = None, 'skipped: ENTSO-E is refusing this API key'
            else:
                text, reason = _get(url, deadline)
            if text is None and reason == OUT_OF_TIME:
                with lock:
                    counts['out of time'] += 1
                continue                 # counted in the summary, not one line each
            if text is None:
                if reason in ('HTTP 401', 'HTTP 429'):
                    _stop.set()
                outcome, line = 'failed', f'FAIL ({reason})'
            else:
                parsed = parser(text)
                if parsed:
                    with lock:
                        results[key] = parsed
                    outcome, line = 'ok', f'OK ({describe(parsed)})'
                else:
                    outcome, line = 'no data', 'no data'
            with lock:
                counts[outcome] += 1
            log(f'  {label}: {name}... {line} [{time.monotonic() - t0:.1f}s]')

    # Daemon threads: one stuck in a socket cannot keep the script from exiting.
    threads = [threading.Thread(target=work, daemon=True)
               for _ in range(min(MAX_WORKERS, len(jobs)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(max(0.0, deadline + GRACE_S - time.monotonic()))

    with lock:
        done = dict(results)
        tally = dict(counts)
    out_of_time = tally['out of time'] + len(jobs) - sum(tally.values())
    log(f"  {label}: {len(jobs)} requests in {time.monotonic() - started:.0f}s — "
        f"{tally['ok']} ok, {tally['no data']} no data, {tally['failed']} failed, "
        f"{out_of_time} out of time")
    return done


def write_json(path, payload, **dump_args):
    """Write JSON through a temp file + rename, so a run that is interrupted
    mid-write can never leave a truncated file for the commit step to pick up."""
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or '.', prefix='.', suffix='.tmp')
    try:
        with os.fdopen(fd, 'w') as f:
            json.dump(payload, f, **dump_args)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# What a failed request says about going on with the run.
REFUSED = ('HTTP 401', 'HTTP 429')      # the key was rejected, or the token is suspended


def _slow(reason):
    """True when a request failed for lack of an answer in time, as opposed to
    an answer that says something is wrong."""
    return reason == OUT_OF_TIME or 'timed out' in (reason or '') or (reason or '').startswith('timeout')


def find_working_endpoint(deadline=None):
    """Which endpoint this run uses, or None when ENTSO-E is refusing the API
    key and there is no point in going on.

    One small request (today's prices for Portugal) is sent to the main
    endpoint. Its answer chooses the endpoint; it does not decide whether the
    run goes ahead. On 16 Nov 2026 (run 2375) that one request took over 45
    seconds on a morning when ENTSO-E was slow but answering most requests,
    and the run ended there without asking for prices, generation or flows.
    Now:

      - an answer with data: the endpoint is confirmed;
      - no answer in PROBE_MAX_S: ENTSO-E is slow, not gone. The run goes on
        with the main endpoint and brings what it can within its time budget;
        what does not answer is kept by the usual rules (carry_over.py). The
        alternatives are not tried: they would only take more of the budget;
      - HTTP 401 or 429: the key is rejected or the token suspended. Every
        other request would get the same answer (or prolong the suspension),
        so the run ends here: None;
      - anything else (connection refused, HTTP 5xx, a page that is not data):
        the alternatives get one short try each and, if none of them works
        either, the run still goes on with the main endpoint.

    Whether ENTSO-E gave anything is settled by the requests that matter, at
    the end of main(): with nothing at all the step fails, as before.

    The wait is short on purpose. Its time comes out of the run's budget, and
    since the run goes on either way, waiting for the test buys nothing but the
    confirmation. On a good day ENTSO-E answers in under a second and 5 seconds
    are plenty; on a slow one it sits on an answer for 20 seconds and more (a
    first limit of 20 s never got one, in any of the four runs of the afternoon
    of 16 Nov 2026), and those seconds are better spent on the data."""
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'
    params = urllib.parse.urlencode({
        'securityToken': API_KEY,
        'documentType': 'A44',
        'in_Domain': '10YPT-REN------W',
        'out_Domain': '10YPT-REN------W',
        'periodStart': start,
        'periodEnd': end,
    })
    main_endpoint = ALT_URLS[0]
    for i, base in enumerate(ALT_URLS):
        print(f'Testing endpoint: {base}')
        limit = time.monotonic() + (PROBE_MAX_S if i == 0 else 10)
        if deadline is not None:
            limit = min(limit, deadline)
        xml, reason = _get(f'{base}?{params}', limit)
        if xml and 'TimeSeries' in xml:
            print(f'  Working!')
            return base
        if i == 0 and _slow(reason):
            print(f'  No answer in {PROBE_MAX_S} s: ENTSO-E is slow, not gone — going on with {main_endpoint}')
            return main_endpoint
        print(f'  Failed ({reason or "no TimeSeries in the answer"})')
        if reason in REFUSED:
            print(f'  ENTSO-E is refusing the API key ({reason}) — check the ENTSOE_API_KEY secret')
            return None
    print(f'  No endpoint confirmed — going on with {main_endpoint} all the same')
    return main_endpoint


# --- Parsers ---

def get_ns(root):
    tag = root.tag
    if '{' in tag:
        return tag[1:tag.index('}')]
    return ''

def parse_generation_xml(xml_str):
    """Parse ENTSO-E A75 (Aggregated Generation per Type) response.

    For each TimeSeries (one per generation type), pick the value at the
    latest timestamp <= now. The previous implementation iterated all
    points and kept the LAST in the array, which for a 24h request window
    typically pointed to the last hour of the day (23:00). At 23:00 solar
    is 0, so 'live mix' showed solar = 0 even mid-day on sunny days.

    Fallback: if no point has timestamp <= now (e.g. only forecasts are
    available — happens for PT/REN with publication latency), use the
    earliest future point instead. Without this fallback, the country
    would be entirely missing from the output for that fetch.

    Resolution can be PT15M, PT30M or PT60M depending on the country —
    parsed dynamically per Period.
    """
    mix = {}
    now = datetime.now(timezone.utc)
    try:
        root = ET.fromstring(xml_str)
        ns = get_ns(root)
        prefix = f'{{{ns}}}' if ns else ''
        for ts in root.iter(f'{prefix}TimeSeries'):
            psr_el = ts.find(f'{prefix}MktPSRType/{prefix}psrType')
            if psr_el is None:
                continue
            psr_code = psr_el.text
            gen_type = PSR_MAP.get(psr_code, 'Other')

            best_val = None
            best_ts  = None
            # Fallback for "all forecast" responses: track earliest future point
            fallback_val = None
            fallback_ts  = None

            for period in ts.iter(f'{prefix}Period'):
                start_el = period.find(f'{prefix}timeInterval/{prefix}start')
                if start_el is None:
                    continue
                try:
                    start_dt = datetime.fromisoformat(start_el.text.replace('Z', '+00:00'))
                except Exception:
                    continue

                res_el = period.find(f'{prefix}resolution')
                res_min = 60
                if res_el is not None and res_el.text:
                    if res_el.text == 'PT15M':
                        res_min = 15
                    elif res_el.text == 'PT30M':
                        res_min = 30

                for pt in period.iter(f'{prefix}Point'):
                    pos_el = pt.find(f'{prefix}position')
                    qty_el = pt.find(f'{prefix}quantity')
                    if pos_el is None or qty_el is None:
                        continue
                    try:
                        pos = int(pos_el.text)
                        qty = float(qty_el.text)
                    except (TypeError, ValueError):
                        continue
                    point_ts = start_dt + timedelta(minutes=res_min * (pos - 1))
                    if point_ts <= now:
                        if best_ts is None or point_ts > best_ts:
                            best_ts  = point_ts
                            best_val = qty
                    else:
                        if fallback_ts is None or point_ts < fallback_ts:
                            fallback_ts  = point_ts
                            fallback_val = qty

            # Prefer historical point; fall back to earliest future point so
            # countries with only forecast data (PT via REN, occasionally) still
            # appear in the output.
            chosen = best_val if best_val is not None else fallback_val
            if chosen is not None:
                mix[gen_type] = mix.get(gen_type, 0) + chosen
    except Exception as e:
        print(f'  XML parse error: {e}')
    return mix


def parse_flow_xml(xml_str):
    values = []
    try:
        root = ET.fromstring(xml_str)
        ns = get_ns(root)
        prefix = f'{{{ns}}}' if ns else ''
        for ts in root.iter(f'{prefix}TimeSeries'):
            for period in ts.iter(f'{prefix}Period'):
                start_el = period.find(f'{prefix}timeInterval/{prefix}start')
                if start_el is None:
                    continue
                start_dt = datetime.fromisoformat(start_el.text.replace('Z', '+00:00'))
                for pt in period.iter(f'{prefix}Point'):
                    pos = int(pt.find(f'{prefix}position').text)
                    qty = float(pt.find(f'{prefix}quantity').text)
                    hour_dt = start_dt + timedelta(hours=pos - 1)
                    values.append({
                        'time': hour_dt.strftime('%H:%M'),
                        'mw': round(qty),
                    })
    except Exception as e:
        print(f'  XML parse error: {e}')
    return values


# --- The last value ---
#
# Generation and flows are asked for the last 24 hours and each zone and
# corridor shows the last value ENTSO-E has published for it, with the time
# that value is for. The two parsers above are left as they are for
# backfill_archive.py, which reads one whole day at a time; the functions
# below read the same documents with what a rolling window needs:
#
#   - the real time of every point. A Period says how long its intervals are
#     (PT15M, PT30M, PT60M); parse_flow_xml() counts every position as an hour.
#   - curveType A03. ENTSO-E leaves out a point that repeats the one before
#     it, so the last point written stands for every interval up to the end of
#     the Period: the value is that point's, the time is the last interval's.
#   - a series cut in pieces. One technology can come as several TimeSeries
#     (a gap, a change of resolution). Only the piece with the latest value
#     counts: adding the last value of each piece would count it twice.

def last_day(now):
    """(periodStart, periodEnd) for the last 24 hours, in whole hours: from 24
    hours before the hour `now` is in to the end of that hour."""
    hour = now.replace(minute=0, second=0, microsecond=0)
    return ((hour - timedelta(hours=24)).strftime('%Y%m%d%H%M'),
            (hour + timedelta(hours=1)).strftime('%Y%m%d%H%M'))


def _step(text):
    """The length of a Period's intervals: 'PT15M' -> 15 minutes. An hour when
    it cannot be read, as the parsers above assume."""
    found = re.fullmatch(r'PT(\d+)([MH])', (text or '').strip())
    if not found or int(found.group(1)) == 0:
        return timedelta(hours=1)
    return timedelta(**{'minutes' if found.group(2) == 'M' else 'hours': int(found.group(1))})


def _instant(element):
    """The time in a <start> or <end> element, or None."""
    try:
        parsed = datetime.fromisoformat(element.text.strip().replace('Z', '+00:00'))
    except (AttributeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def read_series(xml_str):
    """The TimeSeries of an ENTSO-E answer as plain data, in document order:

        {'psr': 'B16' | None,        the technology (generation)
         'out': bool,                consumption (pumped storage), not generation
         'contract': 'A01' | None,   the kind of market a price is of (A01 day-ahead, A07 intraday)
         'sequence': 1 | None,       which auction of that market, when a zone has more than one
         'periods': [{'start', 'end', 'step', 'variable', 'points': [(position, value)]}]}

    The value of a point is its quantity (generation, flows) or its price
    (prices). `variable` is curveType A03. An answer that is not such a
    document (an error page, "no matching data") gives an empty list."""
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return []
    ns = get_ns(root)
    prefix = f'{{{ns}}}' if ns else ''
    series = []
    for ts in root.iter(f'{prefix}TimeSeries'):
        psr = ts.find(f'{prefix}MktPSRType/{prefix}psrType')
        curve = ts.find(f'{prefix}curveType')
        variable = curve is not None and (curve.text or '').strip() == 'A03'
        periods = []
        for period in ts.iter(f'{prefix}Period'):
            start = _instant(period.find(f'{prefix}timeInterval/{prefix}start'))
            if start is None:
                continue
            resolution = period.find(f'{prefix}resolution')
            points = []
            for pt in period.iter(f'{prefix}Point'):
                value = pt.find(f'{prefix}quantity')
                if value is None:
                    value = pt.find(f'{prefix}price.amount')
                try:
                    points.append((int(pt.find(f'{prefix}position').text), float(value.text)))
                except (AttributeError, TypeError, ValueError):
                    continue
            periods.append({
                'start': start,
                'end': _instant(period.find(f'{prefix}timeInterval/{prefix}end')),
                'step': _step(resolution.text if resolution is not None else None),
                'variable': variable,
                'points': sorted(points),
            })
        contract = ts.find(f'{prefix}contract_MarketAgreement.type')
        sequence = ts.find(f'{prefix}classificationSequence_AttributeInstanceComponent.position')
        try:
            sequence = int(sequence.text)
        except (AttributeError, TypeError, ValueError):
            sequence = None
        series.append({
            'psr': psr.text.strip() if psr is not None and psr.text else None,
            'out': ts.find(f'{prefix}outBiddingZone_Domain.mRID') is not None,
            'contract': contract.text.strip() if contract is not None and contract.text else None,
            'sequence': sequence,
            'periods': periods,
        })
    return series


def _in_force(period, now):
    """(time, quantity) of the value a Period has for `now`: its last point
    that has started, and the start of the interval it is for. None when the
    Period only has later points."""
    start, step = period['start'], period['step']
    at = quantity = None
    for position, value in period['points']:
        point_time = start + step * (position - 1)
        if point_time > now:
            break
        at, quantity = point_time, value
    if at is None:
        return None
    if period['variable'] and period['end'] is not None:
        # The point stands for every interval after it, up to the end of the Period.
        at = max(at, min(period['end'] - step, start + step * ((now - start) // step)))
    return at, quantity


def _first_after(period, now):
    """(time, quantity) of a Period's first point after `now`, or None."""
    for position, value in period['points']:
        point_time = period['start'] + period['step'] * (position - 1)
        if point_time > now:
            return point_time, value
    return None


def _values_at(all_series, now, ahead=True):
    """The value each series of an A75 answer has for `now`, one for each
    technology and direction, in document order:

        [(technology code, consumption?, time, MW)]

    A series is the latest point up to `now` or, with `ahead`, when it only
    has later ones, the earliest of those (as in parse_generation_xml()). A
    technology that comes in pieces, as several TimeSeries, counts once: by
    the piece with the latest value."""
    order = []                 # (technology code, consumption?) in document order
    current, later = {}, {}    # -> [(time, MW)], one for each TimeSeries
    for series in all_series:
        if series['psr'] is None:
            continue
        key = (series['psr'], series['out'])
        if key not in order:
            order.append(key)
        found = [v for v in (_in_force(p, now) for p in series['periods']) if v]
        if found:
            current.setdefault(key, []).append(max(found, key=lambda v: v[0]))
            continue
        found = [v for v in (_first_after(p, now) for p in series['periods']) if v] if ahead else []
        if found:
            later.setdefault(key, []).append(min(found, key=lambda v: v[0]))

    values = []
    for key in order:
        if key in current:
            at = max(t for t, _mw in current[key])       # the piece with the latest value
            pieces = current[key]
        elif key in later:
            at = min(t for t, _mw in later[key])
            pieces = later[key]
        else:
            continue
        values.append((key[0], key[1], at, sum(mw for t, mw in pieces if t == at)))
    return values


def _generating(values):
    """The MW of the generation series in a list of _values_at(), consumption left out."""
    return sum(mw for _code, consumption, _at, mw in values if not consumption)


def _last_with_generation(all_series, before):
    """_values_at() for the last interval before `before` in which the
    generation series add up to more than zero, or None when the answer has
    no such interval."""
    moments = set()
    for series in all_series:
        if series['psr'] is None or series['out']:
            continue
        for period in series['periods']:
            count = period['points'][-1][0] if period['points'] else 0
            if period['end'] is not None:
                count = max(count, int((period['end'] - period['start']) / period['step']))
            moments.update(period['start'] + period['step'] * i for i in range(min(count, 2000)))
    for moment in sorted((m for m in moments if m < before), reverse=True):
        values = _values_at(all_series, moment, ahead=False)
        if _generating(values) > 0:
            return values
    return None


def latest_generation(xml_str, now):
    """The generation mix an A75 answer has for `now`:

        {'mix': {technology: MW}, 'data_time': ISO time, 'newest': ISO time,
         'behind': [(technology, ISO time, MW)],
         'consumption': [(technology, ISO time, MW)][, 'zero_at': ISO time]}

    or None when it has no data. Each technology has the last value ENTSO-E
    published for it (as in parse_generation_xml(): the latest point up to
    `now` or, when it only has later ones, the earliest of those).

    `data_time` is the time of the oldest of those values and `newest` of the
    most recent: they differ when a country publishes some technologies later
    than others, and `behind` says which ones, for the run log.

    Consumption is not generation. ENTSO-E sends what a technology takes from
    the grid (the pumps of pumped storage, mostly) as series of their own, the
    ones with outBiddingZone_Domain instead of inBiddingZone_Domain. They used
    to be added to the generation of the same technology, so a country that
    was pumping 1 000 MW showed 1 000 MW more generation, all of it "hydro".
    They are left out of the mix, and so of the total and of the renewable
    share; `consumption` lists them for the run log. A country with nothing
    but consumption series has no mix: None.

    A latest interval that adds up to zero is not a country that stopped.
    Montenegro now and then publishes its newest interval with every
    technology at 0 and fills it in later, and showed 0 MW meanwhile. When the
    last values add up to zero the mix is read from the last interval before
    it in which they do not, with the time of that interval (`zero_at` says
    which interval was passed over); with no such interval in the answer, the
    country has no mix: None, and counts as not obtained in the run."""
    all_series = read_series(xml_str)
    values = _values_at(all_series, now)
    generation = [value for value in values if not value[1]]
    if not generation:
        return None
    zero_at = None
    if _generating(values) <= 0:
        zero_at = max(at for _code, _consumption, at, _mw in generation)
        values = _last_with_generation(all_series, zero_at)
        if values is None:
            return None
        generation = [value for value in values if not value[1]]
    mix = {}
    for code, _consumption, _at, mw in generation:
        name = PSR_MAP.get(code, 'Other')
        mix[name] = mix.get(name, 0) + mw
    newest = max(at for _code, _consumption, at, _mw in generation)
    found = {'zero_at': zero_at.isoformat()} if zero_at else {}
    return {
        **found,
        'mix': mix,
        DATA_TIME: min(at for _code, _consumption, at, _mw in generation).isoformat(),
        'newest': newest.isoformat(),
        'behind': [(PSR_MAP.get(code, 'Other'), at.isoformat(), mw)
                   for code, _consumption, at, mw in generation if at < newest],
        'consumption': [(PSR_MAP.get(code, 'Other'), at.isoformat(), mw)
                        for code, consumption, at, mw in values if consumption],
    }


def latest_flow(xml_str, now):
    """What an A11 answer has for one corridor:

        {'latest': {time, mw}, 'values': [{time, mw}], 'data_time': ISO time}

    or None when it has no data.

    `values` is what it always was, the points of the current UTC day as
    ENTSO-E writes them, and never has a point of the day before: just after
    midnight it is empty until ENTSO-E has something for the new day. When a
    Period that began the day before goes on into this one without a point at
    midnight (curveType A03: the value did not change), the value in force at
    midnight is given as the 00:00 point, which is what a request starting at
    midnight gets. `latest` is the last of `values` or, when the day has none
    yet, the last point of the day before. `data_time` is the start of the
    last interval the answer has a value for."""
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    points = {}          # time -> MW, as published
    covered = None
    for series in read_series(xml_str):
        for period in series['periods']:
            start, step = period['start'], period['step']
            for position, value in period['points']:
                points[start + step * (position - 1)] = value
            in_force = _in_force(period, now)
            if in_force is None:
                continue
            covered = in_force[0] if covered is None else max(covered, in_force[0])
            if period['variable'] and start < day <= in_force[0] and day not in points:
                points[day] = _in_force(period, day)[1]     # the day before's last point
    if not points or covered is None:
        return None

    def point(at):
        return {'time': at.strftime('%H:%M'), 'mw': round(points[at])}

    values = [point(at) for at in sorted(points) if at >= day]
    return {
        'latest': values[-1] if values else point(max(points)),
        'values': values,
        DATA_TIME: covered.isoformat(),
    }


def _data_of(parsed, now):
    """For the run log: the time a zone's or a corridor's value is for, and how
    long before the run that is."""
    oldest = carry_over.parse_time(parsed[DATA_TIME])
    text = f"data of {oldest:%d %b %H:%M}"
    newest = carry_over.parse_time(parsed.get('newest'))
    if newest is not None and newest != oldest:
        text += f" to {newest:%H:%M}"
    minutes = round((now - oldest).total_seconds() / 60)
    text += f', {minutes} min before the run' if minutes >= 0 else ', ahead of the run'
    if parsed.get('behind'):
        text += '; behind the rest: ' + ', '.join(
            f'{name} {carry_over.parse_time(at):%d %b %H:%M} ({mw:.0f} MW)' for name, at, mw in parsed['behind'])
    if parsed.get('zero_at'):
        text += (f"; the interval of {carry_over.parse_time(parsed['zero_at']):%d %b %H:%M} adds up to zero: "
                 f'read from the last one with generation')
    if parsed.get('consumption'):
        text += '; consumption, not in the mix: ' + ', '.join(
            f'{name} {mw:.0f} MW ({carry_over.parse_time(at):%d %b %H:%M})' for name, at, mw in parsed['consumption'])
    return text


# --- The prices of the day ---
#
# spot-prices.json has, for each zone, the day-ahead prices of the delivery day
# in course on the market's clock (Central European Time, as in
# omie-prices.json): one value for each hour of that day, 00:00 to 23:00.
#
# The reading that was here before (parse_prices_xml, now gone) took every
# position for an hour, did not put back the points curveType A03 leaves out
# and added up every series of the answer.
# On 17 Nov 2026 that gave Portugal 86 "hours" and an average of 68.76 (64.56
# with the 96 quarter-hours put back, OMIE's figure) and Spain 263 "hours" from
# three series. What an A44 answer really has, as seen in the answers of that
# day:
#
#   - Intervals of 15 minutes for most zones since October 2025 (60 for a
#     few), in a Period that says how long they are. An hour's price is the
#     mean of its intervals.
#   - curveType A03: a point that repeats the price before it is left out.
#   - More than one market. Spain's answer also has the prices of the intraday
#     auctions (contract_MarketAgreement.type A07, one series for each
#     auction); only A01, the day-ahead market, is read.
#   - More than one day-ahead auction. Germany and Austria have two day-ahead
#     series for the same day, told apart by
#     classificationSequence_AttributeInstanceComponent.position. Position 1
#     is the coupled European auction (it is the one Energy-Charts and SMARD
#     publish: 211.58 and 245.77 on that day); position 2 is a second, local
#     auction. The series with the lowest position is read, a series with no
#     position counting as the first (western Denmark has one of each, equal).
#   - Delivery days on other clocks. Moldova's day begins an hour earlier, at
#     22:00 UTC in winter. Each hour is taken from the Period that covers it,
#     whatever the day that Period belongs to.

DAY_AHEAD = 'A01'                 # contract_MarketAgreement.type of the day-ahead market
HOUR = timedelta(hours=1)


def _cents(value):
    """A price as the file has it: two decimals, halves rounded up."""
    return float(Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def _every_interval(period):
    """(start, value) of every interval of a Period, with the points curveType
    A03 leaves out put back: such a point repeats the one before it."""
    points = dict(period['points'])
    if not points:
        return
    count = max(points)
    if period['variable'] and period['end'] is not None:
        count = max(count, int((period['end'] - period['start']) / period['step']))
    value = None
    for position in range(1, min(count, 40000) + 1):      # a year of quarter-hours is 35 136
        if position in points:
            value = points[position]
        elif not period['variable']:
            value = None                  # a fixed curve with a point missing has a gap there
        if value is not None:
            yield period['start'] + period['step'] * (position - 1), value


def prices_by_day(all_series):
    """The day-ahead prices the series of an A44 answer have, for every
    delivery day they have a complete hour of:

        {day: [(hour, price)]}    hour 0 to 23 on the market's clock, in order;
                                  price a Decimal, not rounded

    An hour's price is the mean of the intervals of that hour, and an hour
    with an interval missing is left out. On the day the clocks go back the
    two hours called 02:00 are one entry, their mean; on the day they go
    forward there is no 02:00. Each day is read from one auction only: the
    first that has an hour of it.

    One answer may cover one day (a run of the feed) or a whole year (the
    rewriting of the archive, backfill_archive.py): every day is read the
    same way."""
    days = {}       # day -> position of the auction -> {start of an hour: {interval length: {start: price}}}
    for series in all_series:
        if series['contract'] not in (None, DAY_AHEAD):
            continue
        position = series['sequence'] or 0
        for period in series['periods']:
            step = period['step']
            if step > HOUR or HOUR % step:
                continue                  # not a part of an hour: nothing an hourly price can be made of
            for at, value in _every_interval(period):
                hour = at.replace(minute=0, second=0, microsecond=0)
                days.setdefault(at.astimezone(MARKET_TZ).date(), {}).setdefault(position, {}) \
                    .setdefault(hour, {}).setdefault(step, {})[at] = Decimal(repr(value))

    found = {}
    for day, auctions in days.items():
        for position in sorted(auctions):
            by_label = {}                 # hour on the market's clock -> the mean of each hour so called
            for hour in sorted(auctions[position]):
                for step, values in sorted(auctions[position][hour].items()):      # the finest intervals first
                    if step * len(values) == HOUR:
                        by_label.setdefault(hour.astimezone(MARKET_TZ).hour, []).append(
                            sum(values.values()) / len(values))
                        break
            if by_label:
                found[day] = [(label, sum(means) / len(means)) for label, means in sorted(by_label.items())]
                break
    return found


def read_prices(xml_str, day):
    """The day-ahead prices an A44 answer has for the delivery day `day`, as
    prices_by_day() gives them: [(hour, price)], or [] when it has none."""
    return prices_by_day(read_series(xml_str)).get(day, [])


def price_entry(eic, hours, day):
    """A zone of spot-prices.json from its [(hour, price)] of the delivery day.

    `avg` is the plain mean of the hourly values (taken before they are
    rounded, so that on a day of 24 hours it is the mean of every interval of
    the day, to the cent); `min` and `max` are the lowest and the highest of
    the hourly values as published."""
    prices = [{'time': f'{hour:02d}:00', 'hour': hour, 'price': _cents(value)} for hour, value in hours]
    published = [p['price'] for p in prices]
    return {
        'eic': eic,
        'prices': prices,
        'latest': prices[-1],
        'avg': _cents(sum(value for _hour, value in hours) / len(hours)),
        'min': min(published),
        'max': max(published),
        DELIVERY_DATE: day.isoformat(),
    }


def price_request(base, eic, day):
    """The address that asks ENTSO-E for a zone's prices of a delivery day:
    from midnight to midnight on the market's clock."""
    start, end = carry_over.day_bounds(day)
    params = urllib.parse.urlencode({
        'securityToken': API_KEY,
        'documentType': 'A44',
        'in_Domain': eic,
        'out_Domain': eic,
        'periodStart': start.strftime('%Y%m%d%H%M'),
        'periodEnd': end.strftime('%Y%m%d%H%M'),
    })
    return f'{base}?{params}'


def price_zones(found, day):
    """The zones of spot-prices.json from {zone: [(hour, price)]}, with the
    countries put together from several zones (CONSOLIDATE).

    Such a country is made hour by hour over the corrected series of its
    zones: an hour's price is the mean of the zones' prices for that hour, and
    only the hours every one of its zones has are published."""
    zones, hourly = {}, {}
    for code, eic in ZONES.items():      # ZONES order, so the file keeps its zone order
        if found.get(code):
            zones[code] = price_entry(eic, found[code], day)
            hourly[code] = dict(found[code])

    for country, zone_codes in CONSOLIDATE.items():
        if not any(zc in zones for zc in zone_codes):
            continue
        if _incomplete(country, zone_codes, zones):
            continue
        eic = ', '.join(zones[zc]['eic'] for zc in zone_codes)
        shared = [hour for hour in sorted(hourly[zone_codes[0]]) if all(hour in hourly[zc] for zc in zone_codes)]
        for zc in zone_codes:
            zones.pop(zc, None)
        if not shared:
            print(f'  {country}: {"+".join(zone_codes)} have no hour in common — not put together')
            continue
        zones[country] = price_entry(
            eic, [(hour, sum(hourly[zc][hour] for zc in zone_codes) / len(zone_codes)) for hour in shared], day)
        print(f'  Consolidated {"+".join(zone_codes)} → {country}')
    return zones


# --- Fetchers ---

def _incomplete(country, zone_codes, zones):
    """True when only some of a country's zones answered. The country is then
    not built from the part that did: half of Norway is not Norway, and for
    generation it would show the country's output falling by the missing part.
    Its zones are dropped, so the country counts as not obtained in this run:
    it keeps what was published before, while carry_over.py allows it, or is
    left out."""
    missing = [zc for zc in zone_codes if zc not in zones]
    if not missing:
        return False
    answered = [zc for zc in zone_codes if zc in zones]
    for zc in zone_codes:
        zones.pop(zc, None)
    print(f'  {country}: {"+".join(answered)} answered but {"+".join(missing)} did not — '
          f'not put together from part of the country')
    return True


def fetch_prices(base, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + TOTAL_BUDGET_S
    now = utcnow()
    day = carry_over.delivery_day(now)   # the delivery day in course on the market's clock

    prices = {
        'updated': now.isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'endpoint': base,
        'zones': {},
    }

    jobs = [(code, code, price_request(base, eic, day)) for code, eic in ZONES.items()]
    fetched = fetch_all('Prices', jobs, lambda text: read_prices(text, day),
                        lambda hours: f'{len(hours)} hours of {day:%d %b}', deadline)
    prices['zones'] = price_zones(fetched, day)
    return prices


def fetch_generation(base, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + TOTAL_BUDGET_S
    now = utcnow()
    start, end = last_day(now)

    gen = {
        'updated': now.isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'zones': {},
    }

    codes = [code for code in GEN_COUNTRIES + ['NO1', 'NO2', 'SE1', 'SE3', 'DK1', 'DK2']
             if ZONES.get(code, '')]
    jobs = []
    for code in codes:
        params = urllib.parse.urlencode({
            'securityToken': API_KEY,
            'documentType': 'A75',
            'processType': 'A16',
            'in_Domain': ZONES[code],
            'periodStart': start,
            'periodEnd': end,
        })
        jobs.append((code, code, f'{base}?{params}'))
    fetched = fetch_all('Generation', jobs, lambda text: latest_generation(text, now),
                        lambda got: (f"{len(got['mix'])} types, {sum(got['mix'].values()):.0f} MW, "
                                     f"{_data_of(got, now)}"), deadline)

    for code in codes:                   # same order as before, so the file keeps its zone order
        got = fetched.get(code)
        if got:
            mix = got['mix']
            total = sum(mix.values())
            renewable = sum(mix.get(rt, 0) for rt in RENEWABLE_TYPES)
            gen['zones'][code] = {
                'mix': mix,
                'total_mw': total,
                'renewable_pct': round((renewable / total) * 100, 1) if total > 0 else 0,
                DATA_TIME: got[DATA_TIME],
            }

    for country, zone_codes in CONSOLIDATE.items():
        zone_data = [gen['zones'].get(zc) for zc in zone_codes if zc in gen['zones']]
        if not zone_data:
            continue
        if _incomplete(country, zone_codes, gen['zones']):
            continue
        merged_mix = {}
        for z in zone_data:
            for k, v in z.get('mix', {}).items():
                merged_mix[k] = merged_mix.get(k, 0) + v
        total = sum(merged_mix.values())
        renewable = sum(merged_mix.get(rt, 0) for rt in RENEWABLE_TYPES)
        gen['zones'][country] = {
            'mix': merged_mix,
            'total_mw': total,
            'renewable_pct': round((renewable / total) * 100, 1) if total > 0 else 0,
        }
        data_time = carry_over.oldest(z.get(DATA_TIME) for z in zone_data)
        if data_time:
            gen['zones'][country][DATA_TIME] = data_time     # as old as the oldest of its zones
        for zc in zone_codes:
            gen['zones'].pop(zc, None)
        print(f'  Consolidated generation {"+".join(zone_codes)} → {country}')

    return gen


def fetch_flows(base, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + TOTAL_BUDGET_S
    now = utcnow()
    start, end = last_day(now)

    flows = {
        'updated': now.isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'corridors': {},
        'net': {},
    }

    jobs = []
    for frm, to in FLOW_CORRIDORS:
        from_eic = FLOW_ZONES.get(frm, '')
        to_eic = FLOW_ZONES.get(to, '')
        if not from_eic or not to_eic:
            continue
        params = urllib.parse.urlencode({
            'securityToken': API_KEY,
            'documentType': 'A11',
            'in_Domain': to_eic,
            'out_Domain': from_eic,
            'periodStart': start,
            'periodEnd': end,
        })
        jobs.append(((frm, to), f'{frm}→{to}', f'{base}?{params}'))
    fetched = fetch_all('Flow', jobs, lambda text: latest_flow(text, now),
                        lambda got: (f"{got['latest']['mw']} MW, {len(got['values'])} points today, "
                                     f"{_data_of(got, now)}"), deadline)

    for frm, to in FLOW_CORRIDORS:       # same order as before, so the file keeps its corridor order
        got = fetched.get((frm, to))
        if got:
            key = f'{frm}→{to}'
            flows['corridors'][key] = {
                'from': frm,
                'to': to,
                'latest': got['latest'],
                'values': got['values'],
                DATA_TIME: got[DATA_TIME],
            }

    flows['net'] = net_flows(flows['corridors'])
    return flows


def net_flows(corridors):
    """The `net` block: the net flow of each pair of zones, from the two
    corridors of the pair as they are in the file.

    A pair is as old as the older of its two corridors, in the time its data
    are for (data_time) and in the time they were obtained (fetched_at), and is
    marked carried_over when either of them was kept from an earlier run, so
    the number never looks fresher than what it was worked out from.

    A pair with neither of its corridors in the file is left out: it used to
    be written as a net flow of 0 MW, with no time, and the site showed that
    as a measurement (NO-SE, LT-SE, IT-ME and AL-MK, for which ENTSO-E has no
    data between the zones the feed asks for). A pair with one corridor is
    worked out from it, as before: the other direction counts as zero."""
    net = {}
    for frm, to in FLOW_CORRIDORS:
        pair = '-'.join(sorted([frm, to]))
        if pair in net:
            continue
        both = [corridors.get(f'{frm}→{to}'), corridors.get(f'{to}→{frm}')]
        if not any(both):
            continue
        fwd, rev = [(c or {}).get('latest', {}).get('mw', 0) for c in both]
        balance = fwd - rev
        entry = {
            'from': frm if balance >= 0 else to,
            'to': to if balance >= 0 else frm,
            'net_mw': abs(round(balance)),
        }
        data_time = carry_over.oldest(c.get(DATA_TIME) for c in both if c)
        if data_time:
            entry[DATA_TIME] = data_time
        fetched_at = carry_over.oldest(c.get(FETCHED_AT) for c in both if c)
        if fetched_at:
            entry[FETCHED_AT] = fetched_at
            if any(c and c.get(CARRIED_OVER) for c in both):
                entry[CARRIED_OVER] = True
        net[pair] = entry
    return net


def fetch_omip(deadline=None):
    """Fetch OMIP settlement prices by scraping omip.pt."""
    print('Fetching OMIP forward curves...')

    def time_left():
        # Per-request timeout: 30 s, or whatever is left of the run's budget.
        return 30 if deadline is None else min(30, deadline - time.monotonic())

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
        'Cache-Control': 'no-cache',
    }

    urls = [
        'https://www.omip.pt/en',
        'https://www.omip.pt/en/plazo-hoy',
        'https://www.omip.pt/en/dados-mercado',
    ]

    html = None
    for url in urls:
        if time_left() <= 0:
            print('  Out of time')
            break
        print(f'  Trying: {url}')
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=time_left()) as r:
                html = r.read().decode('utf-8')
            if html and 'Settlement Price' in html:
                print(f'  Got HTML with settlement data ({len(html)} bytes)')
                break
            elif html:
                print(f'  Got HTML but no settlement data ({len(html)} bytes)')
                html = None
        except Exception as e:
            print(f'  Failed: {e}')

    if not html:
        for url in urls[:1]:
            if time_left() <= 0:
                break
            print(f'  Retry without SSL: {url}')
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=time_left(), context=ctx) as r:
                    html = r.read().decode('utf-8')
                if html and 'Settlement Price' in html:
                    print(f'  Got HTML with settlement data')
                    break
            except Exception as e:
                print(f'  Failed: {e}')

    if not html:
        print('  Could not reach omip.pt or no settlement data found')
        return None

    contracts = []
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    import re

    clean = re.sub(r'<[^>]+>', ' ', html)
    clean = re.sub(r'\s+', ' ', clean)
    clean = clean.replace('&middot;', '·').replace('&#183;', '·').replace('•', '·').replace('‧', '·')

    SEP = r'[\s·\-–—|/,;]+'

    # Pattern 1: label_hint € price Eur/MWh Settlement Price for <desc> Contract
    matches = re.findall(
        rf'([\w/\.\-]+(?:\s+[\w/\.\-]+){{0,2}})\s+€\s*(-?[\d.,]+){SEP}Eur/MWh{SEP}Settlement\s+Price\s+for\s+(.*?)\s+Contract',
        clean, re.IGNORECASE
    )

    # Pattern 2: ordem invertida
    if not matches:
        rev_matches = re.findall(
            rf'Settlement\s+Price\s+for\s+(.*?)\s+Contract.*?€\s*(-?[\d.,]+)',
            clean, re.IGNORECASE
        )
        matches = [('', price, desc) for desc, price in rev_matches]

    # Pattern 3: € perto de Settlement Price within 300 chars
    if not matches:
        for m in re.finditer(r'€\s*(-?[\d.,]+)', clean):
            price_str = m.group(1)
            context = clean[m.start():min(len(clean), m.start() + 300)]
            sm = re.search(r'Settlement\s+Price\s+for\s+(.*?)\s+Contract', context, re.IGNORECASE)
            if sm:
                matches.append(('', price_str, sm.group(1)))

    # Pattern 4: FTB blocks fallback
    if not matches:
        ftb_matches = re.findall(rf'FTB{SEP}([\w\s/\-]+?){SEP}€\s*(-?[\d.,]+)', clean)
        for label, price_str in ftb_matches:
            matches.append((label.strip(), price_str, f'Spain Power Base Futures {label.strip()}'))

    print(f'  Found {len(matches)} raw matches')

    for label_hint, price_str, desc in matches:
        try:
            price = float(price_str.replace(',', '.'))
        except ValueError:
            continue
        if price == 0:
            continue

        # Parse zone
        zone = 'SPEL'
        if re.search(r'Portugal|PTEL', desc, re.I): zone = 'PTEL'
        elif re.search(r'Germany|DEEL', desc, re.I): zone = 'DEEL'
        elif re.search(r'France|FREL', desc, re.I): zone = 'FREL'
        elif re.search(r'Gas|PVB', desc, re.I): zone = 'PVB'

        # Parse profile
        profile = 'base'
        if 'Peak' in desc: profile = 'peak'
        elif 'Solar' in desc: profile = 'solar'

        # Parse product type — mais específico primeiro
        product = 'unknown'
        if re.search(r'PPA\s*10', desc): product = 'PPA10Y'
        elif re.search(r'PPA\s*5', desc): product = 'PPA5Y'
        elif re.search(r'PPA\s*3', desc): product = 'PPA3Y'
        elif re.search(r'Year|YR-\d{2}', desc): product = 'year'
        elif re.search(r'Quarter|Q\d-\d{2}', desc): product = 'quarter'
        elif re.search(r'Semester|Season', desc): product = 'semester'
        elif re.search(r'Month|M\s+\w+-\d{2}', desc): product = 'month'
        elif re.search(r'\bBOM\b|\bBoM\b|Balance.of.Month', desc): product = 'bom'
        elif 'Weekend' in desc: product = 'weekend'
        elif 'Weekday' in desc: product = 'weekday'
        elif re.search(r'\bWeek\b|Wk\d+', desc): product = 'week'
        elif 'Day' in desc: product = 'day'

        # Extract label — label_hint primeiro (tem período real), depois desc
        label = ''
        for _src in [label_hint, desc]:
            lm = re.search(r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{2})', _src, re.I)
            if lm: label = lm.group(1); break
            lm = re.search(r'(Q[1-4]-\d{2})', _src)
            if lm: label = lm.group(1); break
            lm = re.search(r'(YR-\d{2})', _src, re.I)
            if lm: label = lm.group(1); break
            lm = re.search(r'((?:Win|Sum|Spr|Aut)-\d{2})', _src, re.I)
            if lm: label = lm.group(1); break
            lm = re.search(r'(WkDs\d+-\d{2})', _src, re.I)
            if lm: label = lm.group(1); break
            lm = re.search(r'(Wk\d+-\d{2})', _src, re.I)
            if lm: label = lm.group(1); break
            lm = re.search(r'(PPA\s*[\d/]+)', _src)
            if lm: label = lm.group(1); break

        # Deduplicate
        dup = False
        for c in contracts:
            if c['zone'] == zone and c['product'] == product and c['label'] == label and c['profile'] == profile:
                dup = True
                break
        if dup:
            continue

        contracts.append({
            'zone': zone,
            'profile': profile,
            'product': product,
            'label': label,
            'price': round(price, 2),
            'desc': desc.strip(),
        })

    # Always write debug file
    import re as re2
    euro_positions = [m.start() for m in re2.finditer('€', clean)]
    settle_positions = [m.start() for m in re2.finditer('Settlement Price', clean)]
    os.makedirs('data', exist_ok=True)
    with open('data/omip-debug.txt', 'w', encoding='utf-8') as dbg:
        dbg.write(f'=== OMIP Debug — {datetime.now(timezone.utc).isoformat()} ===\n\n')
        dbg.write(f'Status: {"SUCCESS" if contracts else "FAILED — no contracts parsed"}\n')
        dbg.write(f'Contracts found: {len(contracts)}\n\n')
        dbg.write(f'HTML length: {len(html)}\n')
        dbg.write(f'Clean length: {len(clean)}\n')
        dbg.write(f'Contains "Settlement": {"Settlement" in clean}\n')
        dbg.write(f'Contains "FTB": {"FTB" in clean}\n')
        dbg.write(f'Contains "€": {"€" in clean}\n')
        dbg.write(f'Contains "Eur/MWh": {"Eur/MWh" in clean}\n\n')
        dbg.write(f'Euro sign positions: {len(euro_positions)}\n\n')
        for i, pos in enumerate(euro_positions[:15]):
            start = max(0, pos - 20)
            end = min(len(clean), pos + 200)
            snippet = clean[start:end].replace('\n', ' ')
            dbg.write(f'--- Euro #{i+1} at pos {pos} ---\n{snippet}\n\n')
        dbg.write(f'\nSettlement Price positions: {len(settle_positions)}\n\n')
        for i, pos in enumerate(settle_positions[:10]):
            start = max(0, pos - 100)
            end = min(len(clean), pos + 100)
            snippet = clean[start:end].replace('\n', ' ')
            dbg.write(f'--- Settlement #{i+1} at pos {pos} ---\n{snippet}\n\n')
        if contracts:
            dbg.write('\n=== Contracts Parsed ===\n')
            for c in contracts:
                dbg.write(f'  {c["zone"]} | {c["profile"]} | {c["product"]} | {c["label"]} | €{c["price"]} | desc={c["desc"]}\n')

    if not contracts:
        print('  No contracts parsed — debug saved to data/omip-debug.txt')
        return None

    print(f'  → {len(contracts)} contracts parsed — debug saved to data/omip-debug.txt')

    date_match = re.search(r'for date (\d{4}-\d{2}-\d{2})', html)
    data_date = date_match.group(1) if date_match else today

    return {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'OMIP',
        'latest': {
            'date': data_date,
            'contracts': contracts,
        },
    }


def archive_snapshot(dataset, payload, day=None):
    """Write today's snapshot to data/archive/<dataset>-YYYY-MM-DD.json.

    Rolling 5-year history for the date filters on the Overview map (prices;
    generation, from which the frontend derives carbon intensity and renewable
    share) and on the Grid Monitor map (flows). Overwritten on every fetch.

    The date is the UTC day or, with `day`, that day: the snapshot of the
    prices is named after the delivery day it has, which between 23:00 and
    midnight UTC (22:00 in summer) is already the next day.
    """
    os.makedirs('data/archive', exist_ok=True)
    today_str = (day or utcnow()).strftime('%Y-%m-%d')
    archive_path = f'data/archive/{dataset}-{today_str}.json'
    try:
        write_json(archive_path, payload, separators=(',', ':'))
        print(f'Archived to {archive_path}')
    except OSError as e:
        print(f'  Warning: could not write archive: {e}')


def refresh_archive_index():
    """Prune the archive and rebuild data/archive_index.json."""
    os.makedirs('data/archive', exist_ok=True)

    # ── Unified cleanup: delete archive files older than ~5 years (1830 days).
    # Applies to spot-prices-*.json, generation-mix-*.json and cross-border-flows-*.json.
    try:
        cutoff = (utcnow() - timedelta(days=1830)).strftime('%Y-%m-%d')
        removed = 0
        for fname in os.listdir('data/archive'):
            if not fname.endswith('.json'):
                continue
            # Extract YYYY-MM-DD from any known prefix
            date_part = None
            for prefix in ('spot-prices-', 'generation-mix-', 'cross-border-flows-'):
                if fname.startswith(prefix):
                    date_part = fname[len(prefix):-len('.json')]
                    break
            if date_part and len(date_part) == 10 and date_part < cutoff:
                os.remove(os.path.join('data/archive', fname))
                removed += 1
        if removed:
            print(f'Cleaned up {removed} archive file(s) older than ~5 years')
    except OSError as e:
        print(f'  Warning: archive cleanup failed: {e}')

    # ── Build archive index — one source of truth for which dates are
    # actually available per dataset. Frontend date pickers fetch this index
    # to know what to allow / disallow / snap to. Avoids the situation where
    # a date is within the 5-year window but the file was never written
    # (failed fetch, gap in backfill, etc.).
    try:
        from collections import defaultdict
        index = defaultdict(list)
        for fname in os.listdir('data/archive'):
            if not fname.endswith('.json'):
                continue
            for prefix in ('spot-prices-', 'generation-mix-', 'cross-border-flows-'):
                if fname.startswith(prefix):
                    date_part = fname[len(prefix):-len('.json')]
                    if len(date_part) == 10:
                        # dataset key without trailing dash
                        index[prefix[:-1]].append(date_part)
                    break
        # Sort each dataset's date list ascending (chronological)
        index_sorted = {k: sorted(v) for k, v in index.items()}
        index_payload = {
            'updated': utcnow().isoformat(),
            'datasets': index_sorted,
        }
        write_json('data/archive_index.json', index_payload, separators=(',', ':'))
        counts = ', '.join(f'{k}={len(v)}' for k, v in index_sorted.items())
        print(f'Wrote archive_index.json ({counts})')
    except OSError as e:
        print(f'  Warning: archive index write failed: {e}')


def update_forward_curves(deadline=None):
    """Scrape OMIP and write data/forward-curves.json (kept as it is when the
    scrape returns nothing)."""
    try:
        omip = fetch_omip(deadline)
        omip_count = len(omip['latest']['contracts']) if omip and omip.get('latest') else 0

        existing_fc = {}
        if os.path.exists('data/forward-curves.json'):
            try:
                with open('data/forward-curves.json') as f:
                    existing_fc = json.load(f)
            except Exception:
                pass

        if omip_count > 0:
            omip['history'] = existing_fc.get('history', [])
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            if not any(h.get('date') == today_str for h in omip['history']):
                year_prices = {}
                for c in omip['latest']['contracts']:
                    if c.get('product') == 'year' and c.get('profile') == 'base':
                        year_prices[c['zone']] = c['price']
                if year_prices:
                    omip['history'].append({'date': today_str, **year_prices})
                    omip['history'] = omip['history'][-365:]

            write_json('data/forward-curves.json', omip, separators=(',', ':'))
            print(f'Saved forward-curves.json ({omip_count} contracts, {len(omip["history"])} history points)')
        elif existing_fc:
            print(f'OMIP scraper returned no data, keeping existing forward-curves.json')
    except Exception as e:
        print(f'  OMIP update failed: {type(e).__name__}: {e}')


def publish(doc, key, path, order, dataset):
    """Write one of the three files and its archive snapshot: the entries this
    run obtained plus the ones kept from before, for as long as the file's rule
    allows (see carry_over.py).

    Returns how many entries came from this run. With none, nothing is written:
    the published file, its `updated` included, stays exactly as it was."""
    fresh = doc[key]
    name = os.path.basename(path)
    if not fresh:
        print(f'Nothing obtained in this run: {name} is left exactly as it was')
        return 0

    rule = carry_over.RULES[path]
    entries, kept, dropped = carry_over.merge(fresh, load_json(path), key, order, doc['updated'], rule)
    doc[key] = entries
    if key == 'corridors':
        doc['net'] = net_flows(entries)      # over what the file shows, kept corridors included
    write_json(path, doc, separators=(',', ':'))
    print(f'Saved {name} ({len(entries)} {key}: {len(fresh)} from this run, '
          f'{len(kept)} {rule.kept})')
    if kept:
        print('  Kept, not obtained in this run: ' + ', '.join(f'{n} (from {t})' for n, t in kept))
    if dropped:
        print(f'  Left out, {rule.dropped}: ' + ', '.join(f'{n} ({t})' for n, t in dropped))

    # The archive follows what is published. The zones fetch_gb.py fills in
    # were never part of it, and stay out.
    snapshot = doc
    if key == 'zones':
        snapshot = dict(doc, zones={n: e for n, e in entries.items() if n not in ELEXON_ZONES})
    # The prices are filed under the delivery day they are of.
    archive_snapshot(dataset, snapshot, carry_over.delivery_day(doc['updated']) if dataset == 'spot-prices' else None)
    return len(fresh)


def write_status(status, run_started):
    """status.json: the run's outcome plus, for each of the three files as they
    are published now, how many entries it has, how many of them this run
    obtained and how many are from earlier runs."""
    counts = carry_over.published_counts(run_started)
    for existing in ('prices_zones', 'generation_zones', 'flow_corridors'):
        status[existing] = counts[existing]
    status['run_started'] = run_started.isoformat()
    status.update(counts)
    write_json('data/status.json', status, indent=2)


def main():
    try:
        sys.stdout.reconfigure(line_buffering=True)   # show progress while running
    except (AttributeError, ValueError):
        pass

    if not API_KEY:
        print('ERROR: ENTSOE_API_KEY not set')
        return 1

    started = time.monotonic()
    run_started = utcnow()
    run_deadline = started + TOTAL_BUDGET_S
    os.makedirs('data', exist_ok=True)

    # OMIP is a different site: fetch it alongside ENTSO-E, so neither waits for
    # the other. The thread writes forward-curves.json itself when it is done.
    omip_thread = threading.Thread(target=update_forward_curves, args=(run_deadline,), daemon=True)
    omip_thread.start()

    def wait_for_omip():
        omip_thread.join(max(0.0, run_deadline + GRACE_S - time.monotonic()))
        if omip_thread.is_alive():
            print('OMIP did not finish within the time budget — forward-curves.json left as it was')

    print('Finding working ENTSO-E endpoint...')
    base = find_working_endpoint(run_deadline)
    if not base:
        print('ERROR: ENTSO-E is refusing the API key — nothing was asked for')
        write_status({'status': 'error', 'time': utcnow().isoformat()}, run_started)
        wait_for_omip()
        return 1

    print(f'\nUsing endpoint: {base}\n')

    # Each dataset gets a share of the time still left, in proportion to its
    # number of requests. One that finishes early leaves its time to the next.
    requests_left = [len(ZONES), len(GEN_COUNTRIES) + 6, len(FLOW_CORRIDORS)]

    def next_deadline():
        now = time.monotonic()
        share = requests_left[0] / sum(requests_left)
        requests_left.pop(0)
        return now + max(0.0, run_deadline - now) * share

    # Every file is written as soon as its dataset is done, so a run that is
    # cut short keeps whatever it had already fetched. A zone or corridor that
    # did not answer keeps what was published before, for a while (see carry_over.py).
    print('Fetching day-ahead prices...')
    prices = fetch_prices(base, next_deadline())
    print(f"  → {len(prices['zones'])} zones\n")
    zone_count = publish(prices, 'zones', 'data/spot-prices.json', PRICE_ORDER, 'spot-prices')
    if zone_count > 0:
        refresh_archive_index()

    print('\nFetching generation mix...')
    gen = fetch_generation(base, next_deadline())
    print(f"  → {len(gen['zones'])} zones\n")
    gen_count = publish(gen, 'zones', 'data/generation-mix.json', GEN_ORDER, 'generation-mix')
    if gen_count > 0:
        refresh_archive_index()

    print('\nFetching cross-border flows...')
    flows = fetch_flows(base, next_deadline())
    print(f"  → {len(flows['corridors'])} corridors\n")
    flow_count = publish(flows, 'corridors', 'data/cross-border-flows.json', FLOW_ORDER, 'cross-border-flows')
    # Also when a dataset came back empty: the cleanup and the index do not depend on it.
    refresh_archive_index()

    wait_for_omip()

    write_status({'status': 'ok', 'endpoint': base, 'time': utcnow().isoformat()}, run_started)

    print(f'\nDone in {time.monotonic() - started:.0f}s')
    # Fail the step only when ENTSO-E gave nothing at all, so it shows in Actions.
    return 0 if (zone_count or gen_count or flow_count) else 1


if __name__ == '__main__':
    sys.exit(main())
