"""
EU Power Data — Historical archive backfill

One-shot script to populate data/archive/ with up to ~5 years (1830 days) of
historical snapshots from the ENTSO-E Transparency Platform.

Archives THREE datasets per day:
  - spot-prices-YYYY-MM-DD.json         (day-ahead prices)
  - generation-mix-YYYY-MM-DD.json      (generation mix → feeds Carbon & Renewables)
  - cross-border-flows-YYYY-MM-DD.json  (physical flows → feeds Grid Monitor)

Designed to be triggered manually via GitHub Actions workflow_dispatch. The
workflow is configured to self-re-trigger if work remains after a run hits
the safety timeout — so a single manual start will eventually complete the
full backfill across multiple chained runs.

Idempotent: dates that already have an archive file for a given dataset are
skipped, so the script can be re-run safely (useful when chained runs pick
up where the previous one left off, or if a run times out mid-batch).

Reuses constants and helpers from fetch_entsoe.py so any future updates to
zone codes, consolidation rules or XML parsing stay in a single place.
"""

import contextlib
import glob
import io
import os
import sys
import json
import time
import subprocess
import urllib.parse
from datetime import date, datetime, timedelta, timezone

# Reuse zone codes, consolidation rules, HTTP helper and XML parser from the
# regular fetcher — single source of truth.
import fetch_entsoe as fe


# ---------------------------------------------------------------------------
# Spot prices for one past date
# ---------------------------------------------------------------------------

def fetch_prices_for_date(base, target_date):
    """Fetch ENTSO-E day-ahead prices for one delivery day.

    Returns the same dict shape as fetch_entsoe.fetch_prices() so the
    archived file is interchangeable with files produced by the regular cron:
    one value for each hour of the delivery day on the market's clock (Central
    European Time), read by the same code (fetch_entsoe.read_prices and
    price_zones), which knows the length of the intervals, puts back the
    points curveType A03 leaves out and reads the day-ahead market only.

    target_date: a datetime.date object, the delivery day.
    """
    prices = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'date': target_date.strftime('%Y-%m-%d'),
        'source': 'ENTSO-E Transparency Platform (backfilled)',
        'endpoint': base,
        'zones': {},
    }

    found = {}
    for code, eic in fe.ZONES.items():
        xml = fe.http_get(fe.price_request(base, eic, target_date))
        if xml:
            hours = fe.read_prices(xml, target_date)
            if hours:
                found[code] = hours
        time.sleep(0.15)   # gentle on the ENTSO-E API

    # The zones in the feed's order, with the same Nordic consolidation as the
    # regular fetcher (NO1+NO2 -> NO, SE1+SE3 -> SE, DK1+DK2 -> DK).
    prices['zones'] = fe.price_zones(found, target_date)
    return prices


# ---------------------------------------------------------------------------
# Generation mix for one past date
# ---------------------------------------------------------------------------

def fetch_gen_for_date(base, target_date):
    """Fetch ENTSO-E generation mix (Actual Generation per Production Type,
    documentType A75 / processType A16) for a specific UTC date.

    Matches the shape of fetch_entsoe.fetch_generation() so Overview-map JS
    can treat historical and live snapshots interchangeably.
    """
    start = target_date.strftime('%Y%m%d') + '0000'
    end_dt = target_date + timedelta(days=1)
    end = end_dt.strftime('%Y%m%d') + '0000'

    gen = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'date': target_date.strftime('%Y-%m-%d'),
        'source': 'ENTSO-E Transparency Platform (backfilled)',
        'zones': {},
    }

    # Same zone list as live fetcher: main countries + Nordic/Baltic sub-zones
    zone_list = list(fe.GEN_COUNTRIES) + ['NO1', 'NO2', 'SE1', 'SE3', 'DK1', 'DK2']

    for code in zone_list:
        eic = fe.ZONES.get(code, '')
        if not eic:
            continue
        params = urllib.parse.urlencode({
            'securityToken': fe.API_KEY,
            'documentType': 'A75',
            'processType': 'A16',
            'in_Domain': eic,
            'periodStart': start,
            'periodEnd': end,
        })
        xml = fe.http_get(f'{base}?{params}')
        if not xml:
            continue
        mix = fe.parse_generation_xml(xml)
        if mix:
            total = sum(mix.values())
            renewable = sum(mix.get(rt, 0) for rt in fe.RENEWABLE_TYPES)
            gen['zones'][code] = {
                'mix': mix,
                'total_mw': total,
                'renewable_pct': round((renewable / total) * 100, 1) if total > 0 else 0,
            }
        time.sleep(0.2)

    # Nordic consolidation (same as live fetcher)
    for country, zone_codes in fe.CONSOLIDATE.items():
        zone_data = [gen['zones'].get(zc) for zc in zone_codes if zc in gen['zones']]
        if not zone_data:
            continue
        merged_mix = {}
        for z in zone_data:
            for k, v in z.get('mix', {}).items():
                merged_mix[k] = merged_mix.get(k, 0) + v
        total = sum(merged_mix.values())
        renewable = sum(merged_mix.get(rt, 0) for rt in fe.RENEWABLE_TYPES)
        gen['zones'][country] = {
            'mix': merged_mix,
            'total_mw': total,
            'renewable_pct': round((renewable / total) * 100, 1) if total > 0 else 0,
        }
        for zc in zone_codes:
            gen['zones'].pop(zc, None)

    return gen


# ---------------------------------------------------------------------------
# Cross-border flows for one past date
# ---------------------------------------------------------------------------

def fetch_flows_for_date(base, target_date):
    """Fetch ENTSO-E physical flows (documentType A11) for a specific UTC date.

    Returns the same dict shape as fetch_entsoe.fetch_flows() so the archived
    file is interchangeable with files produced by the regular cron. Feeds
    the Grid Monitor map historical date filter.

    target_date: a datetime.date object (UTC date).
    """
    start = target_date.strftime('%Y%m%d') + '0000'
    end_dt = target_date + timedelta(days=1)
    end = end_dt.strftime('%Y%m%d') + '0000'

    flows = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'date': target_date.strftime('%Y-%m-%d'),
        'source': 'ENTSO-E Transparency Platform (backfilled)',
        'corridors': {},
        'net': {},
    }

    for frm, to in fe.FLOW_CORRIDORS:
        from_eic = fe.FLOW_ZONES.get(frm, '')
        to_eic   = fe.FLOW_ZONES.get(to, '')
        if not from_eic or not to_eic:
            continue
        params = urllib.parse.urlencode({
            'securityToken': fe.API_KEY,
            'documentType':  'A11',
            'in_Domain':     to_eic,
            'out_Domain':    from_eic,
            'periodStart':   start,
            'periodEnd':     end,
        })
        xml = fe.http_get(f'{base}?{params}')
        if not xml:
            continue
        values = fe.parse_flow_xml(xml)
        if values:
            flows['corridors'][f'{frm}→{to}'] = {
                'from':   frm,
                'to':     to,
                'latest': values[-1],
                'values': values,
            }
        time.sleep(0.15)   # gentle on the ENTSO-E API

    # Derive net flow per unordered pair — matches fetch_entsoe.fetch_flows()
    processed = set()
    for frm, to in fe.FLOW_CORRIDORS:
        pair = '-'.join(sorted([frm, to]))
        if pair in processed:
            continue
        processed.add(pair)
        fwd = flows['corridors'].get(f'{frm}→{to}', {}).get('latest', {}).get('mw', 0)
        rev = flows['corridors'].get(f'{to}→{frm}', {}).get('latest', {}).get('mw', 0)
        net = fwd - rev
        flows['net'][pair] = {
            'from':   frm if net >= 0 else to,
            'to':     to  if net >= 0 else frm,
            'net_mw': abs(round(net)),
        }

    return flows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Rewriting the archive of the prices
# ---------------------------------------------------------------------------
#
# Every day of the archive written before 17 Nov 2026 has wrong prices: until
# October 2025 the average ran over two delivery days, and since then quarter-
# hours were read as hours. `python backfill_archive.py --rewrite-prices`
# puts them right, by the cheap way: one request to ENTSO-E for each zone and
# each year (about 200 in all, where day by day it would be 64 800), with
# every day of the answer read by the same code as the regular runs
# (fetch_entsoe.prices_by_day and price_zones) and its file written over.
#
#   - Only the files that still have a zone of ENTSO-E in the old shape (with
#     no delivery_date) are rewritten, so a second run takes up where the first
#     left off and leaves the days of the new reading alone.
#   - A zone that does not come from ENTSO-E (the United Kingdom) stays in each
#     file as it is. A zone ENTSO-E has no data for on a day leaves that day.
#   - A request that fails is repeated, and after that asked again in two
#     halves, down to a month. It never counts as "no data": if a zone cannot
#     be had for part of a year, no file of that year is written and the run
#     ends in failure, to be started again.
#   - The archives of generation and of flows are not touched.

ENTSOE_PRICE_ZONES = (set(fe.ZONES) - set(fe.ELEXON_ZONES)) | set(fe.CONSOLIDATE)
PRICE_FILES = 'data/archive/spot-prices-{day}.json'
REWRITE_WAITS = (15, 45, 90)      # seconds before each repetition of a failed request
SMALLEST_SPAN = 31                # days: a failed request for no more than this is not split again


class RewriteFailed(Exception):
    pass


def _archive_day(path):
    return date.fromisoformat(os.path.basename(path)[len('spot-prices-'):-len('.json')])


def old_reader_days():
    """{day: document} of the price files that still have a zone of ENTSO-E
    with no delivery_date, that is, written by the old reading."""
    found = {}
    for path in sorted(glob.glob(PRICE_FILES.format(day='*'))):
        try:
            with open(path, encoding='utf-8') as f:
                doc = json.load(f)
            day = _archive_day(path)
        except (OSError, ValueError):
            continue
        zones = doc.get('zones') if isinstance(doc, dict) else None
        if isinstance(zones, dict) and any(name in ENTSOE_PRICE_ZONES and fe.DELIVERY_DATE not in entry
                                           for name, entry in zones.items() if isinstance(entry, dict)):
            found[day] = doc
    return found


def _ask(url):
    """One request for a long period: (answer, None) or (None, reason)."""
    return fe._get(url, socket_timeout=120, max_s=300)


def prices_of_span(base, code, first, last, expected, ask=_ask, sleep=time.sleep):
    """{day: [(hour, price)]} of a zone for the delivery days `first` to
    `last`, from one request or, when that fails, from the requests for its
    two halves. Raises RewriteFailed when a part of it cannot be had.

    `expected` says whether the archive has this zone somewhere in the span:
    an answer with nothing at all for a zone the archive has is asked again in
    halves before it is believed."""
    span = (last - first).days + 1
    start = fe.carry_over.day_bounds(first - timedelta(days=1))[0]      # a day more on each side: a zone
    end = fe.carry_over.day_bounds(last + timedelta(days=1))[1]         # whose day is on another clock
    params = urllib.parse.urlencode({
        'securityToken': fe.API_KEY, 'documentType': 'A44',
        'in_Domain': fe.ZONES[code], 'out_Domain': fe.ZONES[code],
        'periodStart': start.strftime('%Y%m%d%H%M'), 'periodEnd': end.strftime('%Y%m%d%H%M'),
    })
    text, reason = None, 'not asked'
    for wait in (0,) + REWRITE_WAITS:
        if wait:
            sleep(wait)
        text, reason = ask(f'{base}?{params}')
        if text is not None or (reason or '').startswith('HTTP 4'):
            break                        # an answer, or a refusal that repeating will not change
        print(f'    {code} {first} to {last}: failed ({reason}), asking again')

    by_day = None
    if text is not None:
        by_day = {day: hours for day, hours in fe.prices_by_day(fe.read_series(text)).items() if first <= day <= last}
        if by_day or not expected or span <= SMALLEST_SPAN:
            return by_day
        reason = 'nothing in the answer, for a zone the archive has'
    if span <= SMALLEST_SPAN:
        raise RewriteFailed(f'{code} {first} to {last}: {reason}')
    print(f'    {code} {first} to {last}: {reason} - asking for its two halves')
    middle = first + timedelta(days=span // 2 - 1)
    found = prices_of_span(base, code, first, middle, expected, ask, sleep)
    found.update(prices_of_span(base, code, middle + timedelta(days=1), last, expected, ask, sleep))
    return found


def rewrite_day(day, old, found, base, now):
    """Write the file of one day over. Returns (zones that left, zones that came)."""
    with contextlib.redirect_stdout(io.StringIO()):          # one "Consolidated" line a day is too many
        new = fe.price_zones({code: by_day[day] for code, by_day in found.items() if day in by_day}, day)
    for entry in new.values():
        entry[fe.FETCHED_AT] = now
    before = old.get('zones') or {}
    zones = {}
    for name in fe.PRICE_ORDER:                              # the feed's order
        if name in new:
            zones[name] = new[name]
        elif name in before and name not in ENTSOE_PRICE_ZONES:
            zones[name] = before[name]                       # not ENTSO-E's: as it is
    for name, entry in before.items():
        if name not in zones and name not in ENTSOE_PRICE_ZONES and name not in fe.ZONES:
            zones[name] = entry
    doc = dict(old, updated=now, endpoint=base, zones=zones)
    fe.write_json(PRICE_FILES.format(day=day.isoformat()), doc, separators=(',', ':'))
    return ([name for name in before if name in ENTSOE_PRICE_ZONES and name not in new],
            [name for name in new if name not in before])


def rewrite_prices(base, years=None, ask=_ask, sleep=time.sleep, commit=None):
    """Rewrite the price files of the old reading, a year at a time. Returns
    the exit code: 0, or 1 when a year could not be had and was left alone."""
    waiting = old_reader_days()
    wanted = sorted({day.year for day in waiting} & set(years or {day.year for day in waiting}))
    print(f'{len(waiting)} files of the old reading, in the years {sorted({day.year for day in waiting})}; '
          f'rewriting {wanted}')
    parent = {zone: country for country, zones in fe.CONSOLIDATE.items() for zone in zones}
    left, came, rewritten, empty, failed = {}, {}, 0, [], []
    for year in wanted:
        days = sorted(day for day in waiting if day.year == year)
        first, last = days[0], days[-1]
        print(f'\n{year}: {len(days)} files, {first} to {last}')
        found = {}
        try:
            for code in fe.ZONES:
                if code in fe.ELEXON_ZONES:
                    continue
                name = parent.get(code, code)
                expected = any(name in (waiting[day].get('zones') or {}) for day in days)
                started = time.monotonic()
                found[code] = prices_of_span(base, code, first, last, expected, ask, sleep)
                print(f'  {code}: {len(found[code])} days with prices [{time.monotonic() - started:.0f}s]')
        except RewriteFailed as e:
            print(f'  {year} NOT rewritten: {e}')
            failed.append(year)
            continue
        now = datetime.now(timezone.utc).isoformat()
        for day in days:
            gone, new = rewrite_day(day, waiting[day], found, base, now)
            rewritten += 1
            for name in gone:
                left.setdefault(name, []).append(day)
            for name in new:
                came.setdefault(name, []).append(day)
            if not any(day in by_day for by_day in found.values()):
                empty.append(day)
        if commit:
            commit(f'Rewrite the archive of the prices of {year} with the new reading ({len(days)} days)')

    print(f'\nRewritten: {rewritten} files. Years left alone: {failed or "none"}.')
    print(f'Zone-days that left a file, because ENTSO-E has no prices for them: {sum(map(len, left.values()))}')
    for name, days in sorted(left.items()):
        print(f'  {name}: {len(days)} days, {days[0]} to {days[-1]}')
    print(f'Zone-days that came into a file that did not have them: {sum(map(len, came.values()))}')
    for name, days in sorted(came.items()):
        print(f'  {name}: {len(days)} days, {days[0]} to {days[-1]}')
    print(f'Days left with no zone of ENTSO-E: {len(empty)}' + (f' ({empty[0]} to {empty[-1]})' if empty else ''))
    return 1 if failed else 0


def main():
    if not fe.API_KEY:
        print('ERROR: ENTSOE_API_KEY environment variable is not set')
        sys.exit(1)

    if '--rewrite-prices' in sys.argv:
        years = [int(arg) for arg in sys.argv[1:] if arg.isdigit()]
        _run_git(['config', 'user.name', 'github-actions[bot]'])
        _run_git(['config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com'])
        sys.exit(rewrite_prices(fe.ALT_URLS[0], years or None, commit=_commit_and_push))

    # How many days to backfill. Defaults to 1830 (~5 years); override via env.
    # Caps at 1830 to match the cleanup cutoff in fetch_entsoe.py.
    try:
        days_back = int(os.environ.get('BACKFILL_DAYS', '1830'))
    except ValueError:
        print('ERROR: BACKFILL_DAYS must be an integer')
        sys.exit(1)
    days_back = max(1, min(1830, days_back))

    # Which datasets to fetch. Default 'all'; override via env DATASETS.
    # Accepts: 'all' / 'both', 'prices', 'gen', 'flows'. Useful for debugging
    # one at a time. ('both' kept as alias for 'all' for backwards compat.)
    datasets_env = os.environ.get('DATASETS', 'all').lower().strip()
    fetch_prices_flag = datasets_env in ('all', 'both', 'prices')
    fetch_gen_flag    = datasets_env in ('all', 'both', 'gen')
    fetch_flows_flag  = datasets_env in ('all', 'both', 'flows')

    # Per-run time budget. GitHub-hosted runners have a hard 6h limit per job;
    # we stop gracefully well before that so there's time for the final commit
    # and the self-re-trigger step in the workflow. Default 5h (18000s).
    try:
        time_budget_s = int(os.environ.get('TIME_BUDGET_SECONDS', '18000'))
    except ValueError:
        time_budget_s = 18000
    run_start = time.monotonic()

    print(f'Backfilling up to {days_back} days of historical data')
    print(f'Datasets: prices={fetch_prices_flag}, gen={fetch_gen_flag}, flows={fetch_flows_flag}')
    print(f'Time budget: {time_budget_s}s ({time_budget_s // 3600}h '
          f'{(time_budget_s % 3600) // 60}m)\n')

    # Endpoint discovery with retry.
    base = None
    for attempt in range(1, 6):
        base = fe.find_working_endpoint()
        if base:
            break
        wait_s = min(60 * attempt, 300)
        print(f'  No endpoint available yet, waiting {wait_s}s before retry '
              f'({attempt}/5)...')
        time.sleep(wait_s)
    if not base:
        print('ERROR: No working ENTSO-E endpoint found after 5 attempts')
        sys.exit(1)
    print(f'Using endpoint: {base}\n')

    os.makedirs('data/archive', exist_ok=True)

    # Configure git once (used for incremental commits inside the loop).
    _run_git(['config', 'user.name', 'github-actions[bot]'])
    _run_git(['config', 'user.email',
              '41898282+github-actions[bot]@users.noreply.github.com'])

    today = datetime.now(timezone.utc).date()
    summary = {
        'prices_archived': 0, 'prices_skipped': 0, 'prices_empty': 0, 'prices_failed': 0,
        'gen_archived':    0, 'gen_skipped':    0, 'gen_empty':    0, 'gen_failed':    0,
        'flows_archived':  0, 'flows_skipped':  0, 'flows_empty':  0, 'flows_failed':  0,
    }
    # Commit & push every N successful file writes. Roughly every 30 writes
    # covers ~15 days (2 datasets per day) — small enough to protect progress,
    # large enough to keep git overhead low.
    COMMIT_EVERY = 30
    writes_since_last_commit = 0
    time_budget_hit = False

    # Iterate yesterday -> days_back days ago.
    for offset in range(1, days_back + 1):
        # Time-budget guard: if we're close to the per-run cap, stop gracefully
        # so the workflow can commit the final batch and chain the next run.
        elapsed = time.monotonic() - run_start
        if elapsed >= time_budget_s:
            print(f'\n[TIME BUDGET] {int(elapsed)}s elapsed, stopping this run. '
                  f'Next run will continue from here (idempotent).')
            time_budget_hit = True
            break

        target = today - timedelta(days=offset)
        date_str = target.strftime('%Y-%m-%d')
        prices_path = f'data/archive/spot-prices-{date_str}.json'
        gen_path    = f'data/archive/generation-mix-{date_str}.json'
        flows_path  = f'data/archive/cross-border-flows-{date_str}.json'

        # --- Prices --------------------------------------------------------
        if fetch_prices_flag:
            if os.path.exists(prices_path):
                summary['prices_skipped'] += 1
            else:
                print(f'[{offset}/{days_back}] {date_str}: fetching prices')
                try:
                    data = fetch_prices_for_date(base, target)
                    if len(data.get('zones', {})) == 0:
                        summary['prices_empty'] += 1
                    else:
                        with open(prices_path, 'w') as f:
                            json.dump(data, f, separators=(',', ':'))
                        summary['prices_archived'] += 1
                        writes_since_last_commit += 1
                except Exception as e:
                    print(f'  prices EXCEPTION: {e}')
                    summary['prices_failed'] += 1

        # --- Generation mix ------------------------------------------------
        if fetch_gen_flag:
            if os.path.exists(gen_path):
                summary['gen_skipped'] += 1
            else:
                print(f'[{offset}/{days_back}] {date_str}: fetching generation mix')
                try:
                    data = fetch_gen_for_date(base, target)
                    if len(data.get('zones', {})) == 0:
                        summary['gen_empty'] += 1
                    else:
                        with open(gen_path, 'w') as f:
                            json.dump(data, f, separators=(',', ':'))
                        summary['gen_archived'] += 1
                        writes_since_last_commit += 1
                except Exception as e:
                    print(f'  generation EXCEPTION: {e}')
                    summary['gen_failed'] += 1

        # --- Cross-border flows --------------------------------------------
        if fetch_flows_flag:
            if os.path.exists(flows_path):
                summary['flows_skipped'] += 1
            else:
                print(f'[{offset}/{days_back}] {date_str}: fetching flows')
                try:
                    data = fetch_flows_for_date(base, target)
                    if len(data.get('corridors', {})) == 0:
                        summary['flows_empty'] += 1
                    else:
                        with open(flows_path, 'w') as f:
                            json.dump(data, f, separators=(',', ':'))
                        summary['flows_archived'] += 1
                        writes_since_last_commit += 1
                except Exception as e:
                    print(f'  flows EXCEPTION: {e}')
                    summary['flows_failed'] += 1

        # Periodic commit + push
        if writes_since_last_commit >= COMMIT_EVERY:
            total = summary['prices_archived'] + summary['gen_archived'] + summary['flows_archived']
            _commit_and_push(f'Backfill batch ({total} files total)')
            writes_since_last_commit = 0

    # Final commit for any remaining files not yet pushed
    if writes_since_last_commit > 0:
        total = summary['prices_archived'] + summary['gen_archived'] + summary['flows_archived']
        tag = 'time-budget-stop' if time_budget_hit else 'final'
        _commit_and_push(f'Backfill batch ({total} files total, {tag})')

    print('\n=== Summary ===')
    print(f'  prices: archived={summary["prices_archived"]}, '
          f'skipped={summary["prices_skipped"]}, '
          f'empty={summary["prices_empty"]}, '
          f'failed={summary["prices_failed"]}')
    print(f'  gen:    archived={summary["gen_archived"]}, '
          f'skipped={summary["gen_skipped"]}, '
          f'empty={summary["gen_empty"]}, '
          f'failed={summary["gen_failed"]}')
    print(f'  flows:  archived={summary["flows_archived"]}, '
          f'skipped={summary["flows_skipped"]}, '
          f'empty={summary["flows_empty"]}, '
          f'failed={summary["flows_failed"]}')

    # Signal file the workflow reads to decide whether to self-retrigger.
    # 'yes' -> re-dispatch the workflow; 'no' -> stop the chain.
    remaining = count_remaining_work(today, days_back,
                                     fetch_prices_flag, fetch_gen_flag,
                                     fetch_flows_flag)
    print(f'  remaining dataset-days to archive: {remaining}')
    os.makedirs('data', exist_ok=True)
    with open('data/.backfill-continue', 'w') as f:
        f.write('yes' if remaining > 0 else 'no')


def count_remaining_work(today, days_back, prices_flag, gen_flag, flows_flag):
    """Count how many dataset-days are still missing in the target window."""
    missing = 0
    for offset in range(1, days_back + 1):
        d = today - timedelta(days=offset)
        s = d.strftime('%Y-%m-%d')
        if prices_flag and not os.path.exists(f'data/archive/spot-prices-{s}.json'):
            missing += 1
        if gen_flag and not os.path.exists(f'data/archive/generation-mix-{s}.json'):
            missing += 1
        if flows_flag and not os.path.exists(f'data/archive/cross-border-flows-{s}.json'):
            missing += 1
    return missing


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _run_git(args, check=False):
    """Run a git command, return (returncode, stdout+stderr combined)."""
    try:
        result = subprocess.run(
            ['git'] + args,
            capture_output=True, text=True, timeout=120, check=check
        )
        return result.returncode, (result.stdout or '') + (result.stderr or '')
    except subprocess.TimeoutExpired:
        return 124, 'git command timed out'
    except Exception as e:
        return 1, str(e)


def _commit_and_push(message):
    """Stage data/archive/, commit, and push with rebase-and-retry.

    The regular fetch_entsoe.py cron runs every 15 minutes and commits to the
    same branch. If it commits during our batch, our push is rejected with
    'non-fast-forward'. We recover with pull --rebase and retry up to 5 times.
    """
    print(f'  -> committing batch: {message}')
    code, out = _run_git(['add', 'data/archive/'])
    if code != 0:
        print(f'    git add failed: {out.strip()}')
        return

    code, _ = _run_git(['diff', '--cached', '--quiet'])
    if code == 0:
        print('    nothing staged, skip')
        return

    code, out = _run_git(['commit', '-m', message])
    if code != 0:
        print(f'    git commit failed: {out.strip()}')
        return

    for attempt in range(1, 6):
        code, out = _run_git(['push'])
        if code == 0:
            print('    pushed')
            return
        print(f'    push failed (attempt {attempt}/5): {out.strip()[:200]}')
        rc, rout = _run_git(['pull', '--rebase', '--autostash'])
        if rc != 0:
            print(f'    pull --rebase failed: {rout.strip()[:200]}')
            time.sleep(10 * attempt)

    print('    push failed after 5 attempts - local commits remain; '
          'the workflow will retry on its next chained run')


if __name__ == '__main__':
    main()
