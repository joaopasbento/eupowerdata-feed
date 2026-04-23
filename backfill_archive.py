"""
EU Power Data — Historical archive backfill

One-shot script to populate data/archive/ with up to ~5 years (1830 days) of
historical snapshots from the ENTSO-E Transparency Platform.

Archives TWO datasets per day:
  - spot-prices-YYYY-MM-DD.json    (day-ahead prices)
  - generation-mix-YYYY-MM-DD.json (generation mix → feeds Carbon & Renewables)

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

import os
import sys
import json
import time
import subprocess
import urllib.parse
from datetime import datetime, timedelta, timezone

# Reuse zone codes, consolidation rules, HTTP helper and XML parser from the
# regular fetcher — single source of truth.
import fetch_entsoe as fe


# ---------------------------------------------------------------------------
# Spot prices for one past date
# ---------------------------------------------------------------------------

def fetch_prices_for_date(base, target_date):
    """Fetch ENTSO-E day-ahead prices for a specific UTC date.

    Returns the same dict shape as fetch_entsoe.fetch_prices() so the
    archived file is interchangeable with files produced by the regular cron.

    target_date: a datetime.date object (UTC date).
    """
    start = target_date.strftime('%Y%m%d') + '0000'
    end_dt = target_date + timedelta(days=1)
    end = end_dt.strftime('%Y%m%d') + '0000'

    prices = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'date': target_date.strftime('%Y-%m-%d'),
        'source': 'ENTSO-E Transparency Platform (backfilled)',
        'endpoint': base,
        'zones': {},
    }

    for code, eic in fe.ZONES.items():
        params = urllib.parse.urlencode({
            'securityToken': fe.API_KEY,
            'documentType': 'A44',
            'in_Domain': eic,
            'out_Domain': eic,
            'periodStart': start,
            'periodEnd': end,
        })
        xml = fe.http_get(f'{base}?{params}')
        if not xml:
            continue
        parsed = fe.parse_prices_xml(xml)
        if parsed:
            price_vals = [p['price'] for p in parsed]
            prices['zones'][code] = {
                'eic': eic,
                'prices': parsed,
                'latest': parsed[-1],
                'avg': round(sum(price_vals) / len(price_vals), 2),
                'min': round(min(price_vals), 2),
                'max': round(max(price_vals), 2),
            }
        time.sleep(0.15)   # gentle on the ENTSO-E API

    # Apply the same Nordic zone consolidation as the regular fetcher
    # (NO1+NO2 -> NO, SE1+SE3 -> SE, DK1+DK2 -> DK).
    for country, zone_codes in fe.CONSOLIDATE.items():
        zone_data = [prices['zones'].get(zc) for zc in zone_codes if zc in prices['zones']]
        if not zone_data:
            continue
        all_avgs = [z['avg'] for z in zone_data if z.get('avg') is not None]
        all_mins = [z['min'] for z in zone_data if z.get('min') is not None]
        all_maxs = [z['max'] for z in zone_data if z.get('max') is not None]
        merged_prices = []
        max_hours = max(len(z.get('prices', [])) for z in zone_data)
        for h in range(max_hours):
            hour_prices = [z['prices'][h]['price'] for z in zone_data if h < len(z.get('prices', []))]
            if hour_prices:
                avg_p = round(sum(hour_prices) / len(hour_prices), 2)
                ref = (zone_data[0]['prices'][h]
                       if h < len(zone_data[0].get('prices', []))
                       else {'time': f'{h:02d}:00', 'hour': h})
                merged_prices.append({'time': ref['time'], 'hour': ref['hour'], 'price': avg_p})
        prices['zones'][country] = {
            'eic': ', '.join(z.get('eic', '') for z in zone_data),
            'prices': merged_prices,
            'latest': merged_prices[-1] if merged_prices else None,
            'avg': round(sum(all_avgs) / len(all_avgs), 2) if all_avgs else 0,
            'min': round(min(all_mins), 2) if all_mins else 0,
            'max': round(max(all_maxs), 2) if all_maxs else 0,
        }
        for zc in zone_codes:
            prices['zones'].pop(zc, None)

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
# Main
# ---------------------------------------------------------------------------

def main():
    if not fe.API_KEY:
        print('ERROR: ENTSOE_API_KEY environment variable is not set')
        sys.exit(1)

    # How many days to backfill. Defaults to 1830 (~5 years); override via env.
    # Caps at 1830 to match the cleanup cutoff in fetch_entsoe.py.
    try:
        days_back = int(os.environ.get('BACKFILL_DAYS', '1830'))
    except ValueError:
        print('ERROR: BACKFILL_DAYS must be an integer')
        sys.exit(1)
    days_back = max(1, min(1830, days_back))

    # Which datasets to fetch. Default 'both'; override via env DATASETS.
    # Accepts: 'both', 'prices', 'gen'. Useful for debugging one at a time.
    datasets_env = os.environ.get('DATASETS', 'both').lower().strip()
    fetch_prices_flag = datasets_env in ('both', 'prices')
    fetch_gen_flag    = datasets_env in ('both', 'gen')

    # Per-run time budget. GitHub-hosted runners have a hard 6h limit per job;
    # we stop gracefully well before that so there's time for the final commit
    # and the self-re-trigger step in the workflow. Default 5h (18000s).
    try:
        time_budget_s = int(os.environ.get('TIME_BUDGET_SECONDS', '18000'))
    except ValueError:
        time_budget_s = 18000
    run_start = time.monotonic()

    print(f'Backfilling up to {days_back} days of historical data')
    print(f'Datasets: prices={fetch_prices_flag}, gen={fetch_gen_flag}')
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

        # Periodic commit + push
        if writes_since_last_commit >= COMMIT_EVERY:
            total = summary['prices_archived'] + summary['gen_archived']
            _commit_and_push(f'Backfill batch ({total} files total)')
            writes_since_last_commit = 0

    # Final commit for any remaining files not yet pushed
    if writes_since_last_commit > 0:
        total = summary['prices_archived'] + summary['gen_archived']
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

    # Signal file the workflow reads to decide whether to self-retrigger.
    # 'yes' -> re-dispatch the workflow; 'no' -> stop the chain.
    remaining = count_remaining_work(today, days_back,
                                     fetch_prices_flag, fetch_gen_flag)
    print(f'  remaining dataset-days to archive: {remaining}')
    os.makedirs('data', exist_ok=True)
    with open('data/.backfill-continue', 'w') as f:
        f.write('yes' if remaining > 0 else 'no')


def count_remaining_work(today, days_back, prices_flag, gen_flag):
    """Count how many dataset-days are still missing in the target window."""
    missing = 0
    for offset in range(1, days_back + 1):
        d = today - timedelta(days=offset)
        s = d.strftime('%Y-%m-%d')
        if prices_flag and not os.path.exists(f'data/archive/spot-prices-{s}.json'):
            missing += 1
        if gen_flag and not os.path.exists(f'data/archive/generation-mix-{s}.json'):
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
