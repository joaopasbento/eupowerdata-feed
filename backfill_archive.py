"""
EU Power Data — Historical archive backfill

One-shot script to populate data/archive/ with up to 365 days of historical
spot-prices snapshots from the ENTSO-E Transparency Platform.

Designed to be triggered manually via GitHub Actions workflow_dispatch.

Idempotent: dates that already have an archive file are skipped, so the script
can be re-run safely (useful if it times out or fails partway through).

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
        print(f'  {code}...', end=' ', flush=True)
        xml = fe.http_get(f'{base}?{params}')
        if not xml:
            print('FAIL')
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
            print(f'OK ({len(parsed)})')
        else:
            print('no data')
        time.sleep(0.2)   # gentle on the ENTSO-E API

    # Apply the same Nordic zone consolidation as the regular fetcher
    # (NO1+NO2 → NO, SE1+SE3 → SE, DK1+DK2 → DK).
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


def main():
    if not fe.API_KEY:
        print('ERROR: ENTSOE_API_KEY environment variable is not set')
        sys.exit(1)

    # How many days to backfill. Defaults to 365; override via env BACKFILL_DAYS.
    try:
        days_back = int(os.environ.get('BACKFILL_DAYS', '365'))
    except ValueError:
        print('ERROR: BACKFILL_DAYS must be an integer')
        sys.exit(1)
    days_back = max(1, min(365, days_back))
    print(f'Backfilling up to {days_back} days of historical spot prices\n')

    # Endpoint discovery with retry — ENTSO-E occasionally returns transient
    # 503s. A single failure at script start used to kill the whole run;
    # now we retry up to 5 times with exponential backoff before giving up.
    base = None
    for attempt in range(1, 6):
        base = fe.find_working_endpoint()
        if base:
            break
        wait_s = min(60 * attempt, 300)  # 60s, 120s, 180s, 240s, 300s (5min cap)
        print(f'  No endpoint available yet, waiting {wait_s}s before retry '
              f'({attempt}/5)...')
        time.sleep(wait_s)
    if not base:
        print('ERROR: No working ENTSO-E endpoint found after 5 attempts')
        sys.exit(1)
    print(f'Using endpoint: {base}\n')

    os.makedirs('data/archive', exist_ok=True)

    # Configure git once — used for incremental commits inside the loop.
    # CI runs do not have git user configured by default; workflow may or may
    # not set it, so we enforce it here to be safe.
    _run_git(['config', 'user.name', 'github-actions[bot]'])
    _run_git(['config', 'user.email',
              '41898282+github-actions[bot]@users.noreply.github.com'])

    today = datetime.now(timezone.utc).date()
    summary = {'archived': 0, 'skipped': 0, 'empty': 0, 'failed': 0}
    # Commit & push every N successful archives. Smaller batches mean work is
    # protected earlier; larger batches mean fewer git operations. 20 is a
    # good middle ground (roughly every 7-8 minutes at typical speeds).
    COMMIT_EVERY = 20
    since_last_commit = 0

    # Iterate yesterday → 365 days ago. Today's snapshot is handled by the
    # regular fetch_entsoe.py cron, so we never overwrite it here.
    for offset in range(1, days_back + 1):
        target = today - timedelta(days=offset)
        date_str = target.strftime('%Y-%m-%d')
        archive_path = f'data/archive/spot-prices-{date_str}.json'

        if os.path.exists(archive_path):
            summary['skipped'] += 1
            print(f'[{offset}/{days_back}] {date_str}: already archived, skip')
            continue

        print(f'[{offset}/{days_back}] {date_str}: fetching')
        try:
            data = fetch_prices_for_date(base, target)
        except Exception as e:
            print(f'  EXCEPTION: {e}')
            summary['failed'] += 1
            continue

        zone_count = len(data.get('zones', {}))
        if zone_count == 0:
            print(f'  no zones returned (likely no historical data for this date)')
            summary['empty'] += 1
            continue

        try:
            with open(archive_path, 'w') as f:
                json.dump(data, f, separators=(',', ':'))
            print(f'  archived ({zone_count} zones)')
            summary['archived'] += 1
            since_last_commit += 1
        except OSError as e:
            print(f'  WRITE FAIL: {e}')
            summary['failed'] += 1
            continue

        if since_last_commit >= COMMIT_EVERY:
            _commit_and_push(f'Backfill batch ({summary["archived"]} total)')
            since_last_commit = 0

    # Final commit for any remaining files not yet pushed
    if since_last_commit > 0:
        _commit_and_push(f'Backfill batch ({summary["archived"]} total, final)')

    print(
        f'\nDone. archived={summary["archived"]}, skipped={summary["skipped"]}, '
        f'empty={summary["empty"]}, failed={summary["failed"]}'
    )


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
    Failures are logged but never abort the script — local archive files stay
    on disk, and the next batch / run will try again.
    """
    print(f'  → committing batch: {message}')
    code, out = _run_git(['add', 'data/archive/'])
    if code != 0:
        print(f'    git add failed: {out.strip()}')
        return

    # Nothing staged? Nothing to do.
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
        # Typical failure: remote has new commits from the regular cron.
        # Rebase local commit on top of them and retry.
        print(f'    push failed (attempt {attempt}/5): {out.strip()[:200]}')
        rc, rout = _run_git(['pull', '--rebase', '--autostash'])
        if rc != 0:
            print(f'    pull --rebase failed: {rout.strip()[:200]}')
            # Sleep before next attempt — remote may be in the middle of a push
            time.sleep(10 * attempt)

    print('    push failed after 5 attempts — files remain committed locally; '
          'next run will push them (git status is idempotent)')


if __name__ == '__main__':
    main()
