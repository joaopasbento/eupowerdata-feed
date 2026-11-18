#!/usr/bin/env python3
"""low_prices_proof.py - Portugal and Spain counted twice must give the same. See SKILL.md.

    python3 .claude/skills/verify/low_prices_proof.py [year]

The daily records of the archive (data/archive/low-prices-YYYY-MM-DD.json)
count the negative, zero and low prices of Portugal and Spain twice: from
OMIE's own files (block `omie`, day-ahead) and from the feed's prices of
ENTSO-E (block `entsoe`). Both are the same market at the same resolution,
so the hours must be equal, day by day. This reads the records of the year
(the one in course unless another is given), with no request, and prints the
totals of each block over the days that have both, and every day that
differs. Exit 0 when no day differs, 1 otherwise.

A day that differs is reported, never put right here: one count is not
adjusted to the other.
"""

import os
import subprocess
import sys
from datetime import date, timedelta

ROOT = subprocess.run(['git', 'rev-parse', '--show-toplevel'], capture_output=True, text=True).stdout.strip()
sys.path.insert(0, ROOT)
os.chdir(ROOT)
import fetch_low_prices as lp  # noqa: E402


def main(argv):
    year = int(argv[0]) if argv else date.today().year
    days = [date(year, 1, 1) + timedelta(days=n) for n in range(366) if (date(year, 1, 1) + timedelta(days=n)).year == year]
    records = {day: lp.load_record(day) for day in days}
    records = {day: record for day, record in records.items() if record}
    if not records:
        print(f'No record of {year} in data/archive')
        return 1
    totals, differ = lp.compare_blocks(records, sorted(records), ('PT', 'ES'))
    print(f'{len(records)} records of {year}, {min(records)} to {max(records)}')
    for zone, total in totals.items():
        market = [day for day in sorted(records)
                  if ((((records[day].get(lp.ENTSOE) or {}).get('zones') or {}).get(zone) or {}).get('basis'))
                  == 'market-resolution']
        print(f"  {zone}: {total['days']} days in both blocks ({len(market)} of ENTSO-E at the market's resolution); "
              f"negative, zero and low hours: OMIE {total['omie']}, ENTSO-E {total['entsoe']}")
    for day, zone, omie, entsoe in differ:
        print(f'  DIFFERS {day} {zone}: OMIE {omie}, ENTSO-E {entsoe}')
    print('PROOF OK: no day differs' if not differ else f'PROOF FAILED: {len(differ)} zone-days differ')
    return 0 if not differ else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
