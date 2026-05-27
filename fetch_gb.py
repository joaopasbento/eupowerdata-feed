#!/usr/bin/env python3
"""
fetch_gb.py - Great Britain (GB) generation + price via Elexon Insights.

Post-Brexit the ENTSO-E Transparency Platform no longer publishes GB
generation-by-type or day-ahead price. (GB cross-border *flows* still come
from ENTSO-E and are handled by fetch_entsoe.py - this script does NOT touch
flows.) This script fills the gap from Elexon's Insights Solution API
(https://data.elexon.co.uk, public / no key required):

  - Generation mix : AGPT  (Actual Aggregated Generation Per Type, B1620)
                     + wind-and-solar (B1630) to recover the embedded solar
                     and wind that AGPT's transmission-metered view misses.
  - Price          : MID   (Market Index Data) - a half-hourly, volume-weighted
                     market index (N2EX), NOT the EPEX day-ahead auction used
                     on the continent. Converted GBP -> EUR (ECB / Frankfurter)
                     so GB is comparable on the EUR-based map and tables.

Design - this is an *augmenter*. It runs AFTER fetch_entsoe.py in the same
workflow job, reads the feed files it produced, and injects only the 'GB'
zone. It is FAIL-SAFE: on any error it leaves the existing files untouched and
exits 0, so it can never corrupt the ENTSO-E feed.

Naming - Elexon AGPT/B1630 use the EU 543/2013 PSR type names (the same
taxonomy as ENTSO-E), so the GB mix drops straight into the site's
categorizeGen() and renewable_pct logic with no new mapping.

NOTE (unit assumption): AGPT 'quantity' is taken to be MW (average power over
the settlement period), matching the ENTSO-E A75 convention. The first live run
should be sanity-checked: a healthy GB total is ~15-40 GW.
"""

import json
import ssl
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

# --- Elexon Insights API (public, no key) -----------------------------------
ELEXON_BASE     = 'https://data.elexon.co.uk/bmrs/api/v1'
EP_GEN          = ELEXON_BASE + '/generation/actual/per-type'
EP_WIND_SOLAR   = ELEXON_BASE + '/generation/actual/per-type/wind-and-solar'
EP_MARKET_INDEX = ELEXON_BASE + '/balancing/pricing/market-index'

# MID has two providers (N2EXMIDP, APXMIDP). The official GB "Market Price" is
# the volume-weighted average of their prices - computed in fetch_gb_price().

# --- FX: GBP -> EUR (ECB via Frankfurter, no key) ---------------------------
FX_URL      = 'https://api.frankfurter.app/latest?from=GBP&to=EUR'
FX_FALLBACK = 1.17  # only used if Frankfurter is unreachable (logged)

GB_EIC = '10YGB----------A'

GEN_FILE   = 'data/generation-mix.json'
PRICE_FILE = 'data/spot-prices.json'

# Sanity bounds to reject obviously-wrong payloads (MW / EUR per MWh).
GB_TOTAL_MIN_MW, GB_TOTAL_MAX_MW = 3000, 80000
PRICE_MIN_EUR,   PRICE_MAX_EUR   = -500, 5000

# Same renewable set the feed uses (for the feed-side renewable_pct; the
# frontend recomputes its own via calcRenewablePct, so this is informational).
RENEWABLE_TYPES = [
    'Biomass', 'Geothermal', 'Hydro Run-of-river', 'Hydro Water Reservoir',
    'Marine', 'Solar', 'Wind Offshore', 'Wind Onshore', 'Other renewable',
]


def http_get_json(url, timeout=30):
    """GET + parse JSON, with a one-shot relaxed-SSL retry (mirrors
    fetch_entsoe.py). Returns parsed JSON or None on failure."""
    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
        'User-Agent': 'EUPowerData/1.0 (+https://eupowerdata.com)',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e1:
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
                return json.loads(r.read().decode('utf-8'))
        except Exception as e2:
            print(f'  HTTP/JSON error: {e2}')
            return None


def _records(payload):
    """Elexon responses are usually {'data': [...]}, occasionally a bare list."""
    if isinstance(payload, dict):
        d = payload.get('data', payload.get('Data'))
        return d if isinstance(d, list) else []
    if isinstance(payload, list):
        return payload
    return []


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ci(rec, *keys):
    """Case-insensitive first-present key lookup on a dict record."""
    if not isinstance(rec, dict):
        return None
    low = {k.lower(): val for k, val in rec.items()}
    for k in keys:
        if k.lower() in low:
            return low[k.lower()]
    return None


def _accum(periods, start, psr, qty):
    if start is None or psr is None or qty is None:
        return
    bucket = periods.setdefault(start, {})
    bucket[psr] = bucket.get(psr, 0.0) + qty


def _latest_period_mix(payload):
    """Return {psrType: MW} for the most recent settlement period.
    Tolerates both the nested shape ({...,'data':[{psrType,quantity}]}) and a
    flat shape ({...,'psrType','quantity'})."""
    periods = {}  # startTime -> {psrType: MW}
    for rec in _records(payload):
        start = _ci(rec, 'startTime', 'start_time', 'settlementDate')
        inner = _ci(rec, 'data')
        if isinstance(inner, list):  # nested
            for row in inner:
                _accum(periods, start,
                       _ci(row, 'psrType', 'fuelType', 'type'),
                       _num(_ci(row, 'quantity', 'generation', 'value')))
        else:                         # flat
            _accum(periods, start,
                   _ci(rec, 'psrType', 'fuelType', 'type'),
                   _num(_ci(rec, 'quantity', 'generation', 'value')))
    if not periods:
        return {}
    keyed = sorted([k for k in periods if k], reverse=True)
    key = keyed[0] if keyed else next(iter(periods))
    return periods[key]


# B1630 (wind-and-solar) publishes several process types per settlement period
# (e.g. day-ahead / intraday / realtime forecasts). They must NOT be summed.
# We pick a single series, preferring the realtime/current estimate.
WS_PROC_PREFER = ('realtime', 'current', 'actual', 'estimate', 'intraday',
                  'day ahead', 'dayahead', 'forecast')


def _proc_key(rec):
    for k in ('processType', 'process_type', 'process', 'businessType'):
        v = _ci(rec, k)
        if v is not None:
            return str(v)
    return ''


def _windsolar_latest(payload):
    """Return {psrType: MW} for the latest period of the wind-and-solar feed,
    using ONE process-type series (never summing across forecast horizons)."""
    recs = _records(payload)
    if recs:
        print(f'  B1630: {len(recs)} records; sample: {recs[0]}')  # diagnostic
    by = {}  # startTime -> processType -> {psrType: MW}
    for rec in recs:
        start = _ci(rec, 'startTime', 'start_time')
        proc = _proc_key(rec)
        psr = _ci(rec, 'psrType', 'fuelType', 'type')
        qty = _num(_ci(rec, 'quantity', 'generation', 'value'))
        if start is None or psr is None or qty is None:
            continue
        slot = by.setdefault(start, {}).setdefault(proc, {})
        # dedup within a (start, proc, psr): keep the max, never sum
        slot[psr] = qty if psr not in slot else max(slot[psr], qty)
    if not by:
        return {}
    keyed = sorted([k for k in by if k], reverse=True)
    start = keyed[0] if keyed else next(iter(by))
    procs = by[start]
    names = list(procs.keys())
    chosen = None
    for pref in WS_PROC_PREFER:
        for n in names:
            if pref in n.lower():
                chosen = n
                break
        if chosen is not None:
            break
    if chosen is None:
        chosen = names[0]
    print(f'  B1630 latest {start}: processTypes={names}; using "{chosen}"')
    return procs[chosen]


def _fx_gbp_eur():
    data = http_get_json(FX_URL)
    try:
        rate = float(data['rates']['EUR'])
        if 0.8 <= rate <= 1.6:
            print(f'  FX GBP->EUR = {rate}')
            return rate
    except Exception:
        pass
    print(f'  FX GBP->EUR unavailable - using fallback {FX_FALLBACK}')
    return FX_FALLBACK


def _window():
    now = datetime.now(timezone.utc)
    frm = now.strftime('%Y-%m-%dT00:00Z')
    to = (now + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%MZ')
    return frm, to


def fetch_gb_generation():
    frm, to = _window()
    q = urllib.parse.urlencode({'from': frm, 'to': to})

    mix = _latest_period_mix(http_get_json(f'{EP_GEN}?{q}'))
    if not mix:
        print('  AGPT: no data')
        return None

    # Recover embedded solar/wind from B1630. AGPT only sees transmission-
    # metered output (GB solar is almost all embedded -> ~0 in AGPT), so we
    # drop AGPT's wind/solar entries and use B1630's complete figures instead,
    # which avoids both undercounting embedded output and double-counting.
    wsmix = _windsolar_latest(http_get_json(f'{EP_WIND_SOLAR}?{q}'))
    if wsmix:
        for k in [k for k in mix if 'solar' in k.lower() or 'wind' in k.lower()]:
            mix.pop(k, None)
        for k, v in wsmix.items():
            if 'solar' in k.lower() or 'wind' in k.lower():
                mix[k] = v
    else:
        print('  wind-and-solar: no data (GB solar may read low)')

    mix = {k: round(v) for k, v in mix.items() if v and v > 0}
    total = sum(mix.values())
    if not (GB_TOTAL_MIN_MW <= total <= GB_TOTAL_MAX_MW):
        print(f'  GB generation total {total} MW out of sane range - skipping')
        return None

    renewable = sum(mix.get(rt, 0) for rt in RENEWABLE_TYPES)
    return {
        'mix': mix,
        'total_mw': total,
        'renewable_pct': round(renewable / total * 100, 1) if total else 0,
        'source': 'Elexon (AGPT + wind/solar)',
    }


def fetch_gb_price():
    frm, to = _window()
    q = urllib.parse.urlencode({'from': frm, 'to': to})
    recs = _records(http_get_json(f'{EP_MARKET_INDEX}?{q}'))
    if not recs:
        print('  MID: no data')
        return None
    print(f'  MID: {len(recs)} records; sample: {recs[0]}')  # diagnostic

    rate = _fx_gbp_eur()

    # The official GB "Market Price" is the volume-weighted average of the MIDP
    # prices for each settlement period. Aggregate per hour, volume-weighting
    # across providers, and skip placeholder rows (price missing or exactly 0,
    # which the recent/unsettled periods of the current day report).
    agg = {}  # hour -> [sum(price*volume), sum(volume), [prices]]
    for rec in recs:
        price = _num(_ci(rec, 'price'))
        vol = _num(_ci(rec, 'volume')) or 0.0
        start = _ci(rec, 'startTime', 'start_time')
        if price is None or price == 0 or not isinstance(start, str) or len(start) < 13:
            continue
        try:
            hour = int(start[11:13])
        except ValueError:
            continue
        a = agg.setdefault(hour, [0.0, 0.0, []])
        a[0] += price * vol
        a[1] += vol
        a[2].append(price)

    prices = []
    for h in sorted(agg):
        psum, vsum, plist = agg[h]
        gbp = psum / vsum if vsum > 0 else sum(plist) / len(plist)
        eur = round(gbp * rate, 2)
        if PRICE_MIN_EUR <= eur <= PRICE_MAX_EUR:
            prices.append({'time': f'{h:02d}:00', 'hour': h, 'price': eur})
    if not prices:
        print('  MID: no usable (non-zero) prices in window')
        return None

    vals = [p['price'] for p in prices]
    return {
        'eic': GB_EIC,
        'prices': prices,
        'latest': prices[-1],
        'avg': round(sum(vals) / len(vals), 2),
        'min': round(min(vals), 2),
        'max': round(max(vals), 2),
        'source': 'Elexon MID (N2EX, market index)',
        'currency': 'EUR',
    }


def _inject(path, zone_key, zone_obj):
    """Add/overwrite a single zone in an existing feed file. Never creates the
    file and never alters any other zone. Returns True on success."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            doc = json.load(f)
    except Exception as e:
        print(f'  cannot read {path} ({e}) - skipping GB injection')
        return False
    if not isinstance(doc, dict) or not isinstance(doc.get('zones'), dict):
        print(f'  unexpected structure in {path} - skipping')
        return False
    doc['zones'][zone_key] = zone_obj
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(doc, f, separators=(',', ':'))
        print(f'  injected GB into {path}')
        return True
    except Exception as e:
        print(f'  cannot write {path} ({e})')
        return False


def main():
    print('Fetching GB generation (Elexon AGPT + wind/solar)...')
    gen = None
    try:
        gen = fetch_gb_generation()
    except Exception as e:
        print(f'  generation error: {e}')
    if gen:
        print(f"  GB mix OK: {gen['total_mw']} MW, {len(gen['mix'])} types, "
              f"renewable {gen['renewable_pct']}%")
        print(f"  GB mix detail: {gen['mix']}")
        _inject(GEN_FILE, 'GB', gen)
    else:
        print('  GB generation not injected (feed left untouched)')

    print('Fetching GB price (Elexon MID -> EUR)...')
    price = None
    try:
        price = fetch_gb_price()
    except Exception as e:
        print(f'  price error: {e}')
    if price:
        print(f"  GB price OK: {len(price['prices'])} hours, avg EUR {price['avg']}/MWh")
        _inject(PRICE_FILE, 'GB', price)
    else:
        print('  GB price not injected (feed left untouched)')

    # Always succeed: GB is supplementary and must never fail the workflow or
    # block the ENTSO-E commit.
    return 0


if __name__ == '__main__':
    sys.exit(main())
