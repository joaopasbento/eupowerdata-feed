"""
EU Power Data — ENTSO-E data fetcher
Runs via GitHub Actions every 15 minutes.
Outputs JSON files consumed by eupowerdata.com
"""

import os
import json
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

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
    'GB': '10YGB----------A',
}

TIER1 = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE']

CORRIDORS = [
    ('PT', 'ES'), ('ES', 'PT'), ('ES', 'FR'), ('FR', 'ES'),
    ('FR', 'DE'), ('DE', 'FR'), ('FR', 'GB'), ('GB', 'FR'),
    ('DE', 'NL'), ('NL', 'DE'), ('DE', 'AT'), ('AT', 'DE'),
    ('FR', 'IT'), ('IT', 'FR'), ('NL', 'BE'), ('BE', 'NL'),
    ('DE', 'PL'), ('PL', 'DE'), ('DE', 'DK1'), ('DK1', 'DE'),
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

# --- HTTP ---

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def http_get(url, timeout=45):
    """Fetch URL with SSL fallback."""
    headers = {
        'Accept': 'application/xml',
        'User-Agent': 'EUPowerData-GitHubAction/1.0',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode('utf-8')
    except Exception:
        pass
    # Retry without SSL verification
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read().decode('utf-8')
    except Exception as e:
        print(f'  HTTP failed: {e}')
        return None


def find_working_endpoint():
    """Test endpoints and return the first one that works."""
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
    for base in ALT_URLS:
        print(f'Testing endpoint: {base}')
        xml = http_get(f'{base}?{params}')
        if xml and 'TimeSeries' in xml:
            print(f'  Working!')
            return base
        print(f'  Failed')
    return None


# --- Parsers ---

def get_ns(root):
    """Extract namespace from root element."""
    tag = root.tag
    if '{' in tag:
        return tag[1:tag.index('}')]
    return ''

def parse_prices_xml(xml_str):
    """Parse ENTSO-E A44 price XML into list of {time, hour, price}."""
    prices = []
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
                    price = float(pt.find(f'{prefix}price.amount').text)
                    hour_dt = start_dt + timedelta(hours=pos - 1)
                    prices.append({
                        'time': hour_dt.strftime('%H:%M'),
                        'hour': pos - 1,
                        'price': round(price, 2),
                    })
    except Exception as e:
        print(f'  XML parse error: {e}')
    return prices


def parse_generation_xml(xml_str):
    """Parse ENTSO-E A75 generation XML into {type: mw} dict."""
    mix = {}
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

            # Get the latest point value
            last_val = 0
            for period in ts.iter(f'{prefix}Period'):
                for pt in period.iter(f'{prefix}Point'):
                    qty_el = pt.find(f'{prefix}quantity')
                    if qty_el is not None:
                        last_val = float(qty_el.text)

            mix[gen_type] = mix.get(gen_type, 0) + last_val
    except Exception as e:
        print(f'  XML parse error: {e}')
    return mix


def parse_flow_xml(xml_str):
    """Parse ENTSO-E A11 flow XML into list of {time, mw}."""
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


# --- Fetchers ---

def fetch_prices(base):
    """Fetch day-ahead prices for all zones."""
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'

    prices = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'endpoint': base,
        'zones': {},
    }

    for code, eic in ZONES.items():
        params = urllib.parse.urlencode({
            'securityToken': API_KEY,
            'documentType': 'A44',
            'in_Domain': eic,
            'out_Domain': eic,
            'periodStart': start,
            'periodEnd': end,
        })
        print(f'  Prices: {code}...', end=' ')
        xml = http_get(f'{base}?{params}')
        if not xml:
            print('FAIL')
            continue

        parsed = parse_prices_xml(xml)
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
            print(f'OK ({len(parsed)} hours)')
        else:
            print('no data')

        import time; time.sleep(0.2)

    return prices


def fetch_generation(base):
    """Fetch generation mix for Tier 1 countries."""
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'

    gen = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'zones': {},
    }

    for code in TIER1:
        eic = ZONES.get(code, '')
        if not eic:
            continue

        params = urllib.parse.urlencode({
            'securityToken': API_KEY,
            'documentType': 'A75',
            'processType': 'A16',
            'in_Domain': eic,
            'periodStart': start,
            'periodEnd': end,
        })
        print(f'  Generation: {code}...', end=' ')
        xml = http_get(f'{base}?{params}')
        if not xml:
            print('FAIL')
            continue

        mix = parse_generation_xml(xml)
        if mix:
            total = sum(mix.values())
            renewable = sum(mix.get(rt, 0) for rt in RENEWABLE_TYPES)
            gen['zones'][code] = {
                'mix': mix,
                'total_mw': total,
                'renewable_pct': round((renewable / total) * 100, 1) if total > 0 else 0,
            }
            print(f'OK ({len(mix)} types, {total:.0f} MW)')
        else:
            print('no data')

        import time; time.sleep(0.3)

    return gen


def fetch_flows(base):
    """Fetch cross-border flows for key corridors."""
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'

    flows = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'corridors': {},
        'net': {},
    }

    for frm, to in CORRIDORS:
        from_eic = ZONES.get(frm, '')
        to_eic = ZONES.get(to, '')
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
        print(f'  Flow: {frm}→{to}...', end=' ')
        xml = http_get(f'{base}?{params}')
        if not xml:
            print('FAIL')
            continue

        values = parse_flow_xml(xml)
        if values:
            key = f'{frm}→{to}'
            flows['corridors'][key] = {
                'from': frm,
                'to': to,
                'latest': values[-1],
                'values': values,
            }
            print(f'OK ({len(values)} points)')
        else:
            print('no data')

        import time; time.sleep(0.2)

    # Calculate net flows
    processed = set()
    for frm, to in CORRIDORS:
        pair = '-'.join(sorted([frm, to]))
        if pair in processed:
            continue
        processed.add(pair)

        fwd = flows['corridors'].get(f'{frm}→{to}', {}).get('latest', {}).get('mw', 0)
        rev = flows['corridors'].get(f'{to}→{frm}', {}).get('latest', {}).get('mw', 0)
        net = fwd - rev

        flows['net'][pair] = {
            'from': frm if net >= 0 else to,
            'to': to if net >= 0 else frm,
            'net_mw': abs(round(net)),
        }

    return flows


# --- Main ---

def main():
    if not API_KEY:
        print('ERROR: ENTSOE_API_KEY not set')
        return

    os.makedirs('data', exist_ok=True)

    print('Finding working ENTSO-E endpoint...')
    base = find_working_endpoint()
    if not base:
        print('ERROR: All ENTSO-E endpoints unreachable')
        # Write error status
        with open('data/status.json', 'w') as f:
            json.dump({'status': 'error', 'time': datetime.now(timezone.utc).isoformat()}, f)
        return

    print(f'\nUsing endpoint: {base}\n')

    print('Fetching day-ahead prices...')
    prices = fetch_prices(base)
    zone_count = len(prices['zones'])
    print(f'  → {zone_count} zones\n')

    print('Fetching generation mix...')
    gen = fetch_generation(base)
    gen_count = len(gen['zones'])
    print(f'  → {gen_count} zones\n')

    print('Fetching cross-border flows...')
    flows = fetch_flows(base)
    flow_count = len(flows['corridors'])
    print(f'  → {flow_count} corridors\n')

    # Only save if we got data
    if zone_count > 0:
        with open('data/spot-prices.json', 'w') as f:
            json.dump(prices, f, separators=(',', ':'))
        print(f'Saved spot-prices.json ({zone_count} zones)')

    if gen_count > 0:
        with open('data/generation-mix.json', 'w') as f:
            json.dump(gen, f, separators=(',', ':'))
        print(f'Saved generation-mix.json ({gen_count} zones)')

    if flow_count > 0:
        with open('data/cross-border-flows.json', 'w') as f:
            json.dump(flows, f, separators=(',', ':'))
        print(f'Saved cross-border-flows.json ({flow_count} corridors)')

    # Status file
    with open('data/status.json', 'w') as f:
        json.dump({
            'status': 'ok',
            'endpoint': base,
            'time': datetime.now(timezone.utc).isoformat(),
            'prices_zones': zone_count,
            'generation_zones': gen_count,
            'flow_corridors': flow_count,
        }, f, indent=2)

    print('\nDone!')


if __name__ == '__main__':
    main()
