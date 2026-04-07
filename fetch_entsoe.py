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

# Zones to consolidate: bidding zones → country average
CONSOLIDATE = {
    'NO': ['NO1', 'NO2'],
    'SE': ['SE1', 'SE3'],
    'DK': ['DK1', 'DK2'],
}

# All countries that should have generation data
GEN_COUNTRIES = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE', 'AT', 'CH', 'PL', 'FI', 'GR', 'IE', 'RO', 'BG', 'HU', 'CZ', 'GB']

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

    # Consolidate multi-zone countries (NO1+NO2→NO, SE1+SE3→SE, DK1+DK2→DK)
    for country, zone_codes in CONSOLIDATE.items():
        zone_data = [prices['zones'].get(zc) for zc in zone_codes if zc in prices['zones']]
        if not zone_data:
            continue
        # Average the prices across zones
        all_avgs = [z['avg'] for z in zone_data if z.get('avg') is not None]
        all_mins = [z['min'] for z in zone_data if z.get('min') is not None]
        all_maxs = [z['max'] for z in zone_data if z.get('max') is not None]
        # Merge hourly prices (average across zones per hour)
        merged_prices = []
        max_hours = max(len(z.get('prices', [])) for z in zone_data)
        for h in range(max_hours):
            hour_prices = [z['prices'][h]['price'] for z in zone_data if h < len(z.get('prices', []))]
            if hour_prices:
                avg_p = round(sum(hour_prices) / len(hour_prices), 2)
                ref = zone_data[0]['prices'][h] if h < len(zone_data[0].get('prices', [])) else {'time': f'{h:02d}:00', 'hour': h}
                merged_prices.append({'time': ref['time'], 'hour': ref['hour'], 'price': avg_p})

        prices['zones'][country] = {
            'eic': ', '.join(z.get('eic', '') for z in zone_data),
            'prices': merged_prices,
            'latest': merged_prices[-1] if merged
