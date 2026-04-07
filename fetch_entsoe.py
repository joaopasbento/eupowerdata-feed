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

CONSOLIDATE = {
    'NO': ['NO1', 'NO2'],
    'SE': ['SE1', 'SE3'],
    'DK': ['DK1', 'DK2'],
}

GEN_COUNTRIES = ['PT', 'ES', 'DE', 'FR', 'IT', 'NL', 'BE', 'AT', 'CH', 'PL', 'FI', 'GR', 'IE', 'RO', 'BG', 'HU', 'CZ', 'GB']

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
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.read().decode('utf-8')
    except Exception as e:
        print(f'  HTTP failed: {e}')
        return None


def find_working_endpoint():
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
    tag = root.tag
    if '{' in tag:
        return tag[1:tag.index('}')]
    return ''

def parse_prices_xml(xml_str):
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

    for country, zone_codes in CONSOLIDATE.items():
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
                ref = zone_data[0]['prices'][h] if h < len(zone_data[0].get('prices', [])) else {'time': f'{h:02d}:00', 'hour': h}
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
        print(f'  Consolidated {"+".join(zone_codes)} → {country}')

    return prices


def fetch_generation(base):
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'

    gen = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'zones': {},
    }

    for code in GEN_COUNTRIES + ['NO1', 'NO2', 'SE1', 'SE3', 'DK1', 'DK2']:
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

    for country, zone_codes in CONSOLIDATE.items():
        zone_data = [gen['zones'].get(zc) for zc in zone_codes if zc in gen['zones']]
        if not zone_data:
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
        for zc in zone_codes:
            gen['zones'].pop(zc, None)
        print(f'  Consolidated generation {"+".join(zone_codes)} → {country}')

    return gen


def fetch_flows(base):
    now = datetime.now(timezone.utc)
    start = now.strftime('%Y%m%d') + '0000'
    end = (now + timedelta(days=1)).strftime('%Y%m%d') + '0000'

    flows = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency Platform',
        'corridors': {},
        'net': {},
    }

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

    processed = set()
    for frm, to in FLOW_CORRIDORS:
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


def fetch_omip():
    """Fetch OMIP settlement prices by scraping omip.pt."""
    print('Fetching OMIP forward curves...')

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
        print(f'  Trying: {url}')
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
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
            print(f'  Retry without SSL: {url}')
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30, context=ctx) as r:
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


def main():
    if not API_KEY:
        print('ERROR: ENTSOE_API_KEY not set')
        return

    os.makedirs('data', exist_ok=True)

    print('Finding working ENTSO-E endpoint...')
    base = find_working_endpoint()
    if not base:
        print('ERROR: All ENTSO-E endpoints unreachable')
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

    omip = fetch_omip()
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

        with open('data/forward-curves.json', 'w') as f:
            json.dump(omip, f, separators=(',', ':'))
        print(f'Saved forward-curves.json ({omip_count} contracts, {len(omip["history"])} history points)')
    elif existing_fc:
        print(f'OMIP scraper returned no data, keeping existing forward-curves.json')

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
