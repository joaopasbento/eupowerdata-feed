"""
EU Power Data - Installed capacity fetcher (ENTSO-E A68 / Installed Capacity per Production Type)
Runs via GitHub Actions (monthly + manual). Outputs data/capacity-installed.json,
consumed server-side by eupowerdata.com to populate the "Installed Capacity" block
(replacing the RenewableNodes capacity numbers, which the upstream source no longer fills).

Diagnostic logs are intentionally verbose for the first runs (one line per country),
mirroring fetch_gb.py / fetch_eurostat.py: ENTSO-E A68 cannot be tested from EasyWP,
so the first GitHub Actions run validates per-country coverage and the chosen year.

Stdlib-only. Fail-safe: a country with no usable A68 response is simply omitted
(the consumer then shows "capacity unavailable" for it, per the inclusion rule).
"""

import os
import json
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

API_KEY = os.environ.get('ENTSOE_API_KEY', '')
BASE_URL = 'https://web-api.tp.entsoe.eu/api'
ALT_URLS = [
    'https://web-api.tp.entsoe.eu/api',
    'https://transparency.entsoe.eu/api',
    'https://webportal.tp.entsoe.eu/api',
]

# Primary domain EIC per country for A68. For the split Nordic/Italian markets,
# ENTSO-E publishes installed capacity at the bidding-zone level, so we try a
# country-level domain first and fall back to summing the listed bidding zones.
DOMAIN = {
    'PT': '10YPT-REN------W', 'ES': '10YES-REE------0', 'DE': '10Y1001A1001A82H',
    'FR': '10YFR-RTE------C', 'IT': '10Y1001A1001A73I', 'NL': '10YNL----------L',
    'BE': '10YBE----------2', 'AT': '10YAT-APG------L', 'CH': '10YCH-SWISSGRIDZ',
    'PL': '10YPL-AREA-----S', 'FI': '10YFI-1--------U', 'GR': '10YGR-HTSO-----Y',
    'IE': '10Y1001A1001A59C', 'RO': '10YRO-TEL------P', 'BG': '10YCA-BULGARIA-R',
    'HU': '10YHU-MAVIR----U', 'CZ': '10YCZ-CEPS-----N', 'EE': '10Y1001A1001A39I',
    'LV': '10YLV-1001A00074', 'LT': '10YLT-1001A0008Q', 'GB': '10YGB----------A',
    # Nordics: country-level domain first
    'NO': '10YNO-0--------C', 'SE': '10YSE-1--------K', 'DK': '10Y1001A1001A65H',
    # Energy Community / South-East Europe
    'SI': '10YSI-ELES-----O', 'HR': '10YHR-HEP------M', 'RS': '10YCS-SERBIATSOV',
    'BA': '10YBA-JPCC-----D', 'ME': '10YCS-CG-TSO---S', 'MK': '10YMK-MEPSO----8',
    'AL': '10YAL-KESH-----5', 'XK': '10Y1001C--00100H',
}

# Bidding-zone fallbacks to sum when the country-level domain returns nothing.
ZONE_FALLBACK = {
    'NO': ['10YNO-1--------2', '10YNO-2--------T', '10YNO-3--------J',
           '10YNO-4--------9', '10Y1001A1001A48H'],          # NO1..NO5
    'SE': ['10Y1001A1001A44P', '10Y1001A1001A45N',
           '10Y1001A1001A46L', '10Y1001A1001A47J'],          # SE1..SE4
    'DK': ['10YDK-1--------W', '10YDK-2--------M'],           # DK1, DK2
}

# 32-country whitelist (must match $eu_codes in eew-engine.php).
COUNTRIES = list(DOMAIN.keys())

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

# Renewable grouping for the capacity block (pumped storage B10 excluded - it's storage).
GROUP = {
    'Solar': ['B16'],
    'Wind': ['B18', 'B19'],
    'Hydro': ['B11', 'B12'],
    'Biomass': ['B01'],
    'Geothermal': ['B09'],
    'Marine': ['B13'],
    'Other renewable': ['B15'],
}
RENEWABLE_GROUPS = ['Solar', 'Wind', 'Hydro', 'Biomass', 'Geothermal', 'Marine', 'Other renewable']

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


def get_ns(root):
    tag = root.tag
    if '{' in tag:
        return tag[1:tag.index('}')]
    return ''


def find_working_base():
    """Probe ALT_URLS with a cheap A68 query; return the first that answers."""
    year = datetime.now(timezone.utc).year - 1
    params = urllib.parse.urlencode({
        'securityToken': API_KEY,
        'documentType': 'A68',
        'processType': 'A33',
        'in_Domain': DOMAIN['PT'],
        'periodStart': f'{year}01010000',
        'periodEnd': f'{year + 1}01010000',
    })
    for base in ALT_URLS:
        body = http_get(f'{base}?{params}')
        if body and ('TimeSeries' in body or 'Acknowledgement' in body):
            print(f'  endpoint OK: {base}')
            return base
    print('  no working endpoint found, defaulting to BASE_URL')
    return BASE_URL


def parse_capacity_xml(xml_str):
    """A68 returns one TimeSeries per production type, each with a single Point
    whose <quantity> is the installed capacity (MW) for the period (year)."""
    by_psr = {}
    try:
        root = ET.fromstring(xml_str)
    except Exception as e:
        print(f'    XML parse error: {e}')
        return by_psr
    ns = get_ns(root)
    prefix = f'{{{ns}}}' if ns else ''
    for ts in root.iter(f'{prefix}TimeSeries'):
        psr_el = ts.find(f'{prefix}MktPSRType/{prefix}psrType')
        if psr_el is None or not psr_el.text:
            continue
        psr = psr_el.text
        qty = None
        for pt in ts.iter(f'{prefix}Point'):
            q_el = pt.find(f'{prefix}quantity')
            if q_el is not None and q_el.text:
                try:
                    qty = float(q_el.text)
                except ValueError:
                    continue
        if qty is not None:
            by_psr[psr] = by_psr.get(psr, 0.0) + qty
    return by_psr


def query_domain(base, domain, year):
    params = urllib.parse.urlencode({
        'securityToken': API_KEY,
        'documentType': 'A68',
        'processType': 'A33',
        'in_Domain': domain,
        'periodStart': f'{year}01010000',
        'periodEnd': f'{year + 1}01010000',
    })
    body = http_get(f'{base}?{params}')
    if not body:
        return {}
    return parse_capacity_xml(body)


def fetch_country(base, code):
    """Try the country domain across recent years; if empty and zone fallback
    exists, sum the bidding zones. Returns (by_psr, year, source_tag) or (None, ...)."""
    years = [datetime.now(timezone.utc).year - y for y in (1, 2, 3, 0)]  # last full year first
    for year in years:
        by_psr = query_domain(base, DOMAIN[code], year)
        if by_psr:
            return by_psr, year, 'domain'
    # Zone-sum fallback (Nordics)
    if code in ZONE_FALLBACK:
        for year in years:
            merged = {}
            for z in ZONE_FALLBACK[code]:
                zp = query_domain(base, z, year)
                for psr, mw in zp.items():
                    merged[psr] = merged.get(psr, 0.0) + mw
            if merged:
                return merged, year, 'zone-sum'
    return None, None, None


def build_country(by_psr):
    groups_mw = {}
    for g in RENEWABLE_GROUPS:
        groups_mw[g] = round(sum(by_psr.get(c, 0.0) for c in GROUP[g]))
    renewable_mw = sum(groups_mw.values())
    total_all_mw = round(sum(by_psr.values()))
    return {
        'capacity_mw':  renewable_mw,                 # total installed RENEWABLE capacity
        'solar_mw':     groups_mw['Solar'],
        'wind_mw':      groups_mw['Wind'],
        'hydro_mw':     groups_mw['Hydro'],
        'biomass_mw':   groups_mw['Biomass'],
        'geothermal_mw': groups_mw['Geothermal'],
        'other_ren_mw': groups_mw['Marine'] + groups_mw['Other renewable'],
        'total_all_mw': total_all_mw,                 # incl. fossil/nuclear (for share calc)
    }


def main():
    print('fetch_capacity.py - ENTSO-E installed capacity per production type (A68)')
    if not API_KEY:
        print('  ENTSOE_API_KEY not set - aborting (feed left untouched)')
        return
    base = find_working_base()

    countries = {}
    for code in COUNTRIES:
        by_psr, year, tag = fetch_country(base, code)
        if not by_psr:
            print(f'  {code}: no A68 data')
            continue
        c = build_country(by_psr)
        countries[code] = {'year': year, **c}
        print(f'  {code}: y={year} src={tag} renewable={c["capacity_mw"]}MW '
              f'(solar={c["solar_mw"]} wind={c["wind_mw"]} hydro={c["hydro_mw"]} '
              f'bio={c["biomass_mw"]} geo={c["geothermal_mw"]}) total_all={c["total_all_mw"]}MW')

    if not countries:
        print('  no countries resolved - NOT writing file (feed left untouched)')
        return

    out = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'ENTSO-E Transparency (A68)',
        'count': len(countries),
        'countries': countries,
    }
    os.makedirs('data', exist_ok=True)
    with open('data/capacity-installed.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    print(f'Wrote data/capacity-installed.json ({len(countries)}/{len(COUNTRIES)} countries)')


if __name__ == '__main__':
    main()
