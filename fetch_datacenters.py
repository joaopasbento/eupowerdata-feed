"""
EU Power Data — Data Center fetcher
Queries OpenStreetMap Overpass API for European data centers.
Embeds Ember Climate 2025 annual energy averages per country.
Outputs data/datacenters.json consumed by eupowerdata.com

Run standalone:
    python fetch_datacenters.py

Or import and call:
    from fetch_datacenters import fetch_datacenters
    fetch_datacenters()
"""

import os
import json
import time
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone

# ─── Ember Climate 2025 annual averages (source: Ember European Electricity Review 2025)
# price_eur_mwh: annual day-ahead average in EUR/MWh
# carbon_gco2:   carbon intensity in gCO2eq/kWh
# clean_pct:     share of clean (renewables + nuclear) electricity generation
# grid_wait_yr:  typical grid connection waiting time for large consumers (Ember/TSO reports)
EMBER_ANNUAL = {
    'AT': {'price_eur_mwh': 83,  'carbon_gco2': 88,  'clean_pct': 77, 'grid_wait_yr': 3},
    'BE': {'price_eur_mwh': 93,  'carbon_gco2': 145, 'clean_pct': 62, 'grid_wait_yr': 4},
    'BG': {'price_eur_mwh': 108, 'carbon_gco2': 385, 'clean_pct': 42, 'grid_wait_yr': 4},
    'CH': {'price_eur_mwh': 76,  'carbon_gco2': 25,  'clean_pct': 92, 'grid_wait_yr': 3},
    'CZ': {'price_eur_mwh': 98,  'carbon_gco2': 390, 'clean_pct': 43, 'grid_wait_yr': 5},
    'DE': {'price_eur_mwh': 92,  'carbon_gco2': 295, 'clean_pct': 68, 'grid_wait_yr': 7},
    'DK': {'price_eur_mwh': 78,  'carbon_gco2': 115, 'clean_pct': 77, 'grid_wait_yr': 3},
    'ES': {'price_eur_mwh': 72,  'carbon_gco2': 145, 'clean_pct': 63, 'grid_wait_yr': 3},
    'FI': {'price_eur_mwh': 43,  'carbon_gco2': 55,  'clean_pct': 88, 'grid_wait_yr': 3},
    'FR': {'price_eur_mwh': 68,  'carbon_gco2': 35,  'clean_pct': 93, 'grid_wait_yr': 5},
    'GB': {'price_eur_mwh': 102, 'carbon_gco2': 185, 'clean_pct': 59, 'grid_wait_yr': 5},
    'GR': {'price_eur_mwh': 108, 'carbon_gco2': 340, 'clean_pct': 47, 'grid_wait_yr': 4},
    'HU': {'price_eur_mwh': 83,  'carbon_gco2': 170, 'clean_pct': 65, 'grid_wait_yr': 4},
    'IE': {'price_eur_mwh': 112, 'carbon_gco2': 280, 'clean_pct': 43, 'grid_wait_yr': 5},
    'IT': {'price_eur_mwh': 105, 'carbon_gco2': 230, 'clean_pct': 53, 'grid_wait_yr': 4},
    'NL': {'price_eur_mwh': 88,  'carbon_gco2': 280, 'clean_pct': 52, 'grid_wait_yr': 6},
    'NO': {'price_eur_mwh': 27,  'carbon_gco2': 15,  'clean_pct': 98, 'grid_wait_yr': 2},
    'PL': {'price_eur_mwh': 122, 'carbon_gco2': 620, 'clean_pct': 25, 'grid_wait_yr': 5},
    'PT': {'price_eur_mwh': 78,  'carbon_gco2': 45,  'clean_pct': 82, 'grid_wait_yr': 2},
    'RO': {'price_eur_mwh': 92,  'carbon_gco2': 260, 'clean_pct': 53, 'grid_wait_yr': 4},
    'SE': {'price_eur_mwh': 32,  'carbon_gco2': 10,  'clean_pct': 97, 'grid_wait_yr': 2},
}

# ─── European country codes (OSM addr:country values)
EU_COUNTRIES = set(EMBER_ANNUAL.keys()) | {'LU', 'SK', 'SI', 'LT', 'LV', 'EE', 'HR', 'RS', 'MK', 'AL', 'ME', 'BA', 'UA', 'MD', 'BY', 'IS', 'MT', 'CY'}

# ─── Approximate bounding boxes for coordinate → country lookup (fallback)
COUNTRY_BOUNDS = {
    'PT': (36.96, -9.50, 42.15, -6.19),
    'ES': (35.17, -9.30, 43.79,  4.33),
    'FR': (41.33, -5.14, 51.09,  9.56),
    'DE': (47.27,  5.86, 55.06, 15.04),
    'IT': (35.49,  6.63, 47.09, 18.52),
    'NL': (50.75,  3.36, 53.55,  7.23),
    'BE': (49.50,  2.54, 51.51,  6.41),
    'AT': (46.37,  9.53, 49.02, 17.16),
    'CH': (45.82,  5.96, 47.81, 10.49),
    'PL': (49.00, 14.12, 54.84, 24.15),
    'DK': (54.56,  8.07, 57.75, 15.20),
    'SE': (55.34, 10.96, 69.06, 24.17),
    'NO': (57.96,  4.64, 71.18, 31.07),
    'FI': (59.69, 19.51, 70.09, 31.58),
    'GR': (34.80, 19.38, 41.75, 29.65),
    'IE': (51.45,-10.48, 55.38, -6.00),
    'GB': (49.91, -7.57, 60.85,  1.76),
    'RO': (43.62, 20.26, 48.27, 29.69),
    'BG': (41.24, 22.36, 44.22, 28.61),
    'HU': (45.74, 16.11, 48.59, 22.90),
    'CZ': (48.55, 12.09, 51.06, 18.86),
}


def coords_to_country(lat, lng):
    """Derive country code from coordinates using bounding boxes."""
    for code, (s, w, n, e) in COUNTRY_BOUNDS.items():
        if s <= lat <= n and w <= lng <= e:
            return code
    return None


def fetch_overpass():
    """Fetch European data centres from OpenStreetMap via Overpass API."""

    # Europe bounding box: south, west, north, east
    bbox = '34,-12,72,42'

    query = f"""[out:json][timeout:90];
(
  node["facility"="data_centre"]({bbox});
  way["facility"="data_centre"]({bbox});
  relation["facility"="data_centre"]({bbox});
  node["building"="data_center"]({bbox});
  way["building"="data_center"]({bbox});
  node["building"="datacenter"]({bbox});
  way["building"="datacenter"]({bbox});
  node["man_made"="data_center"]({bbox});
  way["man_made"="data_center"]({bbox});
);
out center tags;"""

    encoded = urllib.parse.urlencode({'data': query})
    url = 'https://overpass-api.de/api/interpreter?' + encoded

    print('Fetching Overpass API...')
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    headers = {
        'User-Agent': 'EUPowerData-DatacenterFetch/1.0 (eupowerdata.com)',
        'Accept': 'application/json',
    }

    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=90) as r:
                    raw = r.read().decode('utf-8')
                    break
            except ssl.SSLError:
                with urllib.request.urlopen(req, timeout=90, context=ctx) as r:
                    raw = r.read().decode('utf-8')
                    break
        except Exception as e:
            print(f'  Attempt {attempt + 1} failed: {e}')
            if attempt < 2:
                time.sleep(10)
            else:
                print('  All Overpass attempts failed.')
                return []

    data = json.loads(raw)
    elements = data.get('elements', [])
    print(f'  Got {len(elements)} raw elements from Overpass')
    return elements


def parse_elements(elements):
    """Parse Overpass elements into clean datacenter objects."""
    datacenters = []
    seen_coords = set()  # deduplicate by rounded lat/lng

    for el in elements:
        el_type = el.get('type', '')
        tags = el.get('tags', {})

        # Extract coordinates
        if el_type == 'node':
            lat = el.get('lat')
            lng = el.get('lon')
        elif el_type in ('way', 'relation'):
            center = el.get('center', {})
            lat = center.get('lat')
            lng = center.get('lon')
        else:
            continue

        if lat is None or lng is None:
            continue

        # Round to 4 decimals (~11m) for dedup
        coord_key = (round(lat, 4), round(lng, 4))
        if coord_key in seen_coords:
            continue
        seen_coords.add(coord_key)

        # Country: prefer OSM tag, fall back to bounding box lookup
        country = tags.get('addr:country', '').upper().strip()
        if len(country) != 2 or country not in EU_COUNTRIES:
            country = coords_to_country(lat, lng) or ''

        # Only keep European data centers
        if not country:
            continue

        # Name
        name = (
            tags.get('name') or
            tags.get('operator') or
            tags.get('brand') or
            f'Data Centre ({el_type} {el.get("id", "")})'
        )

        # City
        city = (
            tags.get('addr:city') or
            tags.get('addr:town') or
            tags.get('addr:municipality') or
            ''
        )

        # Operator
        operator = tags.get('operator') or tags.get('brand') or ''

        # Capacity in MW (from OSM power=* or capacity=* tags, if present)
        capacity_mw = None
        cap_tag = tags.get('capacity') or tags.get('power:output')
        if cap_tag:
            try:
                # Handle "150 MW", "150MW", "150"
                cap_num = ''.join(c for c in cap_tag if c.isdigit() or c == '.')
                if cap_num:
                    val = float(cap_num)
                    # Assume MW if unit present, otherwise only store if reasonable range
                    if 'kw' in cap_tag.lower():
                        val = val / 1000
                    if 0 < val < 5000:
                        capacity_mw = round(val)
            except ValueError:
                pass

        # Website
        website = tags.get('website') or tags.get('url') or ''

        datacenters.append({
            'id': f'{el_type}_{el.get("id", "")}',
            'name': name.strip(),
            'operator': operator.strip(),
            'country': country,
            'city': city.strip(),
            'lat': round(lat, 5),
            'lng': round(lng, 5),
            'capacity_mw': capacity_mw,
            'website': website,
        })

    # Sort by country, then name
    datacenters.sort(key=lambda d: (d['country'], d['name'].lower()))
    print(f'  Parsed {len(datacenters)} unique data centres across {len(set(d["country"] for d in datacenters))} countries')
    return datacenters


def fetch_datacenters():
    """Main entry point: fetch OSM data, combine with Ember averages, save JSON."""
    os.makedirs('data', exist_ok=True)

    elements = fetch_overpass()

    if not elements:
        # Try to keep existing data if fetch failed
        existing_path = 'data/datacenters.json'
        if os.path.exists(existing_path):
            print('  Fetch failed — keeping existing datacenters.json')
            return False
        print('  No existing data and fetch failed — writing empty skeleton')
        datacenters = []
    else:
        datacenters = parse_elements(elements)

    # Count per country
    country_counts = {}
    for dc in datacenters:
        c = dc['country']
        country_counts[c] = country_counts.get(c, 0) + 1

    # Add count to ember_annual entries
    ember_with_counts = {}
    for code, stats in EMBER_ANNUAL.items():
        ember_with_counts[code] = {
            **stats,
            'dc_count': country_counts.get(code, 0),
        }

    result = {
        'updated': datetime.now(timezone.utc).isoformat(),
        'source': 'OpenStreetMap (Overpass API), Ember Climate 2025',
        'total': len(datacenters),
        'ember_annual': ember_with_counts,
        'datacenters': datacenters,
    }

    with open('data/datacenters.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, separators=(',', ':'))

    print(f'Saved datacenters.json ({len(datacenters)} data centres, {len(ember_with_counts)} countries with energy data)')
    return True


if __name__ == '__main__':
    success = fetch_datacenters()
    if not success:
        exit(1)
