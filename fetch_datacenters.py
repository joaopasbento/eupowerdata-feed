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

# ─── Country acceptance.
#
# The DC map shows datacenters worldwide. The rest of the site only
# covers 24 European markets where we have live energy data. Markers
# in countries without Ember/ENTSO-E data are rendered in neutral
# grey on the frontend.
#
# We accept any valid 2-letter ISO 3166-1 alpha-2 code. In practice
# the set is whatever reverse_geocoder returns — we don't pre-restrict.
# Phase 1 used a hard-coded EXTENDED_COUNTRIES set; Phase 2 dropped
# that gate to allow worldwide coverage.

# ─── Continent mapping for frontend grouping.
#
# Standard ISO continent codes:
#   EU = Europe, AS = Asia, AF = Africa, NA = North America,
#   SA = South America, OC = Oceania, AN = Antarctica
#
# Transcontinental countries (TR, RU, GE, AZ, KZ) are placed where
# most users would look for them — typically the European side for
# countries with European cultural/political affiliation.
COUNTRY_CONTINENT = {
    # Europe
    'AD':'EU','AL':'EU','AT':'EU','AX':'EU','BA':'EU','BE':'EU','BG':'EU',
    'BY':'EU','CH':'EU','CY':'EU','CZ':'EU','DE':'EU','DK':'EU','EE':'EU',
    'ES':'EU','FI':'EU','FO':'EU','FR':'EU','GB':'EU','GG':'EU','GI':'EU',
    'GR':'EU','HR':'EU','HU':'EU','IE':'EU','IM':'EU','IS':'EU','IT':'EU',
    'JE':'EU','LI':'EU','LT':'EU','LU':'EU','LV':'EU','MC':'EU','MD':'EU',
    'ME':'EU','MK':'EU','MT':'EU','NL':'EU','NO':'EU','PL':'EU','PT':'EU',
    'RO':'EU','RS':'EU','RU':'EU','SE':'EU','SI':'EU','SJ':'EU','SK':'EU',
    'SM':'EU','UA':'EU','VA':'EU','XK':'EU',
    # Transcontinental — placed as Europe for the DC map (most DCs in
    # these countries are in their European/European-facing parts).
    'TR':'EU','GE':'EU','AZ':'EU',
    # Asia
    'AE':'AS','AF':'AS','AM':'AS','BD':'AS','BH':'AS','BN':'AS','BT':'AS',
    'CC':'AS','CN':'AS','CX':'AS','HK':'AS','ID':'AS','IL':'AS','IN':'AS',
    'IO':'AS','IQ':'AS','IR':'AS','JO':'AS','JP':'AS','KG':'AS','KH':'AS',
    'KP':'AS','KR':'AS','KW':'AS','KZ':'AS','LA':'AS','LB':'AS','LK':'AS',
    'MM':'AS','MN':'AS','MO':'AS','MV':'AS','MY':'AS','NP':'AS','OM':'AS',
    'PH':'AS','PK':'AS','PS':'AS','QA':'AS','SA':'AS','SG':'AS','SY':'AS',
    'TH':'AS','TJ':'AS','TL':'AS','TM':'AS','TW':'AS','UZ':'AS','VN':'AS',
    'YE':'AS',
    # Africa
    'AO':'AF','BF':'AF','BI':'AF','BJ':'AF','BW':'AF','CD':'AF','CF':'AF',
    'CG':'AF','CI':'AF','CM':'AF','CV':'AF','DJ':'AF','DZ':'AF','EG':'AF',
    'EH':'AF','ER':'AF','ET':'AF','GA':'AF','GH':'AF','GM':'AF','GN':'AF',
    'GQ':'AF','GW':'AF','KE':'AF','KM':'AF','LR':'AF','LS':'AF','LY':'AF',
    'MA':'AF','MG':'AF','ML':'AF','MR':'AF','MU':'AF','MW':'AF','MZ':'AF',
    'NA':'AF','NE':'AF','NG':'AF','RE':'AF','RW':'AF','SC':'AF','SD':'AF',
    'SH':'AF','SL':'AF','SN':'AF','SO':'AF','SS':'AF','ST':'AF','SZ':'AF',
    'TD':'AF','TG':'AF','TN':'AF','TZ':'AF','UG':'AF','YT':'AF','ZA':'AF',
    'ZM':'AF','ZW':'AF',
    # North America (incl. Caribbean & Central America)
    'AG':'NA','AI':'NA','AW':'NA','BB':'NA','BL':'NA','BM':'NA','BQ':'NA',
    'BS':'NA','BZ':'NA','CA':'NA','CR':'NA','CU':'NA','CW':'NA','DM':'NA',
    'DO':'NA','GD':'NA','GL':'NA','GP':'NA','GT':'NA','HN':'NA','HT':'NA',
    'JM':'NA','KN':'NA','KY':'NA','LC':'NA','MF':'NA','MQ':'NA','MS':'NA',
    'MX':'NA','NI':'NA','PA':'NA','PM':'NA','PR':'NA','SV':'NA','SX':'NA',
    'TC':'NA','TT':'NA','US':'NA','VC':'NA','VG':'NA','VI':'NA',
    # South America
    'AR':'SA','BO':'SA','BR':'SA','CL':'SA','CO':'SA','EC':'SA','FK':'SA',
    'GF':'SA','GS':'SA','GY':'SA','PE':'SA','PY':'SA','SR':'SA','UY':'SA',
    'VE':'SA',
    # Oceania
    'AS':'OC','AU':'OC','CK':'OC','FJ':'OC','FM':'OC','GU':'OC','KI':'OC',
    'MH':'OC','MP':'OC','NC':'OC','NF':'OC','NR':'OC','NU':'OC','NZ':'OC',
    'PF':'OC','PG':'OC','PN':'OC','PW':'OC','SB':'OC','TK':'OC','TO':'OC',
    'TV':'OC','UM':'OC','VU':'OC','WF':'OC','WS':'OC',
    # Antarctica
    'AQ':'AN','BV':'AN','HM':'AN','TF':'AN',
}

# ─── reverse_geocoder for coords → country resolution.
#
# Uses an offline dataset of >3M cities; for any (lat, lng) finds the
# nearest city and returns its country code. 100% accuracy for 195
# countries, instant lookups (KDTree), no network calls.
#
# The lib is loaded lazily on first use — first call takes ~0.5s to
# load the dataset, subsequent calls are sub-millisecond.
_rg = None

def _get_rg():
    """Lazy-load reverse_geocoder. Returns None if not installed
    (allows graceful fallback to bbox lookup)."""
    global _rg
    if _rg is None:
        try:
            import reverse_geocoder as rg
            _rg = rg
            print('  reverse_geocoder loaded (offline dataset)')
        except ImportError:
            print('  WARNING: reverse_geocoder not installed; using bbox fallback')
            _rg = False  # sentinel: tried, failed
    return _rg if _rg is not False else None


# ─── Approximate bounding boxes (offline fallback)
#
# Used only when reverse_geocoder is unavailable (CI without the lib
# installed, etc). Retains Europe-only coverage from Phase 1 — for
# Phase 2 worldwide, reverse_geocoder is the primary path. Bboxes
# are rectangles; imprecise near borders. Iterated smallest-first
# in coords_to_country to avoid being shadowed by larger neighbours.
COUNTRY_BOUNDS = {
    # Existing 21
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
    'GR': (34.80, 19.38, 41.75, 28.50),
    'IE': (51.45,-10.48, 55.38, -6.00),
    'GB': (49.91, -7.57, 60.85,  1.76),
    'RO': (43.62, 20.26, 48.27, 29.69),
    'BG': (41.24, 22.36, 44.22, 28.61),
    'HU': (45.74, 16.11, 48.59, 22.90),
    'CZ': (48.55, 12.09, 51.06, 18.86),
    # In-EU additions (gaps)
    'LU': (49.45,  5.73, 50.18,  6.53),
    'SK': (47.73, 16.83, 49.61, 22.57),
    'SI': (45.42, 13.38, 46.88, 16.61),
    'HR': (42.39, 13.49, 46.55, 19.45),
    'LT': (53.89, 20.95, 56.45, 26.84),
    'LV': (55.67, 20.97, 58.09, 28.24),
    'EE': (57.51, 21.76, 59.69, 28.21),
    'MT': (35.79, 14.18, 36.09, 14.58),
    'CY': (34.55, 32.27, 35.71, 34.60),
    # Western Balkans
    'RS': (42.23, 18.85, 46.18, 23.01),
    'MK': (40.85, 20.46, 42.36, 23.04),
    'AL': (39.65, 19.30, 42.65, 21.06),
    'ME': (41.85, 18.46, 43.55, 20.36),
    'BA': (42.55, 15.74, 45.27, 19.62),
    'XK': (42.20, 20.01, 43.27, 21.79),
    # Eastern Europe
    'UA': (44.39, 22.13, 52.38, 40.23),
    'MD': (45.47, 26.62, 48.49, 30.16),
    'BY': (51.26, 23.18, 56.17, 32.78),
    # Iceland
    'IS': (63.30,-24.55, 66.55,-13.50),
    # Phase 1 additions
    'TR': (35.85, 26.04, 42.10, 41.50),
    'RU': (41.20, 19.64, 81.86, 60.00),  # Western Russia (up to Urals)
    'GE': (41.05, 39.96, 43.59, 46.71),
    'AM': (38.84, 43.45, 41.30, 46.63),
    'AZ': (38.39, 44.78, 41.91, 50.37),
}


def coords_to_country(lat, lng):
    """Resolve coordinates to ISO 3166-1 alpha-2 country code.

    Primary: reverse_geocoder (offline dataset, instant).
    Fallback: bounding box lookup (Europe-only, less accurate).

    Single-coord interface; for many calls in a batch use
    coords_to_country_batch which is much faster.
    """
    rg = _get_rg()
    if rg is not None:
        try:
            results = rg.search([(lat, lng)], mode=1)
            if results:
                return results[0]['cc']
        except Exception as e:
            print(f'  reverse_geocoder error for ({lat}, {lng}): {e}')

    # Fallback: bbox (Europe-only)
    for code, (s, w, n, e) in _bounds_by_area:
        if s <= lat <= n and w <= lng <= e:
            return code
    return None


def coords_to_country_batch(coords):
    """Resolve many coordinates in one call. ~10000x faster than
    repeated single-coord calls because the KDTree is queried once.

    Args:
        coords: list of (lat, lng) tuples
    Returns:
        list of country codes (None for unresolved)
    """
    if not coords:
        return []
    rg = _get_rg()
    if rg is not None:
        try:
            results = rg.search(coords, mode=1)
            return [r['cc'] for r in results]
        except Exception as e:
            print(f'  reverse_geocoder batch error: {e}')

    # Fallback: per-coord bbox lookup
    return [coords_to_country(lat, lng) for lat, lng in coords]

# Pre-sort once at module load for efficiency (used only by bbox fallback)
_bounds_by_area = sorted(
    COUNTRY_BOUNDS.items(),
    key=lambda kv: (kv[1][2] - kv[1][0]) * (kv[1][3] - kv[1][1]),
)


def fetch_overpass():
    """Fetch worldwide data centres from OpenStreetMap via Overpass API."""

    # Worldwide bounding box: south, west, north, east
    # Phase 2: full world coverage. Country resolution uses
    # reverse_geocoder (offline dataset) — no longer dependent on
    # bbox-defined country list.
    bbox = '-90,-180,90,180'

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
    """Parse Overpass elements into clean datacenter objects.

    Country resolution uses a two-pass approach for performance:
      Pass 1: walk all elements, extract OSM tag where present,
              collect coords for ones without a tag.
      Pass 2: batch-resolve all the unknowns in a single
              reverse_geocoder call (~1ms for thousands of points).
    Then we re-walk and emit the final list.
    """
    # ─── Pass 1: stage all candidates ───────────────────────────────────
    staged = []          # list of dicts, country=None pending
    pending_coords = []  # (index_in_staged, lat, lng)
    seen_coords = set()

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

        # Country: prefer OSM tag if it's a valid 2-letter alpha code.
        # Phase 2 accepts any country in COUNTRY_CONTINENT (worldwide).
        osm_country = tags.get('addr:country', '').upper().strip()
        country = None
        if len(osm_country) == 2 and osm_country.isalpha() and osm_country in COUNTRY_CONTINENT:
            country = osm_country

        staged.append({
            'el': el, 'el_type': el_type, 'tags': tags,
            'lat': lat, 'lng': lng, 'country': country,
        })
        if country is None:
            pending_coords.append((len(staged) - 1, lat, lng))

    # ─── Pass 2: batch-resolve unknowns ─────────────────────────────────
    if pending_coords:
        coords = [(lat, lng) for _, lat, lng in pending_coords]
        resolved = coords_to_country_batch(coords)
        for (idx, _, _), country in zip(pending_coords, resolved):
            staged[idx]['country'] = country

    # ─── Pass 3: emit final datacenter records ──────────────────────────
    datacenters = []
    skipped_no_country = 0
    for s in staged:
        country = s['country']
        if not country or country not in COUNTRY_CONTINENT:
            skipped_no_country += 1
            continue

        el, tags, lat, lng, el_type = s['el'], s['tags'], s['lat'], s['lng'], s['el_type']
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
    countries_count = len(set(d["country"] for d in datacenters))
    print(f'  Parsed {len(datacenters)} unique data centres across {countries_count} countries')
    if skipped_no_country:
        print(f'  ({skipped_no_country} elements skipped: unresolvable country)')
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

    # ── Quarterly snapshot: on the first day of each quarter (Jan/Apr/Jul/Oct
    # 1st) also copy the current dataset to data/archive/datacenters-YYYY-Qn.json.
    # Powers the Data Centers page date picker (quarterly granularity — DCs
    # move slowly so daily archiving would add noise without value).
    now_utc = datetime.now(timezone.utc)
    if now_utc.day == 1 and now_utc.month in (1, 4, 7, 10):
        os.makedirs('data/archive', exist_ok=True)
        quarter = (now_utc.month - 1) // 3 + 1
        snapshot_path = f'data/archive/datacenters-{now_utc.year}-Q{quarter}.json'
        try:
            with open(snapshot_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, separators=(',', ':'))
            print(f'Archived quarterly snapshot to {snapshot_path}')
        except OSError as e:
            print(f'  Warning: could not write quarterly snapshot: {e}')

    # ── Cleanup: delete quarterly snapshots older than ~5 years (20 quarters).
    # Matches the retention policy of the daily price/generation archive.
    try:
        import re
        cutoff_year = now_utc.year - 5
        archive_dir = 'data/archive'
        if os.path.isdir(archive_dir):
            removed = 0
            for fname in os.listdir(archive_dir):
                m = re.match(r'^datacenters-(\d{4})-Q([1-4])\.json$', fname)
                if not m:
                    continue
                file_year = int(m.group(1))
                if file_year < cutoff_year:
                    os.remove(os.path.join(archive_dir, fname))
                    removed += 1
            if removed:
                print(f'Cleaned up {removed} quarterly snapshot(s) older than 5 years')
    except OSError as e:
        print(f'  Warning: quarterly snapshot cleanup failed: {e}')

    return True


if __name__ == '__main__':
    success = fetch_datacenters()
    if not success:
        exit(1)
