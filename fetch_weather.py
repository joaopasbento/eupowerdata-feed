#!/usr/bin/env python3
"""fetch_weather.py - weather for 21 European capitals, from MET Norway.

Writes data/weather.json from the Locationforecast 2.0 API of the Norwegian
Meteorological Institute (https://api.met.no/weatherapi/locationforecast/2.0/),
for the cities of WEATHER_CITIES in the site's eew-analytics.js, in the same
order. Each city carries the fields the site's JavaScript reads today from
Open-Meteo, in the same units:

    current.temperature_2m   degrees Celsius
    current.wind_speed_10m   km/h          (MET Norway gives m/s)
    current.weather_code     WMO code      (converted from MET Norway's symbol_code)
    daily.sunshine_duration  [seconds]     for the city's local day - an ESTIMATE

MET Norway does not publish sunshine duration. It is estimated here from the
hourly cloud cover during the daylight hours of the city's local day (see
estimate_sunshine) and the file says so.

Terms of service (https://api.met.no/doc/TermsOfService, revision of
2026-11-05) and how this script follows them:

  - The terms "must be read, understood and accepted by a human in authority
    before using our service". The script therefore makes no request at all
    unless MET_NORWAY_TOS_ACCEPTED=true is set in its environment (in GitHub
    Actions: a repository variable, set by the person who accepted them).
  - Identification: every request carries the feed's User-Agent, with the site
    as the contact (feed_common.USER_AGENT). No Referer, no browser disguise.
  - Coordinates: 4 decimals at most (the cities use 2).
  - Caching: a location is never requested again before the time in its
    Expires header, nor sooner than 3 hours after the last request (the
    product's minimum outside the Nordic region; inside it the minimum is 30
    minutes, but the documentation does not say which places count as Nordic,
    so 3 hours is used for every city). When it is requested again, the
    request carries If-Modified-Since with the exact Last-Modified received
    before. The runner has no memory, so that - and the forecast itself - is
    kept between runs in data/weather-state.json.
  - Traffic: requests go out one at a time, a second apart, and the schedule
    is not on the hour. gzip is accepted. HTTPS only.
  - Status codes: 429 (throttled) and 403 (refused) stop the run's requests at
    once, and so do three failures in a row; 203 (deprecated version) is
    logged as a warning.
  - Attribution (CC BY 4.0 / NLOD 2.0): the file names MET Norway as the
    source, links the licence and says what was changed.

If MET Norway cannot be reached, cities keep the forecast already stored; if
there is nothing to show for any city, the previous weather.json is kept.
"""

import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

from feed_common import USER_AGENT, http_get, load_json, write_json

OUT_PATH = 'data/weather.json'
STATE_PATH = 'data/weather-state.json'
API = 'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}'
TOS_URL = 'https://api.met.no/doc/TermsOfService'
PAUSE_S = 1.0                 # between requests

# Same cities, coordinates and order as WEATHER_CITIES in eew/js/eew-analytics.js
# (the site matches the list to its cities by position). The time zone is for
# the city's local day.
CITIES = [
    ('PT', 'Lisbon', 38.72, -9.14, 'Europe/Lisbon'),
    ('ES', 'Madrid', 40.42, -3.70, 'Europe/Madrid'),
    ('DE', 'Berlin', 52.52, 13.41, 'Europe/Berlin'),
    ('FR', 'Paris', 48.86, 2.35, 'Europe/Paris'),
    ('IT', 'Rome', 41.90, 12.50, 'Europe/Rome'),
    ('NL', 'Amsterdam', 52.37, 4.90, 'Europe/Amsterdam'),
    ('BE', 'Brussels', 50.85, 4.35, 'Europe/Brussels'),
    ('AT', 'Vienna', 48.21, 16.37, 'Europe/Vienna'),
    ('CH', 'Bern', 46.95, 7.45, 'Europe/Zurich'),
    ('PL', 'Warsaw', 52.23, 21.01, 'Europe/Warsaw'),
    ('NO', 'Oslo', 59.91, 10.75, 'Europe/Oslo'),
    ('SE', 'Stockholm', 59.33, 18.07, 'Europe/Stockholm'),
    ('DK', 'Copenhagen', 55.68, 12.57, 'Europe/Copenhagen'),
    ('FI', 'Helsinki', 60.17, 24.94, 'Europe/Helsinki'),
    ('GR', 'Athens', 37.98, 23.73, 'Europe/Athens'),
    ('IE', 'Dublin', 53.35, -6.26, 'Europe/Dublin'),
    ('GB', 'London', 51.51, -0.13, 'Europe/London'),
    ('RO', 'Bucharest', 44.43, 26.10, 'Europe/Bucharest'),
    ('BG', 'Sofia', 42.70, 23.32, 'Europe/Sofia'),
    ('HU', 'Budapest', 47.50, 19.04, 'Europe/Budapest'),
    ('CZ', 'Prague', 50.08, 14.44, 'Europe/Prague'),
]

# "Identical requests should not be repeated more often than every 30 mins for
# the Nordic region, or every 3rd hour for the rest of the world." Which places
# are in the Nordic region is not defined, so every city waits 3 hours.
MIN_REPEAT = timedelta(hours=3)
MAX_FAILURES_IN_A_ROW = 3     # then MET Norway is taken to be down for this run

KEEP_AHEAD = timedelta(hours=60)      # hourly forecast kept in the state
STALE_AFTER = timedelta(hours=3)      # a "current" value older than this is not shown

# MET Norway symbol_code (without the _day / _night / _polartwilight suffix)
# -> WMO weather code (table 4677), the codes Open-Meteo uses where it has one.
# Anything "...andthunder" is a thunderstorm (95).
WMO = {
    'clearsky': 0, 'fair': 1, 'partlycloudy': 2, 'cloudy': 3, 'fog': 45,
    'lightrain': 61, 'rain': 63, 'heavyrain': 65,
    'lightsleet': 68, 'sleet': 69, 'heavysleet': 69,           # rain and snow mixed
    'lightsnow': 71, 'snow': 73, 'heavysnow': 75,
    'lightrainshowers': 80, 'rainshowers': 81, 'heavyrainshowers': 82,
    'lightsleetshowers': 83, 'sleetshowers': 84, 'heavysleetshowers': 84,
    'lightsnowshowers': 85, 'snowshowers': 86, 'heavysnowshowers': 86,
}

LICENSE = {
    'name': 'CC BY 4.0',
    'url': 'https://creativecommons.org/licenses/by/4.0/',
    'also': 'Norwegian Licence for Open Government Data (NLOD) 2.0',
    'credit': 'Based on data from MET Norway (The Norwegian Meteorological Institute)',
    'source_url': 'https://api.met.no/weatherapi/locationforecast/2.0/documentation',
    'policy_url': 'https://api.met.no/doc/License',
    'changes': 'Wind speed converted from m/s to km/h; symbol_code converted to a WMO weather '
               'code; sunshine duration is not MET Norway data but an estimate made by this '
               'feed from MET Norway cloud cover. Converted and estimated values are not the '
               'responsibility of MET Norway.',
}

SUNSHINE_NOTE = ('Estimate, not a MET Norway product: for each hour between sunrise and sunset '
                 'of the local day, daylight seconds x (1 - cloud_area_fraction). Hours with no '
                 'cloud data are filled in at the average of the hours that have it.')


def wmo_code(symbol_code):
    """WMO weather code for a MET Norway symbol_code, or None if unknown."""
    if not symbol_code:
        return None
    base = symbol_code.split('_')[0]
    if 'thunder' in base:
        return 95
    return WMO.get(base)


def coordinate(value):
    """A coordinate for the request URL: 4 decimals at most, no padding."""
    return f'{round(value, 4):.4f}'.rstrip('0').rstrip('.')


def parse_http_date(value):
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def hour_key(moment):
    return moment.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%MZ')


# --- The forecast kept between runs ------------------------------------------

def next_allowed(city_state):
    """When this city may be requested again: not before Expires, and not
    sooner than the product's minimum interval."""
    checked = parse_time(city_state['checked']) if city_state.get('checked') else None
    if checked is None:
        return datetime.min.replace(tzinfo=timezone.utc)
    allowed = checked + MIN_REPEAT
    expires = parse_http_date(city_state.get('expires'))
    return max(allowed, expires) if expires else allowed


def store_forecast(city_state, payload, now, local_midnight):
    """Merge a Locationforecast answer into the city's stored hours: new values
    replace the stored ones, hours that have passed since keep the last value
    forecast for them, hours before today's local midnight are dropped."""
    hours = dict(city_state.get('hours') or {})
    for entry in payload['properties']['timeseries']:
        when = parse_time(entry['time'])
        if when > now + KEEP_AHEAD:
            break
        details = entry['data']['instant']['details']
        symbol = None
        for period in ('next_1_hours', 'next_6_hours', 'next_12_hours'):
            symbol = ((entry['data'].get(period) or {}).get('summary') or {}).get('symbol_code')
            if symbol:
                break
        hours[hour_key(when)] = [details.get('air_temperature'), details.get('wind_speed'),
                                 details.get('cloud_area_fraction'), symbol]
    floor = hour_key(local_midnight)
    city_state['hours'] = {k: v for k, v in sorted(hours.items()) if k >= floor}
    city_state['forecast_updated_at'] = payload['properties']['meta'].get('updated_at')


class StopRequests(Exception):
    """MET Norway is throttling or refusing us: make no more requests this run."""


def refresh_city(code, lat, lon, city_state, now, local_midnight, get=http_get):
    """Ask MET Norway for the city if it is allowed and due. Returns a word for the log."""
    if now < next_allowed(city_state):
        return 'not due'

    headers = {}
    if city_state.get('last_modified') and city_state.get('hours'):
        headers['If-Modified-Since'] = city_state['last_modified']     # exactly as received
    resp = get(API.format(lat=coordinate(lat), lon=coordinate(lon)), timeout=20, headers=headers)

    if resp.status == 429:
        raise StopRequests('HTTP 429: MET Norway is throttling this client')
    if resp.status == 403:
        raise StopRequests('HTTP 403: MET Norway refused the request (identification or terms)')
    if resp.status not in (200, 203, 304):
        return f'failed ({resp.describe()})'

    if resp.status == 304:
        outcome = 'not modified'
    else:
        try:
            store_forecast(city_state, resp.json(), now, local_midnight)
        except (ValueError, KeyError, TypeError) as e:
            return f'failed (unreadable answer: {type(e).__name__}: {e})'
        if resp.headers.get('last-modified'):
            city_state['last_modified'] = resp.headers['last-modified']
        city_state['fetched'] = now.replace(microsecond=0).isoformat()
        outcome = 'updated'
        if resp.status == 203:
            outcome += ' (WARNING: HTTP 203 - this version of the product is deprecated or in beta)'
    # A 304 also brings the new Expires. Either way, remember when we asked.
    if resp.headers.get('expires'):
        city_state['expires'] = resp.headers['expires']
    city_state['checked'] = now.replace(microsecond=0).isoformat()
    return outcome


# --- Sunshine estimate ---------------------------------------------------------

def sun_times(day, lat, lon):
    """Sunrise and sunset (UTC) of the calendar day `day` at lat/lon, by the
    standard sunrise equation. Returns (None, None) when the sun does not rise,
    and the whole solar day when it does not set."""
    n = day.toordinal() + 1721425 - 2451545 + 0.0008          # days since J2000.0, at noon
    j_star = n - lon / 360.0                                  # mean solar noon
    m = math.radians((357.5291 + 0.98560028 * j_star) % 360)
    c = 1.9148 * math.sin(m) + 0.0200 * math.sin(2 * m) + 0.0003 * math.sin(3 * m)
    lam = math.radians((math.degrees(m) + c + 180 + 102.9372) % 360)
    j_transit = 2451545.0 + j_star + 0.0053 * math.sin(m) - 0.0069 * math.sin(2 * lam)
    sin_dec = math.sin(lam) * math.sin(math.radians(23.4397))
    cos_dec = math.cos(math.asin(sin_dec))
    phi = math.radians(lat)
    cos_omega = (math.sin(math.radians(-0.833)) - math.sin(phi) * sin_dec) / (math.cos(phi) * cos_dec)
    if cos_omega > 1:
        return None, None                                     # polar night
    omega = 180.0 if cos_omega < -1 else math.degrees(math.acos(cos_omega))

    def to_utc(julian):
        return datetime(2000, 1, 1, 12, tzinfo=timezone.utc) + timedelta(days=julian - 2451545.0)

    return to_utc(j_transit - omega / 360.0), to_utc(j_transit + omega / 360.0)


def estimate_sunshine(hours, sunrise, sunset):
    """Estimated seconds of sunshine between `sunrise` and `sunset`, from the
    stored hourly cloud cover (percent). Returns (seconds, coverage) where
    coverage is the share of the daylight that had cloud data, or (None, 0)."""
    if sunrise is None:
        return 0.0, 1.0
    daylight = (sunset - sunrise).total_seconds()
    covered = sunny = 0.0
    slot = sunrise.replace(minute=0, second=0, microsecond=0)
    while slot < sunset:
        nxt = slot + timedelta(hours=1)
        seconds = (min(nxt, sunset) - max(slot, sunrise)).total_seconds()
        clouds = [h[2] for h in (hours.get(hour_key(slot)), hours.get(hour_key(nxt)))
                  if h and h[2] is not None]
        if clouds and seconds > 0:
            covered += seconds
            sunny += seconds * (1 - (sum(clouds) / len(clouds)) / 100.0)
        slot = nxt
    if covered <= 0 or daylight <= 0:
        return None, 0.0
    return sunny * daylight / covered, covered / daylight


# --- The file --------------------------------------------------------------------

def city_entry(code, name, lat, lon, tz, city_state, now):
    """One city of weather.json, from its stored forecast."""
    zone = ZoneInfo(tz)
    local_now = now.astimezone(zone)
    hours = city_state.get('hours') or {}

    current = None
    past = [k for k in hours if k <= hour_key(now)]
    if past and now - parse_time(past[-1]) <= STALE_AFTER:
        temp, wind, _cloud, symbol = hours[past[-1]]
        if temp is not None and wind is not None:
            current = {
                'time': parse_time(past[-1]).astimezone(zone).strftime('%Y-%m-%dT%H:%M'),
                'temperature_2m': temp,
                'wind_speed_10m': round(wind * 3.6, 1),
                'weather_code': wmo_code(symbol),
                'symbol_code': symbol,
            }

    sunrise, sunset = sun_times(local_now.date(), lat, lon)
    seconds, coverage = estimate_sunshine(hours, sunrise, sunset)
    return {
        'code': code,
        'name': name,
        'latitude': lat,
        'longitude': lon,
        'timezone': tz,
        'fetched': city_state.get('fetched'),
        'forecast_updated_at': city_state.get('forecast_updated_at'),
        'current_units': {'time': 'iso8601', 'temperature_2m': '°C', 'wind_speed_10m': 'km/h',
                          'weather_code': 'wmo code'},
        'current': current,
        'daily_units': {'time': 'iso8601', 'sunshine_duration': 's'},
        'daily': {
            'time': [local_now.date().isoformat()],
            'sunshine_duration': [None if seconds is None else round(seconds, 1)],
            'sunshine_duration_estimated': True,
            'sunshine_duration_coverage': round(coverage, 2),
        },
    }


def build(state, now):
    cities = [city_entry(code, name, lat, lon, tz, state['cities'].get(code) or {}, now)
              for code, name, lat, lon, tz in CITIES]
    fetched = [c['fetched'] for c in cities if c['fetched']]
    return {
        'updated': max(fetched) if fetched else None,     # when MET Norway last gave new data
        'source': 'MET Norway Locationforecast 2.0 (api.met.no)',
        'license': LICENSE,
        'estimates': {'daily.sunshine_duration': SUNSHINE_NOTE},
        'cities': cities,
    }


def main(now=None, get=http_get, pause=PAUSE_S):
    if os.environ.get('MET_NORWAY_TOS_ACCEPTED', '').strip().lower() != 'true':
        print('Weather not fetched: MET Norway\'s terms of service must be read and accepted by a')
        print(f'person before the API is used ({TOS_URL}). Once that is done, set the')
        print('repository variable MET_NORWAY_TOS_ACCEPTED to "true".')
        return 0

    now = now or datetime.now(timezone.utc)
    state = load_json(STATE_PATH)
    if not isinstance(state, dict) or not isinstance(state.get('cities'), dict):
        state = {'cities': {}}
    state['about'] = ('Working state of fetch_weather.py: the forecast and the HTTP caching '
                      'headers of each city, kept between runs. Not read by the site.')

    print(f'MET Norway Locationforecast, {len(CITIES)} cities, as "{USER_AGENT}"')
    stopped = None
    asked = False
    failures = 0
    in_a_row = 0
    for code, name, lat, lon, tz in CITIES:
        city_state = state['cities'].setdefault(code, {})
        zone = ZoneInfo(tz)
        local = now.astimezone(zone)
        midnight = datetime(local.year, local.month, local.day, tzinfo=zone).astimezone(timezone.utc)
        if stopped:
            outcome = 'skipped'
        else:
            due = now >= next_allowed(city_state)
            if due and asked:
                time.sleep(pause)
            try:
                outcome = refresh_city(code, lat, lon, city_state, now, midnight, get=get)
            except StopRequests as e:
                stopped = str(e)
                outcome = f'stopped ({e})'
            asked = asked or due
            in_a_row = in_a_row + 1 if outcome.startswith('failed') else 0
            if in_a_row >= MAX_FAILURES_IN_A_ROW:
                stopped = f'{in_a_row} failures in a row'
        # Drop the hours before today's local midnight even when nothing was fetched.
        floor = hour_key(midnight)
        city_state['hours'] = {k: v for k, v in (city_state.get('hours') or {}).items() if k >= floor}
        failures += outcome.startswith('failed')
        print(f'  {code} {name}: {outcome}')

    write_json(STATE_PATH, state, separators=(',', ':'))

    result = build(state, now)
    shown = sum(1 for c in result['cities'] if c['current'])
    if not shown:
        print(f'No current weather for any city - keeping the previous {OUT_PATH}')
        return 1
    if load_json(OUT_PATH) == result:
        print(f'{OUT_PATH} is already up to date ({shown}/{len(CITIES)} cities)')
    else:
        write_json(OUT_PATH, result, indent=2)
        print(f'Wrote {OUT_PATH} ({shown}/{len(CITIES)} cities with current weather)')
    # The file is written either way; a non-zero exit only flags the step in Actions.
    return 1 if (stopped or failures) else 0


if __name__ == '__main__':
    sys.exit(main())
