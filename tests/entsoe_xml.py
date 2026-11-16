"""ENTSO-E answers for the tests: the XML documents the API gives for
generation (A75) and for flows (A11), written by hand with the elements the
feed reads. Not a test module."""

from datetime import datetime, timedelta, timezone

GENERATION_NS = 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
FLOW_NS = 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'

# What ENTSO-E answers when it has nothing for the period asked.
NO_DATA = ('<?xml version="1.0" encoding="UTF-8"?>'
           '<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
           '<mRID>x</mRID><Reason><code>999</code><text>No matching data found for Data item Actual Generation '
           'per Production Type [16.1.B&amp;C]</text></Reason></Acknowledgement_MarketDocument>')


def at(text):
    """'2026-11-16 10:00:05' as a UTC time."""
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def iso(text):
    return at(text).isoformat()


def series(start, values, minutes=60, psr=None, out=False, curve='A01', end=None):
    """One TimeSeries with one Period that starts at `start`: `values` are the
    quantities of positions 1, 2, 3..., with None for a position that is left
    out (curveType A03: the same value as the one before)."""
    start = at(start) if isinstance(start, str) else start
    end = at(end) if isinstance(end, str) else end or start + timedelta(minutes=minutes * len(values))
    domain = 'outBiddingZone_Domain.mRID' if out else 'inBiddingZone_Domain.mRID'
    technology = f'<MktPSRType><psrType>{psr}</psrType></MktPSRType>' if psr else ''
    points = ''.join(f'<Point><position>{position}</position><quantity>{value}</quantity></Point>'
                     for position, value in enumerate(values, 1) if value is not None)
    return (f'<TimeSeries><mRID>1</mRID><businessType>A01</businessType>'
            f'<{domain} codingScheme="A01">10YPT-REN------W</{domain}>'
            f'<quantity_Measure_Unit.name>MAW</quantity_Measure_Unit.name><curveType>{curve}</curveType>'
            f'{technology}<Period><timeInterval><start>{start:%Y-%m-%dT%H:%MZ}</start>'
            f'<end>{end:%Y-%m-%dT%H:%MZ}</end></timeInterval><resolution>PT{minutes}M</resolution>'
            f'{points}</Period></TimeSeries>')


def document(*all_series, ns=GENERATION_NS):
    root = 'GL_MarketDocument' if ns == GENERATION_NS else 'Publication_MarketDocument'
    return (f'<?xml version="1.0" encoding="UTF-8"?><{root} xmlns="{ns}"><mRID>x</mRID>'
            f'<type>A75</type>{"".join(all_series)}</{root}>')


def _hours(when, last):
    """The start of the 24 hours asked for at `when`, and how many hourly
    points ENTSO-E has from there up to `last` (by default the hour before
    the run)."""
    hour = when.replace(minute=0, second=0, microsecond=0)
    start = hour - timedelta(hours=24)
    last = at(last) if last else hour - timedelta(hours=1)
    return start, int((last - start) / timedelta(hours=1)) + 1


def generation_answer(mw, when, last=None):
    """A75 for a zone generating `mw`: a quarter solar, the rest gas, hourly."""
    start, count = _hours(when, last)
    if count < 1:
        return NO_DATA
    return document(series(start, [mw * 0.25] * count, psr='B16'),
                    series(start, [mw * 0.75] * count, psr='B04'))


def flow_answer(mw, when, last=None):
    """A11 for a corridor: `mw` in the last hour ENTSO-E has, 10 less before."""
    start, count = _hours(when, last)
    if count < 1:
        return NO_DATA
    return document(series(start, [mw - 10] * (count - 1) + [mw]), ns=FLOW_NS)
