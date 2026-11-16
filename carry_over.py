"""Keeping what a source did not answer for - for a limited time only.

Shared by fetch_entsoe.py, fetch_gb.py, fetch_ree.py and fetch_ren.py.

When ENTSO-E does not answer in time for a zone (or a corridor), the published
file used to be written without it and the country vanished from the site
until the next run. Now the entry keeps what the published file already had
for it, for as long as the file's rule allows:

    spot-prices.json           while the value was obtained on the same UTC
                               day as the run. Prices are per delivery day:
                               yesterday's must never pass for today's.
    generation-mix.json        while the value was obtained less than three
    cross-border-flows.json    hours before the run, whatever the day.

Three fields say what an entry is, on every zone of generation-mix.json and
on every corridor (and net pair) of cross-border-flows.json; the zones of
spot-prices.json have the first two:

    fetched_at    when the entry's data were obtained from the source: the
                  `updated` time of the run that got them. Present on every
                  entry.
    carried_over  true on an entry kept from an earlier run. Absent otherwise.
    data_time     the time the value itself is for: the start of the last
                  interval the source had published when it was asked. Not
                  the time of the request. An entry put together from several
                  series (the technologies of a mix, the zones of a country,
                  the two corridors of a net pair) has the oldest of theirs;
                  a consumption series that has stopped does not date a mix
                  (see latest_generation() in fetch_entsoe.py).

The rules, which exist so that old data can never pass for new:

  - Only within the file's limit. An entry last obtained before it is left
    out, as it always was.
  - A kept entry keeps its original fetched_at and data_time, however many
    runs it is kept for. It never takes the time of the run that kept it.
  - A file's `updated` only moves when at least one of its entries was obtained
    in the run. If none was, the file is not rewritten at all.
  - Files published before these fields existed have no fetched_at: their
    entries are taken to be as old as the file's own `updated`.
"""

from collections import namedtuple
from datetime import datetime, timedelta, timezone

from feed_common import load_json

FETCHED_AT = 'fetched_at'
CARRIED_OVER = 'carried_over'
DATA_TIME = 'data_time'

# Published file -> (key of its entries, prefix of its counts in status.json)
FILES = {
    'data/spot-prices.json': ('zones', 'prices_zones'),
    'data/generation-mix.json': ('zones', 'generation_zones'),
    'data/cross-border-flows.json': ('corridors', 'flow_corridors'),
}


def parse_time(value):
    """An ISO 8601 time as an aware datetime, or None."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def utc_day(value):
    """The UTC date of an ISO 8601 time, or None."""
    parsed = parse_time(value)
    return parsed.astimezone(timezone.utc).date() if parsed else None


def oldest(times):
    """The earliest of some ISO 8601 times, as it was written, or None."""
    known = [t for t in times if parse_time(t) is not None]
    return min(known, key=parse_time) if known else None


# --- For how long an entry may be kept ---

MAX_AGE = timedelta(hours=3)


def _same_utc_day(original, run_time):
    day = utc_day(run_time)
    return day is not None and utc_day(original) == day


def _under_max_age(original, run_time):
    then, now = parse_time(original), parse_time(run_time)
    return then is not None and now is not None and now - then < MAX_AGE


# allows(original fetched_at, time of the run) -> whether the entry may be kept;
# `kept` and `dropped` are how the run log puts it.
Rule = namedtuple('Rule', 'allows kept dropped')
SAME_UTC_DAY = Rule(_same_utc_day, 'kept from earlier today', 'last obtained on an earlier day')
UNDER_THREE_HOURS = Rule(_under_max_age, 'kept from under 3 hours ago', 'last obtained 3 hours ago or more')

RULES = {
    'data/spot-prices.json': SAME_UTC_DAY,
    'data/generation-mix.json': UNDER_THREE_HOURS,
    'data/cross-border-flows.json': UNDER_THREE_HOURS,
}


def entry_time(entry, doc):
    """When an entry's data were obtained: its own fetched_at or, in a file
    published before entries had one, the file's `updated`."""
    own = entry.get(FETCHED_AT) if isinstance(entry, dict) else None
    return own or (doc.get('updated') if isinstance(doc, dict) else None)


def _entries(doc, key):
    """The entries of a published document, or {} if it is not what it should be."""
    entries = doc.get(key) if isinstance(doc, dict) else None
    if not isinstance(entries, dict):
        return {}
    return {name: entry for name, entry in entries.items() if isinstance(entry, dict)}


def merge(fresh, previous, key, order, fetched_at, rule=SAME_UTC_DAY):
    """Put this run's entries together with what the published file had.

    fresh       {name: entry} obtained in this run
    previous    the published document, or None
    key         'zones' or 'corridors'
    order       the names in the order they have in the file
    fetched_at  the time of this run's data (the new file's `updated`)
    rule        for how long the file keeps an entry that did not answer

    Returns (entries, kept, dropped):
      entries  {name: entry} in file order - this run's entries, stamped with
               `fetched_at`, plus the published entries this run did not get,
               if the rule still allows them, marked carried_over and with
               the time they already had;
      kept     [(name, time)] of those;
      dropped  [(name, time)] of the published entries left out because they
               were last obtained too long ago.
    """
    before = _entries(previous, key)
    names = list(dict.fromkeys(
        [n for n in order] + [n for n in fresh if n not in order] + [n for n in before if n not in order]))

    entries, kept, dropped = {}, [], []
    for name in names:
        if name in fresh:
            entry = dict(fresh[name])
            entry.pop(CARRIED_OVER, None)
            entry[FETCHED_AT] = fetched_at
            entries[name] = entry
        elif name in before:
            original = entry_time(before[name], previous)
            if rule.allows(original, fetched_at):
                entry = dict(before[name])
                entry[FETCHED_AT] = original          # never the time of this run
                entry[CARRIED_OVER] = True
                entries[name] = entry
                kept.append((name, original))
            else:
                dropped.append((name, original))
    return entries, kept, dropped


def count(doc, key, since):
    """(entries in the file, obtained since `since`, kept from before)."""
    entries = _entries(doc, key)
    fetched = 0
    for entry in entries.values():
        when = parse_time(entry_time(entry, doc))
        if not entry.get(CARRIED_OVER) and when is not None and when >= since:
            fetched += 1
    return len(entries), fetched, len(entries) - fetched


def published_counts(since, files=None):
    """The counts for status.json, read from the published files as they are
    now: how many entries each has, how many of them were obtained in this run
    (since `since`) and how many are from earlier runs."""
    counts = {}
    for path, (key, prefix) in FILES.items():
        if files is not None and path not in files:
            continue
        total, fetched, carried = count(load_json(path), key, since)
        counts[prefix] = total
        counts[prefix + '_fetched'] = fetched
        counts[prefix + '_carried_over'] = carried
    return counts
