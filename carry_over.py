"""Keeping what a source did not answer for - within the same UTC day only.

Shared by fetch_entsoe.py, fetch_gb.py, fetch_ree.py and fetch_ren.py.

When ENTSO-E does not answer in time for a zone (or a corridor), the published
file used to be written without it and the country vanished from the site
until the next run. Now the entry keeps what the published file already had
for it, provided that value was obtained on the same UTC day as the run. Two
fields say so, on every zone of spot-prices.json and generation-mix.json and on
every corridor (and net pair) of cross-border-flows.json:

    fetched_at    when the entry's data were obtained from the source: the
                  `updated` time of the run that got them. Present on every
                  entry.
    carried_over  true on an entry kept from an earlier run. Absent otherwise.

The rules, which exist so that old data can never pass for new:

  - Only the same UTC day. An entry last obtained on an earlier day is left
    out, as it always was.
  - A kept entry keeps its original fetched_at, however many runs it is kept
    for. It never takes the time of the run that kept it.
  - A file's `updated` only moves when at least one of its entries was obtained
    in the run. If none was, the file is not rewritten at all.
  - Files published before these fields existed have no fetched_at: their
    entries are taken to be as old as the file's own `updated`.
"""

from datetime import datetime, timezone

from feed_common import load_json

FETCHED_AT = 'fetched_at'
CARRIED_OVER = 'carried_over'

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


def merge(fresh, previous, key, order, fetched_at):
    """Put this run's entries together with what the published file had.

    fresh       {name: entry} obtained in this run
    previous    the published document, or None
    key         'zones' or 'corridors'
    order       the names in the order they have in the file
    fetched_at  the time of this run's data (the new file's `updated`)

    Returns (entries, kept, dropped):
      entries  {name: entry} in file order - this run's entries, stamped with
               `fetched_at`, plus the published entries this run did not get,
               if they were obtained on the same UTC day, marked carried_over
               and with the time they already had;
      kept     [(name, time)] of those;
      dropped  [(name, time)] of the published entries left out because they
               were last obtained on another day.
    """
    day = utc_day(fetched_at)
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
            if day is not None and utc_day(original) == day:
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
