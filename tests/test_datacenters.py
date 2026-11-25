"""Tests for what fetch_datacenters.py does when Overpass does not answer: it
tries again later in the same run and, if it still has nothing, the run ends
well with a warning while the file kept is at most 7 days old. On a real
answer of Overpass (tests/fixtures/datacenters, the data centres of Lisbon on
24 Nov 2026) and the error the runs saw ("Network is unreachable"). No network.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone

import fetch_datacenters as fd

ANSWER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'datacenters', 'overpass_lisbon.json')


class Overpass:
    """Fails `failures` times as the runs saw, then gives the saved answer."""

    def __init__(self, failures):
        self.failures, self.calls = failures, 0

    def __call__(self, request, timeout=90, **_kwargs):
        self.calls += 1
        if self.calls <= self.failures:
            raise urllib.error.URLError(OSError(101, 'Network is unreachable'))
        return open(ANSWER, 'rb')


def quiet(function, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()) as log:
        result = function(*args, **kwargs)
    return result, log.getvalue()


class TryingAgainLater(unittest.TestCase):

    def test_after_three_quick_failures_it_waits_and_gets_the_answer(self):
        waits = []
        overpass = Overpass(failures=4)
        elements, log = quiet(fd.fetch_overpass, urlopen=overpass, sleep=waits.append)
        with open(ANSWER, encoding='utf-8') as f:
            self.assertEqual(elements, json.load(f)['elements'])
        self.assertEqual(waits, [10, 10, 300, 600])
        self.assertIn('Trying again in 5 minutes', log)

    def test_when_every_attempt_fails_it_gives_nothing(self):
        waits = []
        overpass = Overpass(failures=99)
        elements, log = quiet(fd.fetch_overpass, urlopen=overpass, sleep=waits.append)
        self.assertEqual((elements, overpass.calls), ([], 6))
        self.assertEqual(sum(waits), 10 + 10 + 300 + 600 + 900)
        self.assertIn('All Overpass attempts failed', log)


class TheWebsite(unittest.TestCase):

    def test_only_a_web_address_is_kept(self):
        def one(tags):
            element = {'type': 'node', 'id': 1, 'lat': 38.7, 'lon': -9.1, 'tags': {'name': 'DC', **tags}}
            found = fd.parse_elements([element])
            return found[0]['website'] if found else None
        self.assertEqual(one({'website': 'https://example.com'}), 'https://example.com')
        self.assertEqual(one({'website': 'javascript:alert(1)'}), '')
        self.assertEqual(one({'url': 'data:text/html,x'}), '')


class TheEndOfTheRun(unittest.TestCase):

    NOW = datetime(2026, 11, 24, 4, 30, tzinfo=timezone.utc)

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = os.path.join(folder.name, 'datacenters.json')

    def kept(self, days_old):
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump({'updated': (self.NOW - timedelta(days=days_old)).isoformat(), 'datacenters': []}, f)

    def test_a_run_that_wrote_the_file_ends_well(self):
        self.assertEqual(fd.exit_code(True, self.path, self.NOW), 0)

    def test_nothing_from_overpass_and_a_file_of_3_days_ends_well_with_a_warning(self):
        self.kept(3)
        code, log = quiet(fd.exit_code, False, self.path, self.NOW)
        self.assertEqual(code, 0)
        self.assertIn('::warning::', log)

    def test_nothing_from_overpass_and_a_file_of_8_days_fails(self):
        self.kept(8)
        code, log = quiet(fd.exit_code, False, self.path, self.NOW)
        self.assertEqual(code, 1)
        self.assertIn('::error::', log)

    def test_nothing_from_overpass_and_no_file_fails(self):
        self.assertEqual(quiet(fd.exit_code, False, self.path, self.NOW)[0], 1)

    def test_the_file_kept_is_not_touched(self):
        folder = os.path.dirname(self.path)
        here = os.getcwd()
        self.addCleanup(os.chdir, here)
        os.chdir(folder)
        os.makedirs('data')
        with open('data/datacenters.json', 'w', encoding='utf-8') as f:
            f.write('{"updated":"2026-11-23T04:00:00+00:00","datacenters":[]}')
        written, _log = quiet(fd.fetch_datacenters, fetch=lambda: [])
        self.assertFalse(written)
        with open('data/datacenters.json', encoding='utf-8') as f:
            self.assertEqual(f.read(), '{"updated":"2026-11-23T04:00:00+00:00","datacenters":[]}')


if __name__ == '__main__':
    unittest.main()
