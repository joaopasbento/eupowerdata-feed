"""Tests for the test of the ENTSO-E endpoint at the start of a run
(fetch_entsoe.find_working_endpoint): a slow answer to that one request must
not be the reason a whole run is thrown away, as it was in run 2375.

Run from the repository root:  python -m unittest discover -s tests -v

No network: ENTSO-E's answers are simulated, since its slowness cannot be
brought about on purpose.
"""

import contextlib
import io
import time
import unittest
from unittest import mock

import fetch_entsoe as fe
from test_carry_over import FLOWS, GENERATION, PRICES, STATUS, Feed, fingerprint, read
from entsoe_xml import at

MAIN, SECOND, THIRD = fe.ALT_URLS
DATA = ('<?xml version="1.0"?><Publication_MarketDocument><TimeSeries><Period>'
        '<Point><position>1</position><price.amount>50</price.amount></Point>'
        '</Period></TimeSeries></Publication_MarketDocument>')
SLOW = (None, fe.OUT_OF_TIME)                  # nothing in the seconds the test waits
REFUSED_CONNECTION = (None, '[Errno 111] Connection refused')


class TheTest(unittest.TestCase):
    """find_working_endpoint() on its own."""

    def ask(self, answers, deadline=None):
        """Run it with ENTSO-E answering {endpoint: (body, reason)}; the main
        endpoint's answer is the default. Returns (endpoint, endpoints asked,
        seconds each was given, log)."""
        asked, given = [], []

        def get(url, limit=None, **_limits):
            base = url.split('?')[0]
            asked.append(base)
            given.append(limit - time.monotonic())
            return answers.get(base, answers[MAIN])

        log = io.StringIO()
        with mock.patch.object(fe, 'API_KEY', 'not-a-real-key'), mock.patch.object(fe, '_get', get), \
                contextlib.redirect_stdout(log):
            endpoint = fe.find_working_endpoint(deadline)
        return endpoint, asked, given, log.getvalue()

    def test_an_answer_with_data_confirms_the_endpoint(self):
        endpoint, asked, _given, log = self.ask({MAIN: (DATA, None)})
        self.assertEqual((endpoint, asked), (MAIN, [MAIN]))
        self.assertIn('Working!', log)

    def test_no_answer_in_time_is_not_a_reason_to_give_up(self):
        for reason in (fe.OUT_OF_TIME, 'The read operation timed out', 'timed out',
                       'timeout: response too slow'):
            endpoint, asked, _given, log = self.ask({MAIN: (None, reason)})
            self.assertEqual(endpoint, MAIN, reason)
            self.assertEqual(asked, [MAIN], reason)            # the alternatives are not tried
            self.assertIn(f'going on with {MAIN}', log)

    def test_a_refused_key_ends_the_run(self):
        for reason in ('HTTP 401', 'HTTP 429'):
            endpoint, asked, _given, log = self.ask({MAIN: (None, reason)})
            self.assertIsNone(endpoint, reason)
            self.assertEqual(asked, [MAIN], reason)
            self.assertIn(f'refusing the API key ({reason})', log)

    def test_an_endpoint_that_is_gone_gives_way_to_an_alternative(self):
        endpoint, asked, _given, _log = self.ask({MAIN: REFUSED_CONNECTION, SECOND: (DATA, None)})
        self.assertEqual((endpoint, asked), (SECOND, [MAIN, SECOND]))

    def test_with_no_endpoint_confirmed_the_run_goes_on_with_the_main_one(self):
        for answer in (REFUSED_CONNECTION, (None, 'HTTP 503'), ('<html>Maintenance</html>', None)):
            endpoint, asked, _given, log = self.ask({MAIN: answer})
            self.assertEqual(endpoint, MAIN, answer)
            self.assertEqual(asked, [MAIN, SECOND, THIRD], answer)
            self.assertIn(f'going on with {MAIN} all the same', log)

    def test_a_refused_key_on_an_alternative_ends_the_run_too(self):
        endpoint, asked, _given, _log = self.ask({MAIN: REFUSED_CONNECTION, SECOND: (None, 'HTTP 401')})
        self.assertIsNone(endpoint)
        self.assertEqual(asked, [MAIN, SECOND])

    def test_waits_5_seconds_for_the_main_endpoint_and_10_for_each_alternative(self):
        _endpoint, _asked, given, _log = self.ask({MAIN: REFUSED_CONNECTION})
        self.assertEqual([round(seconds) for seconds in given], [5, 10, 10])
        self.assertEqual(fe.PROBE_MAX_S, 5)

    def test_never_waits_past_the_runs_own_deadline(self):
        _endpoint, _asked, given, _log = self.ask({MAIN: REFUSED_CONNECTION}, deadline=time.monotonic() + 4)
        self.assertTrue(all(seconds <= 4 for seconds in given), given)


class TheRun(Feed):
    """main(): what a run does after each outcome of the test."""

    NOW = at('2026-11-16 10:22:54')
    EVERYTHING = dict(prices={'PT': 50, 'ES': 60}, generation={'PT': 4000, 'ES': 30000},
                      flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200})

    def test_a_slow_test_and_the_run_brings_what_entsoe_has(self):
        # Run 2375: the test did not answer in time, on a morning when ENTSO-E
        # was answering most requests.
        code, log = self.entsoe(self.NOW, probe=lambda url: SLOW, **self.EVERYTHING)
        self.assertEqual(code, 0)
        self.assertIn('ENTSO-E is slow, not gone', log)
        self.assertNotIn('ERROR', log)
        self.assertEqual(list(read(PRICES)['zones']), ['PT', 'ES'])
        self.assertEqual(list(read(GENERATION)['zones']), ['PT', 'ES'])
        self.assertEqual(list(read(FLOWS)['corridors']), ['PT→ES', 'ES→PT'])
        self.assertEqual(read(PRICES)['endpoint'], MAIN)
        status = read(STATUS)
        self.assertEqual((status['status'], status['endpoint']), ('ok', MAIN))
        for label in ('Prices', 'Generation', 'Flow'):
            self.assertTrue(all(url.startswith(MAIN + '?') for url in self.asked[label]), label)

    def test_what_does_not_answer_after_a_slow_test_is_kept_by_the_usual_rules(self):
        self.entsoe(at('2026-11-16 10:07:54'), **self.EVERYTHING)
        code, _log = self.entsoe(self.NOW, probe=lambda url: SLOW, prices={'PT': 55}, generation={'PT': 4400},
                                 flows={('PT', 'ES'): 1200})
        self.assertEqual(code, 0)
        for path, key, kept in ((PRICES, 'zones', 'ES'), (GENERATION, 'zones', 'ES'), (FLOWS, 'corridors', 'ES→PT')):
            entry = read(path)[key][kept]
            self.assertEqual((entry['fetched_at'], entry['carried_over']),
                             (at('2026-11-16 10:07:54').isoformat(), True), path)

    def test_a_slow_test_and_nothing_from_entsoe_fails_the_step_and_touches_no_file(self):
        self.entsoe(at('2026-11-16 10:07:54'), **self.EVERYTHING)
        before = [fingerprint(path) for path in (PRICES, GENERATION, FLOWS)]
        code, _log = self.entsoe(self.NOW, probe=lambda url: SLOW)
        self.assertEqual(code, 1)                                           # shows as failed in Actions
        self.assertEqual([fingerprint(path) for path in (PRICES, GENERATION, FLOWS)], before)
        self.assertEqual(sorted(self.asked), ['Flow', 'Generation', 'Prices'])   # it did ask

    def test_a_refused_key_ends_the_run_before_anything_is_asked(self):
        self.entsoe(at('2026-11-16 10:07:54'), **self.EVERYTHING)
        before = [fingerprint(path) for path in (PRICES, GENERATION, FLOWS)]
        code, log = self.entsoe(self.NOW, probe=lambda url: (None, 'HTTP 401'), **self.EVERYTHING)
        self.assertEqual(code, 1)
        self.assertEqual(self.asked, {})
        self.assertIn('ERROR: ENTSO-E is refusing the API key', log)
        self.assertEqual(read(STATUS)['status'], 'error')
        self.assertEqual([fingerprint(path) for path in (PRICES, GENERATION, FLOWS)], before)

    def test_the_time_spent_on_the_test_comes_out_of_the_runs_budget(self):
        clock = [5000.0]

        def slow_probe(url):
            clock[0] += fe.PROBE_MAX_S                                      # the seconds it waited
            return SLOW

        with mock.patch.object(fe.time, 'monotonic', lambda: clock[0]):
            code, _log = self.entsoe(self.NOW, probe=slow_probe, **self.EVERYTHING)
        self.assertEqual(code, 0)
        budget = fe.TOTAL_BUDGET_S
        requests = [len(fe.ZONES), len(fe.GEN_COUNTRIES) + 6, len(fe.FLOW_CORRIDORS)]
        # Prices get their share of what is left, not of the whole budget.
        spent = fe.PROBE_MAX_S
        self.assertAlmostEqual(self.deadlines['Prices'],
                               5000.0 + spent + (budget - spent) * requests[0] / sum(requests))
        # And the last dataset ends where the run's budget ends: 300 s after its start.
        self.assertAlmostEqual(self.deadlines['Flow'], 5000.0 + budget)


if __name__ == '__main__':
    unittest.main()
