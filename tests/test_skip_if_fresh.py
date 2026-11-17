"""Tests for skipping a run when the published data are under 7 minutes old
(skip_if_fresh.py).

Run from the repository root:  python -m unittest discover -s tests -v

No network. The files are written by the real fetch_entsoe.py and fetch_gb.py,
run through the harness of test_carry_over.py, so that "fresh" means what
those scripts really leave behind.
"""

import contextlib
import io
import json
import os
import unittest
from datetime import timedelta
from unittest import mock

import skip_if_fresh
from entsoe_xml import at
from test_carry_over import FLOWS, GENERATION, PRICES, Feed, read

EVERYTHING = dict(prices={'PT': 50, 'ES': 60}, generation={'PT': 4000, 'ES': 30000},
                  flows={('PT', 'ES'): 1000, ('ES', 'PT'): 200})
RUN = at('2026-11-16 10:07:54')            # when a run's first file is dated


class Fresh(Feed):

    def decide(self, when, force=False):
        return skip_if_fresh.decide(at(when) if isinstance(when, str) else when, force)

    def test_a_run_right_behind_another_is_skipped(self):
        self.entsoe(RUN, **EVERYTHING)
        # The run took its 300 s and 30 s more to publish and for the next job to get here.
        skip, line = self.decide(RUN + timedelta(seconds=330))
        self.assertTrue(skip)
        self.assertIn('Skipped: the published data are 5 min 30 s old', line)
        self.assertIn('No source was asked for anything', line)

    def test_the_next_ordinary_run_is_never_skipped(self):
        self.entsoe(RUN, **EVERYTHING)
        # 15 minutes later, less the minute the first file may take to be dated.
        skip, line = self.decide(RUN + timedelta(minutes=14))
        self.assertFalse(skip)
        self.assertIn('Running: the published data are 14 min 00 s old', line)

    def test_the_limit_is_seven_minutes(self):
        self.entsoe(RUN, **EVERYTHING)
        self.assertTrue(self.decide(RUN + timedelta(minutes=6, seconds=59))[0])
        self.assertFalse(self.decide(RUN + timedelta(minutes=7))[0])

    def test_the_age_is_that_of_the_oldest_of_the_three_files(self):
        self.entsoe(RUN, **EVERYTHING)
        # The next run got prices and generation, but nothing for the flows.
        later = RUN + timedelta(minutes=15)
        self.entsoe(later, prices={'PT': 55, 'ES': 65}, generation={'PT': 4400, 'ES': 31000})
        skip, line = self.decide(later + timedelta(minutes=2))
        self.assertFalse(skip)
        self.assertIn('17 min 00 s old (the oldest is data/cross-border-flows.json', line)

    def test_a_run_that_got_nothing_from_entsoe_does_not_make_the_data_fresh(self):
        self.entsoe(RUN, **EVERYTHING)
        later = RUN + timedelta(minutes=15)
        code, _log = self.entsoe(later)                       # nothing answered
        self.assertEqual(code, 1)
        self.elexon(later + timedelta(minutes=3))             # GB did, and status.json is from this run
        self.assertEqual(read('data/status.json')['prices_zones_fetched'], 1)
        skip, line = self.decide(later + timedelta(minutes=4))
        self.assertFalse(skip)
        self.assertIn('19 min 00 s old', line)

    def test_a_run_that_ended_in_error_does_not_make_the_data_fresh(self):
        self.entsoe(RUN, **EVERYTHING)
        later = RUN + timedelta(minutes=15)
        code, _log = self.entsoe(later, endpoint=None)         # ENTSO-E refused the key
        self.assertEqual(code, 1)
        self.assertEqual(read('data/status.json')['status'], 'error')
        self.assertFalse(self.decide(later + timedelta(minutes=1))[0])

    def test_a_held_run_let_go_after_the_one_asked_for_again_is_skipped(self):
        # 10:37:54: the scheduler asks for a run and GitHub holds it. Two
        # minutes on it asks again; that second run starts at once.
        second = at('2026-11-17 10:40:05')
        self.entsoe(second, **EVERYTHING)
        # The held run is let go while the second is still going, waits for
        # its turn, and starts when the second has published, at 10:43:35.
        skip, line = self.decide(second + timedelta(minutes=3, seconds=30))
        self.assertTrue(skip)
        self.assertIn('Skipped: the published data are 3 min 30 s old', line)

    def test_a_held_run_let_go_much_later_runs_and_it_is_the_next_one_that_is_skipped(self):
        # The shortest hold seen was 14 minutes: asked for at 10:37:54, let go
        # at 10:51:54, when the second run's data are 11 min 49 s old.
        second = at('2026-11-17 10:40:05')
        self.entsoe(second, **EVERYTHING)
        let_go = at('2026-11-17 10:51:54')
        self.assertFalse(self.decide(let_go)[0])
        self.entsoe(let_go + timedelta(seconds=10), **EVERYTHING)          # so it runs, in place of...
        # ...the run of 10:52:54, which waits behind it and starts when it ends.
        skip, line = self.decide(let_go + timedelta(minutes=4))
        self.assertTrue(skip)
        self.assertIn('3 min 50 s old', line)

    def test_forced_it_runs_whatever_the_age(self):
        self.entsoe(RUN, **EVERYTHING)
        skip, line = self.decide(RUN + timedelta(minutes=2), force=True)
        self.assertFalse(skip)
        self.assertIn('Running: forced by hand; the published data are 2 min 00 s old', line)

    def test_when_the_age_cannot_be_told_it_runs(self):
        self.assertFalse(self.decide(RUN)[0])                 # no files at all
        self.entsoe(RUN, **EVERYTHING)
        with open(GENERATION, 'w', encoding='utf-8') as f:
            f.write('{"updated": ')                           # cut short
        skip, line = self.decide(RUN + timedelta(minutes=1))
        self.assertFalse(skip)
        self.assertIn('data/generation-mix.json has no usable `updated`', line)
        with open(GENERATION, 'w', encoding='utf-8') as f:
            json.dump({'updated': 'yesterday', 'zones': {}}, f)
        self.assertFalse(self.decide(RUN + timedelta(minutes=1))[0])

    def test_a_file_dated_after_this_run_is_not_fresh(self):
        self.entsoe(RUN, **EVERYTHING)
        self.assertFalse(self.decide(RUN - timedelta(minutes=5))[0])


class TheStep(Feed):
    """What the workflow sees: one line in the log, `skip` in GITHUB_OUTPUT,
    exit code 0 whatever happens."""

    def step(self, when, force=None):
        env = {key: value for key, value in os.environ.items() if key != 'FORCE'}
        env['GITHUB_OUTPUT'] = 'github_output.txt'
        if force is not None:
            env['FORCE'] = force
        log = io.StringIO()
        with mock.patch.object(skip_if_fresh, 'utcnow', lambda: when), \
                mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(log):
            code = skip_if_fresh.main()
        with open('github_output.txt', encoding='utf-8') as f:
            outputs = f.read().splitlines()
        os.remove('github_output.txt')
        return code, outputs, log.getvalue().splitlines()

    def test_skips_with_one_line_that_shows_on_the_runs_page(self):
        self.entsoe(RUN, **EVERYTHING)
        code, outputs, lines = self.step(RUN + timedelta(minutes=3))
        self.assertEqual((code, outputs), (0, ['skip=true']))
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith('::notice::Skipped: the published data are 3 min 00 s old'))

    def test_runs_when_the_data_are_old(self):
        self.entsoe(RUN, **EVERYTHING)
        code, outputs, lines = self.step(RUN + timedelta(minutes=15))
        self.assertEqual((code, outputs), (0, ['skip=false']))
        self.assertEqual(len(lines), 1)
        self.assertTrue(lines[0].startswith('Running: the published data are 15 min 00 s old'))

    def test_the_force_input_as_the_workflow_passes_it(self):
        self.entsoe(RUN, **EVERYTHING)
        fresh = RUN + timedelta(minutes=3)
        self.assertEqual(self.step(fresh, force='true')[1], ['skip=false'])
        self.assertEqual(self.step(fresh, force='false')[1], ['skip=true'])     # a dispatch without it
        self.assertEqual(self.step(fresh, force='')[1], ['skip=true'])          # a scheduled run: no inputs

    def test_never_fails_the_run(self):
        self.entsoe(RUN, **EVERYTHING)
        with mock.patch.object(skip_if_fresh, 'decide', side_effect=RuntimeError('boom')):
            code, outputs, lines = self.step(RUN + timedelta(minutes=3))
        self.assertEqual((code, outputs), (0, ['skip=false']))
        self.assertIn('could not be worked out (RuntimeError: boom)', lines[0])

    def test_a_skipped_run_leaves_every_file_as_it_was(self):
        self.entsoe(RUN, **EVERYTHING)
        def contents():
            found = {}
            for path in (PRICES, GENERATION, FLOWS, 'data/status.json'):
                with open(path, 'rb') as f:
                    found[path] = f.read()
            return found

        before = contents()
        self.step(RUN + timedelta(minutes=3))
        self.assertEqual(contents(), before)
        self.assertEqual(sorted(os.listdir('data')),
                         ['archive', 'archive_index.json', 'cross-border-flows.json', 'generation-mix.json',
                          'spot-prices.json', 'status.json'])


if __name__ == '__main__':
    unittest.main()
