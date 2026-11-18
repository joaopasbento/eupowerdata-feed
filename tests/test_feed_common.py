"""Tests for what the newer fetchers share (feed_common.py): reading ENTSO-E's
answers, the intervals of a Period, the rule for kept values and the sending
of the requests.

Run from the repository root:  python -m unittest discover -s tests -v
"""

import contextlib
import io
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from unittest import mock
from xml.etree import ElementTree as ET

import feed_common as fc
from entsoe_real import KEY, Entsoe, sample

NOW = datetime(2026, 11, 18, 18, 7, 42, tzinfo=timezone.utc)


def period(points, resolution='PT15M', start='2026-11-18T17:00Z', end='2026-11-18T18:00Z', tag='quantity'):
    """A Period element; `points` are (position, value), with None for a point that has no value."""
    body = ''.join(f'<Point><position>{position}</position>'
                   + (f'<{tag}>{value}</{tag}>' if value is not None else '') + '</Point>'
                   for position, value in points)
    return ET.fromstring(f'<Period xmlns="urn:x"><timeInterval><start>{start}</start><end>{end}</end></timeInterval>'
                         f'<resolution>{resolution}</resolution>{body}</Period>')


def values(element, tag='quantity', variable=True):
    return [value for _start, value in fc.period_intervals(element, tag, variable)]


class TheIntervalsOfAPeriod(unittest.TestCase):

    def test_a_position_left_out_repeats_the_point_before_it(self):
        self.assertEqual(values(period([(1, 10), (3, 30)])), [10.0, 10.0, 30.0, 30.0])

    def test_the_last_point_stands_until_the_end_of_the_period(self):
        got = list(fc.period_intervals(period([(1, 5)], end='2026-11-18T17:45Z'), 'quantity'))
        self.assertEqual([value for _start, value in got], [5.0, 5.0, 5.0])
        self.assertEqual(got[-1][0], datetime(2026, 11, 18, 17, 30, tzinfo=timezone.utc))

    def test_a_point_with_no_value_means_none_from_there_on(self):
        self.assertEqual(values(period([(1, 10), (2, None), (4, 40)])), [10.0, None, None, 40.0])

    def test_a_fixed_curve_with_a_point_missing_has_a_gap_there(self):
        self.assertEqual(values(period([(1, 10), (3, 30)]), variable=False), [10.0, None, 30.0, None])

    def test_intervals_of_four_seconds(self):
        got = list(fc.period_intervals(period([(1, 1), (225, 2)], 'PT4S', end='2026-11-18T17:15Z'), 'quantity'))
        self.assertEqual(len(got), 225)
        self.assertEqual(got[1][0] - got[0][0], timedelta(seconds=4))
        self.assertEqual((got[223][1], got[224][1]), (1.0, 2.0))

    def test_a_period_that_cannot_be_read_gives_nothing(self):
        self.assertEqual(values(period([(1, 1)], resolution='P1D')), [])
        self.assertEqual(values(period([])), [])


class WhatAnAnswerHas(unittest.TestCase):

    def test_no_matching_data_is_an_answer_with_nothing_and_no_problem(self):
        answer = fc.entsoe_answer(fc.Response(200, {}, sample('no_data.xml')))
        self.assertEqual((answer.documents, answer.problem), ([], None))

    def test_a_document_is_itself(self):
        body = sample('balancing_A16_IT.xml')
        self.assertEqual(fc.entsoe_answer(fc.Response(200, {}, body)), fc.Answer([body], None))

    def test_a_zip_is_its_files(self):
        packed = io.BytesIO()
        with zipfile.ZipFile(packed, 'w') as archive:
            archive.writestr('001.xml', '<a/>')
            archive.writestr('002.xml', '<b/>')
        answer = fc.entsoe_answer(fc.Response(200, {}, packed.getvalue()))
        self.assertEqual((answer.documents, answer.problem), ([b'<a/>', b'<b/>'], None))

    def test_a_refusal_says_why(self):
        body = (b'<Acknowledgement_MarketDocument xmlns="urn:x"><Reason><code>999</code><text>The number of '
                b'instances (242) exceeds the allowed maximum (200)</text></Reason></Acknowledgement_MarketDocument>')
        answer = fc.entsoe_answer(fc.Response(400, {}, body, error='Bad Request'))
        self.assertEqual(answer.documents, [])
        self.assertIn('exceeds the allowed maximum', answer.problem)

    def test_no_answer_and_a_page_that_is_not_xml_are_problems(self):
        self.assertIn('timed out', fc.entsoe_answer(fc.Response(error='timed out')).problem)
        self.assertEqual(fc.entsoe_answer(fc.Response(503, {}, b'<html>Service unavailable')).problem, 'HTTP 503, not XML')
        self.assertEqual(fc.entsoe_answer(None).problem, 'not asked')


class KeepingWhatDidNotAnswer(unittest.TestCase):

    BEFORE = {'PT': {'v': 1, 'fetched_at': '2026-11-18T17:52:00+00:00'},
              'ES': {'v': 2, 'fetched_at': '2026-11-18T17:52:00+00:00'},
              'FR': {'v': 3, 'fetched_at': '2026-11-18T17:07:00+00:00'}}

    def run_once(self, obtained, failed, may_keep=lambda name, entry: True):
        return fc.carry(self.BEFORE, obtained, failed, '2026-11-18T18:07:42+00:00', may_keep)

    def test_an_entry_obtained_has_the_time_of_the_run(self):
        entries, kept, dropped = self.run_once({'PT': {'v': 9, 'carried_over': True}}, set())
        self.assertEqual(entries['PT'], {'v': 9, 'fetched_at': '2026-11-18T18:07:42+00:00'})
        self.assertEqual((kept, dropped), ([], []))

    def test_one_that_did_not_answer_keeps_its_value_its_time_and_a_mark(self):
        entries, kept, _dropped = self.run_once({'PT': {'v': 9}}, {'ES'})
        self.assertEqual(entries['ES'], {'v': 2, 'fetched_at': '2026-11-18T17:52:00+00:00', 'carried_over': True})
        self.assertEqual(kept, ['ES'])

    def test_one_that_was_not_asked_for_stays_exactly_as_it_was(self):
        entries, kept, _dropped = self.run_once({'PT': {'v': 9}}, {'ES'})
        self.assertEqual(entries['FR'], self.BEFORE['FR'])
        self.assertNotIn('FR', kept)

    def test_one_that_may_no_longer_be_shown_is_left_out(self):
        entries, _kept, dropped = self.run_once({}, {'ES'}, lambda name, entry: name != 'FR')
        self.assertEqual((sorted(entries), dropped), (['ES', 'PT'], ['FR']))

    def test_a_new_name_is_added_after_the_ones_already_there(self):
        entries, _kept, _dropped = self.run_once({'DE': {'v': 4}}, set())
        self.assertEqual(list(entries), ['PT', 'ES', 'FR', 'DE'])


class OnceAnHour(unittest.TestCase):

    def test_an_entry_is_due_when_absent_kept_or_obtained_fifty_minutes_ago(self):
        self.assertTrue(fc.due(None, NOW))
        self.assertTrue(fc.due({'fetched_at': fc.iso(NOW - timedelta(minutes=5)), 'carried_over': True}, NOW))
        self.assertTrue(fc.due({'fetched_at': fc.iso(NOW - timedelta(minutes=60))}, NOW))
        self.assertFalse(fc.due({'fetched_at': fc.iso(NOW - timedelta(minutes=45))}, NOW))

    def test_with_runs_fifteen_minutes_apart_that_is_every_fourth_run(self):
        obtained = {'fetched_at': fc.iso(NOW)}
        asked = [fc.due(obtained, NOW + timedelta(minutes=15 * run, seconds=20)) for run in range(1, 5)]
        self.assertEqual(asked, [False, False, False, True])

    def test_what_entsoe_has_no_data_for_is_asked_again_a_day_later(self):
        checked = fc.iso(NOW)
        self.assertTrue(fc.asked_recently(checked, NOW + timedelta(hours=22)))
        self.assertFalse(fc.asked_recently(checked, NOW + timedelta(hours=23, minutes=5)))
        self.assertFalse(fc.asked_recently(None, NOW))


class SendingTheRequests(unittest.TestCase):

    def ask(self, jobs, entsoe, budget=60):
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            answers = fc.entsoe_ask(jobs, fc.Budget(budget), get=entsoe, interval=0)
        return answers, log.getvalue()

    def test_every_job_gets_its_answer_and_the_log_never_has_the_key_or_the_address(self):
        entsoe = Entsoe([({'in_Domain': 'A'}, 'balancing_A16_IT.xml')])
        jobs = [(code, f'zone {code}', {'documentType': 'A69', 'in_Domain': code}) for code in 'ABC']
        answers, log = self.ask(jobs, entsoe)
        self.assertEqual(sorted(answers), ['A', 'B', 'C'])
        self.assertEqual(len(entsoe.asked), 3)
        self.assertEqual(log.count('ENTSO-E: zone'), 3)
        self.assertNotIn(KEY, log)
        self.assertNotIn('entsoe.eu', log)
        self.assertNotIn('securityToken', log)

    def test_with_no_time_left_nothing_is_sent(self):
        entsoe = Entsoe()
        answers, log = self.ask([('A', 'zone A', {'in_Domain': 'A'})], entsoe, budget=0)
        self.assertEqual((answers, entsoe.asked), ({}, []))
        self.assertIn('left for the next run', log)

    def test_once_the_key_is_refused_the_rest_is_not_asked_for(self):
        entsoe = Entsoe([({}, fc.Response(401, {}, b'Unauthorized', error='Unauthorized'))])
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            answers = fc.entsoe_ask([(n, f'zone {n}', {'in_Domain': str(n)}) for n in range(6)], fc.Budget(60),
                                    get=entsoe, interval=0, workers=1)
        self.assertEqual(len(entsoe.asked), 1)
        self.assertEqual(list(answers), [0])
        self.assertIn('refusing the API key', log.getvalue())


if __name__ == '__main__':
    unittest.main()
