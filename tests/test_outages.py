"""Tests for the outages (fetch_outages.py), on real answers of ENTSO-E
(tests/fixtures/entsoe, obtained on 18 Nov 2026 at 18:07 UTC).

Run from the repository root:  python -m unittest discover -s tests -v

The expected numbers were read from the documents by hand (and counted with a
plain search of the XML), not with the code under test.
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from unittest import mock

import feed_common as fc
import fetch_outages as fo
from entsoe_real import ASKED_AT, KEY, Entsoe, sample

PATH = fo.OUT_PATH
PT, ES, FR, DE = (fo.ZONES[code] for code in ('PT', 'ES', 'FR', 'DE'))


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def files(name):
    with zipfile.ZipFile(io.BytesIO(sample(name))) as archive:
        return [archive.read(member) for member in archive.namelist()]


def find(name, text):
    """The document of a kept zip that has `text` in it."""
    return next(document for document in files(name) if text.encode() in document)


def unit(identifier='doc-1', start='2026-11-18T10:00Z', end='2026-11-18T20:00Z', nominal=100, available=40,
         status=None, business='A53', kind='A80', revision=1, name='Unit 1'):
    """An outage document of a generation unit, written by hand."""
    def day_and_time(moment):
        return moment[:10], moment[11:16] + ':00Z'
    (start_date, start_time), (end_date, end_time) = day_and_time(start), day_and_time(end)
    return (f'<Unavailability_MarketDocument xmlns="urn:x"><mRID>{identifier}</mRID><revisionNumber>{revision}'
            f'</revisionNumber><type>{kind}</type><createdDateTime>2026-11-17T09:00:00Z</createdDateTime>'
            + (f'<docStatus><value>{status}</value></docStatus>' if status else '') +
            f'<TimeSeries><businessType>{business}</businessType>'
            f'<start_DateAndOrTime.date>{start_date}</start_DateAndOrTime.date>'
            f'<start_DateAndOrTime.time>{start_time}</start_DateAndOrTime.time>'
            f'<end_DateAndOrTime.date>{end_date}</end_DateAndOrTime.date>'
            f'<end_DateAndOrTime.time>{end_time}</end_DateAndOrTime.time>'
            f'<production_RegisteredResource.name>Plant</production_RegisteredResource.name>'
            f'<production_RegisteredResource.pSRType.psrType>B04</production_RegisteredResource.pSRType.psrType>'
            f'<production_RegisteredResource.pSRType.powerSystemResources.name>{name}'
            f'</production_RegisteredResource.pSRType.powerSystemResources.name>'
            f'<production_RegisteredResource.pSRType.powerSystemResources.nominalP>{nominal}'
            f'</production_RegisteredResource.pSRType.powerSystemResources.nominalP>'
            f'<Available_Period><timeInterval><start>{start}</start><end>{end}</end></timeInterval>'
            f'<resolution>PT1M</resolution><Point><position>1</position><quantity>{available}</quantity></Point>'
            f'</Available_Period></TimeSeries><Reason><code>B19</code></Reason></Unavailability_MarketDocument>').encode()


def packed(documents):
    """The zip ENTSO-E would send with these documents."""
    data = io.BytesIO()
    with zipfile.ZipFile(data, 'w') as archive:
        for number, document in enumerate(documents, 1):
            archive.writestr(f'{number:03d}.xml', document)
    return data.getvalue()


class AUnit(unittest.TestCase):

    def test_a_generation_unit_of_portugal(self):
        record = fo.read_outage(find('outages_A80_PT.zip', 'O77H44gGZbVUPyUgb6jzCA'), ASKED_AT)
        self.assertEqual(record, {
            'id': 'O77H44gGZbVUPyUgb6jzCA', 'revision': 1, 'start': '2026-04-06T07:00:00+00:00',
            'end': '2027-02-28T18:00:00+00:00', 'planned': True, 'status': 'active',
            'published': '2026-10-06T14:35:56+00:00', 'reason': 'B19', 'phase': 'ongoing',
            'asset_type': 'generation_unit', 'name': 'Valeira - G1', 'plant': 'Valeira', 'location': 'S.J.PESQUEIRA',
            'asset_eic': '16WVALEIR1-----0', 'technology': 'Hydro Run-of-river', 'psr_type': 'B11',
            'nominal_mw': 80, 'available_mw': 0, 'unavailable_mw': 80})

    def test_a_production_unit_of_spain_has_the_name_of_the_plant(self):
        record = fo.read_outage(find('outages_A77_ES.zip', 'U0tQw0H7umnJRTWU6TpxWg'), ASKED_AT)
        self.assertEqual((record['asset_type'], record['name'], record['plant'], record['location']),
                         ('production_unit', 'P.G.RODR 5', 'P.G.RODR 5', 'A Coruña'))
        self.assertEqual((record['revision'], record['technology'], record['asset_eic']),
                         (2, 'Fossil Gas', '18WPGR5-123456-J'))
        self.assertEqual((record['nominal_mw'], record['available_mw'], record['unavailable_mw']), (855.6, 0, 855.6))

    def test_what_is_unavailable_is_the_nominal_power_less_what_stays_available(self):
        record = fo.read_outage(find('outages_A80_ES.zip', 'mH_CWUV-BomoNvJWGseXhQ'), ASKED_AT)
        self.assertEqual((record['name'], record['nominal_mw'], record['available_mw'], record['unavailable_mw']),
                         ('PGR5TV', 302.7, 124, 178.7))

    def test_never_less_than_zero(self):
        record = fo.read_outage(unit(nominal=821, available=847.2), ASKED_AT)
        self.assertEqual((record['nominal_mw'], record['available_mw'], record['unavailable_mw']), (821, 847.2, 0))

    def test_planned_or_not_and_the_three_states(self):
        self.assertFalse(fo.read_outage(unit(business='A54'), ASKED_AT)['planned'])
        self.assertEqual(fo.read_outage(unit(), ASKED_AT)['status'], 'active')
        self.assertEqual(fo.read_outage(unit(status='A09'), ASKED_AT)['status'], 'cancelled')
        self.assertEqual(fo.read_outage(unit(), ASKED_AT, withdrawn=True)['status'], 'withdrawn')

    def test_in_course_to_come_and_over(self):
        def phase(start, end):
            record = fo.read_outage(unit(start=start, end=end), ASKED_AT)
            return record and record['phase']
        self.assertEqual(phase('2026-11-18T10:00Z', '2026-11-18T20:00Z'), 'ongoing')
        self.assertEqual(phase('2026-11-25T18:00Z', '2026-11-30T20:00Z'), 'upcoming')       # starts within 7 days
        self.assertEqual(phase('2026-11-10T10:00Z', '2026-11-16T19:00Z'), 'ended')          # ended 47 hours ago
        self.assertIsNone(phase('2026-11-10T10:00Z', '2026-11-16T17:00Z'))                  # ended 49 hours ago
        self.assertIsNone(phase('2026-11-25T19:00Z', '2026-11-30T20:00Z'))                  # starts in over 7 days

    def test_a_document_that_is_not_well_formed_is_left_out(self):
        self.assertIsNone(fo.read_outage(b'<Unavailability_MarketDocument><name>A & B</name>', ASKED_AT))


class ALine(unittest.TestCase):

    def setUp(self):
        names = {eic: code for code, eic in fo.fe.LINK_ZONES.items()}
        records = [fo.read_outage(document, ASKED_AT)
                   for name in ('outages_A78_PT_ES.zip', 'outages_A78_ES_PT.zip') for document in files(name)]
        self.outages = fo.merge_directions(records, names)

    def test_the_two_documents_of_an_outage_are_one_outage_with_both_directions(self):
        outage = next(o for o in self.outages if o['name'] == 'Pereiros-Rio Maior 1' and o['start'].startswith('2026-01-28'))
        self.assertEqual(outage['ids'], {'ES→PT': 'ktwGmFy47jxtjeVS5IARWQ', 'PT→ES': '9WpCjv2ajm6ELEhAe8VJ2Q'})
        self.assertEqual((outage['asset_type'], outage['planned'], outage['status'], outage['phase']),
                         ('transmission', False, 'active', 'ongoing'))
        self.assertEqual(outage['assets'], [{'name': 'Pereiros-Rio Maior 1', 'type': 'AC link',
                                             'location': 'Pereiros-Rio Maior'}])
        # At 18:07 UTC of 18 Nov the last change of each curve was at minute 423 541 of the outage:
        # 2 800 MW from Portugal to Spain, 3 500 MW from Spain to Portugal. The lowest of both is 0.
        self.assertEqual(outage['available_mw'], {'ES→PT': 3500, 'PT→ES': 2800})
        self.assertEqual(outage['min_available_mw'], {'ES→PT': 0, 'PT→ES': 0})
        self.assertNotIn('nominal_mw', outage)
        self.assertNotIn('unavailable_mw', outage)

    def test_every_document_has_its_pair(self):
        self.assertEqual(len(self.outages), 38)                      # 38 documents in each direction
        self.assertTrue(all(sorted(o['ids']) == ['ES→PT', 'PT→ES'] for o in self.outages))

    def test_two_outages_alike_in_everything_stay_two(self):
        # Two documents with no asset, for 23 to 25 Nov, in each direction.
        alike = [o for o in self.outages if o['name'] is None and o['start'] == '2026-11-23T07:00:00+00:00'
                 and o['end'] == '2026-11-25T18:00:00+00:00']
        self.assertEqual(len(alike), 2)
        self.assertEqual(len({o['id'] for o in alike}), 2)

    def test_cancelled_ones_are_marked(self):
        self.assertEqual(sum(o['status'] == 'cancelled' for o in self.outages), 24)


class Feed(unittest.TestCase):

    ANSWERS = [
        ({'documentType': 'A80', 'biddingZone_Domain': PT, 'docStatus': 'A13'}, 'outages_A80_PT_withdrawn.zip'),
        ({'documentType': 'A80', 'biddingZone_Domain': PT}, 'outages_A80_PT.zip'),
        ({'documentType': 'A80', 'biddingZone_Domain': ES, 'docStatus': 'A13'}, 'no_data_outages.xml'),
        ({'documentType': 'A80', 'biddingZone_Domain': ES}, 'outages_A80_ES.zip'),
        ({'documentType': 'A77', 'biddingZone_Domain': ES, 'docStatus': 'A13'}, 'no_data_outages.xml'),
        ({'documentType': 'A77', 'biddingZone_Domain': ES}, 'outages_A77_ES.zip'),
        ({'documentType': 'A78', 'docStatus': 'A13'}, 'no_data_outages.xml'),
        ({'documentType': 'A78', 'out_Domain': PT, 'in_Domain': ES}, 'outages_A78_PT_ES.zip'),
        ({'documentType': 'A78', 'out_Domain': ES, 'in_Domain': PT}, 'outages_A78_ES_PT.zip'),
        ({'documentType': 'A80', 'biddingZone_Domain': FR, 'docStatus': 'A13'}, 'no_data_outages.xml'),
        ({'documentType': 'A80', 'biddingZone_Domain': FR}, packed([unit('fr-1', name='Tricastin 1')])),
    ]

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(folder.name)
        os.makedirs('data')

    def run_at(self, when, answers=None, budget=60):
        entsoe = Entsoe(self.ANSWERS if answers is None else answers)
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            code = fo.main(now=when, get=entsoe, budget=fc.Budget(budget), interval=0)
        return code, entsoe, log.getvalue()

    def doc(self, path=PATH):
        with open(path, encoding='utf-8') as f:
            return json.load(f)


class ARun(Feed):

    def test_writes_the_two_blocks_each_with_its_licence(self):
        code, _entsoe, log = self.run_at(ASKED_AT)
        doc = self.doc()
        self.assertEqual(code, 0)
        self.assertEqual(list(doc), ['updated', 'data_time', 'window', 'generation', 'transmission'])
        self.assertEqual((doc['generation']['license'], doc['transmission']['license']), ('source-terms', 'cc-by-4.0'))
        self.assertEqual(doc['window'], {'from': '2026-11-16T18:07:42+00:00', 'to': '2026-11-25T18:07:42+00:00'})
        self.assertIn('since 22 Oct 2026', doc['generation']['known_issue'])
        self.assertNotIn('known_issue', doc['transmission'])
        self.assertEqual(doc['updated'], '2026-11-18T18:07:42+00:00')
        # The most recent publication among the outages: a line of the border, at 15:13 UTC.
        self.assertEqual(doc['data_time'], '2026-11-18T15:13:22+00:00')
        self.assertNotIn(KEY, log)
        self.assertNotIn('entsoe.eu', log)

    def test_portugal_has_its_generation_units_withdrawn_ones_included(self):
        self.run_at(ASKED_AT)
        portugal = self.doc()['generation']['zones']['PT']
        # 37 active and 9 cancelled in the answer, 73 in the answer for the withdrawn.
        self.assertEqual(len(portugal['outages']), 119)
        self.assertEqual(sum(o['status'] == 'withdrawn' for o in portugal['outages']), 73)
        self.assertEqual(sum(o['status'] == 'cancelled' for o in portugal['outages']), 9)
        # Counted over the active ones only.
        self.assertEqual(portugal['counts'], {'ongoing': 9, 'upcoming': 17, 'ended': 11})
        self.assertEqual((portugal['fetched_at'], portugal['data_time']),
                         ('2026-11-18T18:07:42+00:00', '2026-11-18T14:48:12+00:00'))
        self.assertTrue(all(o['zone'] == 'PT' and o['asset_type'] == 'generation_unit' for o in portugal['outages']))
        phases = [o['phase'] for o in portugal['outages']]
        self.assertEqual(phases, sorted(phases, key=fo.PHASES.index))          # in course first, then to come, then over

    def test_portugal_publishes_no_production_units(self):
        self.run_at(ASKED_AT)
        self.assertEqual(self.doc()['generation']['not_published']['A77']['PT'], '2026-11-18T18:07:42+00:00')

    def test_spain_has_generation_units_and_production_units(self):
        self.run_at(ASKED_AT)
        spain = self.doc()['generation']['zones']['ES']
        kinds = [o['asset_type'] for o in spain['outages']]
        self.assertEqual((kinds.count('generation_unit'), kinds.count('production_unit')), (39, 28))
        self.assertEqual(spain['counts'], {'ongoing': 41, 'upcoming': 19, 'ended': 6})

    def test_the_border_has_one_outage_for_each_pair_of_documents(self):
        self.run_at(ASKED_AT)
        border = self.doc()['transmission']['borders']['ES-PT']
        self.assertEqual((border['zones'], len(border['outages'])), (['ES', 'PT'], 38))
        self.assertTrue(all(o['border'] == 'ES-PT' and o['asset_type'] == 'transmission' for o in border['outages']))
        self.assertEqual(border['data_time'], '2026-11-18T15:13:22+00:00')

    def test_the_withdrawn_ones_are_only_asked_for_where_there_are_others(self):
        _code, entsoe, _log = self.run_at(ASKED_AT)
        self.assertEqual(len(entsoe.asked_for(documentType='A80', biddingZone_Domain=FR)), 2)
        self.assertEqual(len(entsoe.asked_for(documentType='A80', biddingZone_Domain=DE)), 1)
        self.assertEqual(entsoe.asked_for(documentType='A80', biddingZone_Domain=DE, docStatus='A13'), [])
        self.assertTrue(all(params['offset'] == '0' for params in entsoe.asked))

    def test_the_united_kingdom_and_its_cables_are_never_asked_for(self):
        _code, entsoe, _log = self.run_at(ASKED_AT)
        gb = fo.fe.ZONES['GB']
        self.assertFalse([p for p in entsoe.asked if gb in (p.get('biddingZone_Domain'), p.get('in_Domain'), p.get('out_Domain'))])
        self.assertNotIn('FR-GB', fo.BORDERS)

    def test_a_zone_with_more_than_two_hundred_outages_is_asked_for_page_by_page(self):
        first = packed([unit(f'de-{n}') for n in range(200)])
        second = packed([unit(f'de-{n}') for n in range(200, 205)])
        answers = [({'documentType': 'A80', 'biddingZone_Domain': DE, 'docStatus': 'A13'}, 'no_data_outages.xml'),
                   ({'documentType': 'A80', 'biddingZone_Domain': DE, 'offset': '0'}, first),
                   ({'documentType': 'A80', 'biddingZone_Domain': DE, 'offset': '200'}, second)]
        _code, entsoe, _log = self.run_at(ASKED_AT, answers + self.ANSWERS)
        self.assertEqual(len(self.doc()['generation']['zones']['DE']['outages']), 205)
        self.assertEqual([p['offset'] for p in entsoe.asked_for(documentType='A80', biddingZone_Domain=DE)
                          if 'docStatus' not in p], ['0', '200'])

    def test_a_zone_one_of_whose_pages_fails_is_not_published_from_the_others(self):
        refusal = fc.Response(400, {}, b'<Acknowledgement_MarketDocument xmlns="urn:x"><Reason><text>Something went wrong'
                                       b'</text></Reason></Acknowledgement_MarketDocument>', error='Bad Request')
        answers = [({'documentType': 'A80', 'biddingZone_Domain': DE, 'offset': '0'}, packed([unit(f'de-{n}') for n in range(200)])),
                   ({'documentType': 'A80', 'biddingZone_Domain': DE, 'offset': '200'}, refusal)]
        self.run_at(ASKED_AT, answers + self.ANSWERS)
        self.assertNotIn('DE', self.doc()['generation']['zones'])

    def test_documents_that_are_not_well_formed_are_left_out_and_counted(self):
        answers = [({'documentType': 'A80', 'biddingZone_Domain': FR, 'docStatus': 'A13'}, 'no_data_outages.xml'),
                   ({'documentType': 'A80', 'biddingZone_Domain': FR},
                    packed([unit('fr-1'), b'<Unavailability_MarketDocument><name>A & B</name>']))]
        _code, _entsoe, log = self.run_at(ASKED_AT, answers + self.ANSWERS)
        self.assertEqual([o['id'] for o in self.doc()['generation']['zones']['FR']['outages']], ['fr-1'])
        self.assertIn('1 documents were not well-formed XML', log)


class TheNextRuns(Feed):

    def asked(self, entsoe):
        return {(p['documentType'], p.get('biddingZone_Domain') or f"{p['out_Domain']}>{p['in_Domain']}")
                for p in entsoe.asked}

    def test_a_quarter_of_an_hour_later_only_portugal_spain_and_their_border_are_asked_for(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=15))
        # The border of Spain with France has no outages in these answers: it is asked for again in an hour.
        self.assertEqual(self.asked(entsoe), {('A80', PT), ('A80', ES), ('A77', ES), ('A78', f'{PT}>{ES}'),
                                              ('A78', f'{ES}>{PT}')})
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=60))
        self.assertIn(('A78', f'{ES}>{FR}'), self.asked(entsoe))

    def test_an_hour_later_the_other_zones_with_outages_are_asked_for_again(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(minutes=60))
        asked = self.asked(entsoe)
        self.assertIn(('A80', FR), asked)
        self.assertIn(('A77', PT), asked)                 # Portugal and Spain: what has no data, once an hour
        self.assertNotIn(('A77', FR), asked)              # the others: every six hours
        self.assertNotIn(('A80', DE), asked)

    def test_six_hours_later_what_had_no_outages_is_asked_for_again(self):
        self.run_at(ASKED_AT)
        _code, entsoe, _log = self.run_at(ASKED_AT + timedelta(hours=6, minutes=15))
        self.assertIn(('A80', DE), self.asked(entsoe))

    def test_a_zone_that_does_not_answer_keeps_its_outages_for_under_three_hours(self):
        self.run_at(ASKED_AT)
        silent = [({'biddingZone_Domain': ES}, None)] + self.ANSWERS
        code, _entsoe, log = self.run_at(ASKED_AT + timedelta(minutes=15), silent)
        spain = self.doc()['generation']['zones']['ES']
        self.assertEqual(code, 0)
        self.assertEqual((spain['fetched_at'], spain.get('carried_over'), len(spain['outages'])),
                         ('2026-11-18T18:07:42+00:00', True, 67))
        self.assertNotIn('carried_over', self.doc()['generation']['zones']['PT'])
        self.assertIn('Kept, not obtained in this run: ES', log)
        _code, _entsoe, log = self.run_at(ASKED_AT + timedelta(hours=3, minutes=1), silent)
        self.assertNotIn('ES', self.doc()['generation']['zones'])
        self.assertIn('Left out, last obtained 3 hours ago or more: ES', log)

    def test_a_border_one_of_whose_directions_fails_is_kept_whole(self):
        self.run_at(ASKED_AT)
        self.run_at(ASKED_AT + timedelta(minutes=15), [({'documentType': 'A78', 'out_Domain': ES, 'in_Domain': PT}, None)]
                    + self.ANSWERS)
        border = self.doc()['transmission']['borders']['ES-PT']
        self.assertEqual((border['fetched_at'], border.get('carried_over'), len(border['outages'])),
                         ('2026-11-18T18:07:42+00:00', True, 38))

    def test_on_a_slow_day_portugal_and_spain_are_whole_before_anything_else_is_asked_for(self):
        # Every request costs 6 of the 90 seconds: there is time for 15.
        clock = [0.0]

        class Slow(Entsoe):
            def __call__(self, url, **limits):
                clock[0] += 6
                return super().__call__(url, **limits)

        entsoe = Slow(self.ANSWERS)
        log = io.StringIO()
        with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(log):
            code = fo.main(now=ASKED_AT, get=entsoe, budget=fc.Budget(90, clock=lambda: clock[0]), interval=0)
        doc = self.doc()
        self.assertEqual(code, 0)
        self.assertEqual(list(doc['generation']['zones']), ['PT', 'ES'])
        self.assertEqual(len(doc['generation']['zones']['PT']['outages']), 119)      # the withdrawn ones included
        self.assertEqual(list(doc['transmission']['borders']), ['ES-PT'])
        self.assertIn('left for the next run', log.getvalue())
        # The 14 requests of Portugal, Spain and their borders went first; then one batch of the rest began.
        first = [(p['documentType'], p.get('biddingZone_Domain') or f"{p['out_Domain']}>{p['in_Domain']}")
                 for p in entsoe.asked[:14]]
        self.assertEqual(set(first), {('A80', PT), ('A77', PT), ('A80', ES), ('A77', ES), ('A78', f'{PT}>{ES}'),
                                      ('A78', f'{ES}>{PT}'), ('A78', f'{ES}>{FR}'), ('A78', f'{FR}>{ES}')})

    def test_what_a_run_does_not_get_to_is_first_in_line_in_the_next(self):
        clock = [0.0]

        class Slow(Entsoe):
            def __call__(self, url, **limits):
                clock[0] += 3
                return super().__call__(url, **limits)

        def run(when):
            clock[0] = 0.0
            entsoe = Slow(self.ANSWERS)
            with mock.patch.dict('os.environ', {'ENTSOE_API_KEY': KEY}), contextlib.redirect_stdout(io.StringIO()):
                fo.main(now=when, get=entsoe, budget=fc.Budget(90, clock=lambda: clock[0]), interval=0)
            return [p.get('biddingZone_Domain') for p in entsoe.asked if p['documentType'] == 'A80']

        first = run(ASKED_AT)
        second = run(ASKED_AT + timedelta(minutes=15))
        self.assertIn(FR, first)                                   # France was reached in the first run...
        self.assertNotIn(FR, second)                               # ...and is not asked for again within the hour
        reached = set(first) - {PT, ES}
        self.assertTrue(reached and set(second) - {PT, ES} and not (set(second) - {PT, ES}) & reached)

    def test_with_nothing_obtained_the_file_is_not_touched(self):
        self.run_at(ASKED_AT)
        with open(PATH, 'rb') as f:
            before = (f.read(), os.stat(PATH).st_ino)
        code, _entsoe, log = self.run_at(ASKED_AT + timedelta(minutes=15), [({}, None)])
        with open(PATH, 'rb') as f:
            self.assertEqual((f.read(), os.stat(PATH).st_ino), before)
        self.assertEqual(code, 1)
        self.assertIn('left exactly as it was', log)

    def test_a_zone_whose_outages_are_all_over_stays_with_none(self):
        self.run_at(ASKED_AT)
        answers = [({'documentType': 'A80', 'biddingZone_Domain': FR}, 'no_data_outages.xml')] + self.ANSWERS
        self.run_at(ASKED_AT + timedelta(minutes=60), answers)
        france = self.doc()['generation']['zones']['FR']
        self.assertEqual((france['outages'], france['counts'], france['fetched_at']),
                         ([], {'ongoing': 0, 'upcoming': 0, 'ended': 0}, '2026-11-18T19:07:42+00:00'))


class TheArchive(Feed):

    def record(self, day):
        return self.doc(fo.ARCHIVE_PATH.format(day=day))

    def test_has_what_was_in_force_on_the_day_and_no_time_of_its_own(self):
        self.run_at(ASKED_AT)
        record = self.record('2026-11-18')
        self.assertEqual(list(record), ['date', 'generation', 'transmission'])
        self.assertEqual((record['generation']['license'], record['transmission']['license']),
                         ('source-terms', 'cc-by-4.0'))
        portugal = record['generation']['zones']['PT']
        # Of the 37 active outages of Portugal, 12 overlap the delivery day of 18 Nov (17 Nov 23:00 to 18 Nov 23:00 UTC).
        self.assertEqual(len(portugal), 12)
        self.assertNotIn('status', portugal[0])
        valeira = next(o for o in portugal if o['id'] == 'O77H44gGZbVUPyUgb6jzCA')
        self.assertEqual(valeira, {'id': 'O77H44gGZbVUPyUgb6jzCA', 'revision': 1, 'asset_type': 'generation_unit',
                                   'name': 'Valeira - G1', 'plant': 'Valeira', 'technology': 'Hydro Run-of-river',
                                   'nominal_mw': 80, 'unavailable_mw': 80, 'start': '2026-04-06T07:00:00+00:00',
                                   'end': '2027-02-28T18:00:00+00:00', 'planned': True,
                                   'published': '2026-10-06T14:35:56+00:00'})
        self.assertTrue(os.path.exists(fo.ARCHIVE_PATH.format(day='2026-11-17')))

    def test_is_not_rewritten_when_nothing_changed(self):
        self.run_at(ASKED_AT)
        path = fo.ARCHIVE_PATH.format(day='2026-11-18')
        before = os.stat(path).st_ino
        self.run_at(ASKED_AT + timedelta(minutes=15))
        self.assertEqual(os.stat(path).st_ino, before)

    def test_an_outage_cancelled_afterwards_leaves_the_record_and_one_that_ended_stays(self):
        france = {'documentType': 'A80', 'biddingZone_Domain': FR}
        first = [(france, packed([unit('fr-1', end='2026-11-18T19:00Z'), unit('fr-2', end='2026-11-18T22:00Z')]))]
        self.run_at(ASKED_AT, first + self.ANSWERS)
        self.assertEqual([o['id'] for o in self.record('2026-11-18')['generation']['zones']['FR']], ['fr-1', 'fr-2'])
        later = [(france, packed([unit('fr-2', end='2026-11-18T22:00Z', status='A09', revision=2)]))]
        self.run_at(ASKED_AT + timedelta(minutes=60), later + self.ANSWERS)
        # fr-1 is no longer in the answer (it would be, in truth, for 48 hours): the record keeps it.
        self.assertEqual([o['id'] for o in self.record('2026-11-18')['generation']['zones']['FR']], ['fr-1'])

    def test_records_older_than_five_years_are_taken_out(self):
        os.makedirs('data/archive')
        for name in ('outages-2021-11-01.json', 'outages-2021-11-30.json', 'spot-prices-2021-11-01.json'):
            with open(os.path.join('data/archive', name), 'w') as f:
                f.write('{}')
        self.run_at(ASKED_AT)
        left = sorted(os.listdir('data/archive'))
        self.assertNotIn('outages-2021-11-01.json', left)
        self.assertIn('outages-2021-11-30.json', left)
        self.assertIn('spot-prices-2021-11-01.json', left)           # not this script's to remove


if __name__ == '__main__':
    unittest.main()
