"""Tests for the reading of the day-ahead prices (fetch_entsoe.read_prices,
price_entry, price_zones, fetch_prices): one value for each hour of the
delivery day in course on the market's clock.

Run from the repository root:  python -m unittest discover -s tests -v

No network: the answers are A44 documents written by hand (tests/entsoe_xml.py)
with what ENTSO-E's really have, as seen in its answers of 17 Nov 2026:
intervals of 15, 30 or 60 minutes, points left out when the price repeats
(curveType A03), the intraday auctions next to the day-ahead market, two
day-ahead auctions for the same day, delivery days on another clock.
"""

import contextlib
import io
import os
import unittest
from datetime import date
from decimal import Decimal

import carry_over
import fetch_entsoe as fe
from entsoe_xml import NO_DATA, at, iso, price_answer, price_document, price_series
from test_carry_over import GENERATION, PRICES, Feed, read

DAY = date(2026, 11, 17)                       # an ordinary day: 23:00 UTC of the 16th to 23:00 UTC of the 17th
START = '2026-11-16 23:00'


def entry(answer, day=DAY):
    return fe.price_entry('10YXX', fe.read_prices(answer, day), day)


class TheDeliveryDay(unittest.TestCase):

    def test_is_the_day_on_the_markets_clock_not_the_utc_day(self):
        self.assertEqual(carry_over.delivery_day(at('2026-11-16 22:59:59')), date(2026, 11, 16))
        self.assertEqual(carry_over.delivery_day(at('2026-11-16 23:00:00')), date(2026, 11, 17))   # 00:00 in Brussels
        self.assertEqual(carry_over.delivery_day(at('2026-07-01 21:59:59')), date(2026, 7, 1))
        self.assertEqual(carry_over.delivery_day(at('2026-07-01 22:00:00')), date(2026, 7, 2))     # summer time
        self.assertEqual(carry_over.delivery_day(iso('2026-11-16 23:00:00')), date(2026, 11, 17))
        self.assertIsNone(carry_over.delivery_day('not a time'))

    def test_runs_from_midnight_to_midnight_on_that_clock(self):
        self.assertEqual(carry_over.day_bounds(DAY), (at('2026-11-16 23:00'), at('2026-11-17 23:00')))
        self.assertEqual(carry_over.day_bounds(date(2026, 7, 2)), (at('2026-07-01 22:00'), at('2026-07-02 22:00')))
        # 23 hours on the day the clocks go forward, 25 on the day they go back.
        self.assertEqual(carry_over.day_bounds(date(2026, 3, 29)), (at('2026-03-28 23:00'), at('2026-03-29 22:00')))
        self.assertEqual(carry_over.day_bounds(date(2026, 10, 25)), (at('2026-10-24 22:00'), at('2026-10-25 23:00')))

    def test_is_what_entsoe_is_asked_for(self):
        url = fe.price_request('https://entsoe.test/api', '10YPT-REN------W', DAY)
        self.assertIn('documentType=A44&in_Domain=10YPT-REN------W&out_Domain=10YPT-REN------W'
                      '&periodStart=202611162300&periodEnd=202611172300', url)


class AnHourIsTheMeanOfItsIntervals(unittest.TestCase):

    def test_quarter_hours(self):
        # Hour h has 4h, 4h+1, 4h+2 and 4h+3: its mean is 4h + 1.5.
        quarters = [float(q) for q in range(96)]
        found = fe.read_prices(price_document(price_series(START, quarters, minutes=15)), DAY)
        self.assertEqual(found, [(h, Decimal(4 * h) + Decimal('1.5')) for h in range(24)])
        zone = entry(price_document(price_series(START, quarters, minutes=15)))
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual(zone['prices'][0], {'time': '00:00', 'hour': 0, 'price': 1.5})
        self.assertEqual(zone['prices'][23], {'time': '23:00', 'hour': 23, 'price': 93.5})
        self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(24)])
        self.assertEqual([p['hour'] for p in zone['prices']], list(range(24)))
        self.assertEqual((zone['avg'], zone['min'], zone['max']), (47.5, 1.5, 93.5))
        self.assertEqual(zone['latest'], zone['prices'][-1])
        self.assertEqual(zone['delivery_date'], '2026-11-17')

    def test_half_hours(self):
        halves = [10.0, 20.0] * 24
        self.assertEqual(fe.read_prices(price_document(price_series(START, halves, minutes=30)), DAY),
                         [(h, Decimal(15)) for h in range(24)])

    def test_a_zone_that_trades_by_the_hour_has_its_own_prices(self):
        hourly = [50.25 + h for h in range(24)]
        zone = entry(price_document(price_series(START, hourly, minutes=60)))
        self.assertEqual([p['price'] for p in zone['prices']], hourly)
        self.assertEqual((zone['avg'], zone['min'], zone['max']), (61.75, 50.25, 73.25))

    def test_halves_of_a_cent_round_up(self):
        # 10.00, 10.01, 10.01, 10.00: the hour is 10.005, published as 10.01.
        quarters = [10.00, 10.01, 10.01, 10.00] * 24
        zone = entry(price_document(price_series(START, quarters, minutes=15)))
        self.assertEqual({p['price'] for p in zone['prices']}, {10.01})
        self.assertEqual(zone['avg'], 10.01)

    def test_the_average_is_that_of_the_hours_before_they_are_rounded(self):
        # Twelve hours at 10.00 and twelve at 10.0075 (three quarters at 10.01
        # and one at 10.00), published as 10.01. The mean of the published
        # hours would be 10.005, that is 10.01; the mean of the day is
        # 10.00375, that is 10.00, which is what OMIE's file has for it.
        quarters = [10.00] * 48 + [10.01, 10.01, 10.01, 10.00] * 12
        zone = entry(price_document(price_series(START, quarters, minutes=15)))
        self.assertEqual([p['price'] for p in zone['prices']], [10.0] * 12 + [10.01] * 12)
        self.assertEqual(zone['avg'], 10.0)
        self.assertEqual((zone['min'], zone['max']), (10.0, 10.01))

    def test_prices_are_numbers_with_decimals_and_hours_are_whole(self):
        zone = entry(price_document(price_series(START, [60] * 24)))
        self.assertIsInstance(zone['prices'][0]['price'], float)
        self.assertIsInstance(zone['prices'][0]['hour'], int)
        for field in ('avg', 'min', 'max'):
            self.assertIsInstance(zone[field], float, field)


class PointsLeftOut(unittest.TestCase):
    """curveType A03: a point that repeats the price before it is not written."""

    def test_are_put_back(self):
        written = [float(q // 8) for q in range(96)]                    # the price changes every two hours
        left_out = [p if q % 8 == 0 else None for q, p in enumerate(written)]
        self.assertEqual(sum(p is not None for p in left_out), 12)
        full = price_document(price_series(START, written, minutes=15, curve='A01'))
        short = price_document(price_series(START, left_out, minutes=15, end='2026-11-17 23:00'))
        self.assertEqual(fe.read_prices(short, DAY), fe.read_prices(full, DAY))
        self.assertEqual(len(fe.read_prices(short, DAY)), 24)

    def test_serbia_24_points_in_a_grid_of_quarter_hours(self):
        # Serbia trades by the hour and comes on a quarter-hour grid: 24
        # points, at positions 1, 5, 9... 93. They were read as hours 0, 4, 8...
        quarters = [100.0 + q // 4 if q % 4 == 0 else None for q in range(96)]
        zone = entry(price_document(price_series(START, quarters, minutes=15, end='2026-11-17 23:00')))
        self.assertEqual([p['price'] for p in zone['prices']], [100.0 + h for h in range(24)])
        self.assertEqual([p['time'] for p in zone['prices']][-1], '23:00')

    def test_up_to_the_end_of_the_period(self):
        # The last hours repeat the price of 20:00 and are not written at all.
        hourly = [50.0 + h for h in range(21)] + [None, None, None]
        zone = entry(price_document(price_series(START, hourly, end='2026-11-17 23:00')))
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual([p['price'] for p in zone['prices']][-4:], [70.0, 70.0, 70.0, 70.0])

    def test_a_fixed_curve_with_a_point_missing_has_a_gap(self):
        # curveType A01: every point is written, so a missing one is missing.
        quarters = [10.0] * 96
        quarters[9] = None                                               # a quarter of hour 2
        zone = entry(price_document(price_series(START, quarters, minutes=15, curve='A01')))
        self.assertEqual([p['hour'] for p in zone['prices']], [0, 1] + list(range(3, 24)))


class OnlyTheDayAheadMarket(unittest.TestCase):
    """An answer may have more than the day-ahead market's series."""

    def test_spain_the_intraday_auctions_are_not_read(self):
        # 17 Nov 2026: the day-ahead market (64.56 on average) and two
        # intraday auctions (69.20 and 77.54), all three for the whole day.
        answer = price_document(
            price_series(START, [77.54] * 96, minutes=15, contract='A07', position=2),
            price_series(START, [64.56] * 96, minutes=15),
            price_series(START, [69.20] * 96, minutes=15, contract='A07', position=1))
        zone = entry(answer)
        self.assertEqual(len(zone['prices']), 24)                        # not 263 "hours"
        self.assertEqual((zone['avg'], zone['min'], zone['max']), (64.56, 64.56, 64.56))

    def test_an_answer_with_intraday_auctions_only_has_no_price_of_the_day(self):
        self.assertEqual(fe.read_prices(price_document(
            price_series(START, [69.20] * 96, minutes=15, contract='A07', position=1)), DAY), [])

    def test_germany_of_two_day_ahead_auctions_the_first_is_read(self):
        # The second auction comes first in the answer, as it does.
        answer = price_document(
            price_series(START, [212.00] * 96, minutes=15, position=2),
            price_series(START, [211.58] * 96, minutes=15, position=1))
        self.assertEqual(entry(answer)['avg'], 211.58)

    def test_western_denmark_a_series_with_no_position_is_the_first(self):
        answer = price_document(
            price_series(START, [300.0] * 96, minutes=15, position=2),
            price_series(START, [209.17] * 96, minutes=15))
        self.assertEqual(entry(answer)['avg'], 209.17)

    def test_a_first_auction_that_is_of_another_day_does_not_hide_the_second(self):
        answer = price_document(
            price_series('2026-11-15 23:00', [100.0] * 24, position=1),          # yesterday's
            price_series(START, [120.0] * 24, position=2))
        self.assertEqual(entry(answer)['avg'], 120.0)

    def test_the_series_of_one_auction_are_never_mixed_with_anothers(self):
        # The first auction has the morning only: its 12 hours are published,
        # not filled in with the second auction's afternoon.
        answer = price_document(
            price_series(START, [100.0] * 12, position=1),
            price_series(START, [200.0] * 24, position=2))
        zone = entry(answer)
        self.assertEqual([p['hour'] for p in zone['prices']], list(range(12)))
        self.assertEqual(zone['avg'], 100.0)


class ThePeriodAndTheDay(unittest.TestCase):
    """Each hour is taken from the Period that covers it, whatever the day
    that Period belongs to."""

    def test_other_days_in_the_answer_are_not_read(self):
        answer = price_document(
            price_series('2026-11-15 23:00', [132.55] * 96, minutes=15),         # the day before
            price_series(START, [64.56] * 96, minutes=15),
            price_series('2026-11-17 23:00', [90.0] * 96, minutes=15))           # the day after
        zone = entry(answer)
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual(zone['avg'], 64.56)

    def test_moldova_whose_day_begins_an_hour_earlier(self):
        # Moldova's delivery day runs from 22:00 UTC to 22:00 UTC. Its first
        # hour is 23:00 of the day before on the market's clock; its 24th is
        # the 22:00 of the market's day. The market's 23:00 is the first hour
        # of Moldova's next day.
        today = price_series('2026-11-16 22:00', [float(h) for h in range(24)])
        zone = entry(price_document(today))
        self.assertEqual([p['hour'] for p in zone['prices']], list(range(23)))   # 00:00 to 22:00
        self.assertEqual(zone['prices'][0], {'time': '00:00', 'hour': 0, 'price': 1.0})   # its second hour
        self.assertEqual(zone['prices'][-1], {'time': '22:00', 'hour': 22, 'price': 23.0})

        tomorrow = price_series('2026-11-17 22:00', [500.0 + h for h in range(24)])
        zone = entry(price_document(today, tomorrow))
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual(zone['prices'][-1], {'time': '23:00', 'hour': 23, 'price': 500.0})
        self.assertEqual(zone['delivery_date'], '2026-11-17')

    def test_periods_that_go_by_the_utc_day(self):
        # Two Periods from midnight to midnight UTC: the market's day takes
        # its first hour from the end of one and the rest from the other.
        answer = price_document(
            price_series('2026-11-16 00:00', [10.0] * 23 + [11.0]),
            price_series('2026-11-17 00:00', [20.0 + h for h in range(24)]))
        zone = entry(answer)
        self.assertEqual([p['price'] for p in zone['prices']], [11.0] + [20.0 + h for h in range(23)])

    def test_a_period_longer_than_the_day(self):
        answer = price_document(price_series('2026-11-16 00:00', [float(h) for h in range(72)]))
        zone = entry(answer)
        self.assertEqual([p['price'] for p in zone['prices']], [float(h) for h in range(23, 47)])


class TheDaysTheClocksChange(unittest.TestCase):

    def test_the_day_of_23_hours_has_no_two_o_clock(self):
        day = date(2026, 3, 29)
        answer = price_document(price_series('2026-03-28 23:00', [float(q) for q in range(92)], minutes=15))
        zone = entry(answer, day)
        self.assertEqual(len(zone['prices']), 23)
        self.assertEqual([p['time'] for p in zone['prices']],
                         ['00:00', '01:00'] + [f'{h:02d}:00' for h in range(3, 24)])
        self.assertEqual([p['hour'] for p in zone['prices']], [0, 1] + list(range(3, 24)))
        self.assertEqual(zone['prices'][1]['price'], 5.5)                # quarters 4 to 7
        self.assertEqual(zone['prices'][2], {'time': '03:00', 'hour': 3, 'price': 9.5})    # quarters 8 to 11
        self.assertEqual(zone['avg'], 45.5)                              # the mean of the 23 hours
        self.assertEqual(zone['delivery_date'], '2026-03-29')

    def test_the_day_of_25_hours_has_one_two_o_clock_the_mean_of_both(self):
        day = date(2026, 10, 25)
        hourly = [10.0, 20.0, 30.0, 50.0] + [100.0] * 21                 # the two 02:00 hours: 30 and 50
        zone = entry(price_document(price_series('2026-10-24 22:00', hourly)), day)
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(24)])
        self.assertEqual(zone['prices'][2], {'time': '02:00', 'hour': 2, 'price': 40.0})
        self.assertEqual(zone['prices'][3], {'time': '03:00', 'hour': 3, 'price': 100.0})
        # The plain mean of the 24 hourly values: (10 + 20 + 40 + 21 x 100) / 24.
        self.assertEqual(zone['avg'], 90.42)
        self.assertEqual((zone['min'], zone['max']), (10.0, 100.0))

    def test_the_day_of_25_hours_in_quarter_hours(self):
        day = date(2026, 10, 25)
        quarters = [10.0] * 8 + [30.0] * 4 + [50.0] * 4 + [100.0] * 84   # 100 quarter-hours
        zone = entry(price_document(price_series('2026-10-24 22:00', quarters, minutes=15)), day)
        self.assertEqual(len(zone['prices']), 24)
        self.assertEqual(zone['prices'][2]['price'], 40.0)


class ACountryOfTwoZones(unittest.TestCase):
    """Norway, Sweden and Denmark are put together hour by hour, over the
    corrected series of their two zones."""

    def zones(self, **answers):
        found = {code: fe.read_prices(answer, DAY) for code, answer in answers.items()}
        with contextlib.redirect_stdout(io.StringIO()):
            return fe.price_zones(found, DAY)

    def test_each_hour_is_the_mean_of_the_two_zones(self):
        zones = self.zones(
            NO1=price_document(price_series(START, [float(q) for q in range(96)], minutes=15)),   # 1.5, 5.5, 9.5...
            NO2=price_document(price_series(START, [100.0] * 24)))                              # by the hour
        self.assertEqual(list(zones), ['NO'])
        no = zones['NO']
        self.assertEqual(no['eic'], '10YNO-1--------2, 10YNO-2--------T')
        self.assertEqual(len(no['prices']), 24)
        self.assertEqual(no['prices'][0], {'time': '00:00', 'hour': 0, 'price': 50.75})          # (1.5 + 100) / 2
        self.assertEqual(no['prices'][23], {'time': '23:00', 'hour': 23, 'price': 96.75})        # (93.5 + 100) / 2
        # avg, min and max are those of the country's own hours.
        self.assertEqual((no['avg'], no['min'], no['max']), (73.75, 50.75, 96.75))
        self.assertEqual(no['latest'], no['prices'][-1])
        self.assertEqual(no['delivery_date'], '2026-11-17')

    def test_only_the_hours_both_zones_have(self):
        zones = self.zones(
            SE1=price_document(price_series(START, [40.0] * 24)),
            SE3=price_document(price_series('2026-11-17 05:00', [60.0] * 18)))                  # from 06:00 on
        self.assertEqual([p['hour'] for p in zones['SE']['prices']], list(range(6, 24)))
        self.assertEqual(zones['SE']['avg'], 50.0)

    def test_with_one_zone_missing_the_country_is_not_put_together(self):
        zones = self.zones(DK1=price_document(price_series(START, [40.0] * 24)), PT=price_answer(50, at(START)))
        self.assertEqual(list(zones), ['PT'])

    def test_with_no_hour_in_common_neither(self):
        zones = self.zones(
            DK1=price_document(price_series(START, [40.0] * 6)),
            DK2=price_document(price_series('2026-11-17 11:00', [60.0] * 6)))
        self.assertEqual(zones, {})


class AnIncompleteZone(unittest.TestCase):

    def test_has_the_hours_it_has_under_their_own_labels(self):
        # The afternoon only: 12:00 to 23:00.
        zone = entry(price_document(price_series('2026-11-17 11:00', [80.0 + h for h in range(12)])))
        self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(12, 24)])
        self.assertEqual((zone['avg'], zone['min'], zone['max']), (85.5, 80.0, 91.0))

    def test_an_hour_with_only_some_of_its_quarters_is_left_out(self):
        # Published up to 10:30: the hour of 10:00 has two quarters of four.
        zone = entry(price_document(price_series(START, [5.0] * 42, minutes=15, curve='A01')))
        self.assertEqual([p['hour'] for p in zone['prices']], list(range(10)))

    def test_with_nothing_of_the_day_is_not_obtained(self):
        for answer in (NO_DATA, '<html></html>', 'not xml', '', price_document(),
                       price_document(price_series('2026-11-15 23:00', [50.0] * 24)),          # the day before only
                       price_document(price_series(START, [], minutes=15))):
            self.assertEqual(fe.read_prices(answer, DAY), [], answer[:70])

    def test_an_unreadable_price_is_a_point_left_out(self):
        answer = price_document(price_series(START, [10.0, 20.0, 30.0], curve='A01')).replace(
            '<price.amount>20.0</price.amount>', '<price.amount>n/a</price.amount>')
        self.assertEqual([p['hour'] for p in entry(answer)['prices']], [0, 2])


class TheFile(Feed):
    """What a run publishes in spot-prices.json."""

    def test_has_one_value_for_each_hour_of_the_delivery_day(self):
        self.entsoe(at('2026-11-17 07:53:00'), prices={'PT': 50, 'ES': 60, 'NO1': 40, 'NO2': 60})
        doc = read(PRICES)
        self.assertEqual(list(doc), ['updated', 'source', 'endpoint', 'zones'])
        self.assertEqual(list(doc['zones']), ['PT', 'ES', 'NO'])
        for code, zone in doc['zones'].items():
            self.assertEqual(list(zone), ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'delivery_date',
                                          'fetched_at'], code)
            self.assertEqual([p['time'] for p in zone['prices']], [f'{h:02d}:00' for h in range(24)], code)
            self.assertEqual([p['hour'] for p in zone['prices']], list(range(24)), code)
            self.assertEqual([sorted(p) for p in zone['prices']], [['hour', 'price', 'time']] * 24, code)
            self.assertEqual(zone['delivery_date'], '2026-11-17', code)
        self.assertEqual(doc['zones']['PT']['avg'], 61.5)
        self.assertEqual((doc['zones']['NO']['avg'], doc['zones']['NO']['min'], doc['zones']['NO']['max']),
                         (61.5, 50.0, 73.0))

    def test_between_23_00_and_midnight_utc_it_is_already_the_next_day(self):
        self.entsoe(at('2026-11-16 23:05:00'), prices={'PT': 50}, generation={'PT': 4000})
        self.assertEqual(read(PRICES)['zones']['PT']['delivery_date'], '2026-11-17')
        self.assertIn('periodStart=202611162300&periodEnd=202611172300', self.asked['Prices'][0])
        # The snapshot of the prices is filed under the delivery day it has;
        # those of generation and flows go by the UTC day, as before.
        self.assertEqual(sorted(os.listdir('data/archive')),
                         ['generation-mix-2026-11-16.json', 'spot-prices-2026-11-17.json'])
        self.assertEqual(read('data/archive/spot-prices-2026-11-17.json'), read(PRICES))

    def test_the_snapshot_of_a_day_is_not_touched_by_the_next_day(self):
        self.entsoe(at('2026-11-16 22:50:00'), prices={'PT': 50})
        self.entsoe(at('2026-11-16 23:05:00'), prices={'PT': 70})
        self.assertEqual(read('data/archive/spot-prices-2026-11-16.json')['zones']['PT']['avg'], 61.5)
        self.assertEqual(read('data/archive/spot-prices-2026-11-17.json')['zones']['PT']['avg'], 81.5)

    def test_the_united_kingdom_is_as_it_was(self):
        self.entsoe(at('2026-11-17 07:53:00'), prices={'PT': 50})
        self.elexon(at('2026-11-17 07:56:00'))
        gb = read(PRICES)['zones']['GB']
        self.assertEqual(list(gb), ['eic', 'prices', 'latest', 'avg', 'min', 'max', 'source', 'currency', 'fetched_at'])
        self.entsoe(at('2026-11-17 08:07:00'), prices={'PT': 51})
        self.elexon(at('2026-11-17 08:10:00'), answers=False)
        gb = read(PRICES)['zones']['GB']                                   # kept by the UTC day it was obtained on
        self.assertEqual((gb['fetched_at'], gb['carried_over']), (iso('2026-11-17 07:56:00'), True))

    def test_the_run_log_says_how_many_hours_each_zone_has(self):
        _code, log = self.entsoe(at('2026-11-17 07:53:00'), prices={'PT': 50})
        self.assertIn('Prices: PT... OK (24 hours of 17 Nov)', log)


if __name__ == '__main__':
    unittest.main()
