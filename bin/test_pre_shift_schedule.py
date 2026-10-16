"""Unit tests for pre_shift_schedule."""

import datetime
import unittest

from bin.lan_bitable_template_portal.pre_shift_schedule import normalize_frequency, occurrences_for_date, upcoming_occurrences

UTC8 = datetime.timezone(datetime.timedelta(hours=8))


def _due(y, m, d, hh, mm):
    return datetime.datetime(y, m, d, hh, mm, tzinfo=UTC8)


def _to_list(rule, day):
    return [(o["due"], o["day"], o["shift"]) for o in occurrences_for_date(rule, day)]


class NormalizeTests(unittest.TestCase):
    def test_legacy_none_default(self):
        rule = normalize_frequency(None, "2026-01-01")
        self.assertEqual(rule["kind"], "shift")
        self.assertEqual(rule["day_time"], "08:50")
        self.assertEqual(rule["night_time"], "17:50")
        self.assertEqual(rule["start_date"], "2026-01-01")
        self.assertIsInstance(rule["start_date"], str)

    def test_start_date_resolution(self):
        rule = normalize_frequency({"kind": "daily", "time": "09:00"},
                                   "2026-03-05")
        self.assertEqual(rule["start_date"], "2026-03-05")
        rule = normalize_frequency(
            {"kind": "daily", "time": "09:00", "start_date": "2026-01-02"},
            "2026-03-05")
        self.assertEqual(rule["start_date"], "2026-01-02")

    def test_numeric_bool_rejected(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "weekly", "weekday": True,
                                 "time": "09:00"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "interval", "every_days": True,
                                 "time": "09:00"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "temporary", "limit": True},
                                "2026-01-01")

    def test_invalid_times(self):
        bad = ["8:00", "08:00:00", "25:00", "08:60", "abc", "", "0800"]
        for t in bad:
            with self.assertRaises(ValueError):
                normalize_frequency({"kind": "daily", "time": t},
                                    "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "shift", "day_time": "07:00"},
                                "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "shift", "night_time": "08:00"},
                                "2026-01-01")

    def test_unknown_fields_rejected(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "daily", "time": "09:00", "bogus": 1},
                                "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "shift", "time": "09:00"},
                                "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "temporary", "cadence": "daily",
                                 "day_time": "08:50"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "temporary", "cadence": "shift",
                                 "time": "09:00"}, "2026-01-01")

    def test_unknown_kind_and_missing_kind(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "weird"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"time": "09:00"}, "2026-01-01")

    def test_invalid_dates(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "daily", "time": "09:00",
                                 "start_date": "2026-02-30"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency("not-a-dict", "2026-01-01")

    def test_temporary_defaults(self):
        rule = normalize_frequency({"kind": "temporary"}, "2026-05-01")
        self.assertEqual(rule["cadence"], "daily")
        self.assertEqual(rule["limit_type"], "occurrences")
        self.assertEqual(rule["limit"], 1)
        self.assertEqual(rule["time"], "08:50")

    def test_numeric_bounds(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "weekly", "weekday": 7,
                                 "time": "09:00"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "interval", "every_days": 0,
                                 "time": "09:00"}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": "temporary", "limit": 400},
                                "2026-01-01")


class ShiftTests(unittest.TestCase):
    def test_normal_day(self):
        rule = normalize_frequency(None, "2026-01-01")
        out = _to_list(rule, "2026-01-01")
        self.assertEqual(out, [
            (_due(2026, 1, 1, 8, 50), "2026-01-01", "\u767d"),
            (_due(2026, 1, 1, 17, 50), "2026-01-01", "\u591c"),
        ])

    def test_before_start_empty(self):
        rule = normalize_frequency(None, "2026-01-10")
        self.assertEqual(_to_list(rule, "2026-01-09"), [])

    def test_night_next_morning(self):
        rule = normalize_frequency(
            {"kind": "shift", "night_time": "02:00"}, "2026-01-01")
        out = _to_list(rule, "2026-01-01")
        self.assertEqual(out, [
            (_due(2026, 1, 1, 8, 50), "2026-01-01", "\u767d"),
            (_due(2026, 1, 2, 2, 0), "2026-01-01", "\u591c"),
        ])


class DailyTests(unittest.TestCase):
    def test_every_day(self):
        rule = normalize_frequency({"kind": "daily", "time": "12:00"},
                                   "2026-01-01")
        self.assertEqual(_to_list(rule, "2026-01-01"),
                         [(_due(2026, 1, 1, 12, 0), "2026-01-01", "\u767d")])
        self.assertEqual(_to_list(rule, "2026-01-02"),
                         [(_due(2026, 1, 2, 12, 0), "2026-01-02", "\u767d")])

    def test_night_shift_mapping(self):
        rule = normalize_frequency({"kind": "daily", "time": "18:00"},
                                   "2026-01-01")
        self.assertEqual(_to_list(rule, "2026-01-01")[0][1:],
                         ("2026-01-01", "\u591c"))

    def test_early_morning_previous_day(self):
        rule = normalize_frequency({"kind": "daily", "time": "02:00"},
                                   "2026-01-01")
        self.assertEqual(_to_list(rule, "2026-01-01")[0][1:],
                         ("2025-12-31", "\u591c"))

    def test_white_boundary(self):
        # 17:49 still white; 17:50 is night.
        rule = normalize_frequency({"kind": "daily", "time": "17:49"},
                                   "2026-01-01")
        occ = occurrences_for_date(rule, "2026-01-01")
        self.assertEqual(occ[0]["shift"], "\u767d")
        rule = normalize_frequency({"kind": "daily", "time": "17:50"},
                                   "2026-01-01")
        occ = occurrences_for_date(rule, "2026-01-01")
        self.assertEqual(occ[0]["shift"], "\u591c")


class WeeklyTests(unittest.TestCase):
    def test_weekday_match(self):
        # 2026-01-05 is a Monday.
        rule = normalize_frequency({"kind": "weekly", "weekday": 0,
                                    "time": "10:00"}, "2026-01-01")
        occ = occurrences_for_date(rule, "2026-01-05")
        self.assertEqual(occ[0]["day"], "2026-01-05")
        self.assertEqual(_to_list(rule, "2026-01-06"), [])  # Tuesday off-day

    def test_weekly_before_start(self):
        rule = normalize_frequency({"kind": "weekly", "weekday": 0,
                                    "time": "10:00"}, "2026-01-05")
        self.assertEqual(occurrences_for_date(rule, "2026-01-05")[0]["day"],
                         "2026-01-05")
        self.assertEqual(occurrences_for_date(rule, "2026-01-12")[0]["day"],
                         "2026-01-12")


class MonthTests(unittest.TestCase):
    def test_month_start(self):
        rule = normalize_frequency({"kind": "month_start", "time": "09:00"},
                                   "2026-01-01")
        self.assertEqual(occurrences_for_date(rule, "2026-02-01")[0]["day"],
                         "2026-02-01")
        self.assertEqual(_to_list(rule, "2026-02-02"), [])

    def test_month_end(self):
        rule = normalize_frequency({"kind": "month_end", "time": "09:00"},
                                   "2026-01-01")
        self.assertEqual(occurrences_for_date(rule, "2026-02-28")[0]["day"],
                         "2026-02-28")
        self.assertEqual(_to_list(rule, "2026-02-27"), [])

    def test_leap_year_feb(self):
        rule = normalize_frequency({"kind": "month_end", "time": "09:00"},
                                   "2024-01-01")
        self.assertEqual(occurrences_for_date(rule, "2024-02-29")[0]["day"],
                         "2024-02-29")
        self.assertEqual(_to_list(rule, "2024-02-28"), [])
        # Non-leap year: last day is Feb 28.
        rule = normalize_frequency({"kind": "month_end", "time": "09:00"},
                                   "2026-01-01")
        self.assertEqual(occurrences_for_date(rule, "2026-02-28")[0]["day"],
                         "2026-02-28")


class IntervalTests(unittest.TestCase):
    def test_anchored_modulo(self):
        rule = normalize_frequency({"kind": "interval", "every_days": 3,
                                    "time": "09:00"}, "2026-01-01")
        self.assertEqual(occurrences_for_date(rule, "2026-01-01")[0]["day"],
                         "2026-01-01")
        self.assertEqual(_to_list(rule, "2026-01-02"), [])
        self.assertEqual(_to_list(rule, "2026-01-03"), [])
        self.assertEqual(occurrences_for_date(rule, "2026-01-04")[0]["day"],
                         "2026-01-04")


class TemporaryTests(unittest.TestCase):
    def test_daily_occurrences_limit(self):
        rule = normalize_frequency(
            {"kind": "temporary", "cadence": "daily", "limit_type": "occurrences",
             "limit": 2, "time": "09:00"}, "2026-01-01")
        self.assertEqual(len(_to_list(rule, "2026-01-01")), 1)
        self.assertEqual(len(_to_list(rule, "2026-01-02")), 1)
        self.assertEqual(_to_list(rule, "2026-01-03"), [])
        self.assertEqual(_to_list(rule, "2026-01-10"), [])

    def test_daily_days_limit(self):
        rule = normalize_frequency(
            {"kind": "temporary", "cadence": "daily", "limit_type": "days",
             "limit": 2, "time": "09:00"}, "2026-01-01")
        self.assertEqual(len(_to_list(rule, "2026-01-01")), 1)
        self.assertEqual(len(_to_list(rule, "2026-01-02")), 1)
        self.assertEqual(_to_list(rule, "2026-01-03"), [])

    def test_shift_days_limit(self):
        rule = normalize_frequency(
            {"kind": "temporary", "cadence": "shift", "limit_type": "days",
             "limit": 2}, "2026-01-01")
        self.assertEqual(len(_to_list(rule, "2026-01-01")), 2)
        self.assertEqual(len(_to_list(rule, "2026-01-02")), 2)
        self.assertEqual(_to_list(rule, "2026-01-03"), [])

    def test_shift_occurrences_ordering(self):
        rule = normalize_frequency(
            {"kind": "temporary", "cadence": "shift",
             "limit_type": "occurrences", "limit": 3}, "2026-01-01")
        # day, night on day 1; day only on day 2.
        day1 = _to_list(rule, "2026-01-01")
        self.assertEqual(len(day1), 2)
        self.assertEqual(day1[0][2], "\u767d")
        self.assertEqual(day1[1][2], "\u591c")
        day2 = _to_list(rule, "2026-01-02")
        self.assertEqual([(d[0], d[2]) for d in day2],
                         [(_due(2026, 1, 2, 8, 50), "\u767d")])
        self.assertEqual(_to_list(rule, "2026-01-03"), [])

    def test_shift_occurrences_single(self):
        rule = normalize_frequency(
            {"kind": "temporary", "cadence": "shift",
             "limit_type": "occurrences", "limit": 1}, "2026-01-01")
        self.assertEqual(_to_list(rule, "2026-01-01")[0][2], "\u767d")
        self.assertEqual(_to_list(rule, "2026-01-02"), [])

    def test_temporary_before_start(self):
        rule = normalize_frequency({"kind": "temporary"}, "2026-01-10")
        self.assertEqual(_to_list(rule, "2026-01-09"), [])


class ClockIndependenceTests(unittest.TestCase):
    def test_no_dependence_on_wall_clock(self):
        # Results are identical regardless of when tests run.
        rule = normalize_frequency(None, "2026-01-01")
        for day in ("2026-01-01", "2026-01-02", "2026-12-31"):
            out1 = _to_list(rule, day)
            out2 = _to_list(rule, datetime.date.fromisoformat(day))
            self.assertEqual(out1, out2)


class StrictJsonSerializableTests(unittest.TestCase):
    def test_start_dates_always_strings(self):
        import json
        for value, default in [
            (None, "2026-01-01"),
            ({"kind": "daily", "time": "09:00"}, "2026-01-01"),
            ({"kind": "temporary"}, "2026-01-02"),
            ({"kind": "shift", "start_date": "2026-03-05"}, "2026-01-01"),
        ]:
            rule = normalize_frequency(value, default)
            self.assertIsInstance(rule["start_date"], str)
            json.dumps(rule)

    def test_strict_compact_and_week_dates_rejected(self):
        bad_dates = [
            "20260101", "2026-W01-1", "2026-001", "2026-1-1",
            "2026-13-01", "2026-02-30", "2026-02-29", " 2026-01-01", "2026-01-01 ",
        ]
        for bad in bad_dates:
            with self.assertRaises(ValueError):
                normalize_frequency({"kind": "daily", "time": "09:00",
                                     "start_date": bad}, "2026-01-01")

    def test_unhashable_kind_raises_value_error(self):
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": ["daily"]}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": {"x": 1}}, "2026-01-01")
        with self.assertRaises(ValueError):
            normalize_frequency({"kind": 5}, "2026-01-01")

    def test_occurrences_parses_string_start_and_date_day_internally(self):
        rule = normalize_frequency(None, "2026-01-01")
        # start_date is a string; occurrences_for_date must parse it.
        self.assertEqual(_to_list(rule, "2026-01-03")[0][1], "2026-01-03")
        # day may also be passed as date object.
        self.assertEqual(_to_list(rule, datetime.date(2026, 1, 3)), _to_list(rule, "2026-01-03"))
        # Rule can even carry an explicit date object for start (legacy tolerance).
        raw = {"kind": "daily", "time": "10:00", "start_date": datetime.date(2026, 1, 1)}
        self.assertEqual(occurrences_for_date(raw, "2026-01-01")[0]["day"], "2026-01-01")


class UpcomingTests(unittest.TestCase):
    def test_next_dates_follow_the_existing_calendar_rules(self):
        current = _due(2026, 10, 16, 20, 0)
        for config, expected in [
            ({'kind': 'weekly', 'weekday': 0, 'time': '09:00'}, '2026-10-19'),
            ({'kind': 'month_start', 'time': '09:00'}, '2026-11-01'),
            ({'kind': 'month_end', 'time': '09:00'}, '2026-10-31'),
            ({'kind': 'interval', 'every_days': 3660, 'time': '09:00'}, '2036-10-23'),
        ]:
            rule = normalize_frequency(config, '2026-10-16')
            rows = [r for r in upcoming_occurrences(rule, current) if r['due'] >= current]
            self.assertEqual(rows[0]['day'], expected)
            self.assertTrue(all(r in occurrences_for_date(rule, r['due'].date()) for r in rows))

    def test_finite_tasks_are_not_recreated_and_overnight_roster_day_is_preserved(self):
        rule = normalize_frequency({'kind': 'temporary', 'cadence': 'shift', 'limit': 1}, '2026-10-14')
        self.assertEqual(upcoming_occurrences(rule, _due(2026, 10, 16, 9, 0)), [])
        rule = normalize_frequency({'kind': 'shift', 'night_time': '01:00'}, '2026-10-14')
        rows = upcoming_occurrences(rule, _due(2026, 10, 15, 2, 0))
        night = next(r for r in rows if r['due'] == _due(2026, 10, 15, 1, 0))
        self.assertEqual((night['day'], night['shift']), ('2026-10-14', '夜'))


if __name__ == "__main__":
    unittest.main()
