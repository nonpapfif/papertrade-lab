"""Check candle availability boundaries used by the read-only campaign."""

import unittest

from tools.validate_toss_stream import mature_bars


class CandleBoundaryTests(unittest.TestCase):
    def test_incomplete_and_future_bars_are_not_mature(self) -> None:
        rows = [{"timestamp": f"2026-11-03T05:{minute:02d}:00+00:00"} for minute in (0, 1, 2)]
        result = mature_bars({"candles": rows}, "2026-11-03T05:01:59+00:00")
        self.assertEqual(list(result), [rows[0]["timestamp"]])

    def test_minute_end_boundary_is_explicit(self) -> None:
        row = {"timestamp": "2026-11-03T14:00:00+09:00", "closePrice": "10"}
        self.assertEqual(
            mature_bars({"candles": [row]}, "2026-11-03T05:01:00+00:00"), {row["timestamp"]: row}
        )

    def test_missing_bar_payload_is_an_error(self) -> None:
        with self.assertRaises(KeyError):
            mature_bars({}, "2026-11-03T05:01:00+00:00")
