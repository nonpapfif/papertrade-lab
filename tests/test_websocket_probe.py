"""Check market-only declarations and safe frame dispatch without credentials."""

import unittest
from types import SimpleNamespace

from tools.probe_toss_websocket import (
    declaration,
    error_details,
    inspect_frame,
    record_source_delta,
)


class ProbeTests(unittest.TestCase):
    def test_close_diagnostics_preserve_codes_and_redact_credentials(self) -> None:
        class ClosedError(Exception):
            rcvd = SimpleNamespace(code=1008, reason="secret-token Bearer private-value")
            sent = SimpleNamespace(code=1000, reason="")
            rcvd_then_sent = True

        details = error_details(ClosedError(), "secret-token")
        self.assertEqual(details["rcvd"]["code"], 1008)
        self.assertEqual(details["sent"]["code"], 1000)
        self.assertNotIn("secret-token", str(details))
        self.assertNotIn("private-value", str(details))

    def test_missing_close_frame_remains_unknown(self) -> None:
        details = error_details(OSError("Do not serialize arbitrary error text"), "")
        self.assertEqual(details["error_type"], "OSError")
        self.assertNotIn("rcvd", details)
        self.assertNotIn("arbitrary", str(details))

    def test_clock_difference_is_not_clamped_or_called_latency(self) -> None:
        stats = {}
        record_source_delta(stats, "2026-11-03T14:00:02+09:00", "2026-11-03T05:00:00+00:00")
        record_source_delta(stats, "2026-11-03T05:00:00+00:00", "2026-11-03T05:00:01+00:00")
        record_source_delta(stats, "invalid", "2026-11-03T05:00:01+00:00")
        self.assertEqual(stats["negative_count"], 1)
        self.assertEqual(stats["min_seconds"], -2)
        self.assertEqual(stats["max_seconds"], 1)
        self.assertEqual(stats["count"], 2)
        self.assertEqual(stats["invalid_timestamp_count"], 1)

    def test_declaration_only_contains_market_channels(self) -> None:
        self.assertEqual(
            declaration("kr", ["005930"]),
            [
                {"type": "trade:kr", "codes": ["005930"]},
                {"type": "orderbook:kr", "codes": ["005930"]},
            ],
        )
        for market, symbols in (
            ("personal", ["3"]),
            ("kr", []),
            ("us", ["AAPL", "AAPL"]),
            ("us", [f"T{i}" for i in range(51)]),
        ):
            with self.assertRaises(ValueError):
                declaration(market, symbols)

    def test_unexpected_or_personal_frames_cannot_be_logged(self) -> None:
        for topic in ("personal:order:3", "trade:us:TSLA"):
            with self.assertRaises(ValueError):
                inspect_frame({"type": "message", "topic": topic, "data": {}}, {"trade:us:AAPL"})

    def test_ack_removes_provider_error_text(self) -> None:
        key, frame = inspect_frame(
            {"type": "subscriptions", "subscribed": [], "rejected": [{"message": "sensitive"}]},
            set(),
        )
        self.assertEqual(key, "subscriptions")
        self.assertEqual(frame["rejected_count"], 1)
        self.assertNotIn("sensitive", str(frame))

    def test_market_payload_and_pong_dispatch(self) -> None:
        source = {"type": "message", "topic": "trade:us:AAPL", "data": {"price": "1"}}
        self.assertEqual(inspect_frame(source, {"trade:us:AAPL"}), ("trade:us:AAPL", source))
        self.assertEqual(inspect_frame({"type": "pong"}, set()), ("pong", {"type": "pong"}))

    def test_unknown_ack_topic_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            inspect_frame(
                {"type": "subscriptions", "subscribed": ["personal:order:3"]}, {"trade:us:AAPL"}
            )
