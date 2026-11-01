"""Event-source provenance and strict exclusion of unverified historical knowledge."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stdout
from email.message import Message
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import patch

from tools.archive_october_events import (
    ReleaseText,
    candidates,
    macro_results,
    number,
    release_time,
    run,
    source_plan,
    validate_url,
)


class Response(io.BytesIO):
    status = 200
    headers: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}


class EventArchiveTests(unittest.TestCase):
    def test_sources_use_approved_hosts_and_reverse_earnings_dates(self) -> None:
        sources = source_plan()
        for source in sources:
            validate_url(source["url"])
        dates = [s["calendar_date"] for s in sources if s["kind"] == "earnings_calendar_snapshot"]
        self.assertEqual(len(dates), 22)
        self.assertEqual(dates, sorted(dates, reverse=True))
        self.assertEqual(dates[0], "2026-10-30")
        self.assertEqual(dates[-1], "2026-10-01")

    def test_unapproved_hosts_and_credentials_are_rejected(self) -> None:
        for url in (
            "http://api.nasdaq.com/",
            "https://example.com/",
            "https://user@api.nasdaq.com/",
            "https://api.nasdaq.com:444/",
        ):
            with self.assertRaises(ValueError):
                validate_url(url)

    def test_calendar_rows_remain_raw_and_application_errors_are_visible(self) -> None:
        raw: dict[str, Any] = {
            "data": {
                "rows": [{"symbol": "TEST", "time": "time-after-hours", "epsForecast": "1.23"}]
            },
            "status": {"rCode": 200},
        }
        self.assertEqual(candidates(json.dumps(raw).encode()), raw["data"]["rows"])
        with self.assertRaises(ValueError):
            candidates(b'{"status":{"rCode":500}}')
        with self.assertRaises(ValueError):
            candidates(b'{"data":{"rows":[{}]}}')

    def test_archive_resume_preserves_hashes_and_never_marks_features_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            raw = b'{"data":{"rows":[{"symbol":"TEST","eps":"2"}]}}'
            opener = unittest.mock.Mock()
            opener.open.side_effect = lambda *args, **kw: Response(raw)
            source = {
                "id": "earnings-test",
                "kind": "earnings_calendar_snapshot",
                "calendar_date": "2026-10-29",
                "url": "https://api.nasdaq.com/api/calendar/earnings?date=2026-10-29",
            }
            with (
                patch("tools.archive_october_events.source_plan", return_value=[source]),
                patch(
                    "tools.archive_october_events.urllib.request.build_opener", return_value=opener
                ),
                redirect_stdout(io.StringIO()),
            ):
                run(output)
                run(output)
            self.assertEqual(opener.open.call_count, 1)
            with sqlite3.connect(output / "events.sqlite3") as db:
                self.assertEqual(
                    db.execute(
                        "SELECT available_at,feature_eligible FROM earnings_candidates"
                    ).fetchone(),
                    (None, 0),
                )
            status = json.loads((output / "status.json").read_text())
            self.assertEqual(status["state"], "complete")
            self.assertEqual(status["coverage"], "partial_source_archive")
            self.assertTrue(status["pending"])

    def test_blocked_host_is_not_requested_repeatedly(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            sources = [s for s in source_plan() if s["kind"] == "earnings_calendar_snapshot"][:2]
            opener = unittest.mock.Mock()
            opener.open.side_effect = urllib.error.HTTPError(
                sources[0]["url"], 403, "Denied", Message(), io.BytesIO(b"blocked")
            )
            with (
                patch("tools.archive_october_events.source_plan", return_value=sources),
                patch(
                    "tools.archive_october_events.urllib.request.build_opener", return_value=opener
                ),
                redirect_stdout(io.StringIO()),
            ):
                run(Path(folder))
            self.assertEqual(opener.open.call_count, 1)
            with sqlite3.connect(Path(folder) / "events.sqlite3") as db:
                self.assertEqual(
                    db.execute("SELECT state FROM sources ORDER BY rowid").fetchall(),
                    [("http_error",), ("skipped_blocked_host",)],
                )

    def test_numeric_values_preserve_signs_and_missing_is_not_zero(self) -> None:
        self.assertEqual(number("($1,200.50)"), -1200.5)
        self.assertEqual(number("0.0%"), 0)
        self.assertIsNone(number("N/A"))
        self.assertIsNone(number("--"))
        for value in ("NaN", "Infinity", "1e9999", "higher", "1.2 billion"):
            with self.assertRaises(ValueError):
                number(value)

    def test_fed_fractional_rate_and_vote_are_numeric_without_commentary(self) -> None:
        parser = ReleaseText()
        parser.feed(
            "<script>target range for the federal funds rate at 0 to 99 percent</script>"
            "<p>The target range for the federal funds rate at 3-3/4 to 4 percent.</p>"
            "<p>By a 10 &ndash; 2 vote. Uncertainty is elevated.</p>"
        )
        facts = macro_results("fed-decision", " ".join(parser.parts))
        values = {row[0]: row[2] for row in facts}
        self.assertEqual(
            values,
            {"fed_funds_lower": 3.75, "fed_funds_upper": 4, "votes_for": 10, "votes_against": 2},
        )

    def test_revision_roles_do_not_overwrite_original_values(self) -> None:
        text = (
            "The change in total nonfarm payroll employment for July was revised down by "
            "31,000, from +21,000 to -10,000, and the change for August was revised down by "
            "29,000, from +162,000 to +133,000."
        )
        facts = {(row[0], row[1]): row[2] for row in macro_results("bls-jobs", text)}
        self.assertEqual(
            facts[("nonfarm_payroll_change_2026-07", "previous_before_revision")], 21000
        )
        self.assertEqual(
            facts[("nonfarm_payroll_change_2026-07", "previous_revised_in_release")], -10000
        )
        self.assertEqual(
            facts[("nonfarm_payroll_change_2026-08", "previous_before_revision")], 162000
        )
        self.assertIsNone(facts[("nonfarm_payroll_change", "actual")])

    def test_release_time_requires_nearby_source_date_and_handles_timezones(self) -> None:
        self.assertEqual(
            release_time("bls-cpi", "2026-10-14", "8:30 a.m. (ET) Wednesday, October 14, 2026"),
            "2026-10-14T12:30:00+00:00",
        )
        self.assertIsNone(release_time("bls-cpi", "2026-10-14", "October 14, 2026"))
        self.assertIsNone(release_time("bls-cpi", "2026-10-14", "8:30 a.m. (ET) November 10, 2026"))
        self.assertIsNone(release_time("bok-decision", "2026-10-22", "2026.10.22"))

    def test_actual_and_forecast_snapshots_are_separate_and_ineligible(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            raw = json.dumps(
                {
                    "data": {
                        "rows": [
                            {
                                "symbol": "TEST",
                                "eps": "($0.12)",
                                "epsForecast": "$0.02",
                                "surprise": "-700",
                                "noOfEsts": "3",
                            },
                            {"symbol": "NONE", "eps": "N/A", "epsForecast": "$1.00"},
                        ]
                    }
                }
            ).encode()
            opener = unittest.mock.Mock()
            opener.open.side_effect = lambda *args, **kw: Response(raw)
            source = next(s for s in source_plan() if s["id"] == "earnings-2026-10-29")
            with (
                patch("tools.archive_october_events.source_plan", return_value=[source]),
                patch(
                    "tools.archive_october_events.urllib.request.build_opener", return_value=opener
                ),
                redirect_stdout(io.StringIO()),
            ):
                run(output)
                run(output, extract_only=True)
            self.assertEqual(opener.open.call_count, 1)
            with sqlite3.connect(output / "events.sqlite3") as db:
                rows = db.execute(
                    "SELECT role,value,available_at,feature_eligible "
                    "FROM numeric_results WHERE event_id=? AND metric='eps' "
                    "ORDER BY role",
                    (source["id"] + ":TEST",),
                ).fetchall()
                self.assertEqual(
                    rows, [("actual", -0.12, None, 0), ("forecast_snapshot", 0.02, None, 0)]
                )
                self.assertEqual(
                    db.execute(
                        "SELECT released_at,time_precision FROM event_schedule LIMIT 1"
                    ).fetchone(),
                    (None, "date"),
                )
                self.assertEqual(
                    db.execute("SELECT state FROM extractions").fetchone(), ("partial",)
                )
            self.assertTrue((output / "numeric_results.csv").exists())

    def test_web_excerpts_keep_separate_provenance_and_do_not_claim_http_success(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            source = next(s for s in source_plan() if s["id"] == "bls-eci")
            excerpt = output / "excerpts.json"
            record = {
                "source_id": source["id"],
                "url": source["url"],
                "retrieved_at": "2026-11-01T00:00:00+00:00",
                "retrieval_method": "web_tool_excerpt",
                "text": "8:30 a.m. (ET) Friday, October 30, 2026. "
                "Compensation costs for civilian workers increased 0.8 percent, "
                "seasonally adjusted. Compensation costs for civilian workers "
                "increased 3.4 percent, not seasonally adjusted.",
            }
            excerpt.write_text(json.dumps([record]))
            with (
                patch("tools.archive_october_events.source_plan", return_value=[source]),
                redirect_stdout(io.StringIO()),
            ):
                run(output, excerpts=excerpt, extract_only=True)
            with sqlite3.connect(output / "events.sqlite3") as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM sources").fetchone()[0], 0)
                self.assertEqual(
                    db.execute(
                        "SELECT value,evidence_kind FROM numeric_results ORDER BY metric"
                    ).fetchall(),
                    [(0.8, "web_tool_excerpt"), (3.4, "web_tool_excerpt")],
                )
            record["url"] = "https://example.com/"
            excerpt.write_text(json.dumps([record]))
            with redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
                run(output, excerpts=excerpt, extract_only=True)


if __name__ == "__main__":
    unittest.main()
