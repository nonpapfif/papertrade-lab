"""Offline checks for the shared-feed operator command spool."""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from tools.slot_control import SPOOL_NAME, SlotCommand, SlotControl, main

POLICY = {"policy": "trend", "selection": "all", "allocation": "equal"}


class SlotControlTest(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name)
        self.spool = self.output / SPOOL_NAME
        self.end = time.time() + 60
        self.add = SlotCommand(
            "req-1", "add", "small", {"capital": 100, "incumbent": POLICY, "candidate": POLICY}
        )
        self.stop = SlotCommand("req-2", "stop", "small")

    def test_preserves_order_and_immutable_receipts_across_reopen(self) -> None:
        with SlotControl.open(self.spool, initialize=True) as control:
            first = control.submit(self.add, end_epoch=self.end)
            control.submit(self.stop, end_epoch=self.end)
            self.assertEqual(control.pending(), [self.add, self.stop])
            self.assertEqual(control.submit(self.add, end_epoch=0), first)
            with self.assertRaisesRegex(ValueError, "different request"):
                control.submit(SlotCommand(self.add.id, "stop", "small"), end_epoch=self.end)

        with SlotControl.open(self.spool) as control:
            control.record(self.add, "accepted", {"sequence": 23})
            control.snapshot([{"id": "small", "state": "active"}])
            original_report = control.report()
            control.record(self.add, "accepted", {"sequence": 23})
            with self.assertRaisesRegex(ValueError, "different evidence"):
                control.record(self.add, "rejected", {"sequence": 24})
            self.assertEqual(control.report(), original_report)
            self.assertEqual(control.pending(), [self.stop])

        with SlotControl.open(self.spool, initialize=True) as control:
            self.assertEqual(control.report(), original_report)
            self.assertEqual(control.pending(), [self.stop])

    def test_invalid_commands_and_closed_window_do_not_write_requests(self) -> None:
        with SlotControl.open(self.spool, initialize=True) as control:
            for bad in (
                SlotCommand("../unsafe", "stop", "slot"),
                SlotCommand("req", "restart", "slot"),
                SlotCommand("req", "add", "slot", {"capital": 1}),
                SlotCommand(
                    "req",
                    "add",
                    "slot",
                    {"capital": float("inf"), "incumbent": POLICY, "candidate": POLICY},
                ),
                SlotCommand(
                    "req", "add", "slot", {"capital": 1, "incumbent": {}, "candidate": POLICY}
                ),
            ):
                with self.subTest(command=bad), self.assertRaises(ValueError):
                    control.submit(bad, end_epoch=self.end)
            with self.assertRaisesRegex(ValueError, "window has closed"):
                control.submit(self.add, end_epoch=0)
            control.close_submissions()
            with self.assertRaisesRegex(ValueError, "window has closed"):
                control.submit(self.stop, end_epoch=self.end)
            self.assertEqual(control.report(), {"slots": [], "closed": True, "commands": []})

    def test_cli_preserves_an_existing_legacy_schedule(self) -> None:
        path = self.output / "status.json"
        status = {"schedule": {"recover": True}, "state": "running"}
        path.write_text(json.dumps(status))
        with (
            patch.object(sys, "argv", ["slot_control", "--output", str(self.output), "list"]),
            patch("sys.stderr"),
            self.assertRaises(SystemExit) as result,
        ):
            main()
        self.assertEqual(result.exception.code, 2)
        self.assertEqual(json.loads(path.read_text()), status)
        self.assertFalse(self.spool.exists())

    def test_cli_lists_queued_and_applied_commands_in_terminal_schedule(self) -> None:
        status_path = self.output / "status.json"
        status = {"schedule": {"dynamic_slots": True, "end_epoch": self.end}, "state": "running"}
        status_path.write_text(json.dumps(status))
        cli = ["slot_control", "--output", str(self.output)]
        add = [
            "add",
            "--command-id",
            self.add.id,
            "--slot-id",
            self.add.slot_id,
            "--capital",
            "100",
            "--incumbent",
            json.dumps(POLICY),
            "--candidate",
            json.dumps(POLICY),
        ]
        stop = ["stop", "--command-id", self.stop.id, "--slot-id", self.stop.slot_id]
        with SlotControl.open(self.spool, initialize=True) as control:
            with patch.object(sys, "argv", [*cli, *add]), redirect_stdout(StringIO()) as stdout:
                self.assertEqual(main(), 0)
            self.assertIsNone(json.loads(stdout.getvalue())["disposition"])
            command = control.pending()[0]
            control.record(command, "applied", {"sequence": 1})
            with patch.object(sys, "argv", [*cli, *stop]), redirect_stdout(StringIO()):
                self.assertEqual(main(), 0)
            self.assertEqual(control.pending(), [self.stop])
            status_path.write_text(json.dumps({**status, "state": "complete"}))
            with patch.object(sys, "argv", [*cli, "list"]), redirect_stdout(StringIO()) as stdout:
                self.assertEqual(main(), 0)
            report = json.loads(stdout.getvalue())
            self.assertFalse(report["accepting_commands"])
            self.assertEqual([row["disposition"] for row in report["commands"]], ["applied", None])
