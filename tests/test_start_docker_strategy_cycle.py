import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from tools.start_docker_strategy_cycle import previous_jobs_ended, snapshot_previous, start


class DockerCutoverTests(unittest.TestCase):
    def test_waits_for_mac_job_receipts_and_process_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            prior = Path(tmp)
            with patch("tools.start_docker_strategy_cycle.subprocess.run") as compose:
                self.assertEqual(start(Path("docker"), Path("env"), prior, prior), 0)
                compose.assert_not_called()
            for market in ("KR", "US"):
                job = prior / f"{market}-job"
                job.mkdir()
                (job / "exit.json").write_text('{"returncode": 0}')
                (job / "target.json").write_text('{"pid": 123, "identity": "started"}')
            with patch(
                "tools.start_docker_strategy_cycle.process_identity", return_value="started"
            ):
                self.assertFalse(previous_jobs_ended(prior))
            with patch("tools.start_docker_strategy_cycle.process_identity", return_value=None):
                self.assertTrue(previous_jobs_ended(prior))

    def test_mac_wal_and_result_are_copied_without_writing_the_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "mac/US/session"
            src.mkdir(parents=True)
            report = src / "comparison.json"
            report.write_text(json.dumps({"state": "closed"}))
            journal = src / "journal.sqlite3"
            with closing(sqlite3.connect(journal)) as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("CREATE TABLE frozen (id INTEGER)")
                db.execute("INSERT INTO frozen VALUES (42)")
                db.commit()
                wal = journal.with_name(journal.name + "-wal")
                original = (journal.read_bytes(), wal.read_bytes(), report.read_bytes())
                target = root / "prior"
                snapshot_previous(root / "mac", target)
                self.assertEqual(
                    original, (journal.read_bytes(), wal.read_bytes(), report.read_bytes())
                )
                copied = target / "US/session/journal.sqlite3"
                with closing(sqlite3.connect(copied)) as result:
                    self.assertEqual(result.execute("SELECT id FROM frozen").fetchall(), [(42,)])
                    self.assertEqual(result.execute("PRAGMA journal_mode").fetchone(), ("delete",))
