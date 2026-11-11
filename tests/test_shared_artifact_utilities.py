"""Verify existing artifact bytes and publication identity after utility extraction."""

from __future__ import annotations

import hashlib
import sqlite3
import stat
import tempfile
import unittest
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from papertrade.clock import now_utc, utc_iso
from papertrade.jsonio import (
    archive_json,
    atomic_json,
    canonical_json,
    compact_unicode_json,
    journal_json,
    paper_json,
    private_json,
    read_json,
    recorded_json,
    save_canonical_json,
)
from tools.research_metadata import read_metadata, write_metadata
from tools.source_identity import file_digest, tree_digest

AT = datetime(2026, 11, 10, 15, 0, 15, tzinfo=UTC)


@dataclass(frozen=True)
class Entry:
    z: int
    a: str


class SharedArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def test_encoder_profiles_preserve_exact_bytes(self) -> None:
        data = {"z": "caf\u00e9", "a": 1}
        ascii_sorted = b'{"a":1,"z":"caf\\u00e9"}'
        unicode_sorted = b'{"a":1,"z":"caf\xc3\xa9"}'
        unicode_ordered = b'{"z":"caf\xc3\xa9","a":1}'
        self.assertEqual(canonical_json(data).encode(), ascii_sorted)
        self.assertEqual(archive_json(data).encode(), unicode_sorted)
        self.assertEqual(compact_unicode_json(data).encode(), unicode_ordered)
        self.assertEqual(recorded_json(data).encode(), unicode_ordered)
        self.assertEqual(journal_json(Entry(1, "caf\u00e9")), '{"a":"caf\\u00e9","z":1}')
        self.assertEqual(
            paper_json({"value": Decimal("1.20"), "at": AT}),
            '{"value":"1.20","at":"2026-11-10T15:00:15+00:00"}',
        )
        self.assertEqual(journal_json(float("nan")), "NaN")
        for encode in (canonical_json, recorded_json):
            with self.assertRaises(ValueError):
                encode({"value": float("inf")})

    def test_file_publication_formats_preserve_modes_and_newlines(self) -> None:
        value = {"z": "caf\u00e9", "a": 1}
        durable, private, canonical = (self.root / name for name in ("d.json", "p.json", "c.json"))
        atomic_json(durable, value)
        private_json(private, value)
        save_canonical_json(canonical, value)
        self.assertEqual(durable.read_bytes(), b'{"z": "caf\xc3\xa9", "a": 1}\n')
        self.assertEqual(private.read_bytes(), b'{"z": "caf\\u00e9", "a": 1}')
        self.assertEqual(canonical.read_bytes(), b'{"a":1,"z":"caf\\u00e9"}\n')
        self.assertEqual(read_json(durable), value)
        self.assertEqual(stat.S_IMODE(durable.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o600)
        canonical.write_text("[]")
        with self.assertRaisesRegex(TypeError, "Expected JSON object"):
            read_json(canonical)

    def test_durable_write_failure_preserves_previous_bytes_and_cleans_staging(self) -> None:
        target = self.root / "status.json"
        atomic_json(target, {"state": "prior"})
        expected = target.read_bytes()
        with (
            patch("papertrade.jsonio.os.replace", side_effect=OSError("fixture failure")),
            self.assertRaises(OSError),
        ):
            atomic_json(target, {"state": "new"})
        self.assertEqual(target.read_bytes(), expected)
        self.assertEqual(list(self.root.iterdir()), [target])

    def test_private_write_rejects_existing_staging_symlink(self) -> None:
        path = self.root / "private.json"
        original = self.root / "original"
        original.write_text("prior")
        path.with_suffix(".json.tmp").symlink_to(original)
        with self.assertRaises(OSError):
            private_json(path, {"state": "new"})
        self.assertFalse(path.exists())
        self.assertEqual(original.read_text(), "prior")

    def test_tree_digest_preserves_file_kind_content_and_order(self) -> None:
        artifact = self.root / "artifact"
        artifact.write_bytes(b"abc")
        artifact.chmod(0o700)
        (self.root / "link").symlink_to("artifact")
        with patch("tools.source_identity.command", return_value="missing\nlink\nartifact\nlink"):
            expected = hashlib.sha256(b"artifactabcexecutablelinksymlink:artifactmissingmissing")
            self.assertEqual(tree_digest(self.root), expected.hexdigest())
            artifact.chmod(0o600)
            self.assertNotEqual(tree_digest(self.root), expected.hexdigest())
        self.assertEqual(file_digest(artifact), hashlib.sha256(b"abc").hexdigest())

    def test_metadata_retains_caller_encoding_and_existing_default(self) -> None:
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            self.assertEqual(read_metadata(db, "key", "missing"), "missing")
            value = {"z": "caf\u00e9", "a": 1}
            write_metadata(db, "key", value, encode=canonical_json)
            (ascii_value,) = db.execute("SELECT value FROM metadata").fetchone()
            self.assertEqual(ascii_value, '{"a":1,"z":"caf\\u00e9"}')
            write_metadata(db, "key", value, encode=recorded_json)
            (unicode_value,) = db.execute("SELECT value FROM metadata").fetchone()
            self.assertEqual(unicode_value, '{"z":"caf\u00e9","a":1}')
            self.assertEqual(read_metadata(db, "key"), value)

    def test_iso_clock_uses_the_utc_datetime_owner(self) -> None:
        self.assertEqual(now_utc().utcoffset(), AT.utcoffset())
        with patch("papertrade.clock.now_utc", return_value=AT):
            self.assertEqual(utc_iso(), "2026-11-10T15:00:15+00:00")

    def test_required_metadata_distinguishes_a_missing_row_from_json_null(self) -> None:
        from tools.run_week_experiment import metadata

        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            with self.assertRaisesRegex(ValueError, "Missing experiment metadata: absent"):
                metadata(db, "absent")
            write_metadata(db, "null", None, encode=canonical_json)
            self.assertIsNone(metadata(db, "null"))
