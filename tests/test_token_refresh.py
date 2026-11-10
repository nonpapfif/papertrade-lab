"""Parallel token rotation, dispatch ordering, and bounded authentication recovery."""

from __future__ import annotations

import hashlib
import io
import json
import tempfile
import time
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from email.message import Message
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import Mock, PropertyMock, patch

from papertrade.api import ApiError, MarketClient, private_json
from papertrade.core import Settings
from tools.download_market_day import Blocked
from tools.download_market_month import GatedClient, RateGate


class Response(io.BytesIO):
    status = 200
    headers: ClassVar[dict[str, str]] = {}


class TokenRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.directory = self.root / "secrets"
        self.directory.mkdir(mode=0o700)
        self.settings = Settings(self.root, {"limits": {"minimum_request_interval": 0}})
        for name, value in (
            ("TOSS_CLIENT_ID", "fixture-id"),
            ("TOSS_CLIENT_SECRET", "fixture-secret"),
        ):
            path = self.directory / name
            path.write_text(value)
            path.chmod(0o600)
        location = patch.object(
            Settings, "secrets_dir", new_callable=PropertyMock, return_value=self.directory
        )
        location.start()
        self.addCleanup(location.stop)

    def cache(self, token: str, expires_at: float | None = None) -> None:
        private_json(
            self.directory / "TOSS_ACCESS_TOKEN.json",
            {
                "access_token": token,
                "expires_at": expires_at if expires_at is not None else time.time() + 3600,
                "client_digest": hashlib.sha256(b"fixture-id").hexdigest(),
            },
        )

    def client(self) -> tuple[GatedClient, RateGate, list[dict[str, Any]], Mock]:
        gate = RateGate(10)
        records: list[dict[str, Any]] = []

        def audit(record: dict[str, Any]) -> None:
            records.append(record)
            gate.observe(record)

        client = GatedClient(self.settings, gate, audit)
        opener = Mock()
        client.opener = opener
        return client, gate, records, opener

    @staticmethod
    def rejected(code: str, status: int = 401) -> urllib.error.HTTPError:
        return urllib.error.HTTPError(
            "https://fixture.invalid",
            status,
            "Rejected",
            Message(),
            io.BytesIO(json.dumps({"error": {"code": code}}).encode()),
        )

    def test_valid_memory_token_adopts_newer_shared_cache_without_issuance(self) -> None:
        client = MarketClient(self.settings)
        client.token, client.expires_at = "old", time.time() + 3600
        self.cache("new")
        with patch.object(client, "_request") as issue:
            self.assertEqual(client.access_token(), "new")
        issue.assert_not_called()

    def test_eight_workers_issue_one_token_when_shared_cache_expires(self) -> None:
        self.cache("expired", time.time() - 1)
        clients = [MarketClient(self.settings) for _ in range(8)]

        def issued(*args: Any, **kwargs: Any) -> dict[str, Any]:
            time.sleep(0.01)
            return {"access_token": "fresh", "expires_in": 3600}

        with (
            patch.object(MarketClient, "_request", side_effect=issued) as issue,
            ThreadPoolExecutor(max_workers=8) as pool,
        ):
            tokens = list(pool.map(lambda client: client.access_token(), clients))
        self.assertEqual(tokens, ["fresh"] * 8)
        issue.assert_called_once()

    def test_token_rotated_while_waiting_is_read_after_dispatch_reservation(self) -> None:
        self.cache("old")
        client, gate, _, opener = self.client()
        client.token, client.expires_at = "old", time.time() + 3600

        def reserved(group: str) -> None:
            self.assertEqual(group, "MARKET_DATA_CHART")
            self.cache("new")

        def opened(request: Any, timeout: float) -> Response:
            self.assertEqual(request.get_header("Authorization"), "Bearer new")
            return Response(b'{"result":{"candles":[]}}')

        opener.open.side_effect = opened
        with patch.object(gate, "reserve", side_effect=reserved):
            self.assertEqual(client.get("/api/v1/candles", symbol="TEST"), {"candles": []})
        self.assertEqual(opener.open.call_count, 1)

    def test_inflight_old_token_recovers_from_cache_without_deleting_replacement(self) -> None:
        self.cache("old")
        client, gate, records, opener = self.client()
        seen = []

        def opened(request: Any, timeout: float) -> Response:
            self.assertEqual(request.get_method(), "GET")
            seen.append(request.get_header("Authorization"))
            if len(seen) == 1:
                self.cache("new")
                raise self.rejected("token-revoked")
            return Response(b'{"result":{"candles":[]}}')

        opener.open.side_effect = opened
        self.assertEqual(client.get("/api/v1/candles", symbol="TEST"), {"candles": []})
        self.assertEqual(seen, ["Bearer old", "Bearer new"])
        self.assertEqual([r["status"] for r in records], [401, 200])
        self.assertIsNone(gate.blocked)
        cached = json.loads((self.directory / "TOSS_ACCESS_TOKEN.json").read_text())
        self.assertEqual(cached["access_token"], "new")

    def test_failed_authentication_retry_blocks_every_worker(self) -> None:
        client, gate, records, opener = self.client()
        opener.open.side_effect = [
            self.rejected("token-revoked"),
            self.rejected("token-revoked"),
        ]
        with (
            patch.object(client, "access_token", return_value="fixture"),
            patch.object(client, "_invalidate_token"),
            self.assertRaises(ApiError) as failure,
        ):
            client.get("/api/v1/candles", symbol="TEST")
        self.assertEqual(failure.exception.status, 401)
        self.assertEqual([r["attempt"] for r in records], [1, 2])
        self.assertEqual(opener.open.call_count, 2)
        for group in ("MARKET_DATA_CHART", "AUTH", "MARKET_INFO"):
            with self.assertRaises(Blocked):
                gate.reserve(group)

    def test_other_access_rejections_still_stop_without_extra_http_requests(self) -> None:
        for code, status in (("login-user-not-found", 401), ("forbidden", 403)):
            with self.subTest(code=code):
                client, gate, _, opener = self.client()
                opener.open.side_effect = self.rejected(code, status)
                with (
                    patch.object(client, "access_token", return_value="fixture"),
                    patch.object(client, "_invalidate_token"),
                    self.assertRaises((ApiError, Blocked)),
                ):
                    client.get("/api/v1/candles", symbol="TEST")
                self.assertEqual(opener.open.call_count, 1)
                self.assertIsNotNone(gate.blocked)


if __name__ == "__main__":
    unittest.main()
