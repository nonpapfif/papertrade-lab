"""Exercise the finite probe against a local server without broker credentials."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


@unittest.skipUnless(
    importlib.util.find_spec("websockets"), "Optional WebSocket dependency required"
)
class LocalProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, malformed: bool) -> dict:
        from websockets.asyncio.server import ServerConnection, serve

        from tools.probe_toss_websocket import run

        async def handler(ws: ServerConnection) -> None:
            async for raw in ws:
                if raw == "PING":
                    await ws.send(json.dumps({"type": "pong"}))
                    continue
                declaration = json.loads(raw)
                topics = [f"{r['type']}:{s}" for r in declaration for s in r["codes"]]
                await ws.send(
                    json.dumps({"type": "subscriptions", "subscribed": topics, "rejected": []})
                )
                if malformed:
                    await ws.send(
                        json.dumps({"type": "message", "topic": "personal:order:1", "data": {}})
                    )
                else:
                    for topic in topics:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "message",
                                    "topic": topic,
                                    "data": {"timestamp": "2026-11-03T00:00:00Z"},
                                }
                            )
                        )

        client = MagicMock()
        client.access_token.return_value = "local-test-token"
        client.calendar.return_value = {}
        client.stocks.return_value = []
        client.prices.return_value = []
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "probe"
            async with serve(handler, "127.0.0.1", 0) as server:
                port = next(iter(server.sockets)).getsockname()[1]
                with (
                    patch("tools.probe_toss_websocket.URL", f"ws://127.0.0.1:{port}"),
                    patch("tools.probe_toss_websocket.CachedTokenClient", return_value=client),
                    patch(
                        "tools.probe_toss_websocket.read_spec",
                        return_value=b'{"info":{"version":"local"}}',
                    ),
                ):
                    if malformed:
                        with self.assertRaises(SystemExit):
                            await run(output, "us", ["AAPL"], 4)
                    else:
                        await run(output, "us", ["AAPL"], 4)
            return json.loads((output / "status.json").read_text())

    async def test_two_normal_connections_restore_declared_channels(self) -> None:
        result = await self.exercise(False)
        self.assertEqual(result["connections_completed"], 2)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["result"], "inconclusive")
        self.assertTrue(result["market_messages_seen_on_every_topic_per_connection"])

    async def test_forbidden_topic_fails_before_market_recording(self) -> None:
        result = await self.exercise(True)
        self.assertEqual(result["state"], "failed")
        self.assertEqual(result["error_type"], "ValueError")
