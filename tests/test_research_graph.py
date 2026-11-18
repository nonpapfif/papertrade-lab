"""Check the internal graph adapter offline; the real graph test needs its extra."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import socket
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from unittest.mock import patch

from papertrade.research.reporting import ROLES
from papertrade.research.service import (
    DEBATE_ROLES,
    UPSTREAM_REVISION,
    CodexModel,
    CodexSettings,
    ResearchActionError,
    ResearchChildError,
    ResearchClosed,
    ResearchResponseError,
    ResearchService,
    ResearchTimeoutError,
    ResearchUnavailableError,
    _codex_command,
    _parse_codex_response,
)
from tools.session_research import Candidate, SessionResearch, _iso, toss_service

_SNAPSHOT = {
    "market": "US",
    "symbol": "AAPL",
    "as_of": _iso(100),
    "candles": [{"timestamp": _iso(90), "closePrice": "345.0"}],
}


def graph_result(model: Any, evidence: dict[str, Any], atr: str, progress: Any) -> Any:
    progress.updates["Market Analyst"] = {"market_report": "Offline market report"}
    for title, slug, keys in DEBATE_ROLES.values():
        value: Any = "Offline role report"
        for key in reversed(keys):
            value = {key: value}
        progress.updates[title] = value
    return {"final_rating": "Hold"}, ("Market Analyst", "Portfolio Manager")


class GraphAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_base_import_and_service_factory_need_no_optional_graph_package(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            service = toss_service(Path(temporary))
            assert isinstance(service, ResearchService)
            protected = service._codex_settings.protected_paths
            self.assertIn(Path.home() / ".config/secrets", protected)
            self.assertIn(Path.home() / ".aws", protected)
            await service.aclose()

    async def test_offline_role_publication_is_cached_and_verifies_through_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = ResearchService(root / "reports", model=object())
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            with patch("papertrade.research.service._run_graph", side_effect=graph_result) as run:
                await adapter.submit([Candidate(_SNAPSHOT)], _iso(105))
                await adapter.drain()
                first = await service.analyze(_SNAPSHOT)
                self.assertEqual(run.call_count, 1)
            self.assertEqual(len(first.reports), 12)
            manifest = json.loads((first.directory / "manifest.json").read_text())
            self.assertEqual(manifest["tradingagents_revision"], UPSTREAM_REVISION)
            self.assertEqual(manifest["reports"]["news-analyst"], "skipped")
            self.assertEqual(adapter.metadata()["attempts"], {"completed": 1})
            for name in ("failure_reason", "exit_code", "failed_roles"):
                self.assertNotIn(name, adapter.publications()[0])
            self.assertFalse(adapter.selection_at(120))
            await adapter.aclose()

    async def test_late_role_failure_publishes_partial_provenance_without_admission(self) -> None:
        def failed(model: Any, evidence: dict[str, Any], atr: str, progress: Any) -> Any:
            progress.updates["Market Analyst"] = {"market_report": "Observed price only"}
            progress.failed.add("Trader")
            raise ResearchChildError(17)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = ResearchService(root / "reports", model=object())
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            with patch("papertrade.research.service._run_graph", side_effect=failed):
                await adapter.submit([Candidate(_SNAPSHOT)], _iso(105))
                await adapter.drain()
            row = adapter.publications()[0]
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["failure_reason"], "child_nonzero")
            self.assertEqual(row["exit_code"], 17)
            self.assertEqual(row["failed_roles"], ["trader"])
            self.assertEqual(adapter.metadata()["errors"][0]["failure_reason"], "child_nonzero")
            self.assertEqual(row["partial_reports"][0]["roles"]["market-analyst"], "completed")
            self.assertEqual(row["partial_reports"][0]["roles"]["trader"], "failed")
            self.assertFalse(adapter.selection_at(120))
            await adapter.aclose()

    async def test_failure_journal_only_keeps_allowlisted_reasons_and_safe_exit_codes(self) -> None:
        failures: list[tuple[Exception, str | None]] = [
            (ResearchUnavailableError("PRIVATE"), "cli_unavailable"),
            (ResearchChildError(17), "child_nonzero"),
            (ResearchTimeoutError("PRIVATE"), "timeout"),
            (ResearchResponseError("PRIVATE"), "invalid_response"),
            (ResearchActionError("PRIVATE"), "prohibited_action"),
        ]

        class UntrustedFailure(RuntimeError):
            reason: ClassVar[list[str]] = ["PRIVATE"]

        failures.append((UntrustedFailure("PRIVATE"), None))
        for failure, expected in failures:
            with self.subTest(reason=expected), tempfile.TemporaryDirectory() as temporary:

                class FailedService:
                    def __init__(self, error: Exception) -> None:
                        self.error = error

                    async def analyze(self, snapshot: Any, inputs: Any = None) -> Any:
                        raise self.error

                    async def aclose(self) -> None:
                        return None

                adapter = SessionResearch(
                    Path(temporary), FailedService(failure), expires_at=_iso(200), clock=lambda: 120
                )
                await adapter.submit([Candidate(_SNAPSHOT)], _iso(105))
                await adapter.drain()
                row = adapter.publications()[0]
                self.assertEqual(row.get("failure_reason"), expected)
                if expected == "child_nonzero":
                    self.assertEqual(row["exit_code"], 17)
                else:
                    self.assertNotIn("exit_code", row)
                self.assertNotIn("PRIVATE", json.dumps(row))
                self.assertNotIn("PRIVATE", json.dumps(adapter.metadata()))
                await adapter.aclose()

    async def test_close_without_finished_roles_retains_an_explicit_empty_partial(self) -> None:
        entered = threading.Event()

        def interrupted(model: Any, evidence: Any, atr: str, progress: Any) -> Any:
            entered.set()
            if not model.stopped.wait(2):
                raise AssertionError("Service did not stop its research model")
            raise ResearchClosed("Research session closed")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = ResearchService(root / "reports")
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            with patch("papertrade.research.service._run_graph", side_effect=interrupted):
                await adapter.submit([Candidate(_SNAPSHOT)], _iso(105))
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                await adapter.aclose()
            row = adapter.publications()[0]
            self.assertEqual(row["status"], "interrupted")
            self.assertNotIn("completed", row["partial_reports"][0]["roles"].values())
            self.assertEqual(row["partial_reports"][0]["roles"]["market-analyst"], "not_run")
            self.assertFalse(adapter.selection_at(120))

    async def test_close_keeps_completed_roles_as_interrupted_partial_reports(self) -> None:
        entered = threading.Event()

        def interrupted(model: Any, evidence: dict[str, Any], atr: str, progress: Any) -> Any:
            progress.updates["Market Analyst"] = {"market_report": "Completed market evidence"}
            entered.set()
            if not model.stopped.wait(2):
                raise AssertionError("Service did not stop its research model")
            raise ResearchClosed("Research session closed")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            service = ResearchService(root / "reports")
            adapter = SessionResearch(
                root / "state", service, expires_at=_iso(200), clock=lambda: 120
            )
            with patch("papertrade.research.service._run_graph", side_effect=interrupted):
                await adapter.submit([Candidate(_SNAPSHOT)], _iso(105))
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                await adapter.aclose()
            row = adapter.publications()[0]
            self.assertEqual(row["status"], "interrupted")
            self.assertNotIn("report", row)
            partial = row["partial_reports"][0]
            self.assertEqual(partial["roles"]["market-analyst"], "completed")
            self.assertEqual(partial["roles"]["portfolio-manager"], "not_run")
            manifest = json.loads(
                (adapter.output_dir / partial["bundle"] / "manifest.json").read_text()
            )
            self.assertIsNone(manifest["rating"])
            self.assertFalse(adapter.selection_at(120))


class CodexIsolationTests(unittest.TestCase):
    def test_codex_command_disables_tools_and_denies_protected_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            settings = CodexSettings(executable="/usr/bin/true", protected_paths=(root,))
            command = _codex_command(settings, root, root / "schema.json")
            self.assertIn("--ignore-user-config", command)
            self.assertIn("--ignore-rules", command)
            self.assertIn("mcp_servers={}", command)
            self.assertIn('web_search="disabled"', command)
            self.assertIn("shell_tool", command)
            self.assertIn("multi_agent", command)
            self.assertIn("(deny process-exec)", command[2])
            self.assertIn(str(root.resolve()), command[2])

    def test_missing_cli_has_safe_typed_reason(self) -> None:
        with (
            patch("papertrade.research.service.shutil.which", return_value=None),
            self.assertRaises(ResearchUnavailableError) as caught,
        ):
            _codex_command(CodexSettings(), Path("."), Path("schema.json"))
        self.assertEqual(caught.exception.reason, "cli_unavailable")

    def test_timeout_has_safe_typed_reason(self) -> None:
        process = SimpleNamespace(pid=12345, communicate=lambda: (b"", b"private stderr"))
        model = CodexModel(CodexSettings(timeout=1))
        with (
            patch("papertrade.research.service.time.monotonic", side_effect=[0, 2]),
            patch("papertrade.research.service.os.killpg"),
            self.assertRaises(ResearchTimeoutError) as caught,
        ):
            model._communicate(cast(subprocess.Popen[bytes], process), b"private prompt")
        self.assertEqual(caught.exception.reason, "timeout")
        self.assertNotIn("private", str(caught.exception))

    def test_nonzero_exit_is_checked_before_parsing_untrusted_output(self) -> None:
        process = SimpleNamespace(returncode=-9)
        with (
            patch("papertrade.research.service._codex_command", return_value=["offline"]),
            patch("papertrade.research.service.subprocess.Popen") as popen,
            patch.object(CodexModel, "_communicate", return_value=b"private invalid response"),
            patch("papertrade.research.service._parse_codex_response") as parse,
        ):
            popen.return_value.__enter__.return_value = process
            with self.assertRaises(ResearchChildError) as caught:
                CodexModel(CodexSettings()).invoke("private prompt")
            parse.assert_not_called()
        self.assertEqual(caught.exception.reason, "child_nonzero")
        self.assertEqual(caught.exception.exit_code, -9)
        self.assertNotIn("private", str(caught.exception))

    def test_invalid_response_has_safe_typed_reason(self) -> None:
        with self.assertRaises(ResearchResponseError) as caught:
            _parse_codex_response("private invalid response")
        self.assertEqual(caught.exception.reason, "invalid_response")
        self.assertNotIn("private", str(caught.exception))

    def test_action_outputs_are_rejected(self) -> None:
        events = [
            {"type": "turn.started"},
            {"type": "item.completed", "item": {"type": "command_execution"}},
            {"type": "turn.completed"},
        ]
        with self.assertRaises(ResearchActionError) as caught:
            _parse_codex_response("\n".join(json.dumps(event) for event in events))
        self.assertEqual(caught.exception.reason, "prohibited_action")

    def test_subprocess_environment_excludes_provider_secrets_and_stderr(self) -> None:
        response = "\n".join(
            json.dumps(event)
            for event in [
                {"type": "turn.started"},
                {
                    "type": "item.completed",
                    "item": {
                        "type": "agent_message",
                        "text": '{"text":"Offline report"}',
                    },
                },
                {"type": "turn.completed"},
            ]
        ).encode()
        process = SimpleNamespace(returncode=0)
        # MagicMock supplies context-manager protocol without launching a subprocess.
        with (
            patch.dict(os.environ, {"TOSS_CLIENT_SECRET": "never-forward"}),
            patch(
                "papertrade.research.service._codex_command",
                return_value=["offline"],
            ),
            patch("papertrade.research.service.subprocess.Popen") as popen,
            patch.object(
                CodexModel,
                "_communicate",
                return_value=response,
            ),
        ):
            popen.return_value.__enter__.return_value = process
            model = CodexModel(CodexSettings())
            self.assertEqual(model.invoke("Observed prices").content, "Offline report")
            self.assertNotIn("TOSS_CLIENT_SECRET", popen.call_args.kwargs["env"])
            self.assertEqual(popen.call_args.kwargs["stderr"], -3)


class OfflineModel:
    """Answer upstream schemas deterministically without a provider connection."""

    def __init__(self) -> None:
        self.calls = 0

    def with_structured_output(self, schema: Any) -> Any:
        return SimpleNamespace(invoke=lambda prompt: self._invoke(prompt, schema))

    def invoke(self, prompt: Any) -> Any:
        return self._invoke(prompt, None)

    def _invoke(self, prompt: Any, schema: Any) -> Any:
        self.calls += 1
        if schema is None:
            return SimpleNamespace(content="Offline research report")
        results = {
            "SentimentReport": {
                "overall_band": "Mixed",
                "overall_score": 5,
                "confidence": "low",
                "narrative": "One supplied source",
            },
            "ResearchPlan": {
                "recommendation": "Hold",
                "rationale": "Observed evidence",
                "strategic_actions": "Wait",
            },
            "TraderProposal": {"action": "Hold", "reasoning": "Observed evidence"},
            "PortfolioDecision": {
                "rating": "Hold",
                "executive_summary": "Paper research",
                "investment_thesis": "Supplied observations",
            },
        }
        return schema.model_validate(results[schema.__name__])


@unittest.skipUnless(importlib.util.find_spec("tradingagents"), "Install the research extra")
class PinnedGraphTests(unittest.IsolatedAsyncioTestCase):
    async def test_actual_pinned_graph_produces_twelve_roles_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            model = OfflineModel()
            service = ResearchService(Path(temporary), model=model)
            with patch.object(socket.socket, "connect", side_effect=AssertionError("network")):
                result = await service.analyze(_SNAPSHOT)
                cached = await service.analyze(_SNAPSHOT)
            self.assertEqual(result, cached)
            self.assertEqual(set(result.reports), {slug for slug, _ in ROLES})
            self.assertEqual(len(result.executed_nodes), 12)
            self.assertEqual(model.calls, 9)
            self.assertEqual(result.rating, "Hold")
            await service.aclose()

    async def test_optional_analysts_use_only_supplied_point_in_time_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            model = OfflineModel()
            service = ResearchService(Path(temporary), model=model)
            records = [
                {
                    "source": "Offline fixture",
                    "published_at": _iso(95),
                    "received_at": _iso(99),
                    "data": {"fact": "Supplied observation"},
                }
            ]
            inputs = {group: records for group in ("news", "sentiment", "fundamentals")}
            with patch.object(socket.socket, "connect", side_effect=AssertionError("network")):
                result = await service.analyze(_SNAPSHOT, inputs)
            manifest = json.loads((result.directory / "manifest.json").read_text())
            self.assertTrue(all(status == "completed" for status in manifest["reports"].values()))
            self.assertEqual(model.calls, 12)
            await service.aclose()
