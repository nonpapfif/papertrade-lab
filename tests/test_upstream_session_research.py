import importlib.util
import json
import socket
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from papertrade.research.service import CodexModel, ResearchError, ResearchResponseError
from papertrade.research.upstream_session import (
    _daily_evidence,
    native_service,
    validate_native_source,
)

TRADE_DATE = "2026-11-19"


def snapshot(*, bars: bool = True) -> dict[str, Any]:
    value = {
        "market": "US",
        "symbol": "TEST",
        "as_of": "2026-11-19T21:15:00+09:00",
        "last": 132.0,
        "daily_scan_hit": {
            "symbol": "TEST",
            "close_date": "2026-11-18",
            "price": 132.0,
            "rsi": 62.2,
            "volume": 54321,
            "setups": ["breakout_20d"],
        },
    }
    if bars:
        dates = [date(2026, 9, 22) + timedelta(days=delta) for delta in range(58)]
        value["daily_bars"] = [
            {
                "date": day.isoformat(),
                "open": 98 + index,
                "high": 101 + index,
                "low": 97 + index,
                "close": 100 + index,
                "volume": 10000 + index,
            }
            for index, day in enumerate(day for day in dates if day.weekday() < 5)
        ]
        value["daily_bar_provenance"] = {
            "source": "fixture captured completed US daily OHLCV",
            "received_at": "2026-11-19T11:59:00+00:00",
        }
    return value


class DailyEvidenceTests(unittest.TestCase):
    def test_no_history_exposes_only_observed_close(self) -> None:
        evidence = _daily_evidence(snapshot(bars=False))
        self.assertFalse(evidence.full_ohlcv)
        self.assertEqual(evidence.rows, ({"date": "2026-11-18", "close": 132.0},))
        self.assertEqual(evidence.scan_rsi, 62.2)

    def test_rejects_current_daily_rows_and_late_history_receipts(self) -> None:
        late = snapshot()
        late["daily_bar_provenance"]["received_at"] = "2026-11-19T12:15:01+00:00"
        with self.assertRaisesRegex(ValueError, "after the research cutoff"):
            _daily_evidence(late)
        future = snapshot()
        future["daily_bars"][-1]["date"] = TRADE_DATE
        with self.assertRaisesRegex(ValueError, "current, future, or duplicate"):
            _daily_evidence(future)
        same_day_scan = snapshot(bars=False)
        same_day_scan["daily_scan_hit"]["close_date"] = TRADE_DATE
        with self.assertRaisesRegex(ValueError, "must precede"):
            _daily_evidence(same_day_scan)

    def test_pin_rejects_different_installed_revision(self) -> None:
        provenance = {
            "url": "https://github.com/TauricResearch/TradingAgents.git",
            "vcs_info": {"vcs": "git", "commit_id": "wrong"},
        }
        distribution = SimpleNamespace(
            read_text=lambda _: json.dumps(provenance),
            locate_file=lambda location: Path(location),
        )
        with (
            patch(
                "papertrade.research.upstream_session.metadata.distribution",
                return_value=distribution,
            ),
            self.assertRaisesRegex(ResearchError, "Install the pinned"),
        ):
            validate_native_source()


class OfflineTransport:
    """Exercise native model binding using source role prompts and simulated replies."""

    def __init__(
        self,
        *,
        invalid_args: bool = False,
        omitted_tools: tuple[str, ...] = (),
    ) -> None:
        self.invalid_args = invalid_args
        self.omitted_tools = omitted_tools
        self.market_system: list[str] = []
        self.market_tool_messages: dict[str, str] = {}
        self.portfolio_prompt = ""
        self.other_prompts: list[str] = []

    def invoke(self, model: CodexModel, prompt: Any) -> Any:
        schema = model.schema
        if schema is None:
            self.other_prompts.append(prompt)
            return SimpleNamespace(content="Evidence-based native debate response")
        if schema.__name__ == "NativeToolReply":
            request = json.loads(prompt)
            self.market_system.append(request["messages"][0]["content"])
            history = {m["name"]: m["content"] for m in request["messages"] if m["type"] == "tool"}
            self.market_tool_messages.update(history)
            future_override = "2026-12-19"
            requests = {
                "get_stock_data": {"start_date": "2026-10-01", "end_date": future_override},
                "get_indicators": {"indicator": "rsi,atr", "curr_date": future_override},
                "get_verified_market_snapshot": {"curr_date": future_override},
            }
            if self.invalid_args:
                requests["get_stock_data"]["trade_date"] = future_override
            remaining = [
                name for name in requests if name not in history and name not in self.omitted_tools
            ]
            self.assert_tools(request, requests)
            response = (
                []
                if not remaining
                else [
                    {
                        "name": remaining[0],
                        "arguments_json": json.dumps(requests[remaining[0]]),
                    }
                ]
            )
            return schema.model_validate(
                {
                    "text": "Native market report on captured observations" if not response else "",
                    "requests": response,
                }
            )
        if schema.__name__ == "PortfolioDecision":
            self.portfolio_prompt = prompt
        outputs = {
            "ResearchPlan": {
                "recommendation": "Hold",
                "rationale": "Research manager rationale",
                "strategic_actions": "Wait",
            },
            "TraderProposal": {"action": "Hold", "reasoning": "Trader rationale"},
            "PortfolioDecision": {
                "rating": "Overweight",
                "executive_summary": "Portfolio manager decision",
                "investment_thesis": "Weighs the native analysis and risk discussion",
            },
        }
        return schema.model_validate(outputs[schema.__name__])

    @staticmethod
    def assert_tools(request: dict, expected: dict) -> None:
        if set(request["graph_tools"]) != set(expected):
            raise AssertionError("Model saw an unexpected tool list")
        properties = request["graph_tools"]["get_stock_data"]["properties"]
        if set(properties) != {"start_date", "end_date"}:
            raise AssertionError("Model saw an injected graph argument")


@unittest.skipUnless(importlib.util.find_spec("tradingagents"), "Install the pinned research extra")
class NativeGraphTests(unittest.IsolatedAsyncioTestCase):
    async def test_original_roles_and_actual_tool_graph_preserve_portfolio_rating(self) -> None:
        transport = OfflineTransport()
        with tempfile.TemporaryDirectory() as temporary:
            service = native_service(Path(temporary), executable="codex", protected_paths=())
            with (
                patch.object(CodexModel, "invoke", autospec=True, side_effect=transport.invoke),
                patch.object(socket.socket, "connect", side_effect=AssertionError("network")),
            ):
                result = await service.analyze(snapshot())
                self.assertEqual(result, await service.analyze(snapshot()))
            await service.aclose()
            manifest = json.loads((result.directory / "manifest.json").read_text())
            evidence = json.loads((result.directory / "evidence.json").read_text())
            self.assertEqual(evidence["selected_analysts"], ["market"])
            self.assertEqual(result.rating, "Overweight")
            self.assertEqual(
                result.executed_nodes,
                (
                    "Market Analyst",
                    "Bull Researcher",
                    "Bear Researcher",
                    "Research Manager",
                    "Trader",
                    "Aggressive Analyst",
                    "Conservative Analyst",
                    "Neutral Analyst",
                    "Portfolio Manager",
                ),
            )
            self.assertEqual(manifest["reports"]["market-analyst"], "completed")
            for slug in ("sentiment-analyst", "news-analyst", "fundamentals-analyst"):
                self.assertEqual(manifest["reports"][slug], "skipped")
            self.assertIn("select the **most relevant indicators**", transport.market_system[0])
            self.assertIn(
                "As the Portfolio Manager, synthesize the risk analysts' debate",
                transport.portfolio_prompt,
            )
            self.assertTrue(any("Bull Analyst" in prompt for prompt in transport.other_prompts))
            tools = transport.market_tool_messages
            self.assertIn("2026-11-18,", tools["get_stock_data"])
            self.assertNotIn("2026-11-19,", tools["get_stock_data"])
            verified = tools["get_verified_market_snapshot"]
            self.assertIn("Latest trading row used: 2026-11-18", verified)
            self.assertIn("### Verified technical indicators (latest row)", verified)
            self.assertIn("| atr | 4.00 |", verified)
            self.assertNotIn("54321", verified)

    async def test_injected_dates_cannot_be_set_by_the_model(self) -> None:
        transport = OfflineTransport(invalid_args=True)
        with tempfile.TemporaryDirectory() as temporary:
            service = native_service(Path(temporary), executable="codex", protected_paths=())
            with (
                patch.object(CodexModel, "invoke", autospec=True, side_effect=transport.invoke),
                self.assertRaisesRegex(ResearchResponseError, "Invalid TradingAgents"),
            ):
                await service.analyze(snapshot())
            await service.aclose()
            (manifest_path,) = (Path(temporary) / "failed").glob("*/manifest.json")
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["status"], "failed")
            self.assertIsNone(manifest["rating"])
            self.assertEqual(transport.market_tool_messages, {})

    async def test_close_only_source_can_complete_without_discretionary_indicators(self) -> None:
        transport = OfflineTransport(omitted_tools=("get_indicators",))
        with tempfile.TemporaryDirectory() as temporary:
            service = native_service(Path(temporary), executable="codex", protected_paths=())
            with patch.object(CodexModel, "invoke", autospec=True, side_effect=transport.invoke):
                result = await service.analyze(snapshot(bars=False))
            await service.aclose()
            self.assertEqual(result.rating, "Overweight")
            verification = transport.market_tool_messages["get_verified_market_snapshot"]
            self.assertIn("O/H/L/Volume unavailable", verification)
            self.assertIn("Scanner-reported Wilder RSI: 62.2", verification)
            self.assertNotIn("54321", verification)

    async def test_missing_native_verification_leaves_research_incomplete(self) -> None:
        transport = OfflineTransport(omitted_tools=("get_verified_market_snapshot",))
        with tempfile.TemporaryDirectory() as temporary:
            service = native_service(Path(temporary), executable="codex", protected_paths=())
            with (
                patch.object(CodexModel, "invoke", autospec=True, side_effect=transport.invoke),
                self.assertRaisesRegex(
                    ResearchError, "did not retrieve the native verified snapshot"
                ),
            ):
                await service.analyze(snapshot())
            await service.aclose()
            (manifest_path,) = (Path(temporary) / "failed").glob("*/manifest.json")
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual(manifest["reports"]["market-analyst"], "failed")
            self.assertEqual(manifest["reports"]["portfolio-manager"], "not_run")
