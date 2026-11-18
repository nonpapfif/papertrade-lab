"""Publish complete role bundles; adapted from the user's TossTradingLab reporting module."""

from __future__ import annotations

import errno
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROLES = (
    ("market-analyst", "Market Analyst"),
    ("sentiment-analyst", "Sentiment Analyst"),
    ("news-analyst", "News Analyst"),
    ("fundamentals-analyst", "Fundamentals Analyst"),
    ("bull-researcher", "Bull Researcher"),
    ("bear-researcher", "Bear Researcher"),
    ("research-manager", "Research Manager"),
    ("trader", "Trader"),
    ("aggressive-analyst", "Aggressive Analyst"),
    ("conservative-analyst", "Conservative Analyst"),
    ("neutral-analyst", "Neutral Analyst"),
    ("portfolio-manager", "Portfolio Manager"),
)


@dataclass(frozen=True)
class ResearchResult:
    run_id: str
    directory: Path
    rating: str | None
    executed_nodes: tuple[str, ...]
    reports: dict[str, Path]


def json_text(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )


# Model text may be rendered by a Markdown viewer. Let readers inspect links and HTML
# as text without loading resources. The report index creates its own local links.
_MARKDOWN_RESOURCE = re.compile(r"!\[|\]\s*\(|\]\s*\[|^\s*\[[^\]\n]+\]:", re.MULTILINE)
_BARE_URL = re.compile(r"(?i)\b(?:https?|ftp)://|\bwww\.")
_CONTROLS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def safe_markdown(value: str) -> str:
    value = _CONTROLS.sub("", value).replace("&", "&amp;")
    value = value.replace("<", "&lt;").replace(">", "&gt;")
    value = _MARKDOWN_RESOURCE.sub(
        lambda match: match[0].replace("[", "&#91;").replace("]", "&#93;"), value
    )
    return _BARE_URL.sub(lambda match: match[0].replace(":", "&#58;").replace(".", "&#46;"), value)


def read_result(directory: Path) -> ResearchResult | None:
    manifest_path = directory / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    reports = {slug: directory / f"{slug}.md" for slug, _ in ROLES}
    if (
        manifest["run_id"] != directory.name
        or set(manifest["reports"]) != reports.keys()
        or any(not path.is_file() for path in reports.values())
        or not (directory / "evidence.json").is_file()
        or not (directory / "report.md").is_file()
    ):
        raise ValueError("Research bundle is incomplete")
    return ResearchResult(
        directory.name,
        directory,
        manifest["rating"],
        tuple(manifest["executed_nodes"]),
        reports,
    )


def publish_reports(
    directory: Path,
    evidence: dict,
    revision: str,
    model: str,
    rating: str | None,
    executed_nodes: tuple[str, ...],
    reports: dict[str, tuple[str, str]],
    atr_summary: str,
    *,
    failure: str | None = None,
) -> ResearchResult:
    """Commit all files together, so readers only see a finished bundle."""
    directory.parent.mkdir(parents=True, exist_ok=True)
    snapshot = evidence["snapshot"]
    with tempfile.TemporaryDirectory(prefix=".research-", dir=directory.parent) as temporary:
        staging = Path(temporary)
        source_context = (
            f"- Instrument: {safe_markdown(snapshot['market'])} "
            f"{safe_markdown(snapshot['symbol'])}\n"
            f"- Evidence cutoff: {safe_markdown(snapshot['as_of'])}\n"
            f"- ATR provenance: {safe_markdown(atr_summary)}\n"
            "- Scope: research and paper simulation only\n"
        )
        index = [
            "# TradingAgents research\n",
            source_context,
            f"- TradingAgents revision: `{revision}`",
            f"- Model: {safe_markdown(model)}",
        ]
        if failure is not None:
            index.append(f"- Research status: failed ({safe_markdown(failure)})\n")
        elif rating is not None:
            index.append(f"- Research rating: {safe_markdown(rating)}\n")
        index.append("## Role reports\n")
        for slug, title in ROLES:
            status, body = reports[slug]
            role = f"# {title}\n\n{source_context}- Status: {status}\n\n"
            (staging / f"{slug}.md").write_text(
                role + safe_markdown(body).strip() + "\n", encoding="utf-8"
            )
            index.append(f"- [{title}]({slug}.md) · {status}")
        (staging / "report.md").write_text("\n".join(index) + "\n", encoding="utf-8")
        (staging / "evidence.json").write_text(
            json_text(evidence, pretty=True) + "\n", encoding="utf-8"
        )
        manifest = {
            "run_id": directory.name,
            "tradingagents_revision": revision,
            "model": model,
            "rating": rating,
            "status": "failed" if failure is not None else "completed",
            "executed_nodes": executed_nodes,
            "reports": {slug: status for slug, (status, _) in reports.items()},
        }
        if failure is not None:
            manifest["error_type"] = failure
        (staging / "manifest.json").write_text(
            json_text(manifest, pretty=True) + "\n", encoding="utf-8"
        )
        try:
            os.rename(staging, directory)
        except OSError as error:
            if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
    result = read_result(directory)
    if result is None:
        raise RuntimeError("Research bundle was not published")
    return result
