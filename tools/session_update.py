"""Evidence for a recurring paper session handoff between frozen sources."""

from __future__ import annotations

import ast
import re
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Any

from papertrade.jsonio import journal_json as encode
from papertrade.jsonio import read_json
from tools.adaptive_journal import AdaptiveJournal, JournalError, digest
from tools.async_layered_paper import seconds
from tools.local_validation import require_report
from tools.source_identity import command, file_digest, tree_digest

UPDATE_SCHEMA = 1
HELD_NAME = "held.sqlite3"
HOLD_EVENT = "session_update_hold"
CUT_EVENT = "session_update_cut"
SOURCE_EVENT = "session_update"
INPUT_FIELDS = ("sequence", "kind", "payload", "received")
REQUEST_FIELDS = {
    "schema", "from_commit", "from_digest", "target_root", "target_commit",
    "target_digest", "validation_report",
}
ECONOMIC_FILES = (
    "papertrade/clock.py",
    "settings.toml",
    "tools/account_executor.py",
    "tools/async_layered_paper.py",
    "tools/kr_broker_profile.py",
    "tools/layered_paper.py",
    "tools/live_execution.py",
    "tools/paper_market.py",
    "tools/search_portfolios.py",
    "tools/slot_control.py",
    "tools/strategy_catalog.py",
    "tools/toss_order_contract.py",
    "tools/week_hypotheses.py",
)
ECONOMIC_DEFINITIONS: dict[str, dict[str, set[str] | None]] = {
    "tools/run_async_live_validation.py": {
        "Runtime": {
            "__init__", "_advance", "_apply_accounts", "_apply_review", "_checkpoint",
            "_consume_event", "_feature_at", "_record_delivery", "cancel_accounts",
            "entry_watch", "identifier", "inherit_accounts", "protected",
        },
        "book_has_usable_depth": None,
        "make_runtime": None,
        "subscription_set": None,
        "us_regular_window": None,
    },
    "tools/shared_feed_slots.py": {
        "SharedFeedSlots": {
            "_add", "_branch", "_command", "_configure_runtime", "_consume_slot",
            "_prepare_cut", "_slot_protection", "_stop", "active",
        },
    },
}


def _sha(value: Any, lengths: tuple[int, ...] = (64,)) -> bool:
    return (
        isinstance(value, str)
        and len(value) in lengths
        and re.fullmatch("[a-f0-9]+", value) is not None
    )


def _absolute(value: Any) -> bool:
    return isinstance(value, str) and Path(value).is_absolute()


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return read_json(path)
    except (OSError, ValueError, TypeError) as error:
        raise JournalError(f"Cannot read session update {path.name}") from error


def load_request(directory: Path) -> dict[str, Any]:
    request = _manifest(directory / "request.json")
    if (
        request.keys() != REQUEST_FIELDS
        or type(request["schema"]) is not int
        or request["schema"] != UPDATE_SCHEMA
        or not _sha(request["from_commit"], (40, 64))
        or not _sha(request["target_commit"], (40, 64))
        or not _sha(request["from_digest"])
        or not _sha(request["target_digest"])
        or request["from_digest"] == request["target_digest"]
        or not _absolute(request["target_root"])
        or not _absolute(request["validation_report"])
    ):
        raise JournalError("Invalid session update request")
    return request


def validate_source(root: Path, commit: str, digest: str) -> None:
    try:
        if (
            Path(command(["git", "rev-parse", "--show-toplevel"], root)).resolve()
            != root.resolve()
            or command(["git", "rev-parse", "HEAD"], root) != commit
            or tree_digest(root) != digest
        ):
            raise JournalError("Session update source changed")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise JournalError("Cannot verify session update source") from error


def economic_digest(root: Path) -> str:
    """Bind behavior that must remain frozen for already-open paper positions."""
    try:
        definitions: dict[str, Any] = {}
        for path, selected in ECONOMIC_DEFINITIONS.items():
            module = ast.parse((root / path).read_text())
            found: dict[str, Any] = {}
            for node in ast.walk(module):
                if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if node.name not in selected:
                    continue
                if node.name in found:
                    raise JournalError("Session economic definitions are ambiguous")
                members = selected[node.name]
                if members is None:
                    found[node.name] = ast.dump(node)
                    continue
                methods = {
                    method.name: ast.dump(method)
                    for method in node.body
                    if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and method.name in members
                }
                if methods.keys() != members:
                    raise JournalError("Session economic methods are incomplete")
                found[node.name] = methods
            if found.keys() != selected.keys():
                raise JournalError("Session economic definitions are incomplete")
            definitions[path] = found
        return digest(encode({
            "files": {path: file_digest(root / path) for path in ECONOMIC_FILES},
            "definitions": definitions,
        }))
    except (OSError, ValueError, SyntaxError) as error:
        raise JournalError("Cannot verify frozen session economics") from error


def validate_target(request: dict[str, Any], *, source_root: Path | None = None) -> None:
    target = Path(request["target_root"])
    validate_source(target, request["target_commit"], request["target_digest"])
    try:
        git_common = ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"]
        if source_root is not None and (
            Path(command(git_common, source_root)).resolve()
            != Path(command(git_common, target)).resolve()
        ):
            raise JournalError("Session update target belongs to another repository")
        if source_root is not None and economic_digest(source_root) != economic_digest(target):
            raise JournalError("Session update changed frozen economic behavior")
        require_report(Path(request["validation_report"]), request["target_digest"])
    except (
        OSError, ValueError, TypeError, AttributeError, subprocess.SubprocessError,
    ) as error:
        raise JournalError("Session update target validation failed") from error
    except RuntimeError as error:
        if isinstance(error, JournalError):
            raise
        raise JournalError("Session update target validation failed") from error


def _valid_anchor(anchor: Any) -> bool:
    return (
        isinstance(anchor, dict)
        and anchor.keys() == {"records", "cursor", "chain_sha256"}
        and type(anchor["records"]) is int
        and type(anchor["cursor"]) is int
        and 0 <= anchor["cursor"] <= anchor["records"]
        and _sha(anchor["chain_sha256"])
    )


def load_manifest(directory: Path) -> dict[str, Any]:
    ready = _manifest(directory / "ready.json")
    if (
        ready.keys() != {"schema", "request", "journal", "journal_path", "sequence", "held"}
        or type(ready["schema"]) is not int
        or ready["schema"] != UPDATE_SCHEMA
        or ready["request"] != load_request(directory)
        or not _absolute(ready["journal_path"])
        or type(ready["sequence"]) is not int
        or ready["sequence"] < 0
        or not _valid_anchor(ready["journal"])
        or not _valid_anchor(ready["held"])
    ):
        raise JournalError("Invalid session update ready evidence")
    return ready


def held_identity(request: dict[str, Any], journal_path: Path) -> dict[str, Any]:
    return {
        "experiment": "session_update_held",
        "request": request,
        "journal_path": str(journal_path),
    }


def held_inputs(directory: Path, ready: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
    with AdaptiveJournal.read_completed(
        directory / HELD_NAME, ready["held"]["chain_sha256"]
    ) as held:
        if (
            held.identity != held_identity(ready["request"], Path(ready["journal_path"]))
            or held.evidence() != ready["held"]
        ):
            raise JournalError("Held inputs do not match the handoff evidence")
        for offset, event in enumerate(held.events(), 1):
            if (
                event.keys() != {*INPUT_FIELDS, "index"}
                or type(event["sequence"]) is not int
                or event["sequence"] != ready["sequence"] + offset
                or event["kind"] not in (
                    "book", "calendar", "clock", "feature", "fx", "selection",
                )
                or not isinstance(event["received"], str)
            ):
                raise JournalError("Invalid held input")
            seconds(event["received"])
            yield {key: event[key] for key in INPUT_FIELDS}


def source_marker(
    ready: dict[str, Any], source_sha256: dict[str, str]
) -> dict[str, Any]:
    return {"kind": SOURCE_EVENT, "handoff": ready, "source_sha256": source_sha256}


def cut_marker(ready: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": CUT_EVENT,
        **{key: ready[key] for key in ("request", "journal_path", "sequence", "held")},
    }


def marker_source(event: dict[str, Any]) -> dict[str, str]:
    source = event.get("source_sha256")
    if (
        event.keys() != {"kind", "handoff", "source_sha256", "index"}
        or not isinstance(source, dict)
        or not source
        or any(not isinstance(name, str) or not _sha(value) for name, value in source.items())
        or not isinstance(event["handoff"], dict)
        or not _valid_anchor(event["handoff"].get("journal"))
        or event["handoff"]["journal"]["records"] != event["index"] - 1
    ):
        raise JournalError("Invalid session update source marker")
    return source


def verify_resume(
    journal: AdaptiveJournal, directory: Path, ready: dict[str, Any],
    *, source_sha256: dict[str, str] | None = None,
) -> tuple[bool, int]:
    """Verify the old cut and any committed prefix of this handoff's held inputs."""
    if (
        journal.identity.get("experiment") != "shared_feed_slots"
        or "campaign_seeds" not in journal.identity
    ):
        raise JournalError("Session update requires a recurring shared-slot journal")
    marker = False
    applied = sequence = 0
    withheld = held_inputs(directory, ready)
    try:
        expected = next(withheld, None)
        for event in journal.verified_events(ready["journal"]):
            index = event["index"]
            if index <= ready["journal"]["records"]:
                if event["kind"] == "session_close":
                    raise JournalError("Cannot update a closed paper session")
                if event["kind"] not in (HOLD_EVENT, CUT_EVENT, SOURCE_EVENT):
                    sequence = event["sequence"]
                if index == ready["journal"]["records"] and event != {
                    **cut_marker(ready), "index": index
                }:
                    raise JournalError("Journal handoff cut does not match the sealed spool")
                continue
            if not marker:
                if event["kind"] != SOURCE_EVENT or event.get("handoff") != ready:
                    raise JournalError("Journal changed after the handoff cut")
                source = marker_source(event)
                if source_sha256 is not None and source != source_sha256:
                    raise JournalError("Committed handoff used a different source")
                marker = True
            elif expected is not None:
                if encode({key: event.get(key) for key in INPUT_FIELDS}) != encode(expected):
                    raise JournalError("Committed held input does not match its spool")
                applied += 1
                expected = next(withheld, None)
            elif event["kind"] in (HOLD_EVENT, CUT_EVENT, SOURCE_EVENT, "session_close"):
                raise JournalError("A newer session boundary follows this handoff")
        if sequence != ready["sequence"]:
            raise JournalError("Handoff sequence differs from the committed cut")
        for _ in withheld:
            pass
    finally:
        withheld.close()
    return marker, applied


def load_ready(directory: Path) -> dict[str, Any]:
    ready = load_manifest(directory)
    with AdaptiveJournal.read_active(Path(ready["journal_path"])) as journal:
        verify_resume(journal, directory, ready)
    return ready
