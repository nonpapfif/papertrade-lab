"""Validate a frozen checkout locally and retain the command evidence."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from tools.autotest_jobs import atomic_json


def validate(repo: Path, output: Path, python: str) -> dict[str, Any]:
    from tools.autotest_supervisor import tree_digest

    repo = repo.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    initial = tree_digest(repo)
    checks: list[dict[str, Any]] = []
    report = {
        "state": "running",
        "source_digest": initial,
        "python": python,
        "checks": checks,
    }
    path = output / "validation.json"
    atomic_json(path, report)
    for name, executable, arguments in (
        ("ruff", shutil.which("ruff"), ["check", "."]),
        ("mypy", shutil.which("mypy"), ["--explicit-package-bases", "papertrade", "tools", "tests"]),
        ("unittest", shutil.which(python), ["-m", "unittest", "discover", "-s", "tests"]),
    ):
        if executable is not None:
            executable = os.path.abspath(executable)
        entry: dict[str, Any] = {"name": name, "argv": [executable, *arguments]}
        checks.append(entry)
        if executable is None:
            entry.update(state="missing_tool", returncode=None)
            atomic_json(path, report)
            continue
        started = time.perf_counter()
        try:
            result = subprocess.run(
                [executable, *arguments], cwd=repo, check=False, capture_output=True
            )
        except OSError as error:
            entry.update(
                state="launch_failed", error_type=type(error).__name__, returncode=None,
                duration_seconds=time.perf_counter() - started,
            )
            atomic_json(path, report)
            continue
        entry["duration_seconds"] = time.perf_counter() - started
        for stream, data in (("stdout", result.stdout), ("stderr", result.stderr)):
            log = f"{name}.{stream}.log"
            (output / log).write_bytes(data)
            entry[stream] = {"file": log, "sha256": hashlib.sha256(data).hexdigest()}
        passed = result.returncode == 0
        if name == "unittest":
            summary = result.stderr.decode(errors="replace")
            count = re.search(r"^Ran (\d+) tests? in ", summary, flags=re.MULTILINE)
            skipped = re.search(r"\bskipped=(\d+)", summary)
            entry.update(
                tests=int(count[1]) if count else 0,
                skipped=int(skipped[1]) if skipped else 0,
            )
            passed = (
                passed and entry["tests"] > 0 and entry["skipped"] == 0
                and re.search(r"^OK$", summary, flags=re.MULTILINE) is not None
            )
        entry.update(returncode=result.returncode, state="passed" if passed else "failed")
        atomic_json(path, report)
    final = tree_digest(repo)
    passed = initial == final and all(entry["state"] == "passed" for entry in checks)
    report.update(final_source_digest=final, state="passed" if passed else "failed")
    atomic_json(path, report)
    return report


def require_report(path: Path, digest: str) -> None:
    from tools.autotest_supervisor import read_json

    report = read_json(path)
    if (
        report.get("state") != "passed"
        or report.get("source_digest") != digest
        or report.get("final_source_digest") != digest
    ):
        raise RuntimeError("Local validation is missing, failed or stale")
    checks = report.get("checks")
    commands = {
        "ruff": ["check", "."],
        "mypy": ["--explicit-package-bases", "papertrade", "tools", "tests"],
        "unittest": ["-m", "unittest", "discover", "-s", "tests"],
    }
    if not isinstance(checks, list) or len(checks) != len(commands):
        raise RuntimeError("Local validation command set is incomplete")
    for (name, arguments), entry in zip(commands.items(), checks, strict=True):
        if (
            entry.get("name") != name
            or entry.get("state") != "passed"
            or entry.get("returncode") != 0
            or entry.get("argv", [])[1:] != arguments
        ):
            raise RuntimeError(f"Local validation command did not pass: {name}")
        for stream in ("stdout", "stderr"):
            artifact = entry.get(stream, {})
            log = f"{name}.{stream}.log"
            if (
                artifact.get("file") != log
                or hashlib.sha256((path.parent / log).read_bytes()).hexdigest()
                != artifact.get("sha256")
            ):
                raise RuntimeError(f"Local validation log changed: {log}")
        if name == "unittest" and (entry.get("tests", 0) <= 0 or entry.get("skipped") != 0):
            raise RuntimeError("Local validation requires a nonempty, unskipped suite")


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    result = validate(args.repo, args.output, args.python)
    print(f"Local validation {result['state']}: {args.output / 'validation.json'}")
    return 0 if result["state"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
