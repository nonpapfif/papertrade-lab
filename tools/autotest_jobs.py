"""Durable detached commands with fail-closed recovery and no model polling."""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from papertrade.ai import codex_environment
from papertrade.core import process_identity
from papertrade.jsonio import atomic_json, read_json

ROOT = Path(__file__).resolve().parents[1]
STARTUP_SECONDS = 5
STOP_SECONDS = 5


def _read(path: Path) -> dict[str, Any]:
    try:
        return read_json(path)
    except (OSError, ValueError) as error:
        raise RuntimeError(f"Invalid job file: {path.name}") from error
    except TypeError:
        raise TypeError(f"Invalid job object: {path.name}") from None


def _claim(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise RuntimeError("Job has already been claimed") from error
    with os.fdopen(descriptor, "w") as stream:
        stream.write(str(os.getpid()) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _validate(spec: dict[str, Any]) -> None:
    argv = spec.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or any(not isinstance(item, str) or "\x00" in item for item in argv)
    ):
        raise RuntimeError("Job argv must be a nonempty string list")
    cwd = spec.get("cwd")
    if not isinstance(cwd, str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
        raise RuntimeError("Job cwd must be an existing absolute directory")
    source = spec.get("stdin")
    if source is not None and (not isinstance(source, str) or not Path(source).is_absolute()):
        raise RuntimeError("Job stdin must be an absolute path")
    stop_after = spec.get("stop_after")
    if stop_after is not None and (
        type(stop_after) not in (int, float) or not math.isfinite(stop_after)
    ):
        raise RuntimeError("Job stop time must be finite")


def start_job(
    directory: Path,
    argv: list[str],
    cwd: Path,
    *,
    stdin: Path | None = None,
    environment: dict[str, str] | None = None,
    stop_after: float | None = None,
) -> None:
    """Reserve once and start a detached worker; an existing job is never restarted."""
    directory = directory.resolve()
    spec = {
        "argv": argv,
        "cwd": str(cwd.resolve()),
        "stdin": str(stdin.resolve()) if stdin is not None else None,
        "created_at": time.time(),
        **({"stop_after": stop_after} if stop_after is not None else {}),
    }
    _validate(spec)
    target_environment = codex_environment() if environment is None else environment
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in target_environment.items()
    ):
        raise RuntimeError("Job environment must contain strings")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if (directory / "spec.json").exists():
        raise RuntimeError("Job already exists")
    _claim(directory / "start.claim")
    # Keep environment values in a private execution file, outside the call audit.
    atomic_json(directory / "environment.json", target_environment)
    atomic_json(directory / "spec.json", spec)
    with (
        (directory / "stdout.log").open("ab") as output,
        (directory / "stderr.log").open("ab") as errors,
    ):
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "tools.autotest_jobs", "--child", str(directory)],
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=errors,
                env=codex_environment(),
                start_new_session=True,
            )
        except OSError as error:
            atomic_json(
                directory / "exit.json",
                {
                    "returncode": -1,
                    "finished_at": time.time(),
                    "error_type": type(error).__name__,
                },
            )
            raise
    threading.Thread(target=process.wait, daemon=True).start()
    # This is diagnostic only: the worker owns its authoritative identity file.
    atomic_json(
        directory / "launcher.json",
        {
            "pid": process.pid,
            "identity": process_identity(process.pid),
        },
    )


def poll_job(directory: Path) -> dict[str, Any] | None:
    """Read completion or process identity without making any AI call."""
    if (directory / "exit.json").exists():
        result = _read(directory / "exit.json")
        if type(result.get("returncode")) is not int:
            raise RuntimeError("Invalid job exit status")
        return result
    spec = _read(directory / "spec.json")
    _validate(spec)
    worker_path = directory / "worker.json"
    if worker_path.exists():
        worker = _read(worker_path)
        pid, identity = worker.get("pid"), worker.get("identity")
        if (
            type(pid) is int
            and pid > 0
            and isinstance(identity, str)
            and identity
            and process_identity(pid) == identity
        ):
            return None
        # Completion can be published between the first check and process inspection.
        if (directory / "exit.json").exists():
            return poll_job(directory)
        raise RuntimeError("Job worker is absent or changed; refusing to rerun")
    created = spec.get("created_at")
    if isinstance(created, (int, float)) and 0 <= time.time() - created <= STARTUP_SECONDS:
        return None
    raise RuntimeError("Job worker identity is missing; refusing to rerun")


def _child(directory: Path) -> int:
    try:
        _claim(directory / "child.claim")
    except RuntimeError:
        return 1
    try:
        identity = process_identity(os.getpid())
        if not identity:
            raise RuntimeError("Cannot establish job worker identity")
        atomic_json(directory / "worker.json", {"pid": os.getpid(), "identity": identity})
        spec = _read(directory / "spec.json")
        _validate(spec)
        environment = _read(directory / "environment.json")
        if any(not isinstance(value, str) for value in environment.values()):
            raise RuntimeError("Invalid job environment")
        with (
            Path(spec["stdin"] or os.devnull).open("rb") as source,
            (directory / "stdout.log").open("ab") as output,
            (directory / "stderr.log").open("ab") as errors,
        ):
            process = subprocess.Popen(
                spec["argv"],
                cwd=spec["cwd"],
                env=environment,
                stdin=source,
                stdout=output,
                stderr=errors,
            )
            atomic_json(
                directory / "target.json",
                {
                    "pid": process.pid,
                    "identity": process_identity(process.pid),
                },
            )
            cutoff_stop = False
            stop_after = spec.get("stop_after")
            try:
                returncode = process.wait(
                    timeout=None if stop_after is None else max(0, stop_after - time.time())
                )
            except subprocess.TimeoutExpired:
                # Only the parent holding this Popen may signal the target.
                cutoff_stop = True
                process.terminate()
                try:
                    returncode = process.wait(timeout=STOP_SECONDS)
                except subprocess.TimeoutExpired:
                    process.kill()
                    returncode = process.wait()
        atomic_json(
            directory / "exit.json",
            {
                "returncode": returncode,
                "finished_at": time.time(),
                **({"cutoff_stop": True} if cutoff_stop else {}),
            },
        )
        return 0
    except Exception as error:  # noqa: BLE001 - persist a failed child result, never success
        atomic_json(
            directory / "exit.json",
            {
                "returncode": -1,
                "finished_at": time.time(),
                "error_type": type(error).__name__,
            },
        )
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", required=True, type=Path)
    arguments = parser.parse_args()
    return _child(arguments.child.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
