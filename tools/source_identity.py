"""Content identities for repository snapshots and recorded inputs."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path


def command(argv: list[str], cwd: Path) -> str:
    result = subprocess.run(argv, cwd=cwd, check=True, text=True, capture_output=True)
    return result.stdout.strip()


def tree_digest(repo: Path) -> str:
    paths = command(["git", "ls-files", "--cached", "--others", "--exclude-standard"], repo)
    digest = hashlib.sha256()
    for name in sorted(set(paths.splitlines())):
        path = repo / name
        digest.update(name.encode())
        if path.is_symlink():
            digest.update(b"symlink:" + os.readlink(path).encode())
        elif path.is_file():
            digest.update(path.read_bytes())
            digest.update(b"executable" if path.stat().st_mode & 0o111 else b"regular")
        else:
            digest.update(b"missing")
    return digest.hexdigest()


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
