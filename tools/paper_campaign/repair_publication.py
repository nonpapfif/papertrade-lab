"""Reconcile an exact paper-session repair PR before authorizing a resumed source."""

from __future__ import annotations

import json
import re
import subprocess
import time
from typing import TYPE_CHECKING

from tools.source_identity import command, tree_digest

if TYPE_CHECKING:
    from tools.autotest_supervisor import Supervisor

REPOSITORY = "nonpapfif/papertrade-lab"


def finish_publication(supervisor: Supervisor) -> None:
    state, repo = supervisor.state, supervisor.repo
    digest = supervisor.verify_refactor_evidence()
    sha = state["publish_sha"]
    url = state["pr"]
    if (
        not re.fullmatch(r"https://github\.com/nonpapfif/papertrade-lab/pull/[1-9][0-9]*", url)
        or command(["git", "rev-parse", "HEAD"], supervisor.worktree) != sha
        or command(["git", "status", "--porcelain"], supervisor.worktree)
        or digest != state["publication_digest"]
        or state["review"]["publish_ok"] is not True
        or state["review"]["decision"] != "complete"
        or set(state["review"]["satisfied"]) != set(supervisor.requirements)
    ):
        raise RuntimeError("Repair publication no longer matches its review")
    info = json.loads(command(
        [
            "gh", "pr", "view", "--repo", REPOSITORY, url, "--json",
            "baseRefName,headRefOid,headRefName,isCrossRepository,state,mergeCommit",
        ],
        repo,
    ))
    if (
        info["headRefOid"] != sha or info["headRefName"] != state["branch"]
        or info["baseRefName"] != "master" or info["isCrossRepository"] is not False
    ):
        raise RuntimeError("Repair PR base or reviewed head differs")
    if info["state"] == "OPEN" and state["phase"] == "repair_merge":
        try:
            command(["gh", "pr", "checks", "--repo", REPOSITORY, url, "--required"], repo)
        except subprocess.CalledProcessError as error:
            if error.returncode == 8:
                return
            # GitHub reports that there are no required checks on unprotected branches.
            if "no required checks reported" not in (error.stderr or "").lower():
                raise
        command(["git", "fetch", "origin"], repo)
        if command(["git", "rev-parse", "origin/master"], repo) != state["base"]:
            raise RuntimeError("Master advanced after the repair was reviewed")
        if time.time() >= supervisor.repair_deadline():
            supervisor.transition("blocked", reason="The failed session window ended before merge")
            return
        supervisor.transition("repair_merging")
        try:
            command(
                ["gh", "pr", "merge", "--repo", REPOSITORY, url, "--merge",
                 "--match-head-commit", sha],
                repo,
            )
        except subprocess.CalledProcessError:
            # The next step observes GitHub before deciding whether the mutation occurred.
            return
        return
    if info["state"] != "MERGED" or state["phase"] != "repair_merging":
        raise RuntimeError("Repair merge outcome is uncertain; reconcile GitHub before retry")
    merged = info["mergeCommit"]["oid"]
    if not re.fullmatch(r"[0-9a-f]{40}", merged):
        raise RuntimeError("Repair merge did not return a full source identity")
    command(["git", "fetch", "origin"], repo)
    if (
        command(["git", "rev-parse", "origin/master"], repo) != merged
        or command(["git", "rev-parse", merged + "^{tree}"], repo)
        != command(["git", "rev-parse", sha + "^{tree}"], supervisor.worktree)
        or command(["git", "branch", "--show-current"], repo) != "master"
        or command(["git", "status", "--porcelain"], repo)
    ):
        raise RuntimeError("Fetched master does not match the reviewed repair tree")
    command(["git", "merge", "--ff-only", merged], repo)
    if command(["git", "rev-parse", "HEAD"], repo) != merged or tree_digest(repo) != digest:
        raise RuntimeError("Synced master does not match the reviewed repair")
    pinned = supervisor.output / "merged-source"
    if not pinned.exists():
        command(["git", "worktree", "add", "--detach", str(pinned), merged], repo)
    if command(["git", "rev-parse", "HEAD"], pinned) != merged or tree_digest(pinned) != digest:
        raise RuntimeError("Pinned recovery source differs from reviewed master")
    supervisor.transition("repair_ready", merged_sha=merged, merged_digest=digest,
                          runtime_checkout=str(pinned))
