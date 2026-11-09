"""Run recurring KR and US broker-constrained market-data-only paper sessions."""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path

from papertrade.api import MarketClient
from papertrade.core import Settings
from tools.autotest_jobs import ROOT
from tools.autotest_supervisor import command, read_json, tree_digest
from tools.paper_campaign.calendar import MarketCalendar
from tools.paper_campaign.controller import Campaign
from tools.paper_campaign.policy import Admission
from tools.paper_campaign.review import AstraReview, run_review
from tools.paper_campaign.runner import SessionRunner
from tools.paper_market import KR, US


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--capacity", type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--source-digest")
    parser.add_argument("--not-before-kr", type=datetime.fromisoformat)
    parser.add_argument("--not-before-us", type=datetime.fromisoformat)
    parser.add_argument("--once", action="store_true", help="Reconcile one pass and exit")
    parser.add_argument("--review", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.review is not None:
        return asyncio.run(run_review(args.review.resolve()))
    if (
        not args.output
        or not args.capacity
        or not re.fullmatch(r"[0-9a-f]{40}", args.source_commit or "")
        or not re.fullmatch(r"[0-9a-f]{64}", args.source_digest or "")
    ):
        parser.error("Campaign requires output, capacity, approved source commit and digest")
    settings = Settings.load(ROOT)
    output = args.output.resolve()
    if not output.is_relative_to(settings.state_dir.resolve()):
        parser.error("Campaign output must be inside the private repository state directory")
    if (
        command(["git", "rev-parse", "HEAD"], ROOT) != args.source_commit
        or tree_digest(ROOT) != args.source_digest
    ):
        parser.error("Campaign source differs from its approval")
    capacity = read_json(args.capacity)
    markets = (KR, US)
    not_before = {
        market.code: value.astimezone(UTC)
        for market, value in zip(markets, (args.not_before_kr, args.not_before_us), strict=True)
        if value is not None and value.utcoffset() is not None
    }
    if len(not_before) != sum(
        value is not None for value in (args.not_before_kr, args.not_before_us)
    ):
        parser.error("Earliest admission requires a timezone offset")
    admissions = {
        market.code: Admission.load(capacity[market.code], market.code, args.source_digest)
        for market in markets
    }
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output / "campaign.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        campaign = Campaign(
            output,
            MarketCalendar(MarketClient(settings), output / "calendar"),
            SessionRunner(args.source_commit, args.source_digest),
            AstraReview(),
            admissions,
            not_before,
        )
        while True:
            for market in markets:
                campaign.tick(market, datetime.now(UTC))
            if args.once:
                return 0
            time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main())
