"""Archive October 2026 event sources without treating current snapshots as past knowledge."""

from __future__ import annotations

import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from papertrade.api import NoRedirect

HOSTS = frozenset(
    {"www.federalreserve.gov", "www.bls.gov", "www.bea.gov", "www.bok.or.kr", "api.nasdaq.com"}
)
SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY, kind TEXT, calendar_date TEXT, url TEXT, state TEXT,
    http_status INTEGER, retrieved_at TEXT, sha256 TEXT, payload_gzip BLOB,
    content_type TEXT, problem TEXT
);
CREATE TABLE IF NOT EXISTS earnings_candidates (
    calendar_date TEXT, symbol TEXT, source_id TEXT, data TEXT,
    available_at TEXT, feature_eligible INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(calendar_date,symbol)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS source_excerpts (
    source_id TEXT PRIMARY KEY, url TEXT NOT NULL, retrieved_at TEXT NOT NULL,
    sha256 TEXT NOT NULL, text TEXT NOT NULL, retrieval_method TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_schedule (
    event_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, calendar_date TEXT NOT NULL,
    country TEXT NOT NULL, symbol TEXT, event_type TEXT NOT NULL, reference_period TEXT,
    scheduled_at TEXT, released_at TEXT, time_precision TEXT NOT NULL,
    schedule_available_at TEXT, feature_eligible INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS numeric_results (
    event_id TEXT NOT NULL, metric TEXT NOT NULL, role TEXT NOT NULL,
    value REAL, unit TEXT NOT NULL, reference_period TEXT,
    source_id TEXT NOT NULL, evidence_sha256 TEXT NOT NULL, evidence_kind TEXT NOT NULL,
    locator TEXT NOT NULL, available_at TEXT, feature_eligible INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(event_id,metric,role)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS extractions (
    source_id TEXT PRIMARY KEY, state TEXT NOT NULL, numeric_values INTEGER NOT NULL,
    missing_metrics TEXT NOT NULL, problem TEXT
);
"""


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def source_plan() -> list[dict[str, str]]:
    base = [
        (
            "fed-calendar",
            "policy_calendar",
            "",
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        ),
        (
            "fed-decision",
            "policy_statement",
            "2026-10-28",
            "https://www.federalreserve.gov/newsevents/pressreleases/monetary20261028a.htm",
        ),
        (
            "fed-presser",
            "press_conference_index",
            "2026-10-28",
            "https://www.federalreserve.gov/monetarypolicy/fomcpresconf20261028.htm",
        ),
        (
            "fed-minutes",
            "minutes_released_in_october",
            "2026-10-07",
            "https://www.federalreserve.gov/monetarypolicy/fomcminutes20260916.htm",
        ),
        ("bls-calendar", "economic_calendar", "", "https://www.bls.gov/schedule/2026/10_sched.htm"),
        (
            "bls-jobs",
            "economic_release",
            "2026-10-02",
            "https://www.bls.gov/news.release/archives/empsit_10022026.htm",
        ),
        (
            "bls-cpi",
            "economic_release",
            "2026-10-14",
            "https://www.bls.gov/news.release/archives/cpi_10142026.htm",
        ),
        (
            "bls-ppi",
            "economic_release",
            "2026-10-15",
            "https://www.bls.gov/news.release/archives/ppi_10152026.htm",
        ),
        (
            "bls-imports",
            "economic_release",
            "2026-10-16",
            "https://www.bls.gov/news.release/archives/ximpim_10162026.htm",
        ),
        (
            "bls-eci",
            "economic_release",
            "2026-10-30",
            "https://www.bls.gov/news.release/archives/eci_10302026.htm",
        ),
        ("bea-archive", "economic_release_index", "", "https://www.bea.gov/news/archive"),
        (
            "bea-gdp",
            "economic_release",
            "2026-10-29",
            "https://www.bea.gov/news/2026/gdp-advance-estimate-3rd-quarter-2026",
        ),
        (
            "bea-pce",
            "economic_release",
            "2026-10-29",
            "https://www.bea.gov/news/2026/personal-income-and-outlays-september-2026",
        ),
        (
            "bok-policy",
            "policy_release_index",
            "2026-10-22",
            "https://www.bok.or.kr/portal/main/contents.do?menuNo=200755",
        ),
        (
            "bok-decision",
            "policy_statement",
            "2026-10-22",
            (
                "https://www.bok.or.kr/eng/bbs/E0000634/view.do?"
                "depth=400423&menuNo=400423&nttId=11065825&programType=newsDataEng&relate=Y"
            ),
        ),
        (
            "bok-gdp",
            "economic_release",
            "2026-10-27",
            (
                "https://www.bok.or.kr/eng/bbs/E0000634/view.do?"
                "depth=400423&menuNo=400423&nttId=11065943&programType=newsDataEng&relate=Y"
            ),
        ),
    ]
    result = [
        {"id": key, "kind": kind, "calendar_date": day, "url": url} for key, kind, day, url in base
    ]
    current = date(2026, 10, 30)
    while current.month == 10:
        if current.weekday() < 5:
            day = current.isoformat()
            result.append(
                {
                    "id": "earnings-" + day,
                    "kind": "earnings_calendar_snapshot",
                    "calendar_date": day,
                    "url": "https://api.nasdaq.com/api/calendar/earnings?date=" + day,
                }
            )
        current -= timedelta(days=1)
    return result


def validate_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in HOSTS
        or parsed.username
        or parsed.password
    ):
        raise ValueError("Event source is outside the approved public-source list")
    if parsed.port not in (None, 443):
        raise ValueError("Unexpected event-source port")
    return str(parsed.hostname)


def candidates(raw: bytes) -> list[dict[str, Any]]:
    page = json.loads(raw)
    status = page.get("status") or {}
    if not isinstance(status, dict) or status.get("rCode", 200) != 200:
        raise ValueError("Earnings provider returned an application error")
    data = page.get("data") or {}
    rows = data.get("rows") or []
    if not isinstance(rows, list) or any(
        not isinstance(r, dict) or not r.get("symbol") for r in rows
    ):
        raise ValueError("Invalid earnings calendar rows")
    return rows


class ReleaseText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style"):
            self.hidden += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data)


def number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("\u2212", "-")
    if text.lower() in ("", "n/a", "na", "--", "-", "none"):
        return None
    text = text.replace("$", "").replace(",", "").replace("%", "")
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        parsed = Decimal(text)
    except InvalidOperation as error:
        raise ValueError("Unexpected numeric source value") from error
    if not parsed.is_finite() or not Decimal(str(float(parsed))).is_finite():
        raise ValueError("Non-finite event result")
    return float(parsed)


def rate_number(value: str) -> float:
    parts = value.strip().split("-")
    if "/" not in parts[-1]:
        return float(value)
    numerator, denominator = parts[-1].split("/")
    return (float(parts[0]) if len(parts) == 2 else 0) + float(numerator) / float(denominator)


def release_time(key: str, day: str, text: str) -> str | None:
    if key == "fed-decision":
        marker = r"For release at 2:00 p\.m\. EDT"
        clock = "14:00:00"
    elif key.startswith(("bls-", "bea-")):
        marker = r"8:30 a\.m\.\s*(?:\(ET\)|EDT)"
        clock = "08:30:00"
    else:
        return None
    # A scheduled date alone never establishes an exact release timestamp.
    expected_date = datetime.fromisoformat(day).strftime("%B %-d, %Y")
    match = re.search(marker, text, re.IGNORECASE)
    if not match or expected_date not in text[max(0, match.start() - 200) : match.end() + 100]:
        return None
    return (
        datetime.fromisoformat(day + "T" + clock)
        .replace(tzinfo=ZoneInfo("America/New_York"))
        .astimezone(UTC)
        .isoformat()
    )


def macro_results(key: str, text: str) -> list[tuple[str, str, float | None, str, str, str]]:
    """Extract fixed October-release fields, never sentiment or narrative features."""
    result: list[tuple[str, str, float | None, str, str, str]] = []

    def field(
        metric: str,
        pattern: str,
        unit: str = "percent",
        period: str = "2026-09",
        role: str = "actual",
        group: int = 1,
    ) -> None:
        match = re.search(pattern, text, re.IGNORECASE)
        value = number(match.group(group)) if match else None
        result.append((metric, role, value, unit, period, pattern))

    n = r"([+-]?[\d,]+(?:\.\d+)?)"
    if key == "fed-decision":
        match = re.search(
            r"target range for the federal funds rate at ([\d./-]+) to "
            r"([\d./-]+) percent",
            text,
            re.IGNORECASE,
        )
        for index, metric in enumerate(("fed_funds_lower", "fed_funds_upper"), 1):
            value = rate_number(match.group(index)) if match else None
            result.append((metric, "actual", value, "percent", "2026-10-28", "target range"))
        field("votes_for", n + r"\s*[-\u2013\u2014]\s*\d+ vote", "count", "2026-10-28")
        field("votes_against", r"\d+\s*[-\u2013\u2014]\s*" + n + r" vote", "count", "2026-10-28")
    elif key == "bok-decision":
        field("base_rate", r"leave the Base Rate unchanged at " + n + "%", period="2026-10-22")
    elif key == "bok-gdp":
        field(
            "real_gdp_qoq",
            r"Real gross domestic product .*?increased by "
            + n
            + r" percent in the third quarter of 2026",
            period="2026-Q3",
        )
        field(
            "real_gdp_yoy",
            r"Real gross domestic product .*?year-on-year terms it increased by " + n + r" percent",
            period="2026-Q3",
        )
    elif key == "bea-gdp":
        field(
            "real_gdp_qoq_annualized",
            r"Real gross domestic product \(GDP\) increased at an "
            r"annual rate of " + n + r" percent",
            period="2026-Q3",
        )
        field(
            "real_gdp_qoq_annualized",
            r"In the second quarter, real GDP increased " + n + r" percent",
            period="2026-Q2",
            role="previous_as_reported",
        )
        for metric, label in (
            ("purchases_price_qoq_annualized", "price index for gross domestic purchases"),
            ("pce_price_qoq_annualized", "personal consumption expenditures (PCE) price index"),
            ("core_pce_price_qoq_annualized", "PCE price index, excluding food and energy"),
        ):
            field(metric, re.escape(label) + r" increased " + n + r" percent", period="2026-Q3")
    elif key == "bea-pce":
        field(
            "pce_price_mom",
            r"From the preceding month, the PCE price index for September "
            r"increased " + n + r" percent",
        )
        field(
            "core_pce_price_mom",
            r"Excluding food and energy, the PCE price index increased " + n + r" percent\.",
        )
        field(
            "pce_price_yoy",
            r"From the same month one year ago, the PCE price index for "
            r"September increased " + n + r" percent",
        )
        field(
            "core_pce_price_yoy",
            r"Excluding food and energy, the PCE price index increased "
            + n
            + r" percent from one year ago",
        )
        field(
            "personal_income_mom",
            r"Personal income increased \$[\d,.]+ billion \(" + n + r" percent at a monthly rate\)",
        )
        field("personal_saving_rate", r"personal saving rate.*?was " + n + r" percent")
    elif key == "bls-jobs":
        field("nonfarm_payroll_change", r"nonfarm payroll employment \(" + n + r"\)", "persons")
        field("unemployment_rate", r"unemployment rate \(" + n + r" percent\)")
        field(
            "hourly_earnings_level",
            r"average hourly earnings for all employees.*?to \$" + n,
            "USD_per_hour",
        )
        field(
            "hourly_earnings_yoy",
            r"Over the past 12 months, average hourly earnings have "
            r"increased by " + n + r" percent",
        )
        for period, month in (("2026-07", "July"), ("2026-08", "August")):
            pattern = r"(?:change in total nonfarm payroll employment for |change for )" + month
            pattern += r" was revised .*?from " + n + r" to " + n
            field(
                "nonfarm_payroll_change_" + period,
                pattern,
                "persons",
                period,
                "previous_before_revision",
                1,
            )
            field(
                "nonfarm_payroll_change_" + period,
                pattern,
                "persons",
                period,
                "previous_revised_in_release",
                2,
            )
    elif key == "bls-cpi":
        field(
            "cpi_mom_sa",
            r"Consumer Price Index for All Urban Consumers \(CPI-U\) increased "
            + n
            + r" percent on a seasonally adjusted basis",
        )
        field(
            "cpi_mom_sa",
            r"September after rising " + n + r" percent in August",
            period="2026-08",
            role="previous_as_reported",
        )
        field(
            "cpi_yoy_nsa",
            r"Over the last 12 months, the all items index increased "
            + n
            + r" percent before seasonal adjustment",
        )
        field(
            "core_cpi_mom_sa",
            r"index for all items less food and energy rose " + n + r" percent in September",
        )
        field(
            "core_cpi_yoy_nsa",
            r"all items less food and energy index rose " + n + r" percent over the year",
        )
    elif key == "bls-eci":
        field(
            "eci_compensation_qoq_sa",
            r"Compensation costs for civilian workers increased "
            + n
            + r" percent, seasonally adjusted",
            period="2026-Q3",
        )
        field(
            "eci_compensation_yoy_nsa",
            r"Compensation costs for civilian workers increased "
            + n
            + r" percent, not seasonally adjusted",
            period="2026-Q3",
        )
    elif key == "bls-ppi":
        field(
            "ppi_final_demand_mom_sa",
            r"Producer Price Index for final demand increased "
            + n
            + r" percent in September, seasonally adjusted",
        )
        field(
            "ppi_final_demand_yoy_nsa",
            r"index for final demand climbed "
            + n
            + r" percent for the 12 months ended in September",
        )
        field(
            "ppi_ex_food_energy_trade_mom_sa",
            r"Prices for final demand less foods, energy, "
            r"and trade services moved up " + n + r" percent in September",
        )
        field(
            "ppi_ex_food_energy_trade_yoy_nsa",
            r"For the 12 months ended in September, the "
            r"index for final demand less foods, energy, and trade services rose "
            + n
            + r" percent",
        )
    elif key == "bls-imports":
        field(
            "import_prices_mom_nsa", r"U.S. import prices increased " + n + r" percent in September"
        )
        field(
            "export_prices_mom_nsa",
            r"Prices for U.S. exports increased " + n + r" percent in September",
        )
    return result


def import_excerpts(db: sqlite3.Connection, path: Path) -> None:
    planned = {source["id"]: source for source in source_plan()}
    records = json.loads(path.read_text())
    for record in records:
        source = planned[record["source_id"]]
        if record["url"] != source["url"] or record["retrieval_method"] != "web_tool_excerpt":
            raise ValueError("Excerpt provenance does not match its planned source")
        validate_url(record["url"])
        if datetime.fromisoformat(record["retrieved_at"]).utcoffset() is None:
            raise ValueError("Excerpt retrieval time must include a timezone")
        text = record["text"]
        db.execute(
            "INSERT OR IGNORE INTO source_excerpts VALUES (?,?,?,?,?,?)",
            (
                record["source_id"],
                record["url"],
                record["retrieved_at"],
                hashlib.sha256(text.encode()).hexdigest(),
                text,
                record["retrieval_method"],
            ),
        )


def extract_results(db: sqlite3.Connection, output: Path) -> None:
    for source in source_plan():
        key, day = source["id"], source["calendar_date"]
        if not day:
            continue
        row = db.execute("SELECT * FROM sources WHERE id=?", (key,)).fetchone()
        excerpt = db.execute("SELECT * FROM source_excerpts WHERE source_id=?", (key,)).fetchone()
        text, digest, evidence_kind = "", "", "http_response"
        if row and row["state"] == "archived":
            raw = gzip.decompress(row["payload_gzip"])
            if hashlib.sha256(raw).hexdigest() != row["sha256"]:
                raise ValueError("Source digest mismatch before numerical extraction")
            parser = ReleaseText()
            parser.feed(raw.decode("utf-8"))
            text, digest = " ".join(" ".join(parser.parts).split()), row["sha256"]
        elif excerpt:
            digest = hashlib.sha256(excerpt["text"].encode()).hexdigest()
            if digest != excerpt["sha256"]:
                raise ValueError("Excerpt digest mismatch")
            text, evidence_kind = " ".join(excerpt["text"].split()), "web_tool_excerpt"
        if source["kind"] == "earnings_calendar_snapshot":
            if not row or row["state"] != "archived":
                continue
            missing = 0
            numeric_values = 0
            for candidate in candidates(gzip.decompress(row["payload_gzip"])):
                event_id = key + ":" + candidate["symbol"]
                period = candidate.get("fiscalQuarterEnding")
                db.execute(
                    "INSERT OR REPLACE INTO event_schedule VALUES (?,?,?,?,?,?,?,?,?,?,NULL,0)",
                    (
                        event_id,
                        key,
                        day,
                        "US",
                        candidate["symbol"],
                        "earnings_release",
                        period,
                        None,
                        None,
                        "date",
                    ),
                )
                for metric, field_name, role, unit in (
                    ("eps", "eps", "actual", "provider_currency_per_share"),
                    ("eps", "epsForecast", "forecast_snapshot", "provider_currency_per_share"),
                    ("eps_surprise", "surprise", "provider_surprise_snapshot", "percent"),
                    ("estimate_count", "noOfEsts", "snapshot", "count"),
                ):
                    value = number(candidate.get(field_name))
                    missing += int(metric == "eps" and role == "actual" and value is None)
                    numeric_values += int(value is not None)
                    db.execute(
                        "INSERT OR REPLACE INTO numeric_results VALUES "
                        "(?,?,?,?,?,?,?,?,?,?,NULL,0)",
                        (
                            event_id,
                            metric,
                            role,
                            value,
                            unit,
                            period,
                            key,
                            digest,
                            evidence_kind,
                            "data.rows[]." + field_name,
                        ),
                    )
            db.execute(
                "INSERT OR REPLACE INTO extractions VALUES (?,?,?,?,NULL)",
                (
                    key,
                    "partial" if missing else "extracted",
                    numeric_values,
                    encode({"unreported_eps": missing}),
                ),
            )
            continue
        facts = macro_results(key, text)
        released_at = release_time(key, day, text) if text else None
        db.execute(
            "INSERT OR REPLACE INTO event_schedule VALUES (?,?,?,?,?,?,?,?,?,?,NULL,0)",
            (
                key,
                key,
                day,
                "KR" if key.startswith("bok-") else "US",
                None,
                source["kind"],
                None,
                released_at,
                released_at,
                "second" if released_at else "date",
            ),
        )
        missing_metrics = []
        for metric, role, value, unit, period, locator in facts:
            if value is None:
                missing_metrics.append(metric + ":" + role)
            db.execute(
                "INSERT OR REPLACE INTO numeric_results VALUES (?,?,?,?,?,?,?,?,?,?,NULL,0)",
                (key, metric, role, value, unit, period, key, digest, evidence_kind, locator),
            )
        state = "partial" if missing_metrics else "extracted" if facts else "not_structured"
        db.execute(
            "INSERT OR REPLACE INTO extractions VALUES (?,?,?,?,?)",
            (
                key,
                state,
                sum(fact[2] is not None for fact in facts),
                encode(missing_metrics),
                None if text else "Source unavailable",
            ),
        )
    for table in ("event_schedule", "numeric_results"):
        with (output / (table + ".csv")).open("w", newline="") as stream:
            cursor = db.execute("SELECT * FROM " + table + " ORDER BY event_id")
            writer = csv.writer(stream)
            writer.writerow(column[0] for column in cursor.description)
            writer.writerows(cursor)


def status(db: sqlite3.Connection, output: Path, state: str, **extra: Any) -> None:
    record = {
        "state": state,
        "updated_at": datetime.now(UTC).isoformat(),
        "pid": os.getpid(),
        "ai_calls": 0,
        "sources": dict(db.execute("SELECT state,COUNT(*) FROM sources GROUP BY state").fetchall()),
        "earnings_candidates": db.execute("SELECT COUNT(*) FROM earnings_candidates").fetchone()[0],
        "numeric_values": db.execute(
            "SELECT COUNT(*) FROM numeric_results WHERE value IS NOT NULL"
        ).fetchone()[0],
        "extractions": dict(
            db.execute("SELECT state,COUNT(*) FROM extractions GROUP BY state").fetchall()
        ),
        "feature_eligible": 0,
        "coverage": "partial_source_archive",
        "pending": [
            "Verified corporate release timestamps",
            "Corporate revenue, profit and numerical guidance from issuer releases",
            "Earnings-call numerical disclosures and their publication times",
            "Historical consensus vintages",
            "Unstructured or unavailable macro releases and Korean corporate earnings",
            "Point-in-time source-content review",
        ],
        **extra,
    }
    temporary = output / "status.json.tmp"
    temporary.write_text(encode(record) + "\n")
    temporary.replace(output / "status.json")
    print(encode(record), flush=True)


def run(output: Path, excerpts: Path | None = None, extract_only: bool = False) -> None:
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (output / "archive.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = sqlite3.connect(output / "events.sqlite3")
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.executescript(SCHEMA)
        opener = urllib.request.build_opener(NoRedirect())
        blocked_hosts = {
            validate_url(row[0])
            for row in db.execute("SELECT url FROM sources WHERE http_status IN (401,403,429)")
        }
        next_at: dict[str, float] = {}
        try:
            if excerpts:
                with db:
                    import_excerpts(db, excerpts)
            status(db, output, "running")
            for source in [] if extract_only else source_plan():
                if db.execute("SELECT 1 FROM sources WHERE id=?", (source["id"],)).fetchone():
                    continue
                host = validate_url(source["url"])
                body = b""
                http_status = None
                content_type = None
                problem = None
                rows = []
                state = "archived"
                if host in blocked_hosts:
                    state, problem = (
                        "skipped_blocked_host",
                        "Earlier access rejection or rate limit",
                    )
                else:
                    time.sleep(max(0, next_at.get(host, 0) - time.monotonic()))
                    request = urllib.request.Request(
                        source["url"],
                        headers={
                            "User-Agent": "PapertradeLab/1.0 (historical market research)",
                            "Accept": "application/json,text/html;q=0.9,*/*;q=0.5",
                        },
                    )
                    try:
                        with opener.open(request, timeout=20) as response:
                            body = response.read()
                            http_status = response.status
                            content_type = response.headers.get("Content-Type")
                        if source["kind"] == "earnings_calendar_snapshot":
                            rows = candidates(body)
                    except urllib.error.HTTPError as error:
                        http_status, body = error.code, error.read()
                        state, problem = "http_error", f"HTTP {error.code}"
                        if error.code in (401, 403, 429):
                            blocked_hosts.add(host)
                    except (
                        urllib.error.URLError,
                        TimeoutError,
                        ValueError,
                        KeyError,
                        TypeError,
                    ) as error:
                        state, problem = "source_error", f"{type(error).__name__}: {error}"
                    finally:
                        next_at[host] = time.monotonic() + 1
                digest = hashlib.sha256(body).hexdigest()
                with db:
                    db.execute(
                        "INSERT INTO sources VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            source["id"],
                            source["kind"],
                            source["calendar_date"],
                            source["url"],
                            state,
                            http_status,
                            datetime.now(UTC).isoformat(),
                            digest,
                            gzip.compress(body, mtime=0),
                            content_type,
                            problem,
                        ),
                    )
                    if state == "archived":
                        db.executemany(
                            "INSERT OR REPLACE INTO earnings_candidates VALUES (?,?,?,?,NULL,0)",
                            [
                                (source["calendar_date"], row["symbol"], source["id"], encode(row))
                                for row in rows
                            ],
                        )
                status(db, output, "running", current_source=source["id"])
            with db:
                extract_results(db, output)
            for row in db.execute("SELECT sha256,payload_gzip FROM sources"):
                if (
                    hashlib.sha256(gzip.decompress(row["payload_gzip"])).hexdigest()
                    != row["sha256"]
                ):
                    raise ValueError("Archived event source failed its hash check")
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Event archive integrity check failed")
            errors = db.execute("SELECT COUNT(*) FROM sources WHERE state!='archived'").fetchone()[
                0
            ]
            partial = db.execute(
                "SELECT COUNT(*) FROM extractions WHERE state!='extracted'"
            ).fetchone()[0]
            status(db, output, "complete_with_errors" if errors or partial else "complete")
        except BaseException as error:
            status(db, output, "failed", error_type=type(error).__name__, message=str(error))
            raise
        finally:
            db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--import-excerpts", type=Path)
    parser.add_argument("--extract-only", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    if args.status:
        print((args.output / "status.json").read_text())
    else:
        run(args.output.resolve(), args.import_excerpts, args.extract_only)


if __name__ == "__main__":
    main()
