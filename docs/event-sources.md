# October 2026 event schedules and numerical results

`tools.archive_october_events` collects public event sources independently of Toss price data.
It uses no broker credentials, account endpoints, orders, or AI calls. The program covers
October 2026 explicitly. It does not claim complete coverage of all events or corporate disclosures.

```zsh
python3 -m tools.archive_october_events --output state/events/2026-10
python3 -m tools.archive_october_events --output state/events/2026-10 --status
python3 -m tools.archive_october_events --output state/events/2026-10 --extract-only
```

## Sources and extracted fields

The source plan contains the Federal Reserve calendar, October decision, press-conference page,
and September minutes released in October; BLS calendar and original employment, CPI, PPI,
import/export prices and employment-cost releases; BEA archive and original GDP and personal
income/outlays releases; Bank of Korea policy index, October decision and GDP release; and
22 weekday Nasdaq earnings-calendar snapshots, October 30 through October 1.

Numerical extraction includes:

- Federal funds target bounds and voting counts; Korean base rate and GDP growth.
- US GDP growth, quarterly price measures, monthly and annual headline/core PCE, income and saving.
- US payroll change, unemployment, hourly earnings, CPI, PPI, import/export prices and employment cost.
- Reported EPS, provider consensus snapshot, provider surprise percentage and estimate count.

Results contain numbers, metric identifiers, roles, units, periods, publication metadata and source
references. They contain no sentiment, causal interpretation, investment opinion, importance score,
or bullish/bearish labels. The original source archive may contain official narrative; that prose is
not copied into numerical features. No model interprets calls or statements.

## Files and provenance

`events.sqlite3` preserves original gzipped HTTP responses, SHA-256, URL, retrieval time, status,
content type and errors in `sources`. `earnings_candidates` keeps the provider rows unchanged.
`event_schedule` holds event dates and verified exact release times where available.
`numeric_results` stores one numerical metric/role per event. Missing values are NULL, never zero.
`extractions` records missing fields and sources that have not been structured.
`event_schedule.csv` and `numeric_results.csv` are local inspection exports.

The `actual`, `previous_as_reported`, `previous_before_revision` and
`previous_revised_in_release` roles stay separate. Older-period revisions become known in the
current release; they must never overwrite what earlier decisions could know. Reported EPS and
`forecast_snapshot` are distinct. A downloaded historical calendar does not prove the estimate was
available before earnings. Currency and GAAP/non-GAAP basis are not inferred for provider EPS.
The EPS unit remains `provider_currency_per_share` until issuer verification.

The collector uses a descriptive User-Agent, approved HTTPS hosts, no redirects and at least one
second after the previous request finishes per host. A 401/403/429 stops further requests to that
host, including later restarts into the same archive. Existing source IDs are not fetched again.
Use a new output directory only when an explicitly new snapshot is intended.

If an official page can be read through the web tool while direct HTTP collection is unavailable,
its verified excerpt can be imported separately:

```zsh
python3 -m tools.archive_october_events --output state/events/2026-10 \
  --import-excerpts state/events/2026-10/official-web-excerpts.json --extract-only
```

The JSON list contains `source_id`, the exact planned `url`, timezone-aware `retrieved_at`,
`retrieval_method` equal to `web_tool_excerpt`, and the source `text`. Preserve exact source wording
and numerical precision. `source_excerpts` hashes this evidence separately. It never changes an
HTTP rejection into a successful download. This is an explicit evidence import, not an automatic
network fallback or an AI-authored result. Never commit the local evidence or downloaded dataset.

## Historical availability and coverage

A precise release time is extracted only when the source supplies both the date and matching time.
Korean pages with dates only remain date-level. Nasdaq session hints do not establish an exact
publication time or earnings-call start. Dates from a release are not proof of a preannounced
historical calendar vintage. `schedule_available_at` and result `available_at` stay NULL, and every
feature remains `feature_eligible=0` pending point-in-time verification.

The status always identifies a `partial_source_archive`. Completion describes the planned download
and extraction pass, not a comprehensive event feed. Missing metrics, HTTP failures, unavailable
EPS and unstructured sources stay visible. Remaining coverage includes issuer-verified revenue,
profit and numerical guidance, Korean corporate earnings, exact corporate publication times,
historical consensus vintages, numerical call disclosures and other macro releases.

Hash checks run before numerical extraction. SQLite integrity and archived hashes are checked at
completion. `--extract-only` reads stored evidence and makes no network requests. The program does
not automatically feed these snapshots to the historical simulation or resume price collection.
