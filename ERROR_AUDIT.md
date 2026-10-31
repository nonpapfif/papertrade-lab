# Error audit and recovery history

This is a technical history of fixes recorded on October 30-31, 2026 KST.
It is not a statement that any service is currently running. Early sections describe superseded
v1/v2 behavior; the free simulation contract in [AGENTS.md](AGENTS.md) takes precedence.
All fills are virtual. Real brokerage order paths remain outside the HTTP allowlist.

Operational ledgers, individual trades, balances, token-use snapshots, backups, and verification
artifacts belong under ignored `state/`. The original documents are preserved locally under
`state/archives/repository-language-*/` with a source commit and SHA-256 manifest.
Published Git history has not been rewritten.

## Initial failure points and fixes

| Failure | Technical fix |
|---|---|
| AI waiting blocked position checks | Separate scan, risk, review, and supervise processes |
| One symbol's error stopped all checks | Record each failure and continue other symbols and settlements |
| Repeated 429 responses lost progress | Shared API-group pacing, server cooldowns, and durable cursors |
| Failed scan slots could not resume | Started/completed/failed states and interrupted-slot recovery |
| Token expiry or 401 could not recover | Expiry checks, locked token refresh, and one 401 retry |
| Legacy fills ignored the KR closing auction | Honor calendar auction timestamps in the legacy strategy |
| Concurrent changes mixed balance snapshots | Read status in one transaction |
| Relative-strength references had mismatched times | Align symbol, benchmark, and lookback timestamps |
| Recent bars hid earlier cache gaps | Identify and fetch missing minute intervals |
| Legacy fills used expired FX or old observations | Check validUntil and actual quote time before filling |
| Top-of-book price marked the whole position | Use depth-weighted prices and costs in the legacy strategy |
| Malformed AI output broke validation | Validate types, symbols, actions, quantities, and usage |
| Interrupted AI usage appeared as zero | Recover JSONL/intermediate records; retain null when unknown |
| Workers did not resume after login | Standard LaunchAgents, RunAtLoad, and KeepAlive installation |
| Corporate actions broke quantity/price consistency | Recheck adjustment ratios and apply evidenced actions idempotently |
| Minute-bar end time was treated as start time | Preserve source time and normalize the legacy internal start by one minute |
| Successful requests left incidents open without a signal | Recover incidents on verified request success, independent of entry signals |
| Ordinary entry rejection looked like an outage | Separate rejected_observation from open incidents |
| Database errors also lost audit records | Separate emergency JSONL and DB-independent emergency reading |
| Ledger damage could recreate starting capital | Refuse to initialize a missing/damaged existing ledger; require recovery verification |

Initial verification covered regression tests, Ruff, mypy, observed market-data responses,
server-directed retry/recovery, worker restart, AI schema/usage collection, and preserved backups.
Passing these checks established technical behavior, not strategy profitability.
Actual run measurements and request identifiers remain in local verification artifacts.

## Diagnostic commands

```zsh
python3 -m papertrade health
python3 -m papertrade audit --open
python3 -m papertrade audit --events
python3 -m papertrade audit --incident INCIDENT_ID
python3 -m papertrade audit --output state/audit-export.json
python3 -m papertrade audit --emergency
```

Exports include times, causes, symbols, paths, server codes, request IDs, retry times, and recoveries.
Never log credentials or authentication headers. Never replace unknown inference usage with zero.
Use current commands to inspect current state instead of treating historical checks as live status.

## One-minute observation and v2 experiments

A separate `market` role was added for full-universe prices on a 60-second start cadence.
Position checks also use a 60-second start cadence. These observation roles do not invoke AI.
`scan` uses one-minute slots and resumes detailed analysis without overlapping runs.
Shared snapshots reduce duplicate universe requests. Long requests can delay the next start;
this is not a guarantee of completing detailed analysis of every symbol every minute.
The full role set is market, scan, risk, review, and supervise.

The historical `momentum-experiment-v2` replaced 30-minute/six-bar breakouts with five-minute/
three-bar breakouts and average-or-higher volume. It used a 15-minute relative-strength window,
a 0.5% spread threshold, a 1.5ATR entry range, and legacy time-based entry/exit rules.
Entry and exit records retained strategy versions, plans, and observed valuation ranges.
These rules are archived in [the v2 guide](docs/history/README-v2.md), not imposed on free v3.

Unsettled sale proceeds were later made reusable for new virtual entries.
`buying_power_usd` consistently represented unused virtual cash across filtering, sizing, input,
and debiting. Position value was not counted again as cash; historical results were preserved.

## Free v3 and duplicate AI requests

The free profile removed legacy session, settlement, margin, depth, quantity, and action checks
from its fill prerequisites. It supports shorts, fractions, additions, partial exits, reversals,
and virtual borrowing. New costs are zero; debt and short liabilities reduce equity.
Historical fills, costs, and results were retained.

During migration, changing market-state classifications incorrectly triggered repeated AI reviews.
The review/candidate workers were stopped while those triggers were removed.
Time changes, sample counts, and broad rising/falling classifications no longer justify repeat calls.
A watch condition already satisfied at registration does not fire immediately and is consumed once.
Duplicate pending input and an uncomputed ATR placeholder were removed from review input.
No arbitrary AI-call, token, reasoning-time, or candidate ceiling was introduced.
Unknown usage and missing provider prices remain explicit audit records.

The profit-first objective permits exploratory directional hypotheses and concentration.
It does not treat capital preservation, loss minimization, or an old low win rate as automatic
reasons to wait. Stops are optional. Existing losses are not erased by restarting an experiment.

## Queue contention and partial batches

A post-exit path resent old universe signals and generated oversized duplicate candidate batches.
Per-symbol job updates repeatedly scanned an unindexed queue. Review could consume a partially
published batch, producing contention and restarts.

- Index jobs by symbol, market, status, and observation time.
- Save the candidate batch and processed-event markers atomically; roll back the whole operation.
- After exits, review held/exited/watched names and current ranked opportunities rather than every old signal.
- Preserve fills, profit, and unknown usage; replace only interrupted pending work.
- Test partial-batch visibility, storage rollback, and the indexed query plan.

The implementation was checked against the local ledger and backed up before recovery.
Performance measurements and backup identifiers remain in local verification records.

## Universe-wide new signals

A later review gate incorrectly depended only on watch prices, exits, and user requests.
New signals now trigger review across the observed universe regardless of holdings, watches,
or country. Watch prices are additional conditions, not eligibility gates.
Apply the same signal-generation rules to new symbols and directions.
Consume each signal once, including the latest observation actually delivered to AI.
Baseline existing observations at migration and review current ranking/holding/watch candidates.
Tests cover a new US signal while KR watch prices remain unchanged, plus duplicate observations.

## External recovery boundaries

IP allowlists, credentials, and account quotas require external verification; do not change
permissions without authorization. Corporate-action evidence requires verified facts, not only
a syntactically valid URL. If evidence is insufficient in the legacy path, defer dependent work.
Do not restore an old backup over newer trades without verifying their preservation.
Do not invent fills for operating-system or network downtime or promise pre-login execution.

The [official Toss specification](https://openapi.tossinvest.com/openapi-docs/latest/openapi.json)
was the reference for candle timestamps and response-header pacing fields.
