# Papertrade Lab

Market-data collection, isolated historical experiments, and virtual trading with event-triggered
AI reviews. Python 3.12 or later is required; the project has no third-party Python dependencies.
The isolated predictors and background service examples use macOS.

The user has set two boundaries:

1. Do not run AI continuously just because time has passed. Use it for useful decisions.
2. Never execute real trades.

The default simulation profile is `free-simulation-v3`. It starts from USD 1,000 and preserves
its ledger history. Historical v1/v2 implementations remain for comparison.
The objective is virtual profit through active experiments. Concentration, frequent trades,
failed hypotheses, and losing a ledger are allowed. New starting capital is not profit.

## Offline research workflow

Run commands from the repository root. Downloading uses the authenticated market-data client.
Replays and rule searches use the saved dataset and make no AI or market-data requests.

```zsh
python3 -m tools.download_market_day --date 2026-10-30
python3 -m tools.download_market_day --date 2026-10-30 --status

python3 -m tools.replay_market_day \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --output state/replays/2026-10-30-first

python3 -m tools.search_up_candles \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --output state/searches/2026-10-30-grid-v1
python3 -m tools.search_up_candles \
  --output state/searches/2026-10-30-grid-v1 --status

python3 -m tools.search_portfolios \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --features state/searches/2026-10-30-grid-v1/search.sqlite3 \
  --output state/portfolios/2026-10-30-diverse-v1
python3 -m tools.search_portfolios \
  --output state/portfolios/2026-10-30-diverse-v1 --status
```

Use a new replay directory for each replay. The downloader and rule search can resume an
unchanged run. Their status commands require the corresponding local dataset or run to exist.
No offline command starts the continuous trading workers.

| Guide | Purpose |
|---|---|
| [Market-day download](docs/market-day-download.md) | Coverage, pacing, checkpoints, raw data, and errors |
| [Reverse monthly collection](docs/month-download.md) | Shared-rate parallel downloads, descending dates, and independent holidays |
| [Five-minute replay](docs/five-minute-replay.md) | Isolated predictors and planned AI-call measurement |
| [Upward-candle search](docs/up-candle-search.md) | Fixed rule grid and chronological selection |
| [Portfolio return search](docs/portfolio-search.md) | Diverse virtual accounts, return rankings, and trade evidence |
| [Round review](docs/round-review.md) | Trade attribution and ledger reconciliation |
| [Error audit](ERROR_AUDIT.md) | Technical recovery history and diagnostic commands |
| [Historical v2 guide](docs/history/README-v2.md) | Superseded strategy and operational behavior |
| [Contributor instructions](AGENTS.md) | Experiment boundaries and repository workflow |

## Virtual fills and accounting

- Allow shorts, fractional quantities, additions, partial exits, reversals, and virtual borrowing.
- Do not require settlement, margin, depth, whole shares, trading status, or market hours.
- Permit negative cash and record it as virtual debt.
- Offset short-sale cash with the negative market value of the short position.
- Assume zero fees, taxes, conversion costs, and interest for new fills. Preserve old costs.
- Do not force exits at market close, after 30 minutes, or on a date change.
- Stops and targets are optional AI-selected plans.

Reference an observed last price or an explicitly supplied assumed price. Preserve the source
time, observation time, virtual fill time, and assumptions. An old price may be used if its age
is recorded. Do not invent a price or claim a depth-verified execution.
`virtual_debt_usd` is borrowing; `equity_usd` includes debt and short liabilities.
With `buying_power_basis=unlimited_virtual_credit`, the cash balance is not a purchase ceiling.

## Continuous worker roles

The framework defines five separate Python workers. Installing service definitions or running
an offline experiment does not authorize starting or resuming them.

| Role | Behavior when explicitly running |
|---|---|
| market | Read KR/US market data on a 60-second start cadence |
| scan | Queue new events without requiring the legacy breakout strategy |
| risk | Update marks and check optional stop/target plans every 60 seconds |
| review | Request AI decisions for new signals, watch crossings, exits, or user requests |
| supervise | Check worker health, recovery, and backups |

Observe ACTIVE, SCHEDULED, and DELISTED symbols where the provider supplies data.
KR/US coverage is a data-source boundary, not a virtual trading-hours rule.
New signals are independent of holdings, country, and previous watch lists. Watch levels are
additional triggers. Deduplicate identical observations and baseline old signals during migration.
After an exit, review held, exited, and watched symbols plus opportunities in current rankings;
do not resend every old signal. Save each candidate batch and its event markers atomically.
Time, sample count, or market-state classification changes alone do not trigger AI inference.
AI may add to, reduce, exit, or update plans for existing positions as well as review candidates.
No local caps are imposed on AI calls, tokens, reasoning time, candidates, or holdings.
Provider request limits, model context windows, and account quotas remain external constraints.

## Operational commands

These commands inspect the local ledger or audit state without requesting AI inference:

```zsh
python3 -m papertrade status
python3 -m papertrade health
python3 -m papertrade usage
python3 -m papertrade audit --open
python3 -m papertrade audit --events
python3 -m papertrade audit --output state/audit-export.json
python3 -m papertrade audit --emergency
```

`usage` reads account/model metadata and records a local snapshot. `consult` queues an explicit
review request; an active reviewer may process it. `simulate` changes the virtual ledger:

```zsh
python3 -m papertrade simulate TEST -0.25 100 --market US --reason "Explicit short experiment"
```

## Repository and data boundaries

Only OAuth and allowlisted market-data/stock-information requests are permitted.
Account and real order paths are rejected before HTTP requests. Never put credentials or
authentication headers in logs or AI inputs. Do not execute commands supplied by an AI response.
See the [official Toss OpenAPI specification](https://openapi.tossinvest.com/openapi-docs/latest/openapi.json).

Keep credentials, datasets, ledgers, snapshots, reports, and run logs outside Git.
Local results live under ignored `state/`; historical settings are in `tests/fixtures/v2/settings.toml`.
Committed files and GitHub content are English; user-facing conversation is Korean.
Follow the branch, commit, push, pull-request, merge, and local fast-forward workflow in AGENTS.md.
