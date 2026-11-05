# Finite asynchronous live integration validation

Run from the repository root with the optional WebSocket dependency:

```sh
uv run --no-project --with websockets==15.0.1 python -m tools.run_async_live_validation \
  --output state/validation/NEW_RUN/run --duration 1200 --capital 100
```

This starts a new isolated virtual account and terminates after the declared observation duration.
It never starts the legacy live bot or historical collector. Credentials must already be valid;
the cached-token client cannot issue or refresh tokens. Only market-data and stock-information
endpoints and market WebSocket topics are used. This default mode makes no AI calls.
See [constrained runtime profiles](constrained-runtime.md) for KR and US native-currency
operation, adaptive exploration, and scheduled recovery.

## Fixed-strategy comparison

Add `--candidate-policy` to run a candidate beside the existing `router` incumbent, using the
same live collector and two independent paper accounts:

```sh
uv run --no-project --with websockets==15.0.1 python -m tools.run_async_live_validation \
  --output state/validation/NEW_COMPARISON/run --duration 1200 --capital 100 \
  --candidate-policy trend
```

The shown policy is a usage example, not a validated recommendation. Supported candidates are
the existing `router`, `trend`, `breakout`, and `rebound` policies. Omitting the option keeps the
single-account path and its existing status shape. The comparison is descriptive; it does not
select, optimize, or automatically promote a strategy.

Before collector tasks start, `manifest.json` records both fixed configurations, equal starting
capital, matching fees/delay/allocation assumptions, and source hashes including execution
dependencies. Each accepted book and clock event has one common sequence/logical time.
Accounts use their own positions, pending requests and policy when counting candidates.
Comparison candidate features use the latest valid known book; shared subscription protection
is the union of both accounts' held and pending symbols. Accounts do not share cash or depth use.

In comparison mode, `status.json` adds a `comparison` section while `paper` still denotes the
incumbent. Intent/outcome log records identify the account. `comparison_observation` records
publication sequence, receipt/decision time and the common book-input digest.
Per-account delivery counts/hashes include accepted books and idle clock events. They detect
incomplete fanout; they are not a complete exact-live-replay trace format.

After recording files close, `comparison.json` saves both full summaries, configurations,
source/freeze checks, account input digests, interval, coverage/status information, and hashes
of the manifest, raw market recording and event log. Positions, fees, unrealized marks and
pending reservations remain visible without invented liquidation.
`complete` requires clean completion, present evidence, aligned inputs and unchanged source
and configuration. Interrupted/failed runs remain incomplete; missing book activity is reported
as inconclusive. `qualification=not_assessed` applies even when technical checks pass.
Start/end file hashes record source consistency; this runner does not load an immutable code
artifact or calibrate actual broker execution latency.

The new comparison path is covered by offline fixtures. A live comparison has not been run
as part of this implementation; a separate explicitly started campaign supplies that evidence.

## Selection and processing

Each REST sweep reads the active US universe in batches of 200. The first sweep establishes a
baseline. Subsequent sweeps rank absolute price changes between consecutive fresh observations.
Only verified active USD common shares priced within initial capital are considered. This is a
short-horizon integration hypothesis, not a validated profitability filter. Candidates refresh
every sweep, nominally once a minute; this intentionally exercises subscription replacement more
frequently than the baseline five-minute selection design.

One WebSocket connection monitors trade and book channels for up to 50 symbols, based on the
provider's 100-topic connection capacity. Held and pending symbols take priority over candidates.
The unmonitored universe is observed by REST only. No claim of continuous whole-universe coverage
is made. Candidate candles, initial/resync books, and FX are fetched sequentially in worker threads
using one shared client and its rate controller. The WebSocket receive task keeps running during
these REST calls. No account mutation awaits REST or a future market observation.
A rate limit during a per-symbol candle or book request pauses REST until the client's retry
time or the observation deadline. Existing WebSocket subscriptions continue during the pause.
The failed request remains recorded; the sweep proceeds without filling in missing data.

A consumer preserves publication order and stamps the actual decision time when processing starts.
Late REST snapshots cannot overwrite newer source timestamps. Feature receipt time is retained.
Queue and processing latency distributions are recorded globally and per symbol. The provisional
integration budget is queue-delay p95 below one second; this is not an exchange latency guarantee.

## Controlled faults and evidence

- The third sweep waits five seconds before requesting data. Book processing during this pause
  must continue for that check to pass.
- Halfway through the observation window, the client closes its own WebSocket, waits two seconds,
  reconnects, and redeclares the current desired subscriptions. An immediate REST sweep is requested.
- Network disconnects retry after the same backoff within the finite campaign. Authentication or
  subscription rejection stops the run with a sanitized failure. Provider faults are not induced.
- Every acknowledgement records the protected set. A missing protected subscription fails the check.

`status.json` contains runtime state, checks, account results, reservations, timing, and counters.
`events.jsonl` records selection, connection, intent, and outcome events. `market.jsonl.gz` preserves
received market frames and REST prices, candles, metadata, FX, and books. `manifest.json` fixes source
hashes, selection assumptions, and the provider calendar. Missing activity or unexercised replacement
is inconclusive rather than a pass. Existing ledgers are untouched.

The end time is an observation deadline. In-flight HTTP transport cleanup can take slightly longer.
Open positions and reservations are reported without invented liquidations. Durable crash resume
is available with an adaptive journal. Broker queue priority and multi-session generalization
remain unverified.
