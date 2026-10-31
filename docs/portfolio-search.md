# Diverse portfolio return search

This experiment ranks virtual account returns rather than upward-candle classification scores.
It reads the completed market-day dataset and the sealed causal feature frames from the earlier
upward-candle search. It makes no AI, market-data, account, or order API calls.

```zsh
python3 -m tools.search_portfolios \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --features state/searches/2026-10-30-grid-v1/search.sqlite3 \
  --output state/portfolios/2026-10-30-diverse-v1

python3 -m tools.search_portfolios \
  --output state/portfolios/2026-10-30-diverse-v1 --status
```

Use a new output directory after changing source data, sealed features, code, or parameters.
An unchanged interrupted experiment can restart. Sealed input construction resumes from saved
frames. An interrupted simulation replays from the beginning to reconstruct account state and
replaces result rows by their stable keys. A completed simulation skips that expensive stage.
A file lock prevents two controllers from writing the same run.

## Parameter coverage

The fixed entry grid contains 5,400 signal templates:

| Variable | Values |
|---|---|
| Signal family | Momentum, reversal, breakout, mean reversion, transition, volume-weighted trend |
| Lookback | 5, 10, 15, 30, 60 minutes |
| Direction | Long, short, both |
| Positive oriented-score threshold | Greater than zero; at least 0.1%, 0.3%, 0.5%, 1% |
| Relative-volume filter | Disabled, 1.5, 3 |
| Relative-noise filter | Disabled, 2 |
| Candidate preference | Strongest signal, nearest qualifying threshold |

Scores and available-history behavior are inherited from the
[upward-candle search](up-candle-search.md). Thresholds apply to each family's five-minute-scaled
score, rather than directly to the lookback return. In `both`, a positive score produces a long
candidate and a negative score produces a short candidate. Volume and noise filters are unsigned.
Ties use a stable market/symbol identifier, with no realized future return in the ranking.

Every template receives 12 portfolio profiles. Two shared profiles are single-position/five-minute
and five-position/fifteen-minute long-or-short simulations at 1x with equal compound allocation.
The remaining ten are sampled without duplicates using seed `20261101`, fixed before replay:

| Variable | Values |
|---|---|
| Portfolio slots | 1, 3, 5, 10, 20 |
| Entry notional multiplier | 1x, 2x, 5x, 10x |
| Planned holding period | 5, 10, 15, 30, 60, 120 minutes, no timed exit |
| Exit plan | None; 1% or 3% target; 1% stop; symmetric 1% or 3% target/stop; 1% or 3% trailing |
| Allocation | Equal weights, proportional signal strength |
| Capital basis | Current positive equity, original starting capital |
| Entry sessions | All observed sessions, regular session, other observed sessions |
| Rank exit | Disabled, exit when outside the current preferred ranks or direction changes |
| Reentry cooldown | 0, 1, 3 five-minute checks |

This produces **64,800 configurations per market**, **129,600 market/configuration pairs**,
and **518,400 account results** across full-day, train, validation, and test phases.
The portfolio profiles are a broad deterministic sample, not every possible Cartesian combination.
Slot counts and multipliers are experimental variables, not global trading restrictions.
The report separates leverage groups so its impact is visible.

## Causality and execution

The controller verifies source hashes and original sealed feature-frame hashes. It reads only the
features from that cache; original outcome labels are not inputs to this experiment.
Ranks use exact current quotes, without filtering for whether a future quote will exist.
Every new request is sealed into the output database before the portfolio worker consumes it.

The worker receives only the current five-minute frame and fixed parameters. It runs in the macOS
sandbox with source/output/home reads, network access, writes, and external execution denied.
Actual file, network, and child-execution denial probes must succeed. The worker can read its
script and Python runtime. Prefix-invariance tests check that later price changes cannot alter
earlier account results.

All virtual entries and exits use the current exact five-minute closing quote. There is no
intrabar stop/target fill, high/low hindsight fill, or assumed queued execution. If a price crosses
a level between checks, an exit uses the observed check price. Missing current quotes prevent
execution. Held positions use the latest minute close observed by the current check. A mark whose source
time precedes the check is disclosed as stale. Such marks do not permit a fill without an exact
current quote.
The same-close fill is an explicit idealized assumption, not a claim of executable market depth.

The full universe is considered. Each template retains the first 100 qualifying candidates per
market. This is sufficient for the tested policies: at most 20 holdings and three checks of
cooldown exclusions can precede the next 20 eligible entries. It preserves the tested selections
without sending every lower-ranked candidate to every account. Extending slot/cooldown parameters
requires updating this mathematical bound as well.

## Accounting

Each market and phase receives an independent virtual USD 1,000 account. KR and US accounts are
never pooled. Return ratios assume constant FX; normalized quantities equal virtual USD notional
divided by the raw quoted price. KR quantities are not actual share counts and no historical FX
profit is included. A scalar currency conversion would leave these per-market returns unchanged.
Fees, taxes, spreads, slippage, and borrowing interest are zero.

Opening a position transfers its signed notional from cash to position value. Short proceeds are
offset by the negative position liability. Negative cash remains debt. Ending equity must reconcile
with initial capital plus realized and unrealized P&L. Negative equity and drawdowns above 100%
are preserved, with no fresh funding or discarded losing accounts.

Equal allocation assigns each new position one slot of the chosen capital basis times its entry
multiplier. Score allocation distributes the budget for the newly filled slots by their positive
signal strengths. Existing positions are not continuously resized, so actual gross leverage can
drift. Positive-equity compounding cannot open new positions at nonpositive equity; original-capital
profiles can continue borrowing and trading. These are different allocation hypotheses.

Exit conditions are tested on fresh quotes even outside the chosen entry session. Timed exits,
rank exits, stops, and targets are optional sampled strategy parameters. The experiment does not
force liquidation at phase end or market close. Ending equity includes open positions, short
liabilities, debt, and disclosed stale marks.

## Ranking and retained evidence

Each market's clock is split 60%/20%/20%, aligned to five minutes. Four independent books are scored:
full day, train, validation, test. At a boundary the old book is valued without opening new trades;
the next book starts with its own USD 1,000 using only the current and earlier observations.
This prevents trades or profits from carrying across selection phases.

Training returns shortlist one configuration per family, lookback, direction, and multiplier.
Validation returns select one winner per market/multiplier. Test returns never choose those winners.
Full-day leaders are explicitly exploratory fits. Their returns do not prove next-day performance.
The sessions represented by each phase differ; inspect the manifest's calendar boundaries.

Outputs stay under the ignored local output directory:

- `status.json`: stage, timestamp, controller PID, replay tick, and result count.
- `portfolios.sqlite3`: fixed parameters, source hashes, symbol map, sealed requests, and all results.
- `rankings.csv`: all 518,400 account results sorted by market, phase, and ending equity.
- `report.json`: ten full-day leaders per market/multiplier and chronological validation winners.
- `details` table: complete fills, equity curves, and ending inventories for those leaders/winners,
  across all four phases. Symbol identifiers map to `metadata.symbols`.

The detail pass repeats the same sealed causal replay for selected configurations. Every detailed
summary must exactly match its original result before it is stored. No detailed replays are called
new out-of-sample validation. Final `stage=complete` requires detail verification, unchanged source
hashes, the expected result count, and a successful SQLite integrity check.

No offline command starts or resumes the continuous market-trading workers.
