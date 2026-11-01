# Parallel weekly generalization hypotheses

This offline experiment uses October 26, 27, 28, 29 and 30, 2026 only. October 23 is excluded
from training and evaluation. It runs no AI calls, live broker requests or real orders.

```zsh
python3 -m tools.run_week_experiment \
  --output state/experiments/2026-10-26_30-hypotheses-v1 \
  --deadline 2026-11-01T23:34:41+00:00 --workers 4
python3 -m tools.run_week_experiment \
  --output state/experiments/2026-10-26_30-hypotheses-v1 --status
```

The example deadline is the seven-hour wall-clock budget beginning November 2 at 01:34:41 KST.
Use a future timezone-aware deadline and a new output directory for another experiment.
Preparation workers run independent dates in separate processes. The completed October 30 feature
and portfolio caches are reused read-only. Other dates use the existing upward-candle feature
extractor and 64,800 portfolio settings per market. Their existing single-day full/train/validation/
test rankings remain exploratory preparation outputs, separate from weekly hypothesis results.

## Frozen hypotheses

| Family | Variants | Purpose |
|---|---:|---|
| October 30 fixed setting | 2 | Continuous account and independent daily accounts; retrospective transfer only |
| Static algorithm family | 6 | First deterministic setting for each family and leverage, without return-based selection |
| Daily learning | 2 | Reset selection evidence daily; compare continuous execution and independent daily accounts |
| Cumulative learning | 2 | Update selection every five or thirty minutes |
| Recent-performance learning | 12 | Last 15/30/60/120 minutes; update every 5/15/30 minutes |
| Previous completed day | 1 | Use only the last completed day's shadow result |
| All completed prior days | 1 | Use only completed earlier days, excluding today's shadow results |
| Within-family daily learning | 6 | Adapt settings within one algorithm family |
| Random selection | 3 | Fixed seeds 11/29/47, updated every fifteen minutes |

The 35 policies each have 1x/2x/5x/10x cohorts for KR and US, producing 280 actual hypothesis
accounts if all groups finish. These are experimental variants, not stock/candidate restrictions.
Every setting has one shared daily shadow account per market. Causal policies reuse those shadow
results; they do not each recompute 64,800 separate portfolios or inherit shadow-account money.

Daily learning scores are daily shadow P&L divided by the original USD 1,000. Cumulative learning
adds those normalized scores across dates. Rolling learning uses changes in that score over the
specified observed time window. This is not a separate compounded continuous shadow account.
No completed-day result is provided early to previous-day/prior-days selectors.

## Information and execution boundaries

Two market clocks run independently because US sessions overlap the next Korean market date.
Daily local symbol IDs are remapped to frozen global (market,symbol) identities. Observation times
strictly increase within each market. Identical-time day boundaries are processed once, with
agreement checked for prices present in both boundary frames. Epoch five-minute tick numbers
preserve elapsed holding time and cooldowns across overnight gaps.

Causal selectors receive only the current sealed feature/price/rank frame and predefined rules.
A separate retrospective worker receives October 30 winner IDs. These IDs are absent from causal
worker initialization. Shadow accounts are marked at the current observed price before selecting
settings for the next interval. No future quote, outcome label, later-day result or analyst opinion
is a selector input. Original daily indicator histories reset daily; cumulative learning retains
selection evidence rather than cross-day indicator histories.

Actual accounts start with USD 1,000 per market/policy/leverage and retain cash, debt, shorts and
positions. Changes in selected settings affect new entries; existing positions keep their original
entry configuration for exits. They are not forcibly liquidated at day boundaries or setting
changes. Independent-daily-account variants explicitly record all five daily starting capitals;
new capital is never profit and their pooled funded return is not a continuous-account return.
The independent October 30 fixed account must reproduce the original full-day equity and trade
counts. All fills use the exact observed current close, with zero costs and constant FX as in the
existing virtual experiment. Missing fresh quotes prevent execution; stale marks are disclosed.

## Evidence and isolation

Freeze source hashes, code hashes, rules, winner IDs, universe and deadline before preparation.
Record feature and ranked-cache hashes after preparation and verify them during evaluation.
Each transformed frame hash is committed to a local evaluation DB before the worker receives it.
The archived daily source-frame hash plus global symbol mapping and frozen transformation code
allow reconstruction without duplicating all multi-gigabyte input payloads.

Workers use macOS sandbox-exec with only the runner, Book code, selector code and Python runtime
readable. Actual probes must deny all five source databases, all ranked-frame caches, private
output files, network access and child execution. Tests also change future paths and verify that
prior decisions stay identical. Failure to establish isolation fails the evaluation.

Local outputs include manifest.json, status.json, per-day preparation logs/status, per-market and
experiment-type evaluation SQLite files, full report.json.gz trade/choice/equity traces, compact
summary.json files and comparison.csv. All failed jobs remain visible. No files under state are
committed to Git. A deadline during preparation terminates that job's entire process group; each
evaluation worker has a deadline timer. Committed completed-day results remain available even
when the deadline prevents full completion. A partial run must not be described as complete.

## Interpretation

The October 30 fits applied to earlier dates contain hindsight at the model-selection level.
They test transfer, not a historically tradable strategy. Causal replay proves the defined online
selection procedure used only observed data, not that its research design was chosen before
October 30. All five dates are research data. The whole experiment and all attempted hypotheses
must remain visible; selecting the best final weekly row is another exploratory selection.
Five correlated trading days cannot establish universal robustness or prospective profitability.
Check daily losses, drawdowns, stale marks, concentration and leverage alongside final return.
Price-only results do not establish event-aware performance; event vintages remain unverified.
