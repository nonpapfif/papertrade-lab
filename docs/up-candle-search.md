# Upward-candle rule search

```zsh
python3 -m tools.search_up_candles \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --output state/searches/2026-10-30-grid-v1

python3 -m tools.search_up_candles \
  --output state/searches/2026-10-30-grid-v1 --status
```

Make no AI, market-data, order, or existing-ledger calls. Store forecasts and scores in a separate DB.
Open the source read-only. Reuse the predictor sandbox and actual access-denial checks.
Pass only indicators known at the current time. Save conditions before exposing the next outcome.

## Fixed search space

Use six families, five lookbacks, five movement thresholds, five volume thresholds, and four noise
thresholds: 3,000 combinations plus always-up and never-up baselines.
Freeze parameters and source/code hashes before processing. Reject a restart with changed inputs.

| Dimension | Values |
|---|---|
| Family | Momentum, reversal, upper breakout, mean reversion, transition, volume-weighted trend |
| Lookback | 5, 10, 15, 30, 60 minutes |
| Upward score threshold | Above 0%, or at least 0.1%, 0.3%, 0.5%, 1% |
| Relative volume | No filter, 1x, 1.5x, 2x, 3x |
| Relative noise | No filter, 1x, 2x, 3x |
| Outcome threshold | Above 0%, or at least 0.1%, 0.3%, 0.5%, 1% |

Score formulas:

- Momentum: scale the lookback return to five minutes.
- Reversal: negate the momentum score.
- Upper breakout: compare price with the prior lookback high, excluding the latest five minutes.
- Mean reversion: scale the difference between mean lookback close and price to five minutes.
- Transition: subtract the earlier period's return per five minutes from the latest five-minute return.
- Volume-weighted trend: multiply momentum by the observed increase in relative volume.

Compare recent five-minute volume with the prior lookback's average per five minutes.
Scale mean absolute returns between consecutive observed minute bars to five-minute noise.
Use a 1e-12 denominator floor when historical noise is exactly zero.
Do not invent absent indicators. Preserve actual quote and reference timestamps.

## Samples and selection

An outcome is the target close relative to the current close. An intervening high is not the label.
Score only observed exact endpoints. Do not turn missing bars into zero returns.
Insufficient indicators produce no upward prediction; `eligible` separately counts applicable cases.

`all` contains every scorable case. `common` requires indicators for every family/lookback.
Use `common` for primary comparisons so different warmup periods and indicator availability do not
confound selection. Available indicators do not prove that the underlying minute data are complete.

Split each market's calendar time into 60% training, 20% validation, and 20% testing.
Align boundaries to five minutes and reject forecasts crossing a split boundary.
Select the best training parameters per family/lookback. On validation, choose the highest-F1
candidate or baseline. Break ties by precision, true-positive count, then configuration ID.
Never use test scores to select parameters. Do not claim a valid winner without positive outcomes.

Select chronological winners separately for KR and US. Their split times differ, so do not merge
market-specific folds into a pooled chronological test. Pool only full-day exploratory rankings.
Pooled scores classify samples; they are not combined account returns.
Store precision, recall, F1, TP/FP/FN/TN, and sample counts.
For selected configurations, include test-session scores and chronological error examples.
Inspect session composition: KR and US test periods may represent different trading sessions.
Mean return after an upward signal is an observed conditional mean, not trading-account profit.
Full-day rankings are exploratory and do not represent next-day validation.

## Storage and restart

`search.sqlite3` stores sealed per-step features/conditions, later outcomes, and all configuration scores.
The isolated predictor cannot read this DB. Compute features once and combine fixed predicate
bitmaps instead of rereading the source for every setting. Never incorporate future observations.

Resume with the same command. Rebuild past history and reuse sealed frames after an extraction
interruption. Commit each configuration's scores atomically and skip finished configurations.
Use a file lock to prevent duplicate runs.
`status.json` and `--status` show progress and errors. Write `report.json` only after scoring and
integrity verification. Keep logs, source data, and results under ignored `state/`.
