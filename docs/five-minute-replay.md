# Five-minute forecasts and AI-call planning

```zsh
python3 -m tools.replay_market_day \
  --dataset state/datasets/2026-10-30/market.sqlite3 \
  --output state/replays/2026-10-30-first
```

Use a new output directory for every replay. Open the source database read-only.
Do not call live market data, account/order paths, the existing virtual ledger, or an AI client.
This stage compares forecasts and measures planned calls; it creates no AI decisions or trade ledger.

## Time and isolation

Advance virtual time by five minutes. Build inputs using only bars through time t, then commit
the forecast. Advance to t+5 minutes to score it. Future OHLC values are not in the input.
Use the collected market scope and stop creating forecasts whose targets extend beyond it.
Inspect every symbol with new bars in the current interval. Do not cap the symbol or candidate count.

Run the predictor in a separate macOS sandbox-exec process. Deny network, file writes, and execution
of other programs. Deny home/source/output reads, except the Python runtime and predictor script.
Actually test source DB, private marker, and result DB reads plus network and program execution.
Abort if any isolation check fails. Pass predictor inputs only through standard input.
Only the scorer reads the source and exposes the next interval after forecasts have been committed.

## Initial forecast algorithms

| Name | Next-five-minute return forecast |
|---|---|
| flat | Zero |
| up_10bp | Constant +0.1% baseline |
| momentum_5m | Previous five-minute observed return |
| reversion_5m | Negative of the previous five-minute observed return |
| trend_15m | Previous 15-minute observed return divided by three |
| acceleration | Latest five-minute return minus the prior ten-minute return per five minutes |

Freeze formulas before replay. Return null when history is insufficient.
If a reference bar is absent, use the last earlier observation and retain its actual timestamp.
These references do not always represent exact five- or 15-minute trade intervals.
Record the current price's source time, age, and count of recent minute observations.

Score only when both current and target endpoint prices are observed at their exact timestamps.
Mark absent endpoints `missing_exact_endpoint`; do not fill them with zero or interpolation.
Directional accuracy uses cases with nonzero realized returns.
Record forecast error and unit-exposure directional return; neither is a trading-account return.
These are same-day exploratory scores, not validated future performance or a proven best algorithm.

## AI-call candidates

- `volume_breakout`: break the earlier 15-minute high/low with at least twice observed volume.
- `unusual_move`: a move of at least 0.5% and three times the earlier minute-noise five-minute scale.
- `trend_reversal_conflict`: aligned five-/15-minute trends with a three-standard-deviation price
  displacement, at least 0.5% movement, and at least twice observed volume.

Do not resend a continuously active symbol/event/direction combination.
Rearm after the condition clears or direction changes. Batch a time step's candidates into one
planned AI request; create no request when there are no candidates.
Holding-hypothesis invalidation belongs to a later trading integration.

Every AI request here is `planned_only`; no model is called.
Measure request count, candidates, and input bytes. Bytes are not token counts or reasoning/output use.
Use those measurements to revise a later experiment. Changed criteria require a new version and
output directory so the original result remains available.

## Outputs

- `manifest.json`: source/code hashes, formula names, and isolation results.
- `replay.sqlite3`: input/forecast hashes, symbol forecasts, later outcomes, and planned requests.
- `report.json`: accuracy, errors, call candidates, and input sizes.

Store output under ignored `state/`. The source file hash must match before and after replay.
