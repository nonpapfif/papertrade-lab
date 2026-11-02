# Long-only daily and weighted weekly experiments

This experiment excludes short positions from both actual accounts and learning shadow accounts.
It uses October 26-30, 2026 only, separately for KR and US. October 23 is excluded. No broker,
account, order, or AI calls are made.

```zsh
python3 -m tools.run_week_experiment --long-only \
  --prepared-output state/experiments/2026-10-26_30-hypotheses-v1 \
  --output state/experiments/2026-10-26_30-long-only-v1 --workers 4
python3 -m tools.run_week_experiment \
  --output state/experiments/2026-10-26_30-long-only-v1 --status
```

Use a new output directory. `--prepared-output` must identify a completed earlier experiment.
Its original source, feature and portfolio seals are verified before reuse. Cached full-day
performance rankings and October 30 winner identifiers are not sent to the predictors.
Existing short-enabled experiments and their output directories remain intact.
Long-only runs have no automatic timeout unless `--deadline` is supplied. They exit on completion
or a recorded failure. Preparation uses the requested concurrency; KR and US evaluate in two
separate isolated worker processes. No AI polling or recurring automation is installed.

## Frozen comparisons

The 63 policies each have 1x, 2x, 5x and 10x cohorts in KR and US: 504 actual accounts.
Only the 21,600 long-entry configurations per market are eligible shadow candidates.
The six algorithm families are momentum, reversal, breakout, mean reversion, transition and
volume trend. Their existing parameter grid varies lookback, movement, volume, noise, ranking,
holding, exits, slots, session, cooldown and compounding.

| Experiment | Policies | Behavior |
|---|---:|---|
| Cold daily accounts | 25 | Six static families, all-family daily learner, six within-family daily learners, twelve rolling learners |
| Continuous counterparts | 25 | Same selectors with actual cash, debt and positions carried across days |
| Weighted prior-day or online learners | 12 | Three date weights, two update rules, winner or family ensemble allocation |
| Previous-day learner | 1 | Select from the most recent completed day and freeze that choice during the next day |

Cold accounts start each day with USD 1,000 and no selection history. Five daily accounts have
USD 5,000 total funded capital; their pooled return is never presented as a continuous USD 1,000
account. Open positions are valued at available marks rather than inventing an end-of-day sale.
Daily and rolling learners use only observed current-day shadow results. Continuous daily/rolling
counterparts carry their actual ledger, while their learning window still restarts each day.

Weighted selectors score daily shadow P&L divided by the shadow's daily USD 1,000 initial capital.
They average those scores with equal weights, chronological linear weights 1..n, or exponential
weights 1,2,4,... . Prior-only variants choose once at the first frame using completed earlier
days. Online variants include the observed portion of today and update every five minutes.
Friday's prior-only weights are therefore 1:1:1:1, 1:2:3:4 or 1:2:4:8 for Monday through Thursday.
Monday has no prior-day rewards and uses deterministic configuration-ID tie breaks.

Winner variants choose one configuration. Ensembles choose the best configuration within each
family and convert family scores to normalized softmax weights. Those weights set targets for
new investment. Existing positions keep their entry configuration's exit rules. Holdings are
not forcibly liquidated when weights, settings or dates change. Duplicate symbols are not bought
twice across families; unavailable allocations remain uninvested. No shadow profit is transferred
to the actual account. All new investment subtracts surviving gross exposure from the account's
equity times its leverage cohort; losses and debt are preserved.

## Causal input and execution

Workers retain the existing file, network and child-execution sandbox. The original market,
feature, portfolio and evaluation databases are inaccessible to them. Inputs are sealed before
delivery. Ensemble ranking uses the full current exact-quote feature universe, avoiding the
original single-configuration cache's top-candidate bound. Only numerical features are projected;
cached decisions, future labels and completed-day rankings are excluded. Source feature hashes
are recorded alongside transformed input hashes for reconstruction.

KR and US have separate clocks. A shared US day-boundary timestamp is validated against the
previous day and delivered to initialize the new cold account and its selection cadence. The
continuous actual account does not trade again at that same instant. Other duplicate or backward
frames are rejected. Prices must be positive, finite and known by the observation time.

Fills use the observed current close, zero costs and constant FX. Only positive long entries and
sales closing existing longs are permitted. Missing fresh quotes prevent fills. Virtual borrowing
and fractional quantities remain simulation assumptions. Price-only results do not include events
whose original publication-time availability remains unverified.

## Results and failure evidence

- `comparison.csv`: account returns, drawdowns, concentration, closed-trade win rates including
  flat trades, wins/losses/flats, gross profit/loss, profit factor and longest losing streak.
- `daily-comparison.csv`: all five dates individually, correct daily closed-trade counts, funding,
  P&L, open positions, stale marks and explicitly cumulative drawdown.
- `losing-trades.csv`: every realized losing trade, symbol, entry/exit timestamps and prices,
  configuration, position return, dollar P&L and exit reason. Timestamps retain UTC offsets.
- `results/*/report.json.gz`: all trades, choices, ensemble weights, curves and shadow scores.
- `manifest.json`, per-stage status, isolation probes and evaluation databases: provenance and
  failures. A failed or interrupted run is never marked complete.

An empty losses file means no realized losses and does not establish that open positions are
profitable. Weekly leaders are selected after observing these same five dates. They remain
exploratory research, not untouched prospective evidence or proof of real-broker feasibility.
