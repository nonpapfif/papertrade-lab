# Cash-only whole-share capital sweep

This opt-in experiment replays October 26-30, 2026 with USD 50, 100, 200 and 500 starting capital.
Shorts, fractional quantities and borrowing are excluded from actual and learning accounts.
KR and US are evaluated separately. October 23 is excluded. No broker or AI calls are made.

```zsh
python3 -m tools.run_week_experiment --whole-shares \
  --capitals 50 100 200 500 --krw-per-usd 1334.05 \
  --prepared-output state/experiments/2026-10-26_30-hypotheses-v1 \
  --output state/experiments/2026-10-26_30-whole-shares-v1 --workers 4
python3 -m tools.run_week_experiment \
  --output state/experiments/2026-10-26_30-whole-shares-v1 --status
```

`--whole-shares` implies long-only execution. Capitals must be positive, finite and distinct.
An explicit positive `--krw-per-usd` is required. Runs have no automatic timeout unless a
`--deadline` is supplied. The eight market/capital evaluations use at most the requested number
of Python workers and exit on completion or a recorded failure. Old results remain intact.

## Compared accounts

The existing 63 long-only policies are reused: 25 independent daily policies, 25 continuous
counterparts, twelve weighted winner/ensemble policies and one previous-day selector.
Each has four starting capitals in two markets, yielding 504 actual accounts and 2,520 daily rows.
Only the 7,988 long-entry, 1x configurations per market are eligible learning candidates.
Every market/capital worker recomputes its shadow executions with that capital and whole shares.
Learning results from the USD 1,000 fractional experiment are not reused.

Cold daily accounts receive their cohort's initial capital each day and reset their learning.
Their total funded capital is five times the daily starting amount: USD 250, 500, 1,000 or 2,500.
Continuous accounts receive initial capital once, retain cash and positions, and carry the
defined selection evidence. Scores, fixed-notional sizing and returns use each cohort's actual
initial capital. Winners remain exploratory selections from the same five research dates.

## Quantity, prices and cash

Entries buy positive integer native shares. Exits sell the original held integer quantity.
Requested allocations are rounded down to whole shares, cannot exceed available cash, and leave
the remainder as cash. Allocation/slot rules can prevent a purchase even when one share is below
the total account balance. Ranked unaffordable candidates are skipped and lower-ranked eligible
candidates are considered. Full current numerical features are used, so the old top-100 cached
rank bound cannot hide an affordable candidate. No artificial stock-price ceiling is fixed at
the initial capital; affordability follows current cash and the strategy's allocation.

For US stocks the source price is already USD per native share. For KR stocks, all source quotes
and marks are divided by the fixed KRW/USD assumption before accounting. Quantity therefore
means actual whole shares rather than the earlier normalized fractional exposure. The example
rate 1,334.05 was archived locally as `fx:USD:KRW` at 2026-10-30T06:29:43.522693+00:00. It is a
constant research assumption for all five dates, not a reconstructed daily or point-in-time FX
series. Currency forecasting and currency P&L are not evaluated.

Trade traces label accounting prices as USD and also preserve converted-back native prices,
native currency and the fixed FX value. The original quote and feature seals remain recorded;
dimensionless signal features are unchanged by the conversion. Every actual trade trace is
audited for integer quantities, existing-share sales, correct cash movements, nonnegative cash
within numerical tolerance, and correct initial funding before completion is declared.

## Reports and retained assumptions

- `comparison.csv`: starting capital, market, policy, correct funding, return, drawdown,
  win/loss/flat counts, profit factor and losing streaks.
- `daily-comparison.csv`: each date and account separately, including uninvested cash through
  account valuation, closed-trade statistics, open positions and stale marks.
- `losing-trades.csv`: every realized loss with whole quantity, USD/native prices, timestamps,
  original configuration and exit reason.
- Full compressed trade/choice/curve reports, sandbox probes, sealed inputs, manifest and errors
  remain available under the experiment output directory.

Current-close fills, zero costs, constant FX and the existing source session coverage remain
simulation assumptions. Broker trading eligibility, liquidity and actual execution quality are
not established. Open positions are marked rather than forcibly liquidated. The continuous
real-trading workers remain disabled; this command only runs offline research.
