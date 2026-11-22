# Paired strategy review

Run one dated paper comparison after reviewing a recorded loss. The command freezes the market
calendar, source tree, evidence hash, strategy settings, starting capital, and decision rule
before opening. It writes to a new output directory and leaves the original experiment records
available for replay.

| Market | Loss evidence | Incumbent and candidate |
| --- | --- | --- |
| KR | November 9 audited loss replay, `audited-snapshot-check.json` | Rebound with equal allocation. The candidate requires positive proceeds after entry and exit costs at the rebound target. |
| US | November 19 Day Trader comparison, `comparison.json` | Day Trader opening-range breakout with minimum reward/risk 1.0 versus 2.0. Both accounts use the same native target and scanner without research gating. |

Use the existing WebSocket environment and the market-data credentials. The US command also
uses the pinned Day Trader checkout and its existing Nasdaq and Yahoo premarket scanners.
Run the command before the specified market opens. It waits for the verified date and opening.
Keep the process and its checkout available through close.

```sh
python -m tools.strategy_review_cycle --market KR --day YYYY-MM-DD \
  --evidence /absolute/audited-snapshot-check.json --output /absolute/new-kr-experiment
python -m tools.strategy_review_cycle --market US --day YYYY-MM-DD \
  --evidence /absolute/comparison.json --source /absolute/day-trader-3d0f4ef \
  --output /absolute/new-us-experiment
```

Each new comparison uses two independent equally funded paper accounts. Account balances
and positions remain recorded in their own sessions. A later comparison is a separately
funded experiment with an explicit link to the prior decision.

`plan.json` records the setup. `decision.json` links the plan to the verified report and records
net results and the next action. An interrupted or unmarkable comparison calls for evidence
repair. A positive first result calls for another unseen session. Underperformance calls for
reviewing the trades and revising the candidate. A single session cannot authorize promotion.
After repair or a retest decision, use `--previous-decision /absolute/decision.json` to link the
next pair to the original evidence and preserve the same starting capital.
