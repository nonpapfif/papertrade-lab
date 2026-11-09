# November 9 KR loss diagnosis

## Scope and evaluation boundary

The user requested reproduction of the November 9 KR ledger and separate comparisons of
selection, entry, exit and costs before the next US paper session. Real trading and broker
account/order access remain forbidden. All replays use recorded inputs without AI calls.

The observed interval is 11:13-15:20 KST, not a complete regular session. Each account began
with KRW 1,000,000. The incumbent used trend/all/equal. The challenger used rebound/all/equal
and changed allocation to priority at 14:18. Comparing their final returns does not isolate
the effect of AI. Neither final marked equity nor a successful replay is an executable
liquidation guarantee.

The original journal, frozen source and account losses remain intact. Development comparisons
condition on the recorded subscription coverage. A changed strategy could have protected
different symbols in a live run; replay cannot invent those missing observations.

## Candidate fixed before reading comparison results

`rebound_net_target` retains the original rebound signals and holding rules. Its extra entry
condition is that proceeds at the original mean target, after modeled sell charges, exceed
the all-in cost at the proposed purchase price. It checks the decision quantity and checks
again using the actual conditional fill quantity and depth-weighted price. This matters for
partial fills and US minimum sell charges.

The target is an existing exit reference, not a forecast that price will reach it. This
condition can remove economically inconsistent entries but cannot establish profitability.
The original rebound policy remains available as a control. No spread threshold, holding
period, stop level or target multiplier is optimized against this session.

The original eight configurations remain available. Four corresponding configurations of the
new policy may enter the next paper campaign only after source-bound capacity measurement
and independent review. Matching old/new rebound candidate accounts supplies the direct
comparison; a trend incumbent is not an AI-effect control for a rebound candidate.

KR development results do not qualify a US strategy. The next live observations are a new
validation period. No automatic winner promotion is authorized by this diagnosis.

## Initial verified ledger totals

| Metric | Trend incumbent | Adaptive rebound challenger |
| --- | ---: | ---: |
| Final marked equity, KRW | 890,482.5148 | 711,184.56405 |
| Net return | -10.95175% | -28.88154% |
| Recorded commission and tax, KRW | 22,785.9852 | 71,303.93595 |
| Same fills with recorded charges added back | -8.67315% | -21.75115% |
| Gross purchase-plus-sale turnover, KRW | 20,506,248 | 62,640,973 |
| Entries | 246 | 161 |
| Open positions | 19 | 2 |

Charge add-back is an accounting decomposition. It is not a zero-fee rerun with different
cash availability. Quote spreads and execution prices remain in that decomposition.
The challenger traded fewer entries but much larger total notional. Its losses cannot be
explained solely by explicit charges. The incumbent has one stale final mark, preventing
a complete bid-liquidation estimate.

The final status reports no terminal fault. Detailed events nevertheless contain one
recovered WebSocket disconnect and 210 rate-limited symbol requests. A clean terminal state
does not mean uninterrupted data coverage.

## Recorded-fill decomposition

Reconstructing 526 incumbent fills and 456 challenger fills from their recorded intents and
confirmation books reconciles cash, charges, realized net P&L and final inventory to less
than KRW 0.0000001. The table holds those fills and quantities fixed. It decomposes final
marked P&L into movement between confirmation midpoints, quoted half-spreads, depth beyond
the best quote, and explicit charges. Midpoint prices are analytical references, not assumed
executable fills.

| Contribution to marked P&L, KRW | Trend | Rebound |
| --- | ---: | ---: |
| Midpoint movement before charges | -7,599.50 | -35,904.00 |
| Quoted half-spread burden | -77,444.00 | -164,507.50 |
| Depth beyond best quote | -1,688.00 | -17,100.00 |
| Commission and tax | -22,785.9852 | -71,303.93595 |
| Total | -109,517.4852 | -288,815.43595 |

Of 161 rebound entries, 75 bought at or above their original mean target. Associated closed
net P&L was approximately KRW -147,889.61. This is an association, not the profit improvement
that rejecting those entries would produce: changed cash and holdings affect later trades.
Separately, 88 rebound exit fills followed a target signal while the decision bid remained
below the target and momentum remained valid. Their approximately KRW -100,025.67 net P&L
overlaps other groups and must not be added to the entry group.

Median completed holding duration was approximately 902.5 seconds for trend and 90.1 seconds
for rebound. Six trend positions and 56 rebound positions closed within 60 seconds. Neither
account had a completed position bought and sold on the same feature bar. The evidence does
not establish a duplicate-event immediate round-trip bug.

Selection, entry and exit effects are not additive accounting categories. Only isolated
counterfactuals on the same causal tape can compare those rules. Even those comparisons
retain the subscription-coverage and development-sample limits above.

The selection alternative is a 23-basis-point spread screen within the recorded eligible
watchlist. It is an isolated sensitivity case, not an optimized threshold or a replacement
for full-universe discovery. The entry alternative adds the net-target check. The exit
alternative requires the observed bid to reach the original target for a target exit while
retaining momentum-invalidation exits. The cost alternative reruns with zero modeled charges
and therefore changes cash, sizing and subsequent trades; it is distinct from charge add-back.

An additional frozen-equal run isolates the recorded allocation change. Compare
`entry_rebound_net_target_equal` against `allocation_frozen_equal` to isolate the entry
condition under equal allocation. Comparing it directly against the adaptive original changes
two factors. Historical pre/post allocation-change profit is descriptive, not a matched test.

## Reproduction

Use the original frozen source for imports and the diagnosis tool from the reviewed checkout:

```sh
PYTHONPATH=/absolute/frozen-source python /absolute/checkout/tools/replay_kr_layered_loss.py \
  --capture /absolute/kr-trend-rebound-20261109 \
  --output /absolute/new-report.json
```

The tool requires the sealed journal and completed status to agree, verifies frozen-source
identity and every committed checkpoint, and compares the stable final account state.
Output must not already exist. It never opens the source journal for writing, fetches market
data or invokes AI. Timing counters are excluded from equality because wall-clock replay
speed is not the original live processing latency.
