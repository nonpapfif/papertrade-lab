# Reviewing a virtual trading round

`tools/review_round.py` connects fills to entry episodes in a frozen snapshot.
It does not open the operating ledger, call market-data/order APIs, or change the strategy.

```zsh
python3 tools/review_round.py state/reviews/round/snapshot.json state/reviews/round/analysis.json
```

## Input

Extract the snapshot in one read transaction. Do not mix later trades into that analysis.
Preserve raw `trades`, `ai_calls`, `positions`, `cash`, and `events` rows, plus `names`, `quotes`,
`job_evidence`, and `cutoff_utc`. Keep row JSON `data`, fill IDs, and event keys intact.

## Fills, episodes, and profit attribution

- Emit one output row per recorded fill. Distinguish entries, additions, exits, and reversals.
- Group a symbol's same-direction position until it closes into one entry episode.
- Link a reversal to the old episode's exit and the new opposite episode's entry.
- Allocate closing quantities to open entry lots in proportion to remaining quantities.
- Mark open lots using the ledger's valuation at the snapshot cutoff.
- Proportional attribution matches average-cost accounting; it is not FIFO accounting.
- Fill realized profit and entry-attributed realized profit describe the same profit. Do not sum them.
- Separate fees and currency effects from local price direction. A price rise need not be profitable.

## Decisions and plan changes

Use execution event keys to connect AI instructions with recorded fills.
Record unexecuted instructions separately. Apply entry plans and later plan changes in time order
when reviewing automatic exits. Candidate records are not necessarily the complete prompt sent
to AI. Preserve the original decision text separately from the execution record.

The difference between a stop line and a fill is a same-quantity counterfactual comparison,
not a verified loss saving or missed profit under a different strategy.
Keep unrealized and realized results separate. Exclude open episodes from closed-trade win rates.

## Verification and storage

Check that every fill ID occurs once. Reconcile cash, quantities, equity, fill profit, and
entry-attributed profit with the source. Raise an error if the USD difference exceeds
`0.000000001`; do not add a balancing adjustment. Unknown AI usage is not zero usage.

Keep ledgers, snapshots, and review output under ignored `state/reviews/`.
Commit only the analyzer, synthetic tests, and methodology.
