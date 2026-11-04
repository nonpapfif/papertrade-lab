# Asynchronous layered paper execution

`tools/async_layered_paper.py` separates decision-time intents from later quote confirmations.
The frozen recording runner uses this engine. It does not replace or start the legacy 59-policy
live service, and it has no broker order or account client.

## Data flow

A producer dispatches received market events in receipt order through `enqueue` or `submit`.
Each symbol has its own FIFO worker. No worker waits for a future quote: an intent remains in
`pending` until a later event arrives. A short shared account turn serializes immediate cash and
ledger mutations. It contains no network calls, sleeps, or future-confirmation waits.

The producer must keep synchronous REST calls outside these workers, for example with
`asyncio.to_thread`, as the existing stream validation collector does. Heavy CPU work is not
made parallel by asyncio and must not be placed inside the account turn.

An event contains an ID, symbol, receipt timestamp, observed book, available feature, eligibility,
entry-cadence flag, current candidate count, and observed FX with its receipt timestamp.
Inputs are copied on enqueue. Source times ahead of receipt, stale books, backward source updates,
and identical repeated book snapshots cannot execute. Changed books with the same source timestamp
remain observable. This is snapshot handling, not a claim of lossless trade-tick deduplication.

## Cash and execution

A buy request fixes integer quantity, initial ask limit, strategy ownership, and fee-inclusive
cash reservation using only observations available at the decision. Outstanding reservations
reduce spendable cash. Equal sizing divides currently unreserved cash by the supplied current
candidate count; priority sizing uses currently unreserved cash. Allocation follows receipt order,
not a future-ranked batch. A zero quantity does not become a pending request.

The first valid post-decision quote received after the configured delay evaluates the intent.
Existing fee, depth, tax, and partial-exit accounting is reused. The request limit and original
quantity are unchanged. An unfilled or partially filled remainder is released after that attempt.
Partial holdings retain their original strategy. The confirmation event cannot also create a new
request for the same symbol. Reusing released cash requires a new decision timestamp.

A missing confirmation expires after 30 seconds, preserving the earlier replay horizon. The live
producer must call `advance(receipt_time)` on its health timer even if no symbols update. `close`
drains queues and stops workers; it neither liquidates holdings nor pretends pending requests filled.
Pending reservations remain visible at recording end. Intent and outcome logs are included in results.

Layered summaries include `positions_as_of` and a `positions` mapping keyed by symbol ID.
Each remaining holding records its strategy, quantity, entry price, entry cost, remaining entry
fees, mark price and source time, mark age in seconds, market value, and unrealized P&L.
`entry_cost_usd` excludes fees. Fees are already expensed in account realized P&L;
`remaining_entry_fees_usd` is attribution evidence and must not be deducted again.
Position market values plus cash reconcile to mark equity. Position unrealized P&L sums to
account unrealized P&L. Partial sales report only the remaining quantity and entry cost.
Async marks use the observed book midpoint, including old marks with their original source
times and current ages. They do not imply liquidation or a fresh executable quote.

## Validation

`tests/test_async_layered_paper.py` covers delayed-symbol independence, concurrent reservation,
future-book mutation, full-versus-truncated replay prefixes, missing confirmations, partial fills,
timeouts, duplicate/reconnected snapshots, FIFO ordering, invalid data, and partial-exit ownership.

The recording replay dispatches every observed book without looking ahead for a future confirmation.
Entry cadence is per symbol; confirmation and holding management continue between entry checks.
It preserves the existing 48 configurations and records the new execution source hash.
Old results are retained and are not directly interchangeable with the revised timing semantics.

This is an in-memory research engine. Durable crash recovery and dynamic whole-universe candidate
subscription remain separate integration work. Recorded-source replay does not measure live
production latency, actual broker queue position, or strategy generalization.

Additional adversarial coverage rejects features from another symbol or with nonfinite values before
queueing. Unexpected worker failures propagate to the corresponding caller without leaving an
unresolved acknowledgement. Cancelling an acknowledgement wait does not cancel or duplicate an
already accepted paper intent. Holding quote staleness uses elapsed seconds and the configured
quote-age threshold, including equivalent timestamps in different timezones. Async account marks
are explicitly labelled as book midpoints rather than last trades.
