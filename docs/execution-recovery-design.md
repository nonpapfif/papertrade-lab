# Execution, recovery and reconciliation

Status: target design. Current execution remains paper-only.
See [system design](autotrade-system-design.md) for scope and user-controlled prerequisites.

## Durable account

Reuse SQLite transaction and backup mechanisms after separating the layered account's schema
from legacy free-profile behavior. Do not change or import historical capital automatically.
Each mutation identifies environment, account, market, symbol, currency, strategy version,
intent, operation, observation sequence, and schema version.

Persist the following together when they change: cash movements, position quantity and cost,
reservation remainder, accepted execution evidence, intent outcome, position strategy owner,
and the last financially applied observation/event identity.
Use unique financial-effect keys. Store execution-event identity separately from transport
message identity because reconnects can redeliver the same economic execution in a new message.
Use exact decimal arithmetic for durable money and quantities. Migrate float-based research
results through versioned comparison, not by silently reinterpreting their old rounding.

Reserve sellable position quantity as well as purchase funds, atomically per account/market/symbol.
Available sell quantity is verified owned/sellable quantity minus potentially executable sell
remainders. Strategy exits and account-wide risk exits share this reservation; one cannot sell
quantity already promised by the other. Risk actions retain the affected position's ownership
and record the intervention cause separately from its strategy attribution.

Raw observation capture and financial application have separate cursors. Capture does not mean
the account consumed an event. A checkpoint atomically identifies the financial cursor and its
required trace prefix; only durable trace content can be referenced by a durable checkpoint.
SQLite journal integrity is necessary but insufficient: also verify reservation sums, quantity
conservation, currency totals, execution deduplication and resolvable strategy digests.

## Order lifecycle

Keep a stable local intent identity and a distinct identity for each create/modify/cancel
operation. Journal an operation and its reservation before crossing the external seam.
Only one fenced dispatcher may act for an account. Recheck operating/risk policy immediately
before dispatch; a prior strategy decision is not dispatch authorization.

| Local operation state | Meaning | Recovery behavior |
| --- | --- | --- |
| Prepared | Intent and reservation durably recorded; no dispatch-start record | Eligible for dispatch after current gates pass |
| Dispatching | Dispatch-start record committed before external I/O | Treat as possibly submitted after interruption |
| Acknowledged | Broker or paper adapter supplied attributable acceptance evidence | Track the corresponding order lineage |
| Rejected | Attributable evidence confirms no accepted operation | Release only the reservation no longer needed |
| Uncertain | Timeout, disconnect or crash leaves outcome ambiguous | Retain reservation and reconcile; never blind-resubmit |
| Resolved | Subsequent authoritative evidence identifies the outcome | Apply its effect once and preserve the resolution audit |

Order execution has a separate lifecycle: working, partially filled, filled, cancelled, rejected
or expired. These are internal normalized meanings, not assumed provider enum names.
Cancel-requested is not cancelled. Local elapsed time is not broker expiration.
The paper engine's 30-second intent expiry must not become a live-order timeout or proof of
cancellation. Broker status and accepted execution evidence govern real-order completion.

For a partial fill, debit only the executed cash/cost, create the corresponding position once,
and keep funds reserved for the still-live remainder and applicable cost allowance.
On confirmed cancellation, release only the remaining unfilled reservation.
For sells, partial execution decreases both owned quantity and that request's reserved quantity;
confirmed cancellation releases only its unfilled sell reservation. Uncertain cancellation or
replacement retains all potentially executable quantity. Without a verified atomic-replacement
guarantee, account for both old and replacement orders' possible effects before accepting a
second request; if that would oversell, wait for attributable resolution.
If a fill races with cancellation, book the fill and reconcile the true remainder.
Do not mark an entire order unfilled because cancellation was requested or acknowledged.

Modification and cancellation may create related broker identifiers. Preserve their lineage
under the original local intent and deduplicate economic executions across that lineage.
Do not count a child response as a second independent funded order without evidence.
Manual/unattributed broker activity is quarantined for reconciliation rather than assigned to a
strategy by matching symbol, quantity, price, or nearby timestamps alone.

## Recovery

1. Acquire exclusive account authority with a durable generation. Reject stale dispatcher
   results from previous generations as commands; retain genuine external fills as evidence.
2. Enter recovery mode and preserve the interrupted ledger. Verify schema, database integrity,
   account identity, required trace prefix and every strategy version used by open positions.
3. Load cash, positions, owners, reservations, nonterminal operations and the last committed
   financial cursor. Rebuild derived views without resetting principal or previous losses.
4. Replay only unapplied durable paper evidence. Do not rerun external mutations or replay old
   strategy decisions as fresh orders. Resolve live uncertain operations by reconciliation.
5. Check accounting invariants. Missing dependencies, corrupt evidence or unresolved order
   outcomes leave explicit blocking reasons; restarting cannot clear them.
6. Resume only after all operating, policy, reconciliation and observation requirements pass.
   A pre-existing operator pause or emergency stop remains in force.

Crash tests cover before/after reservation commit, dispatch-start commit, external acceptance,
local acknowledgement, partial-fill commit, snapshot creation, and checkpoint publication.
An old verified backup is not automatically the latest account truth. Restoring it requires
reapplying durable later evidence and reconciling external changes before increasing exposure.
Checkpoint retention may remove only redundant projections, never the last reconstructable
account history or the only artifacts required by an open position.

## Reconciliation

Reconciliation compares internal expected state with an authoritative external observation.
For current development that external observation is supplied by a paper/fake adapter only.
Future real-account queries remain disabled until separately authorized and verified.

Collect account identity, currency cash/available funds, positions, nonterminal order lineage,
and attributable executions. Record provider/source time, local receipt, pagination and coverage.
Do not assume account snapshots and execution queries share an atomic timestamp.
Establish an overlapping evidence window or provider-supported cursor, apply deduplicated
executions, then verify a stable cut. Incomplete pagination or an unresolved moving cut is unknown.

| Difference | Action |
| --- | --- |
| Known execution absent locally | Apply once using its verified economic identity |
| Duplicate or reordered evidence | Preserve raw receipt; apply no second financial effect |
| Unknown external order or position | Block affected new exposure and require attribution |
| Cash/quantity difference without explanation | Preserve both views and block; do not rewrite local cash to match |
| Fee, FX, tax, settlement or corporate-action adjustment | Append a sourced accounting adjustment; retain prior history |
| Uncertain submission cannot be uniquely correlated | Retain worst-case reservation and require explicit resolution |

Position/cash tolerances must come from verified currency precision and provider semantics,
not an arbitrary amount that hides differences. A successful heartbeat is not reconciliation.
Distinguish deposit, withdrawal, conversion, borrowing, proceeds, realized gain and valuation.
Fresh capital and short-sale proceeds never become profit.

## Market-data faults

Follow the shared observation design for source time, publication sequence and decision clock.
Track per-symbol, per-market and FX readiness separately from worker health.

| Fault | Required behavior |
| --- | --- |
| Stale/future/malformed or crossed quote | Reject it for execution; retain the reason and latest valid evidence |
| Duplicate snapshot | Preserve economic deduplication; no repeated fill |
| Source-time regression | Do not overwrite a newer accepted state |
| Stream gap or reconnect | Mark coverage uncertain, retain protected subscriptions, resync as observed |
| Missing candles or delayed correction | Mark features unavailable until observed; do not invent bars or revise old decisions |
| Missing/stale FX | Keep native-currency balances; mark converted results unknown and block dependent actions |
| Queue backlog | Record actual decision delay; stale work cannot create backdated exposure |
| Provider cooldown/auth failure | Respect documented limits; no fabricated successful observation |

Equal source timestamps with changed book contents are not identical snapshots.
Do not use the timestamp alone as the duplicate key.

Existing positions do not disappear when inputs are invalid. Preserve them and report valuation
quality. Risk-reducing execution still requires valid execution evidence and an allowed action.
Unknown valuation must not be reported as zero loss or as an automatic risk-policy pass.
Recovery of data does not clear an independent operator pause or unresolved order ambiguity.

## Provider evidence and activation gap

The public [Toss REST specification](https://openapi.tossinvest.com/openapi-docs/latest/openapi.json)
was inspected on 2026-11-04, reporting version 1.2.28. Its order-operation response describes a
new identifier after modification/cancellation, and KR/US modification constraints differ.
This supports preserving lineage and market-specific capabilities; it does not prove an
application-level exactly-once submission guarantee.

No verified server idempotency/correlation guarantee, atomic reconciliation snapshot, or
execution-correction contract is assumed. Before a future broker adapter is activated, record
verified capabilities for lookup after lost response, stable execution identity, pagination,
modification lineage, delayed fills/corrections, costs and market/session permissions.
The public AsyncAPI document could not be retrieved in this review, so no new personal-order
stream guarantees are asserted. No authenticated account/order paths were called.

## Acceptance

- Replaying any completed financial event or crash recovery twice changes no money twice.
- Ambiguous dispatch never releases its reservation or triggers blind retry.
- Partial-fill/cancel/modify races conserve cash and quantity across order lineage.
- Simultaneous strategy/risk exits, unresolved cancellations and lost replacement responses
  cannot reserve or sell the same position quantity twice.
- Reconciliation exposes moving or incomplete evidence cuts and unexplained external activity.
- Corrupt/old checkpoints, missing owner artifacts and stale dispatchers cannot silently resume.
- Data faults preserve positions and correctly label missing valuation and observation coverage.
