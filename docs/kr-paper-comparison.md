# Korean live paper comparison

The dedicated runner compares fixed signals and Astra-gated signals using the same published
snapshots and independent 100,000 KRW virtual accounts. It samples the listed Korean stock
universe by REST. The provider's last trade is observed data and each recorded fill is a virtual
assumption. This is a sampled live comparison.

A change in price direction proposes an entry or exit. The fixed account follows the signal.
Astra medium reviews actionable signals for the other account and can approve or decline each
one. A declined signal can specify a price watch. A later observed crossing wakes that watch.
The two accounts use the same fractional sizing and zero-cost assumptions. A fill requires a
newer source timestamp than the applicable decision. An opposing signal invalidates old pending
actions. Each account retains its own positions, cash and realized and unrealized results.

Use the Korean supervisor profile with an explicit timezone-aware absolute cutoff. The process
stores the frozen policy and source hashes, shared snapshots, AI decisions, assumed fills and both
account results under the run output. The supervisor reviews the result through Astra and routes
necessary offline implementation to Sol. At the cutoff, unfilled requests and open positions
remain visible. An unfinished AI request is cancelled. The supervisor requires at least one
completed live AI review to accept the Korean milestone.

## Parallel Sol-first account

Run `python -m tools.run_kr_sol_astra --market INPUT --output OUTPUT --until TIMESTAMP`
beside a running Korean comparison. It reads newly published snapshots from the comparison's
market JSONL file and starts a separate 100,000 KRW virtual account. Sol medium handles each
batch. Astra medium receives only signals explicitly deferred by Sol that remain actionable.
Snapshot processing and fills continue while either model is in flight. The original run
continues to use its existing policy, and the separate account has a later observation start.

Each AI review records its input snapshot, account context, model, effort, and review ID.
Responses record approval, hold, escalation reasons and optional watches. Symbols sharing
the same hold reason can be grouped in the model response. The recorded result maps each
symbol to its reason. Missing or conflicting decisions invalidate the review and generate
an error record. An Astra escalation links back to the Sol review that requested it.

## Live allocation comparison

Pass `--allocation-comparison` to start four independent accounts on the same snapshot stream.
The Korean supervisor uses this mode for new live runs. The accounts pair fixed rules and
Sol-first AI review at 5% and 10% of decision-time equity per funded entry. They start at
100,000 KRW each. The first 12 snapshots warm the trailing absolute-move history.

Entries with sufficient cash are funded in descending order of signal strength. Strength is
the absolute price move divided by the prior mean absolute move over up to 12 changes.
A position uses the full planned budget or skips the entry. Direction-change signals,
fractional sizing, zero costs, and later-source-price fills are shared across accounts.
Each allocation has its own asynchronous AI review and tagged decision records.
