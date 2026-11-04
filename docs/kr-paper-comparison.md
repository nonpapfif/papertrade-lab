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
