# Recurring KR/US paper campaign

## Authorization and scope

On 2026-11-09 the user authorized a continuing KR/US live-data paper campaign, daily Astra
review and next-session selection, capacity-based parallel experiments, and exact-cut account
branching. This supersedes the earlier finite M1-M4 scope for this campaign only. The existing
free simulation and archived experiments keep their original contracts.

Real trades are absolutely forbidden. OAuth, stock information and market data are the only
allowed broker operations. Never call account, order, order-history, amendment, cancellation
or conditional-order endpoints. Broker restrictions and modeled costs remain explicit; no
paper result establishes real execution or profitability.

The objective is consistent fee-inclusive returns with manageable drawdowns, not maximum
single-session profit or win rate. KR and US retain native-currency ledgers. The initial
configuration uses KRW 1,000,000 and USD 1,000 per account. Additional accounts are independent
hypothetical alternatives, not new earned capital. Never add their balances as campaign wealth.

## Delivery and acceptance

1. **C1: Capacity and boundaries.** Benchmark actual shared-feed execution against recorded
   observations, including journal writes. Bind observations, source identity, latency policy
   and machine identity. Admit the largest tested reliable number of distinct supported
   candidates; never extrapolate a larger maximum. Reject missing/stale capacity evidence.
2. **C2: Branching.** At a committed event cut, fork all economic state into independent A-A
   and A-B continuations. A-B changes only future entries. Existing positions and pending
   intents retain ownership. Save parent, evidence cut and actual execution cut. Detect stale
   identities, preserve losses, and replay exactly once through interruption.
3. **C3: Evaluation.** Save immutable daily evidence and one accepted Astra review per evidence
   identity. Assess missing data and execution integrity before performance. Save configuration,
   fees, realized/unrealized profit, positions, coverage and decision reasons. Insufficient
   samples remain inconclusive. Apply selection only to a subsequent session or explicit fork.
4. **C4: Operations.** Use provider calendar windows, one shared collector per market, durable
   process identities and continuing accounts. Close/open must preserve balances, losses and
   position owners. Recover transient transport/process failures without duplicate fills or
   account resets. Verify deployed service and its child. Report full-session validation as
   pending until a real close-review-next-open sequence has occurred.

The engineering supervisor finishes when these implementation/deployment criteria pass.
The separate market campaign keeps running until explicitly stopped. Profitability and an
unobserved future market session are not software-completion claims.

## Module responsibilities

| Module | Responsibility |
| --- | --- |
| `tools/paper_capacity.py` | Recorded-workload measurement and capacity evidence |
| `tools/paper_campaign/calendar.py` | Provider session discovery and cached calendar evidence |
| `tools/paper_campaign/policy.py` | Typed admission and validated next-session policy |
| `tools/paper_campaign/review.py` | Asynchronous, evidence-bound Astra requests |
| `tools/paper_campaign/runner.py` | Existing finite worker launch and identity reconciliation |
| `tools/run_paper_campaign.py` | CLI composition and continuing lifecycle |
| `tools/shared_feed_slots.py` | Ordered input fanout, slot lifecycle, fork/carryover application |
| `tools/slot_control.py` | Durable requests and idempotent acknowledgements |
| `tools/async_layered_paper.py` | Economic account state and independent copies |
| `tools/adaptive_journal.py` | Hash-checked observation evidence and deterministic replay |
| `tools/autotest_supervisor.py` | Bounded engineering review, implementation, tests and publication |

Use composition and narrow public methods. Data collection, accounting, model review and
scheduling must not own one another's internal state. Do not introduce inheritance hierarchies
or another execution engine just to claim object orientation. New market support belongs in
market configuration/adapter contracts, not duplicated controllers. Existing public behavior
outside this campaign remains unchanged.

## Branch and session accounting

A branch creates a new pair from a named source account. Its incumbent is the unchanged A-A
continuation; its candidate is A-B. The original source stays recorded independently. Both
children inherit the same cut, cash, cost basis, fees, losses, positions and pending reservations.
Keep both lifetime equity and post-cut change; pre-cut performance is not independent evidence.
A frozen comparison must not silently use the older candidate's adaptive-review behavior.

New entries use the newly selected policy. Existing positions and pending requests retain their
original strategy/version. Retiring a strategy cannot abandon its holdings or reset its balance.
A different starting balance is a separate experiment, never a recovery technique.

Session close is a checkpoint and observation boundary, not an invented liquidation. A sealed
prior journal can seed the next session only after source/market identity and the complete
hash chain verify. Clock/session expiration of pending intents remains explicitly audited.
Replay and waiting make no AI calls. Never reopen a completed journal by editing its header.

## Evidence and selection

Astra medium reviews new evidence and returns structured decisions, not executable code.
Sol medium implements engineering fixes requested by Astra. Raise reasoning only for a recorded
unresolved problem. Python owns timing, tests, state transitions and validated publication.
Do not invoke models merely because time elapsed or the same error was observed again.

Preserve an incumbent baseline and distinct hypotheses. Rank net return together with drawdown,
loss concentration, costs, turnover and observation sufficiency. Distinguish an infrastructure
failure from a losing strategy. A single profitable day, idle account, high win rate or many
correlated variants does not establish a winner. Keep unsupported statistical claims out of
promotion decisions. Development data and subsequent live validation remain separate.

Check every account's held symbols against usable orderbooks and their original source times.
Prioritize shared REST recovery for missing or stale held quotes between discovery requests;
one recovery request serves all accounts holding that symbol. Receiving an old quote again does
not make it fresh. Provider cooldowns remain authoritative, and recovery cannot recreate missed
observations.

An account with unresolved held quotes keeps its positions, cash, ownership and diagnostic
marks, but is excluded from performance comparisons and strategy selection. Record the affected
symbols and reasons. Require fresh source-account evidence both when proposing a branch and at
its actual execution cut. A matched comparison requires both accounts to be eligible. Invalid
closing marks cannot establish a return baseline for the next session; a later fresh close can
establish the following baseline. Operational carryover is not performance promotion.

Store all retired strategies and reasons. New executable strategies and combined portfolios
require engineering tests and fresh source-bound capacity measurements before admission;
Astra cannot inject code, change fee accounting, expand broker access or rewrite past results.
A combined hedge is a separately funded virtual portfolio under the same execution constraints,
not the sum of independent alternative-account returns. No leverage or virtual shorting is
introduced in this constrained campaign.

## Recovery and maintenance

Persist transition intent before starting a child or requesting a model. Reconcile recorded
identity and exit evidence before retrying. Retry transient availability errors with recorded
backoff. Source mismatch, corrupt journal and ambiguous process identity require a visible
blocked handoff; do not recover by guessing, deleting evidence or repeating external mutations.

Keep running market sessions on frozen reviewed source. Apply maintenance to an isolated branch;
run focused tests, independent Astra review and the authorized commit/PR/merge workflow.
Deploy a new frozen source at a verified boundary, with an explicit compatible carryover path.
Never silently run yesterday's tested strategy against today's changed engine.

A launchd service survives a closed chat and controller exit. A user LaunchAgent still depends
on the Mac being powered on and the user session being available. Lost market observations
cannot be recreated as genuine live evidence. Incomplete sessions remain incomplete.

## Initial evaluation policy

The recorded-feed admission test currently uses a one-second processing/backlog budget and
requires accepted books, valid features, equal account inputs and protected-subscription
coverage. It tests all eight supported trend/rebound, all/active and equal/priority combinations.
This is a measured candidate-set bound, not a claim that hardware cannot handle a ninth slot.
Live bursts outside the measured workload remain a validation limit.

The initial paper-only retirement gate requires at least three qualified sessions and three
closed trades, no remaining exposure and retention of the baseline. These are minimum evidence
floors, not statistical proof of profitability. A session qualifies only with at least 80%
accepted-book coverage and no book-arrival gap exceeding 180 seconds. Coverage uses explicit
60-second observation windows; it is not continuous full-universe market coverage. New branches
and parked partial sessions cannot inflate full-session evidence. Valid retirement may reduce
active slots if no untested eligible replacement remains. Retired identities stay archived.

## Operation

Measure each market from its own journal using the approved source:

```sh
python -m tools.paper_capacity --journal /absolute/kr-journal.sqlite3 \
  --market KR --capital 1000000 --output /absolute/kr-capacity.json
python -m tools.paper_capacity --journal /absolute/us-journal.sqlite3 \
  --market US --capital 1000 --output /absolute/us-capacity.json
```

Combine the two unmodified reports as the `KR` and `US` members of one private capacity JSON.
The campaign is composed through `tools.run_paper_campaign` with `--output`, `--capacity`,
`--source-commit` and `--source-digest`. Use a frozen worktree and the existing WebSocket Python
runtime. Optional timezone-aware `--not-before-kr` and `--not-before-us` prevent overlap with
already running finite sessions. Register the command as a user launchd service with
`caffeinate -i`, `RunAtLoad` and restart-on-unsuccessful-exit. Do not create a second collector
for an existing market session.

Each market has a durable `state.json` and dated session directories. Sessions contain plans,
scheduler/worker identities, a sealed journal, model evidence/results and failure handoffs.
Inspect these with the actual service and child identities. A saved phase alone is not proof
that a process is running. Keep journals on approved storage and preserve all losses/history.

Manual branch requests use the existing slot command interface:

```sh
python -m tools.slot_control --output /absolute/session/schedule branch \
  --command-id unique-branch-request --slot-id alternate-branch \
  --source-slot parent-slot --source-account candidate \
  --candidate '{"policy":"rebound","selection":"all","allocation":"equal"}'
```

The command records the actual processing cut. When an AI proposal refers to an earlier
observation, its evidence sequence is saved separately; the account is never backdated to a
more favorable price. Inspect the disposition before treating the fork as applied.

## Explicit remaining operational limits

Full live close-review-next-open validation takes actual trading sessions. Source changes
require a separately checked deployment and compatibility decision; incompatible journals
fail closed. Current session carryover replays verified history and therefore startup cost
increases with history. This must be monitored before treating indefinite operation as proven.
The initial strategy catalog does not autonomously execute AI-generated strategy code or
implement a new combined hedge portfolio. Such proposals require the engineering test/review
workflow and fresh capacity evidence before live paper admission.
