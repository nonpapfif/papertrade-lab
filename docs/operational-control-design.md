# Risk policy and operating controls

Status: target design. Numerical user limits and actual alert destinations remain unset.
See [system design](autotrade-system-design.md) for the current paper-only authorization.

## Risk policy

Live risk policy is immutable by the optimizer and identified by a policy digest and generation.
The user controls threshold values, scope, reset rules and permitted breach actions.
The evaluation policy is frozen separately so an optimizer cannot improve its score by weakening
the rules against which it is evaluated. Aggressive free-profile exploration stays distinct.

Required live settings include account capital/available funds, allowed exposure/concentration,
drawdown or loss measures chosen by the user, data/valuation requirements, cost allowance and
the action for every enabled limit. The policy explicitly distinguishes absolute cash loss,
funding-adjusted equity change, daily loss and high-watermark drawdown.
Deposit/withdrawal/FX changes must not silently reset a drawdown or count as strategy profit.
Do not select a numerical loss threshold or forced-exit rule without user policy.

The execution gate checks policy before reservation and again before dispatch. It considers
positions, working remainders, uncertain submissions, expected costs and reserved funds.
Sell quantity reservations are shared by strategy and account-risk actions. Potentially executable
sell remainders reduce available quantity until their fills or cancellation are resolved.
Size buys from actual native-currency spendable funds after reservations; display FX cannot
fund a purchase. An ambiguous sale cannot finance a new purchase before its effect is confirmed.
Independent policy and risk-reduction work continues while strategy optimization is unavailable.

| Breach action | Meaning | Requirement |
| --- | --- | --- |
| Pause entries | Reject new exposure; keep observing existing positions | Baseline containment action |
| Cancel entry remainders | Request cancellation of attributable working entry orders | Explicit enabled policy; reservation retained until resolved |
| Reduce to limit | Reduce only the configured excess | Explicit enabled policy, valid execution inputs and verified ownership |
| Exit affected exposure | Close only the configured exposure scope | Explicit enabled policy and valid execution inputs |

These actions are choices in the policy, not automatic default liquidation rules.
Before a complete live policy exists, live activation is unavailable. A breached limit always
blocks further exposure increases; liquidation or modification requires its configured action.
Risk-reducing actions must not reverse a position, add exposure, or spend cash released only
by an unconfirmed cancellation. Partial fills are rechecked against remaining exposure.

Unknown valuation, stale broker funds or unresolved order state are not evidence that a limit
is satisfied. Preserve the blocking reason and surface what evidence is missing.
Changing a policy re-evaluates existing exposure and reservations before new activity.
Relaxing a policy requires user action and an audit record; AI cannot make it a candidate change.

## Operating state

Persist operating intent separately from process liveness. A restarted launchd worker must read
the same durable stop/pause state before attempting any account action.
Use a set of blocking reasons with scope and generation, rather than one flag that can erase
another reason. A healthy feed cannot clear an operator stop; successful reconciliation cannot
clear an unresolved risk breach.

| Condition | New exposure | Existing-position management | Evidence/reconciliation |
| --- | --- | --- | --- |
| Ready with valid policy | Permitted after gates | Permitted after gates | Continues |
| Entries paused | Blocked | Permitted only within configured policy | Continues |
| Data degraded for a symbol/market | Blocked for affected inputs | Only with valid inputs; never invent marks/fills | Continues where available |
| Reconciliation or recovery required | Blocked for affected account | Only verified risk-reduction actions explicitly allowed by policy | Continues |
| Emergency stop | Blocked | No new commands except explicitly configured emergency action | Continue collecting outcomes of existing commands |
| Read-only diagnostic mode | Blocked | No execution commands | Continues without financial mutation except applying verified existing fills |

Emergency stop is not a claim that already dispatched broker commands vanished.
Its acknowledgement lists queued, dispatched, uncertain and working operations and their
reservation exposure. Record a gate generation at dispatch and make the durable stop effective
for all later gate decisions. Enumerate in-flight operations that crossed the gate earlier.
Apply genuine late execution evidence even while stopped; refusing to book it would corrupt cash.

Stopping entries, requesting cancellation, exiting positions, and stopping the Python process
are different commands. Do not turn one into another by implication.
An explicit request to cancel/exit is only available in a future authorized adapter and under
its configured policy. A process restart never serves as an implicit resume command.

## Commands and resume

Each local control command has an immutable ID, actor, scope, requested action, reason, expected
state generation, and timestamp. Record acceptance/rejection and effects durably.
The intended operator may issue commands; the optimizer and strategy process cannot write them.
Protect control files and database writes with existing local ownership/permissions.
No remote dashboard or messaging integration is required for the first implementation.

Resume is an explicit audited command. It verifies account identity, complete required policy,
compatible artifacts, successful recovery/reconciliation, necessary data readiness, and every
remaining blocking reason. Resume cannot clear reasons outside its authority or scope.
A failed resume keeps the previous state and reports the exact blockers.
Duplicate commands have no repeated economic or state-generation effect.

Strategy rollback redirects new entries only, as defined in the strategy lifecycle design.
Code rollback preserves current ledger/schema compatibility and pinned strategy artifacts.
If old code cannot read current financial state, remain stopped rather than restore an old
ledger or silently downgrade schema. Database restoration follows recovery and reconciliation.

## Alerts and audit

Events include incident ID, severity, account/environment/market scope, first/last observation,
state generation, evidence references, and whether operator action is required.
Redact credentials, authentication headers and unneeded personal information.
An append-only audit record is separate from a deduplicated current incident view.

Notify on a new actionable incident, meaningful severity/state change, failed recovery,
required user action, or completion the user asked to receive. Remain quiet for unchanged
non-actionable state; elapsed time alone does not trigger an AI call or chat notification.
Use the destination and repeat/escalation rules explicitly configured by the user.
Until a destination exists, local status is available but external delivery is unconfigured.
Record queued/sent/failed/acknowledged separately: command success is not user acknowledgement.
Alert transport failure must remain visible locally and must not clear an execution blocker.

## Acceptance

- A durable pause/stop survives clean restart, crash, duplicate command and launchd restart.
- A stale dispatcher cannot create new exposure after observing a newer stop generation.
- In-flight fills are still booked once during stop or recovery.
- Risk gates include working and uncertain exposure and cannot be weakened by strategy artifacts.
- Each configured breach action has partial-fill, stale-input and cancel-race scenarios.
- Resume requires all relevant blockers cleared; one healthy subsystem cannot override another.
- Rollback preserves actual history, prior losses and position ownership.
- Alerts deduplicate unchanged incidents and distinguish delivery failure from acknowledgement.
