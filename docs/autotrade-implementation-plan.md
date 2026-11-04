# Autotrade scope and implementation plan

Status: optional paired execution and reporting are implemented and checked with offline fixtures;
the first live comparison has not been run.
This plan supersedes the earlier assumption that all W01-W11 form a mandatory delivery chain.
The [system design](autotrade-system-design.md) remains a future contract reference, not a list
of prerequisites for every experiment. Implement only what the active milestone requires.

## Active milestone

Complete one finite US live-data paper comparison of an incumbent and a candidate strategy.
Freeze both definitions/configurations before observations arrive. Give independent accounts
the same published observations, starting capital, and cost/fill assumptions. Save comparable
results and provenance. Report observations; do not automatically qualify or promote a winner.

The target is a simultaneous live-input comparison. Recorded/synthetic comparisons support
its tests; they do not satisfy the live demonstration. Exact replay of a live run is separate.
This scope review does not launch a live campaign or change the current authorization.

The [finite runner](async-live-validation.md#fixed-strategy-comparison) now accepts an explicit
candidate policy alongside the existing router incumbent. M1-M3 and the offline portion of M4
have an implementation path; M4's live demonstration remains pending. The milestone is not
complete until that actual live evidence is reported.

## Existing capability and smallest gap

Checked against source baseline `bcbe06a` on 2026-11-04:

- `replay_case` already creates independent `AsyncLayeredPaper` accounts for fixed settings.
- `evaluate` already saves per-account results and assumptions, but runs a broad predefined grid.
- `Runtime` in the finite layered live runner currently owns one account.
- The older parallel live runner already fans out common frames and saves provenance, but uses
  a different engine. Reuse it only if it actually expresses the two intended strategies.
- Existing account summaries already expose trades, intents, pending reservations, cash,
  fees and valuation assumptions. A new accounting module is not a prerequisite.

An offline scope-audit harness used the existing replay function without runtime changes:
identical settings produced equal summaries; different fixed rules could act differently;
the accounts remained independent. Nine existing replay/live-adapter tests passed.
This evidence establishes reusable mechanics, not a completed live comparison or profit claim.

## Required work now

| Step | Necessary delivery | Reuse and limit |
| --- | --- | --- |
| M1 | Specify/freeze the incumbent, candidate and matching execution conditions | Reuse existing rules/configuration; no optimizer or version-loading platform |
| M2 | Fan out one ordered live observation stream to separate accounts | Reuse the current execution engine; compute filters/counts from each account's own state |
| M3 | Save one comparison bundle with metrics, assumptions and failures | Reuse existing summaries/recording helpers; no automatic promotion verdict |
| M4 | Verify independence, equal input and saved evidence, then demonstrate the finite path | Existing fixture tests plus focused fanout/report tests; no full future fault campaign |

Use one observation cut/logical dispatch time for both accounts and record actual processing
and observation limitations. Never silently make the accounts share reservations or consumed
paper liquidity. Preserve union protection for held/pending subscriptions. Fix local duplicated
logic only when it prevents the two accounts from seeing the intended equivalent inputs.
A shared live/replay framework, new database or broad engine extraction is not required.

## Stop criteria

Stop this milestone when all of these are demonstrated:

1. The two fixed strategy/configuration identities and source versions are saved before entry.
2. Independent accounts begin with equal capital and matching cost/fill assumptions.
3. A single publication sequence feeds both accounts; event identity, order and logical time
   are comparable. Different strategies and account states may produce different decisions.
4. Results save interval, observed/subscribed coverage, input/source identity, costs, realized
   and unrealized results, cash, positions, pending requests and valuation limitations.
5. An interruption or failure is recorded as incomplete. Do not silently restart from fresh
   capital, discard losses, invent final liquidation or label the run a clean comparison.
6. Focused tests detect shared-account mutation, unequal inputs/settings and missing evidence;
   the finite live demonstration's actual scope and limitations are reported.

No trade or too little evidence is a valid inconclusive report, not a trigger to add features.
Live results need not prove profitability to complete this plumbing milestone.
Do not expand into automatic promotion, optimization, continuous operation or exact replay
merely because the comparison works or reveals a new future-work opportunity.

## Reclassification of the original plan

| ID | Classification now | Minimum needed now or trigger for later work |
| --- | --- | --- |
| W01 | Required subset; full task conditional | Shared input/order/time for M2. Full trace parity and live/replay extraction only when exact execution reproduction is required |
| W02 | Conditional | Freeze configuration/source identity now. Immutable executable ownership and hot promotion when versions change with open positions or outstanding requests |
| W03 | Conditional | Persistent recovery before resume-after-crash or long-lived account operation. A disposable failed comparison can end as incomplete |
| W04 | Conditional | Preserve existing paper cash/quantity invariants now. Durable risk/control policy before unattended persistent operation or real-money readiness |
| W05 | Deferred | Unknown external submissions/cancel/replace handling when an external order adapter is actually in scope |
| W06 | Deferred | Broker reconciliation when authorized external accounts/orders are introduced |
| W07 | Required subset; full task conditional | Fixed-account comparison and reporting now. Frozen qualification contract and atomic promotion when automatic replacement is enabled |
| W08 | Conditional | Optimizer isolation/authority when AI-generated artifacts are accepted or executed. Two preselected existing rules do not require an AI proposal pipeline |
| W09 | Conditional | Existing shutdown/error reporting now. Persistent stop/resume, rollback and configured alerts before unattended continuous operation |
| W10 | Required subset; full task deferred | M4 verifies the two-account path. Combined recovery/order/promotion fault campaign after those functions exist |
| W11 | Deferred | KR/mixed-market work after the US path reaches its milestone, consistent with the agreed rollout order |

Deferred means not needed for this milestone, not unnecessary for eventual real trading.
Keep the detailed [execution](execution-recovery-design.md),
[strategy](strategy-lifecycle-design.md), and [control](operational-control-design.md) designs
as references when their trigger becomes real. Their dependency descriptions do not block M1-M4.

## Rule for adding work

Name the current acceptance criterion the proposed change enables and the code/test evidence
that it is otherwise blocked. Prefer an existing implementation or a local fix.
If postponing the change does not prevent the milestone, put it in the conditional backlog.
A hypothetical future caller, a large file, or a possible improvement alone is not a blocker.
Code review checks both missing milestone behavior and unrequested/speculative additions.
When the stop criteria pass, report completion and stop implementation of this milestone.
