# Project direction

## Active recurring campaign revision on 2026-11-09

The user authorized the [recurring paper campaign](recurring-paper-campaign.md).
Its C1-C4 criteria govern continuous KR/US scheduling, measured capacity, Astra session review
and account branching. Earlier finite-scope exclusions below remain historical and do not
block this expressly authorized work. Real trades and broker account/order access remain forbidden.

This document records the direction agreed during the project review.
It defines target behavior, not implementation status or permission to start real trading.
Use `GLOSSARY.md` for shared terminology and `AGENTS.md` for the current execution boundaries.
The [operating design](autotrade-system-design.md) specifies the nine requested operating areas;
the [implementation plan](autotrade-implementation-plan.md) separates planned delivery from
verified capabilities.

## Product goal

Autotrade Lab is the intended real-money automated trading system.
Papertrade Lab is its development and validation precursor.
Support both Korean and US stocks through Toss Securities, completing the US path first.
Treat currency, sessions, costs, and execution conditions as market-specific requirements.

Historical simulation is a supporting way to compare strategy candidates.
Building a historical strategy-search platform is not the primary product goal.

The active milestone is to establish a broker-constrained KR/US paper execution contract and
validate strategies within it. The [capability evidence](toss-capability-evidence.md) separates
broker app support, public API support and locally verified behavior. The
[market and strategy design](kr-us-strategy-design.md) defines the revised layers, market modes,
costs, data limits and delivery sequence. It supersedes the earlier assumption that a sampled
price-direction comparison alone satisfies real-money preparation.

Today's recorded comparisons remain useful diagnostic evidence. Their fractional, zero-cost,
interrupted and counterfactual segments retain their original labels. Historical simulation
remains supporting work. Complete the first full US path while keeping KR-specific acceptance
criteria and native-currency accounting.

## Execution and AI responsibilities

Explicit strategy rules make trading decisions and drive execution.
AI analyzes results and proposes improvements rather than deciding every individual trade.
Useful new evidence triggers AI work; elapsed time alone does not.

AI improvement may cover stock selection, strategy assignment, entry and exit conditions,
and purchase quantity calculation. Changes to fill processing, ledger accounting, and broker
connectivity are separate engineering changes, outside automatic strategy improvement.

## Candidate validation and promotion

Compare the incumbent and candidate on the same live observations in separate paper accounts.
Use equal starting capital and matching cost and fill assumptions.
Keep both strategy versions fixed during the comparison; queue further changes for another test.
Replay prioritizes actual publication order and recorded decision timing, including delays.
See [Shared observation design](shared-observation-design.md) for the agreed module scope,
recording requirements, and acceptance criteria; implementation remains pending.
Automatically promote a candidate only after it passes agreed validation criteria.
An AI proposal or a single higher-profit observation does not establish that those criteria exist
or have been met. The criteria remain to be specified below.

Promotion affects new entries. Existing positions retain the strategy version that opened them
until they close. Account-wide risk limits continue to apply to all positions.

## Exploration and real-money risk

Paper experiments may explore aggressive strategies.
Real-money operation requires a separate, less aggressive risk policy.
The user sets live risk limits; AI cannot relax them autonomously.
Quantity improvements and candidate promotion must respect those limits.

These future requirements do not enable broker account/order access in the current repository.
The existing market-data-only restrictions remain in force.

## Required user policy and external evidence

The operating design defines state transitions, persistence, risk-action choices and validation
gates. Their implementation remains planned. These deployment inputs are still unset:

- Numerical promotion margin, evaluation horizon, evidence sufficiency, uncertainty rules,
  and evaluation risk/coverage thresholds.
- Numerical live risk limits and the enabled action for each limit.
- Initial real-money capital, enabled sessions and explicit live-operation authorization.
- Alert destination/acknowledgement rules and verified broker recovery/correlation semantics.

Missing values prevent the corresponding live activation or automatic promotion; they do not
prevent independent implementation and fault testing with labelled synthetic policy values.

## Review findings to resolve

- The asynchronous layered engine is currently in memory. Durable position and pending-intent
  recovery is not implemented, as documented in `docs/async-layered-paper.md`.

The existing finite live-data runner and historical comparisons do not by themselves implement
the agreed fixed-version incumbent/candidate validation and automatic promotion lifecycle.

## Verified controller corrections

Regression tests cover rejection of fix/blocked reviews that authorize code publication and
completion after explicit mechanical-check failure. These controller guards do not implement
runtime strategy promotion or its acceptance criteria.
