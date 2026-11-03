# Project direction

This document records the direction agreed during the project review.
It defines target behavior, not implementation status or permission to start real trading.
Use `GLOSSARY.md` for shared terminology and `AGENTS.md` for the current execution boundaries.

## Product goal

Autotrade Lab is the intended real-money automated trading system.
Papertrade Lab is its development and validation precursor.
Support both Korean and US stocks through Toss Securities, completing the US path first.
Treat currency, sessions, costs, and execution conditions as market-specific requirements.

Historical simulation is a supporting way to compare strategy candidates.
Building a historical strategy-search platform is not the primary product goal.

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

## Open decisions

- Exact promotion criteria, evaluation horizon, and required evidence, including treatment of
  costs, drawdown, concentration, sparse trades, and incomplete market observations.
- Numerical live risk limits and the response when a limit is reached.
- Initial real-money capital and the conditions for moving from paper validation to real trading.
- How strategy versions, account-wide risk actions, and recovery state are persisted.

## Review findings to resolve

- `tools/autotest_supervisor.py` embeds a statement that layered rules override the free-profile
  instructions. Its scope must be reconciled with the profile distinction in `AGENTS.md`.
- The controller can accept contradictory review fields and can complete after failed tests.
  Separate diagnostic tests reproduced these paths; fixes are not included in this document.
- The asynchronous layered engine is currently in memory. Durable position and pending-intent
  recovery is not implemented, as documented in `docs/async-layered-paper.md`.

The existing finite live-data runner and historical comparisons do not by themselves implement
the agreed fixed-version incumbent/candidate validation and automatic promotion lifecycle.
