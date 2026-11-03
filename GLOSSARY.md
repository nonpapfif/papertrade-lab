# Automated trading

Project language for Papertrade Lab and its intended successor, Autotrade Lab.

## Language

**Autotrade Lab**:
The intended automated trading system whose final purpose is real-money trading.
_Avoid_: Papertrade Lab as the final product

**Papertrade Lab**:
The precursor used to develop and validate Autotrade Lab through simulated trading.

**Trading strategy**:
An explicit set of rules used to make trading decisions and drive execution.
_Avoid_: Per-trade AI discretion as the agreed execution model

**AI strategy improvement**:
Analysis of trading results and proposals to improve trading strategies.

**Strategy candidate**:
A proposed strategy awaiting separate paper-trading validation.

**Incumbent strategy**:
The currently adopted strategy against which a strategy candidate is compared.

**Parallel paper validation**:
A comparison of fixed incumbent and candidate strategies in separate paper accounts using
the same live observations, starting capital, costs, and fill assumptions.

**Strategy promotion**:
Automatic adoption of a strategy candidate after it passes agreed validation criteria.
An AI proposal alone is not sufficient for promotion.

**Live risk limits**:
User-defined constraints on real-money trading risk, separate from aggressive paper exploration.
AI strategy improvement operates within these limits and cannot relax them autonomously.

**Position strategy owner**:
The strategy version responsible for an existing position from entry until it is closed.
Strategy promotion affects new entries; account-wide risk limits still apply to existing positions.

**Historical simulation**:
A supporting experiment that compares strategy candidates on past market data.
It is not the project's primary deliverable.

**Observation trace**:
The ordered record of what an execution observed and when its decisions were made,
including processing delays and periods without quote updates.

**Execution reproduction**:
Replay of an observation trace with the same strategy version, account starting state,
and execution assumptions to reproduce the recorded execution's decisions and outcomes.
_Avoid_: Idealized zero-delay simulation

**Strategy artifact**:
An immutable executable definition of a strategy's rules, parameters and declared dependencies.

**Evaluation contract**:
The fixed comparison conditions and qualification criteria established before a candidate's
final evaluation begins.

**Account reservation**:
Funds or sellable position quantity withheld from new use for outstanding or potentially
accepted execution requests.

**Uncertain operation**:
A requested execution change whose outcome lacks attributable acceptance or rejection evidence.

**Reconciliation**:
The evidence-based comparison of local financial state with externally observed account,
order and execution state at a known observation cut.

**Operating blocker**:
A persistent reason that prevents specified account actions until its required resolution.
