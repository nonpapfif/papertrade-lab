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
