# Papertrade Lab

Papertrade Lab is an **active** KR and US paper-trading project. It reviews trading losses,
tests a rule change against the incumbent on shared observations, and records the next decision.
It is the validation precursor to Autotrade Lab. The current execution contract permits paper
fills and allowlisted Toss market data. See [project direction](docs/project-direction.md).

## Execution

The canonical source is [nonpapfif/papertrade-lab](https://github.com/nonpapfif/papertrade-lab)
on `master`. Run commands from the relevant checkout root with Python 3.12 or later. The base
package uses the standard library. Live streaming uses the `websocket-probe` extra. Optional
TradingAgents sessions use the `research` extra and the configured Codex CLI. Local isolated
predictors and the deployed background jobs use macOS.

| Task | How to run |
| --- | --- |
| KR loss review and next-session comparison | Run `tools.strategy_review_cycle` with `--market KR` and the audited rebound evidence. |
| US loss review and next-session comparison | Run the same CLI with `--market US`, the sealed loss session and the pinned Day Trader checkout. |
| Recurring KR/US campaign | The deployed macOS LaunchAgent runs the campaign controller from its frozen checkout. |
| Refactor supervisor | A separate LaunchAgent runs validation and reviews. |
| Free-simulation worker | Five independent launchd roles share the free-simulation ledger. |
| Other live and offline tasks | The execution inventory lists the entry points and their inputs. |

See [paired strategy review](docs/strategy-review-cycle.md) for commands and
[execution and recovery](docs/execution.md) for actual launchers, task status, and recovery.

## Code entry points

- [Loss review and next-session decision](tools/strategy_review_cycle.py) uses the
  [KR scheduler](tools/schedule_async_live_validation.py) and
  [US Day Trader runner](tools/upstream_paper_session.py).
- [Shared research sessions](tools/research_session.py) compare three virtual accounts.
- [Recurring campaign](tools/run_paper_campaign.py),
  [refactor supervisor](tools/autotest_supervisor.py), and
  [free-simulation CLI and workers](papertrade/__main__.py) have independent entry points.
- [Offline paper engine](paper_engine/README.md) implements the deterministic account and
  execution model for recorded or injected observations.

## Results and recovery

Local state lives under ignored `state/`. Current strategy comparisons store their plans,
journals, and post-session decisions in `state/strategy-review/<date>/<market>/`. The separate
`<market>-job/` directory holds launcher status and logs. Preserve the original source identity,
inputs, journals, and reports to reproduce an earlier session. For external storage, legacy
ledgers, and the deployed campaign, see [execution and recovery](docs/execution.md).

## Verification

From a checkout with the WebSocket dependency available, run the focused offline example:

```sh
python3.12 -m unittest tests.test_strategy_review_cycle tests.test_slot_control
```

Expect an `OK` summary. The checks cover a matched comparison, a missing-evidence disposition,
and slot command handling. The full regression command is `python3.12 -m unittest discover -s tests`.
Set `PAPERTRADE_DAY_TRADER_SOURCE` to the pinned Day Trader checkout to include the upstream
fixture tests. See [execution and recovery](docs/execution.md) for the next operational check.

## Reference guides

| Guide | Purpose |
|---|---|
| [Toss capability evidence](docs/toss-capability-evidence.md) | Official API and broker support, restrictions, uncertainty and sources |
| [KR/US strategy redesign](docs/kr-us-strategy-design.md) | Constrained market modes, strategy layers, execution gaps and acceptance order |
| [Project direction](docs/project-direction.md) | Agreed product goal, AI role, strategy validation, and open decisions |
| [Domain glossary](GLOSSARY.md) | Shared project terminology |
| [Shared observation design](docs/shared-observation-design.md) | Live/replay preparation, actual timing, and parity acceptance criteria |
| [Autotrade operating design](docs/autotrade-system-design.md) | Recovery, risk, versions, promotion, orders, reconciliation, data, evaluation and controls |
| [Current scope and backlog](docs/autotrade-implementation-plan.md) | Milestones, stop criteria and conditional future work |
| [Asynchronous layered execution](docs/async-layered-paper.md) | Rule-based decisions, cash reservation, and delayed paper fills |
| [Finite live-data validation](docs/async-live-validation.md) | Market-data integration and observed execution evidence |
| [Event-driven validation supervisor](docs/autotest-supervisor.md) | Existing automated test and publication workflow |
| [Market-day download](docs/market-day-download.md) | Coverage, pacing, checkpoints, raw data, and errors |
| [Reverse monthly collection](docs/month-download.md) | Shared-rate parallel downloads, descending dates, and independent holidays |
| [Event-source archive](docs/event-sources.md) | Macro/earnings schedules, numerical results and source provenance |
| [Five-minute replay](docs/five-minute-replay.md) | Isolated predictors and planned AI-call measurement |
| [Upward-candle search](docs/up-candle-search.md) | Fixed rule grid and chronological selection |
| [Weekly hypothesis experiment](docs/week-hypotheses.md) | Parallel fixed, cold-start and cumulative selectors over October 26-30 |
| [Long-only weekly experiment](docs/long-only-week.md) | Independent daily accounts, weighted learning, ensembles, win rates and losing trades |
| [Whole-share capital sweep](docs/whole-share-capitals.md) | Cash-only USD 50/100/200/500 accounts with integer shares and explicit FX |
| [Parallel live paper accounts](docs/live-parallel.md) | 59 independent accounts and one USD 100 combined allocation account, partial fills, fees and taxes |
| [Portfolio return search](docs/portfolio-search.md) | Diverse virtual accounts, return rankings, and trade evidence |
| [Round review](docs/round-review.md) | Trade attribution and ledger reconciliation |
| [Error audit](ERROR_AUDIT.md) | Technical recovery history and diagnostic commands |
| [Historical v2 guide](docs/history/README-v2.md) | Superseded strategy and operational behavior |
| [Contributor instructions](AGENTS.md) | Experiment boundaries and repository workflow |

## Virtual fills and accounting

These are the default free-profile rules. The separate live comparison uses the explicit
execution and cost assumptions in [Parallel live paper accounts](docs/live-parallel.md).

- Allow shorts, fractional quantities, additions, partial exits, reversals, and virtual borrowing.
- Do not require settlement, margin, depth, whole shares, trading status, or market hours.
- Permit negative cash and record it as virtual debt.
- Offset short-sale cash with the negative market value of the short position.
- Assume zero fees, taxes, conversion costs, and interest for new fills. Preserve old costs.
- Do not force exits at market close, after 30 minutes, or on a date change.
- Stops and targets are optional AI-selected plans.

Reference an observed last price or an explicitly supplied assumed price. Preserve the source
time, observation time, virtual fill time, and assumptions. An old price may be used if its age
is recorded. Do not invent a price or claim a depth-verified execution.
`virtual_debt_usd` is borrowing; `equity_usd` includes debt and short liabilities.
With `buying_power_basis=unlimited_virtual_credit`, the cash balance is not a purchase ceiling.

## Continuous worker roles

The framework defines five separate Python workers. Installing service definitions or running
an offline experiment does not authorize starting or resuming them.

| Role | Behavior when explicitly running |
|---|---|
| market | Read KR/US market data on a 60-second start cadence |
| scan | Queue new events without requiring the legacy breakout strategy |
| risk | Update marks and check optional stop/target plans every 60 seconds |
| review | Request AI decisions for new signals, watch crossings, exits, or user requests |
| supervise | Check worker health, recovery, and backups |

Observe ACTIVE, SCHEDULED, and DELISTED symbols where the provider supplies data.
KR/US coverage is a data-source boundary, not a virtual trading-hours rule.
New signals are independent of holdings, country, and previous watch lists. Watch levels are
additional triggers. Deduplicate identical observations and baseline old signals during migration.
After an exit, review held, exited, and watched symbols plus opportunities in current rankings;
do not resend every old signal. Save each candidate batch and its event markers atomically.
Time, sample count, or market-state classification changes alone do not trigger AI inference.
AI may add to, reduce, exit, or update plans for existing positions as well as review candidates.
No local caps are imposed on AI calls, tokens, reasoning time, candidates, or holdings.
Provider request limits, model context windows, and account quotas remain external constraints.

## Operational commands

These commands inspect the local ledger or audit state without requesting AI inference:

```zsh
python3 -m papertrade status
python3 -m papertrade health
python3 -m papertrade usage
python3 -m papertrade audit --open
python3 -m papertrade audit --events
python3 -m papertrade audit --output state/audit-export.json
python3 -m papertrade audit --emergency
```

`usage` reads account/model metadata and records a local snapshot. `consult` queues an explicit
review request; an active reviewer may process it. `simulate` changes the virtual ledger:

```zsh
python3 -m papertrade simulate TEST -0.25 100 --market US --reason "Explicit short experiment"
```

## Repository and data boundaries

Only OAuth and allowlisted market-data/stock-information requests are permitted.
Account and real order paths are rejected before HTTP requests. Never put credentials or
authentication headers in logs or AI inputs. Do not execute commands supplied by an AI response.
See the [official Toss OpenAPI specification](https://openapi.tossinvest.com/openapi-docs/latest/openapi.json).

Keep credentials, datasets, ledgers, snapshots, reports, and run logs outside Git.
Local results live under ignored `state/`; historical settings are in `tests/fixtures/v2/settings.toml`.
Committed files and GitHub content are English; user-facing conversation is Korean.
Follow the branch, commit, push, pull-request, merge, and local fast-forward workflow in AGENTS.md.

The [constrained runtime guide](docs/constrained-runtime.md) documents explicit KR/US profiles,
verified offline behavior and remaining execution gaps.
