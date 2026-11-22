# Execution and recovery

## Source and current deployment

The canonical source is [nonpapfif/papertrade-lab](https://github.com/nonpapfif/papertrade-lab)
on `master`. This is an active project. The table records observations on November 23, 2026
(KST), separately from the project status. Paths starting with `Finance/` or
`Finance-strategy-review/` are relative to `~/Projects/`. Use the task's checkout as its
working directory. The common data root on this Mac is `~/Projects/Finance/state/`.

| Task | Actual launcher, checkout, and Python runtime | Observed state and progress |
| --- | --- | --- |
| KR paired review | Detached `tools.autotest_jobs` job, `/usr/bin/caffeinate -i`, then `python -m tools.strategy_review_cycle --market KR`. Checkout `Finance-strategy-review/` at `4b0995b`. Runtime `Finance/state/runtime/research-session-venv/bin/python`. | Running and waiting for the November 23 continuous session. The plan and scheduler waiting state were saved at 01:38 KST. |
| US paired review | Separate detached job with the same checkout and runtime and `--market US`. Uses the Day Trader checkout at `Finance/state/runtime/day-trader-3d0f4ef/` at `3d0f4ef`. | Running and waiting for the US premarket scan. The universe was saved at 01:39 KST. Neither market had a decision at inspection. |
| Recurring KR/US campaign | `~/Library/LaunchAgents/com.elsholtzia.papertrade.recurring.plist` runs `tools.run_paper_campaign` with `caffeinate -i`. Checkout `Finance/state/runtime/recurring-paper-20261112-recovery-source/` at `7528a89`. Runtime `Finance/state/runtime/paper-recovery-venv/bin/python`. | Controller running. KR remains blocked at its November 12 failed session and US at its November 11 failed session. |
| Refactor validation supervisor | `~/Library/LaunchAgents/com.elsholtzia.papertrade.test-supervisor-refactor.plist` invokes `tools.autotest_supervisor --scope concurrent-refactor` with `caffeinate -i`. Checkout `Finance/` at inspection was `0a4f06b`. Runtime `Finance/state/runtime/us-night-venv/bin/python`. | Registered and stopped. Last recorded phase on November 10 was blocked by a model-call failure. |
| Free-simulation market, scan, risk, review, and supervise | The [five launchd templates](../launchd/) dispatch `python3 -m papertrade --root ~/Projects/Finance worker ROLE` from `Finance/`, using `~/.local/bin/python3`. | All five labels disabled. The preserved ledger is available for inspection. |

The ordinary `Finance/` checkout was on an unmerged development branch at inspection.
The local `master` worktree is `Finance/state/runtime/paper-repair-master/`. The running
tasks above use the specific checkouts and revisions in the table.

## Background operation

### KR and US paired reviews

Start an individual market with its [documented CLI command](strategy-review-cycle.md) from
the desired frozen checkout before its verified opening. Each invocation requires a fresh
output directory. The deployed detached runner stores the exact command and checkout in
`state/strategy-review/2026-11-23/{KR,US}-job/spec.json`. The recurring controller and these
one-off jobs have separate output directories.

- **Status and logs.** Read `plan.json` and the corresponding job's `target.json`, `exit.json`,
  `stdout.log`, and `stderr.log`. Check the recorded process and its command with
  `ps -p PID -o pid=,ppid=,stat=,command=`. The KR scheduler also writes
  `schedule/status.json`. The US runner writes `session/status.json` after launch.
- **Stop and restart.** Interrupt a foreground CLI with Ctrl-C. For a detached launch, check
  the target PID and its descendants against the job directory before signaling them with
  `TERM`. Check the scheduler or session worker as well because the launcher and the runner
  have separate processes. A detached job does not restart automatically. A claimed output
  is retained. A started session may be recoverable through its original runner and frozen
  inputs. The paired CLI itself requires a new output for another dated run.
- **Results.** `decision.json` is the decision linked to the frozen plan and observed result
  when the paired runner finishes. For KR keep the schedule journal, attempt status, and slot
  state. For US keep the session journal, watchlist, source configuration, and comparison.
  Pass the audited KR report or the archived US `comparison.json` as `--evidence`. Keep
  the US comparison beside its sealed `journal.sqlite3` for loss attribution. Retest or
  evidence-repair decisions can be linked with `--previous-decision`.

### Recurring campaign and refactor supervisor

Inspect the installed service with `launchctl print "gui/$(id -u)/<label>"`. To stop a
registered service use `launchctl bootout "gui/$(id -u)/<label>"`. To start an authorized
service using the existing definition, use
`launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/<label>.plist"`.
Use the labels from the deployment table. Both definitions start at load and restart after
an unsuccessful process exit. A blocked application state may leave the controller waiting
without launching another session.

The campaign stores market state, dated plans, journals and repair handoffs in
`Finance/state/runtime/recurring-paper-20261112-recovery-source/state/campaign/`.
Its stdout and stderr are under
`Finance/state/validation/paper-runtime-recovery-20261112/deployment/`. Resume only with the
same source and capacity evidence, or use the recorded recovery handoff and verify any
approved replacement source. Keep committed balances and positions.
See [campaign recovery](recurring-paper-campaign.md).

The supervisor saves `status.json`, `service.stdout.log`, `service.stderr.log`, and review
cycles under `Finance/state/validation/autotest-concurrent-refactor-20261110/`. Keep its
inherited input directory and earlier review records. Check independent child jobs before
any restart. See [supervisor operation](autotest-supervisor.md).

### Free-simulation workers

The roles are `market` (collect data), `scan` (queue opportunities), `risk` (mark positions and
check optional exits), `review` (respond to evidence), and `supervise` (health and backups).
With express authorization, enable a disabled label with `launchctl enable "gui/$(id -u)/<label>"`
and register the repository template with
`launchctl bootstrap "gui/$(id -u)" "$HOME/Projects/Finance/launchd/<label>.plist"`.
The templates run at load and have `KeepAlive` enabled. Check worker status with
`python3.12 -m papertrade --root "$HOME/Projects/Finance" health` from the project root and
inspect `state/<role>.{stdout,stderr}.log`. Keep `state/paper.sqlite3`, its SQLite
sidecars, audit events, and backups. Do not start a second set of workers against that ledger.

## Command-line inventory

All commands below use `python3.12 -m <module>` from the checkout root unless a guide
specifies a frozen historical checkout. WebSocket and optional research dependencies follow
the README. The input and saved result in each row give the success check. Use `--help` to
inspect parameters before any live invocation. `load_toss_env.sh` is a shell helper for
authorized commands that need environment variables. Source it from zsh or bash when
required. The normal market client reads the private credential files directly.

| Task | Entry point | Representative input and saved evidence |
| --- | --- | --- |
| Free ledger operations | `papertrade` | `--help` lists commands. For ledger inspection, use `--root /path/to/Finance status` with the existing state. Worker roles are above. |
| Post-close loss review and next plan | `tools.strategy_review_handoff` | Dated predecessor and verified next market calendar. Inspect review, next plan, and lifecycle. [Guide](docker-strategy-cycle.md) |
| Docker market cycle | `tools.docker_strategy_cycle` | Compose market service with a sealed predecessor and dedicated output. Check its lifecycle, dated session, and handoff. [Guide](docker-strategy-cycle.md) |
| Mac Docker cutover | `tools.start_docker_strategy_cycle` | Host launch command with previous job receipts, image environment and new results root. Check the cutover receipt and Compose state. [Guide](docker-strategy-cycle.md) |
| Paired loss review | `tools.strategy_review_cycle` | Audited loss evidence, market and future day. Check the frozen plan and linked decision. [Guide](strategy-review-cycle.md) |
| Shared-feed research | `tools.research_session` | Run with market and date or replay a saved session. Check `comparison.json` and replay evidence. [Guide](research-session.md) |
| US Day Trader | `tools.upstream_paper_session` | Run with source, calendar and watchlist or verify a sealed session. Check the comparison and journal hash. [Guide](upstream-paper-session.md) |
| Finite layered validation | `tools.run_async_live_validation` | Market/profile and finite duration. Check `status.json`, observations and comparison quality. [Guide](async-live-validation.md) |
| Schedule finite validation | `tools.schedule_async_live_validation` | A dated window and source identity. Check scheduler and worker status and sealed journal when configured. [Guide](async-live-validation.md) |
| Shared-slot operation | `tools.slot_control` | `--output /path/to/schedule list` reads the slot spool and prints command status. [Guide](recurring-paper-campaign.md) |
| Recurring comparison | `tools.run_paper_campaign` | Frozen capacity and source evidence. Check per-market status, journal and handoff as described above. |
| Refactor validation | `tools.autotest_supervisor` | Repository, inherited evidence and scope. Check the durable phase and reviewed requirements. |
| Detached job launcher | `tools.autotest_jobs` | Internal `--child /path/to/job` uses an existing job specification and saves `exit.json`. Inspect existing jobs through their status files. |
| Parallel US paper accounts | `tools.run_live_parallel` | A market date and starting capital. The isolated output contains `live.sqlite3` and run reports. [Guide](live-parallel.md) |
| KR allocation and AI comparison | `tools.run_kr_paper_comparison` | An end time and profile. Inspect the independent account results on a shared feed. [Guide](kr-paper-comparison.md) |
| Continue KR comparison | `tools.continue_kr_paper_comparison` | Previous job and feed paths. Inspect the continuation's preserved prefix and new output. [Guide](kr-paper-comparison.md) |
| KR Sol/Astra side account | `tools.run_kr_sol_astra` | Shared snapshots and a cutoff. Inspect the independent KR account output. [Guide](kr-paper-comparison.md) |
| KR cost reconstruction | `tools.replay_kr_paper_costs` | Saved comparisons and listing metadata. Check fee-inclusive outputs in a separate directory. [Guide](kr-paper-comparison.md) |
| KR loss replay | `tools.replay_kr_layered_loss` | Sealed capture and frozen source. Check reconciled baseline and rule alternatives. [Guide](kr-loss-replay-20261109.md) |
| Layered stream replay | `tools.replay_layered_stream` | Recorded market stream. Inspect per-account results and recorded limitations. [Guide](async-live-validation.md) |
| TossTradingLab selection comparison | `tools.compare_research_selection` | Capture, freeze, FX history and completed research selection. Compare isolated output accounts. [Guide](research-selection-comparison.md) |
| Session capacity measurement | `tools.paper_capacity` | Sealed observed journal and measured workload. Check report and source identity. [Guide](recurring-paper-campaign.md) |
| Market-day download | `tools.download_market_day` | A date. `--status` checks an existing output's `market.sqlite3` and completion status. [Guide](market-day-download.md) |
| Reverse monthly download | `tools.download_market_month` | A month and seed. `--status` shows per-day progress from the collection output. [Guide](month-download.md) |
| October event archive | `tools.archive_october_events` | Output directory and optional saved excerpts. `--status` checks an existing archive. [Guide](event-sources.md) |
| Five-minute replay | `tools.replay_market_day` | Saved market database and new output. Inspect causal forecasts and planned calls. [Guide](five-minute-replay.md) |
| Upward-candle rules | `tools.search_up_candles` | Market dataset and output. `--status` reads the existing search output. [Guide](up-candle-search.md) |
| Portfolio rules | `tools.search_portfolios` | Market dataset and saved feature database. `--status` reads its portfolio search output. [Guide](portfolio-search.md) |
| Weekly hypotheses, long-only and whole-share sweeps | `tools.run_week_experiment` | Prepared historical inputs and a finite deadline. `--status` reads an existing experiment. [Guide](week-hypotheses.md) |
| Ledger round review | `tools.review_round` | Read-only ledger snapshot and report file. Check transaction attribution and reconciliation. [Guide](round-review.md) |
| WebSocket probe | `tools.probe_toss_websocket` | Explicit market, symbols and finite duration. Check acknowledgment and frame summary in its output. |
| Streaming load validation | `tools.validate_toss_stream` | A finite duration and separate output directory. Inspect subscription, REST and OHLCV evidence. |
| Local verification | `tools.local_validation` | Frozen repo, Python and fresh output directory. Check its validation report and command logs. |
| Architecture report | `tools.check_architecture` | `--root .` inspects local imports, cycles and duplicate bodies and prints a JSON report. |

The historical one-off tasks above are available on demand. Their documented inputs and
artifacts can outlive the worker. At inspection, the reverse monthly downloader and three
older US parallel-service labels were disabled. No evidence of a live historical run was
available.

## Data and recovery

- **Original inputs.** Keep prior session journals, source/config manifests, and original
  inputs that remain available. On this Mac `state/{backups,datasets,experiments,portfolios,replays,searches}`
  link to `/Volumes/external/T7/finance/papertrade-lab/state/`. Five older validation records
  resolve under `/Volumes/external/Mac Archive/Finance/state/validation/`.
- **Reproducible outputs.** A new offline replay belongs in a separate directory and uses
  the matching source and surviving inputs. The removed October 19–30 raw market databases
  require reacquisition for a full replay. The retained `collection-audit.sqlite3` files hold
  collection records.
- **Resume state.** Retain the ledger and SQLite sidecars while a process may use them.
  Preserve journal, plan, source digest, status, and scheduler recovery records as a set.
  Resume through the owning runner with the same dated inputs. Treat a failed or incomplete
  session according to its recovery procedure. Confirm external volumes are mounted before
  restoring or reading their files. Credentials stay under the user's private secrets directory.

## Monitoring

The Mac collector is deployed from `~/Projects/project-monitoring/finance_status.py` with
`~/.local/bin/python3.12`. Its LaunchAgent
`com.elsholtzia.project-monitoring.finance` runs at load and every 30 seconds. It reads the
Finance plans, statuses and launcher evidence, then atomically writes a limited snapshot to
`project-monitoring/.local/finance-source/status.json`. Prometheus scrapes the existing
exporter, which mounts the snapshot directory read-only. The Finance checkout, ledgers, and
credentials are outside the mount.

- **Start and stop.** From `~/Projects/project-monitoring`, use the existing Compose services.
  The collector is managed separately with `launchctl bootstrap "gui/$(id -u)"
  "$HOME/Library/LaunchAgents/com.elsholtzia.project-monitoring.finance.plist"` and
  `launchctl bootout "gui/$(id -u)/com.elsholtzia.project-monitoring.finance"`. Stopping
  monitoring leaves the experiment runners active. The next collector interval refreshes
  the snapshot after a failed invocation.
- **Status and logs.** Use `launchctl print "gui/$(id -u)/com.elsholtzia.project-monitoring.finance"`
  and the collector logs beside the snapshot. At
  [Grafana](http://127.0.0.1:3000/d/finance), phase, process state and last source update
  appear separately. A missing or stale collector snapshot reports source-down and omits
  runtime and phase values. A pre-open plan can legitimately have an old update timestamp.
- **Verification.** In [Prometheus](http://127.0.0.1:9090), query
  `project_status_source_up{project="finance"}` and
  `project_progress_phase_info{project="finance"}`. Compare the labels and timestamps with
  the Mac snapshot. The current paired runs have no known positive total for percentage
  progress. The original reports and journals remain the source for analysis and recovery.

## Next operational check

Verify the November 23 KR and US paired reviews through their saved decisions. Completion
requires a `decision.json` for each market linked to its plan and either a verified comparison
or a recorded reason to repair the evidence. A live comparison or promotion verdict remains
pending until its evidence is available.
