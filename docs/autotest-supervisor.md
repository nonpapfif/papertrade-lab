# Event-driven validation supervisor

`python -m tools.autotest_supervisor` continues the existing finite live validation,
then runs a persistent validation loop without keeping a chat open.

| Stage | Executor | Trigger |
| --- | --- | --- |
| Wait for a test or child process | Python | Process completion, not an AI timer |
| Review evidence and acceptance criteria | `gpt-6-astra`, medium | New results |
| Specify the next change and unittest targets | `gpt-6-astra`, medium | A review with unfinished work |
| Implement tests or a targeted fix | `gpt-6.1-sol`, medium | A concrete offline plan |
| Execute regression tests and experiments | Python | Implementation ready |
| Publish reviewed changes | Python and GitHub CLI | Tests pass and Astra approves the same source |

The first action is to inherit the existing run. A completed run is reviewed immediately.
While a test is running, only Python checks its durable exit record. No model is invoked because
time has elapsed. There are no application-level iteration, token, or model-call timeout caps.
Provider limits, authentication, machine uptime and external service availability still apply.

## Scope and boundaries

The requirement inventory is M1-M4 in [the active implementation plan](autotrade-implementation-plan.md):
fixed configurations, independent shared-input accounts, comparison evidence, focused tests
and one finite US live demonstration. Completion requires Astra to support each with evidence.
The incumbent is `router`; the fixed candidate is `trend`, chosen to exercise comparison plumbing,
not as a profitability recommendation. Profitability and real queue position are not implied
by technical success. If all meaningful work is complete, the supervisor records completion.
If external state prevents progress, it records `blocked`, preserving the exact phase and logs.
It does not burn tokens rechecking the same failure indefinitely. Recovery platforms, exact
replay, optimization, promotion and KR rollout remain deferred. Do not add tasks to stay busy.
Astra supplies an executable next plan for each continuing review. Python schedules the named
tests directly for a clean source tree or runs the regression suite after Sol implements a fix.
Unchanged source and test targets already checked with the same interpreter stop for new evidence.
A legacy review without an executable plan gets a fresh Astra review before scheduling work.
Failed assertions remain offline fix work, and mechanical safety guards still stop the controller.

Only fictional ledgers are permitted. No real broker account or order endpoints are allowed.
The monthly collector stays paused. Existing ledgers and losses remain intact. The layered
experiment remains long-only, whole-share, without leverage, with costs and conditional fills.

The implementation worker has a Codex workspace-write sandbox with tool network access disabled.
Review and planning use the read-only sandbox. MCP, plugins, browser access and subagents are
disabled. Workers are instructed not to read credentials, run tests, publish, or call other models.
The CLI uses saved Codex authentication; no new OpenAI or Toss key is requested.

Generated tests run under a separate macOS sandbox with network access and credential reads
denied. Only their worktree, dedicated artifact directory and private scratch directory are
writable. Control state, model outputs and other worktrees are outside those write roots.
Source digests bind tests, review and publication, including the Git executable bit. A changed
source invalidates the result. The worker cannot modify supervisor or broker/auth machinery.

Live integration always runs from the frozen baseline commit, including its import dependencies.
New engine changes are tested offline until their live deployment is separately reviewed. This
avoids executing newly generated code with broker network access. A live rerun must address a
specific missing observation. It is not a substitute for testing a changed engine.
The frozen runner receives `--candidate-policy trend`, so its live evidence covers two accounts.

## Tests and GitHub

Reviews that request fixes or report a blocked state cannot simultaneously authorize publication.
An explicit mechanical-check failure prevents completion even if all requirements are listed
as satisfied. Initial inherited evidence can still be reviewed without scheduling a new test run.

Automated cycles run the layered regression modules plus every test introduced by earlier
supervisor cycles, and any new tests in the current change. Optional experiments accept one
`--output` directory. Existing legacy modules outside the layered scope are not modified.

The complete repository suite is validated separately at deployment. Some legacy tests create
their own macOS sandbox, which cannot run inside the outer offline sandbox; these are not claimed
as executed in automatic cycles. The sandbox boundary test itself is also checked on the host.
Optional websocket tests explicitly skip when their pinned dependency is not installed.
The recurring-paper offline suite lists seven additional host-only checks in
`host_checks_not_run`. Two open a loopback WebSocket and five launch detached scheduler
children whose identities use macOS `/bin/ps`. All other scheduler tests run offline.
The list records checks that were outside the offline job, with no inferred pass or skip.
Run these checks separately on the host against the exact reviewed source and retain that
result with the deployment validation. The offline network and credential restrictions remain in force.

Changes use an isolated branch and worktree. Failed implementations remain there for the next
correction. After tests and Astra approval, the controller commits, pushes and creates a PR.
It verifies the reviewed head of the open PR and saves terminal state `pr_ready` with the PR URL.
Merging and updating local `master` are separate user actions. An interrupted publication blocks
for reconciliation before any further publication.
Publication runs the installed GitHub workflow's staged-language guard before committing.
Branches use the `codex/` prefix.
PR bodies use the English requirement, changed file paths and test/review status. Localized
model response prose stays in the private audit rather than being copied into GitHub content.

## Durable state and operation

Required arguments:

```sh
python3 -m tools.autotest_supervisor \
  --repo /Users/elsholtzia/Projects/Finance \
  --output /Users/elsholtzia/Projects/Finance/state/validation/autotest-RUN_ID \
  --inherited /absolute/path/to/existing/live/run \
  --codex /absolute/path/to/codex
```

Use launchd with `RunAtLoad`, a restart-on-unsuccessful-exit policy and `caffeinate -i` for
unattended operation. Keep one supervisor per output directory. A filesystem lock rejects a
second instance. Its independent child jobs survive a controller interruption and publish their
own exit records. A claimed job with missing or inconsistent identity is never silently rerun.
A saved transition before an unclaimed launch can resume safely. `status.json` contains the
current phase, completed requirements, history, model usage and the latest quota snapshot.
The requirement definitions are saved in that state. A different or missing saved scope is
rejected before any job starts; preserve earlier audit directories and use a new output for
this milestone rather than silently reusing the previous broad campaign's progress.

Each model directory contains its prompt, evidence manifest, schema, stdout/stderr, parsed result,
exit record and quota snapshot. Tests have the same command/exit audit, without AI usage. Exact
remaining tokens are unavailable when the provider only reports remaining percentages. These
project calls are distinct from tokens used by the interactive chat.

There is no periodic AI heartbeat and no promise of a chat notification from the standalone
daemon. Inspect its status and logs when needed. Stop the launchd service to stop supervision;
inspect child job identities before stopping an already running experiment. Preserve all audit
files when resuming a blocked run, and reconcile its recorded cause before changing its phase.

## Recurring campaign engineering scope

Use `--scope recurring-paper` with a new audit output directory for the explicitly authorized
[C1-C4 campaign](recurring-paper-campaign.md). Historical audit state cannot be reinterpreted.
The initial action reviews implementation and deployment evidence instead of waiting for a
finite inherited market run. Astra directly assigns fixes and tests; no Luna planning stage runs.
Completion requires controller-owned passing checks and source-matching verified deployment
evidence in the inherited directory. That evidence does not claim future sessions completed.
The market service is independent and remains active after the engineering supervisor finishes.
Supervisor authority, broker/auth machinery and scope definitions remain protected from workers.

## Concurrent refactor engineering scope

Use `--scope concurrent-refactor` with a new audit directory and pass the failed run as
`--inherited` for read-only diagnostics. The [R1-R4 contract](concurrent-refactor.md) governs this
offline campaign. It starts with Astra review and uses Sol for code changes. Live plans and live
launches are forbidden in this scope. The earlier finite and recurring deployment gates still
apply to their own scopes.

The runner and explicit data, journal, slot and campaign modules can change for this scope.
Supervisor, broker/auth and execution-boundary protections still apply. Python uses the recurring
test partition and retains introduced regression files across PR cycles. The offline suite must
execute at least one test without skips. Host checks excluded by the partition are listed separately.
Use the repository's pinned optional WebSocket dependency for applicable offline tests.

Publication requires source-matched test and Astra exit records from the current cycle, plus a
passing full local report supplied with `--local-validation <path>/validation.json`. Complete the
R1–R4 corrections on the same branch. `pr_ready` records the reviewed source and leaves live
validation pending. A report marked complete after a separate user merge still requires the
published and local source to match the verified tree.

A failed campaign writes `engineering_handoff.json` in its session directory. Use that directory
as `--inherited` so the existing supervisor reads the task, journal and scheduler references.
Keep the same supervisor output directory when reconciling the task. The campaign records the
handoff and leaves the failed session blocked. An explicitly started supervisor loads that packet
into the first Astra review, then uses its existing reviewed-plan step to assign changes to Sol.
Writing the packet alone starts neither worker. A separate launch approval is required before
using corrected code on market data.
