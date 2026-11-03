# Event-driven validation supervisor

`python -m tools.autotest_supervisor` continues the existing finite live validation,
then runs a persistent validation loop without keeping a chat open.

| Stage | Executor | Trigger |
| --- | --- | --- |
| Wait for a test or child process | Python | Process completion, not an AI timer |
| Review evidence and acceptance criteria | `gpt-6-astra`, medium | New results |
| Choose the next useful test | `gpt-6-luna`, low | A review with unfinished work |
| Implement tests or a targeted fix | `gpt-6.1-sol`, medium | A concrete offline plan |
| Execute regression tests and experiments | Python | Implementation ready |
| Publish reviewed changes | Python and GitHub CLI | Tests pass and Astra approves the same source |

The first action is to inherit the existing run. A completed run is reviewed immediately.
While a test is running, only Python checks its durable exit record. No model is invoked because
time has elapsed. There are no application-level iteration, token, or model-call timeout caps.
Provider limits, authentication, machine uptime and external service availability still apply.

## Scope and boundaries

The requirement inventory covers live integration, causal replay, crash recovery, stream faults,
selection coverage, accounting and out-of-sample performance. Completion requires Astra to
support every requirement with evidence. Profitability and real queue position are not implied
by technical success. If all meaningful work is complete, the supervisor records completion.
If external state prevents progress, it records `blocked`, preserving the exact phase and logs.
It does not burn tokens rechecking the same failure indefinitely.

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

## Tests and GitHub

Automated cycles run the layered regression modules plus every test introduced by earlier
supervisor cycles, and any new tests in the current change. Optional experiments accept one
`--output` directory. Existing legacy modules outside the layered scope are not modified.

The complete repository suite is validated separately at deployment. Some legacy tests create
their own macOS sandbox, which cannot run inside the outer offline sandbox; these are not claimed
as executed in automatic cycles. The sandbox boundary test itself is also checked on the host.
Optional websocket tests explicitly skip when their pinned dependency is not installed.

Changes use an isolated branch/worktree. Failed implementations remain in that worktree for the
next correction. After tests and Astra approval, the controller commits, pushes, creates a PR,
waits for checks, merges with an exact head SHA, and fast-forwards local `master`. It never
overwrites a dirty checkout, rewrites history or ignores failing checks. New PR URLs are retained
in status/history. The standalone CLI cannot attach artifacts to this Codex chat; attach them
when inspecting results here. An interrupted external GitHub mutation blocks for reconciliation
instead of blindly creating or merging a second PR.

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

Each model directory contains its prompt, evidence manifest, schema, stdout/stderr, parsed result,
exit record and quota snapshot. Tests have the same command/exit audit, without AI usage. Exact
remaining tokens are unavailable when the provider only reports remaining percentages. These
project calls are distinct from tokens used by the interactive chat.

There is no periodic AI heartbeat and no promise of a chat notification from the standalone
daemon. Inspect its status and logs when needed. Stop the launchd service to stop supervision;
inspect child job identities before stopping an already running experiment. Preserve all audit
files when resuming a blocked run, and reconcile its recorded cause before changing its phase.
