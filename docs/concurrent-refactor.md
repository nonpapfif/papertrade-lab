# Concurrent paper refactor

The [runtime maintenance contract](runtime-maintenance.md) defines common utility ownership,
dependency direction and error/warning audit coverage for this implementation.

This contract defines the authorized offline engineering scope. Use the constrained paper
execution rules and existing accounting. The failed r4 run remains read-only diagnostic evidence.
Use a new run identity and data directory. Do not resume r4 against changed code or reuse its
deployment status. Real trades and broker account/order access are forbidden. All validation in
this scope uses recorded or synthetic inputs. Actual live validation requires a separate decision.

## Acceptance requirements

- **R1 — Shared data path.** Integrate the gateway into the actual runner. A shared database
  stores reusable data with source identity, source and receipt times, and explicit freshness and
  validity rules. Fresh cache hits produce no provider calls. Concurrent processes requesting the
  same missing data share an in-flight fetch. The gateway batches compatible requests across
  accounts and uses one existing market-data adapter. Verify actual calls for hits, misses,
  overlap, failures and recovery. Credentials stay outside the gateway and account workers.
- **R2 — Concurrent accounts.** Publish one immutable, ordered observation cut and compute
  shared features once per cut. Separate CPU worker processes evaluate independent account state.
  Record process IDs and evidence of overlapping CPU work. Preserve logical cut, source and
  receipt times, subscription union, intra-account order and independent execution/positions.
  Delayed or failed workers must not silently advance account state or contaminate other accounts.
- **R3 — Versioning and durability.** Pin strategy identity, configuration, ownership and exact
  observation/journal cut. Fork from that cut with the same economic state and independent later
  changes. Serialize durable commits. Compare uninterrupted runs to replay and restart after
  worker death, partial results and commit-before-ack failure. Verify no duplicate intents, fills,
  fees, capital, or accounting changes. Preserve positions, pending ownership and losses.
- **R4 — Operations and evidence.** Measure recorded end-to-end capacity including database,
  process IPC, journaling and status overhead. Persist per-account, fee-inclusive outcomes and
  accepted-book marks with their actual observation provenance. Record coverage, failures and
  ambiguous/incomplete runs. Keep Astra strategic review and engineering handoff independent of
  slot availability. Deduplicate both by evidence identity and route runner failures into the
  handoff. Record unresolved live limits. Capacity or strategy evidence does not authorize a
  restart or establish profitability.

## Runtime ownership

Reuse existing modules and classes. These are logical responsibilities, so they can share a file.

| Responsibility | Integration point or ownership |
| --- | --- |
| Data gateway | `tools/market_data_gateway.py`, `tools/market_data_repository.py` |
| Provider adapter | `tools/market_data_adapter.py` with the existing `MarketClient` |
| Features | `Runtime` and shared observation cuts in `tools/shared_feed_slots.py` |
| Concurrent evaluation | `tools/account_executor.py` and its account processes |
| Ordered dispatch | `SharedFeedSlots` and `Runtime` in `tools/run_async_live_validation.py` |
| Strategy registry | Frozen account configurations and policy versions in `Runtime` |
| Forking | Slot branch commands and committed checkpoints in `SharedFeedSlots` |
| Journal and recovery | `tools/adaptive_journal.py` and serialized cut commits in `SharedFeedSlots` |
| Campaign operations | `tools/run_paper_campaign.py` and `tools/paper_campaign/controller.py` |
| Evidence | `tools/paper_campaign/evidence.py` and `tools/paper_capacity.py` |
| Strategic review | `tools/paper_campaign/review.py` |
| Engineering handoff | `Campaign` failure packets and `tools/autotest_supervisor.py` |

`tools/runtime_audit.py` persists incidents and monitors registered workers. Campaign and finite
session watchdogs run separately from the work they observe.

The audit stores warning and error severity with sanitized identity and origin fields. Repeated
incidents share an identity. An active warning can escalate to an error, and a verified successful
observation can resolve the matching incident. When audit storage fails, a separate sanitized
emergency record identifies the failure and the runtime blocks the next economic commit.

Active campaign and account processes scope Python warning, emitted logging, unhandled asynchronous
task and thread capture to their runtime owner. The scopes restore the previous handlers and
warning filters on exit. Diagnostic records keep the source basename, line and category or
exception type. Application messages and arguments stay outside the audit. A logging call that
Python filters before creating a record does not emit a diagnostic.

The scoped `warnings.warn` and `warnings.warn_explicit` functions record warnings raised as
errors before application code can catch them. Previously bound aliases and C-extension
emitters bypass those hooks. The display hook and scope-exit handler still capture displayed
warnings and escaping warning exceptions from those sources. Internally caught exceptions
from those sources can be missed.

Normal queue waits, receive polling, client EOF, expected task cancellation and market or calendar
cutoffs are control flow. Rejected data, protocol violations, unexpected disconnects and failed
cleanup are recorded separately.

The engineering supervisor runs focused offline regression jobs and gives Astra the corresponding
source and evidence. Full local validation checks the same source separately and includes tests
that need host-local capabilities such as loopback sockets and detached children. Publication
stops at an open PR with the reviewed head. Live validation remains pending.

Run the full local gate with existing Ruff and mypy executables on `PATH` and a Python environment
with the test dependencies already available.

```sh
python -m tools.local_validation --python /path/to/python --output /private/path/local-ci
```

Use a new ignored or external output directory. The validation report records the exact commands,
exit codes, timing, log hashes and source digests. It requires a nonempty suite with zero skips
and identical source before and after the checks.

Recorded capacity feeds accepted market-data messages through the gateway repository, concurrent
account workers and serialized journal. It includes status writes at the recorded one-second
cadence and retains per-account accepted marks, modeled costs and worker process evidence.
Capacity limits apply to branch dispatch. Fresh strategy evidence can still trigger analysis
when slots are full. Parent and reaped-child RSS are separate process-lifetime high-water marks.
A measured offline capacity bound leaves live throughput and simultaneous peak memory unresolved.
