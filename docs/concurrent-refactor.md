# Concurrent paper refactor

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

## Planned ownership

Reuse existing modules and classes. These are logical responsibilities, so they can share a file.

| Responsibility | Integration point or ownership |
| --- | --- |
| Data gateway | Shared database boundary used by the paper runner |
| Provider adapter | Existing live feed and market-data adapter boundary |
| Features | Shared preparation at the observation cut |
| Concurrent evaluation | Separate account CPU workers |
| Ordered dispatch | Runner cut, account sequencing and subscription union |
| Strategy registry | Frozen strategy identity and settings |
| Forking | Account lineage and exact-cut state through shared slots |
| Journal and recovery | Adaptive journal and serialized economic commits |
| Campaign operations | Existing campaign runner, scheduling and admission |
| Evidence | Campaign evidence and recorded capacity |
| Strategic review | Existing Astra campaign review |
| Engineering handoff | Durable failure and review evidence for Sol/Astra |

The engineering supervisor runs its own regression jobs, has Astra review the same source and
evidence, and follows the branch/PR workflow. Its completion report leaves live validation pending
and lists host checks still pending outside the sandbox. The R1-R4 offline regressions, including
process overlap and crash recovery, must execute in the controller sandbox.
