# Strategy versions, evaluation and promotion

Status: target design. See [system design](autotrade-system-design.md).
This lifecycle is distinct from the engineering supervisor's GitHub review and publication.

## Versions and position ownership

A strategy artifact is immutable and identified by a content digest of its rules, parameters,
feature interpretation and declared dependencies. Store its manifest, provenance and executable
version; a family name such as `trend` is not a version identifier.
Execution, observation, accounting and risk-policy versions are recorded alongside it, not
silently folded into an optimizer-controlled strategy update.

Every intent pins its strategy artifact before reservation. Its eventual fills and position
owner retain that version. Existing positions continue with the pinned management rules after
promotion, including after restart and partial exit. Retain required artifacts while any
position, nonterminal intent, unresolved operation or evaluation references them.
Risk policy remains account-wide and may override strategy permission independently.

The first versioned account keeps one strategy owner for each account/market/symbol exposure.
A newly promoted version cannot add to or replace a legacy-owned position in the same symbol.
It waits until that position and related nonterminal exposure are resolved. Existing behavior
that disallows additions is not expanded by this ownership work.
If multiple-owner lots are wanted later, they require explicit allocation and broker-net-position
accounting, rather than overwriting one owner field.

Promotion changes the pointer used for new intents, not a live global strategy function.
Unsent entry intents from a retired version are invalidated and their safe-to-release
reservations released. Already dispatched entries remain attributable to their original version;
cancel their remainder only under an explicit operating policy and confirmed cancellation.
Exits and risk-reduction actions for existing positions retain the original intent lineage.

## Candidate lifecycle

```text
proposed -> validated artifact -> evaluation frozen -> evaluating
  -> eligible -> promoted
  -> rejected / inconclusive / invalidated
```

Structural checks reject unresolvable artifacts, forbidden capabilities and nonfinite sizing.
Those checks are not performance qualification. Every lifecycle transition records evidence,
actor, source versions, configuration digest and previous state.
Changing rules, selection, parameters or sizing after evaluation starts creates a new candidate.
It never edits the artifact currently owning a position or evaluated in a frozen comparison.

## Promotion contract

Create an immutable evaluation contract before either account sees evaluation observations.
It specifies equal capital, comparison currency, execution/cost assumptions, incumbent and
candidate digests, risk-policy digest, eligible scope, coverage rules and the evaluation horizon.
Also specify the objective, required improvement margin, evidence-sufficiency rule, uncertainty
method, allowed drawdown/concentration and acceptable valuation uncertainty.
These numerical settings require user-approved policy values. None is chosen by the candidate.

Use net performance after configured costs as the primary objective. Record realized results
and open positions separately. Compare account equity at a common evidence cut with matched
valuation rules and visible liquidation-cost/depth uncertainty; midpoint marks alone do not
prove cash realizability. Missing valuation cannot be silently treated as zero or success.
Separate funding changes, FX translation and corporate-action adjustments from trading return.

| Gate | Pass condition | Otherwise |
| --- | --- | --- |
| Configuration | All required contract and risk values exist and were frozen before exposure | Unqualified |
| Integrity | Source, artifacts, observations and account initial states match their manifests | Invalidated |
| Accounting | No unresolved financial mismatch, duplicate effect or policy violation | Rejected or blocked for explanation |
| Coverage | Predeclared horizon and evidence-sufficiency rule are satisfied | Inconclusive |
| Risk | Candidate satisfies every configured evaluation risk constraint | Rejected |
| Improvement | Net improvement meets the frozen margin and uncertainty rule | Rejected or inconclusive, as predefined |
| Deployment compatibility | Exact artifact is usable with current runtime and user risk policy | Revalidation required |

The verdict is calculated from saved evidence, not granted by AI prose.
An absence of signals/trades, a short winning streak, or clock changes cannot stand in for the
configured sufficiency rule. Trade counts and horizon are configurable evaluation requirements,
not arbitrary caps on AI usage, candidates, or holdings.

An eligible verdict records the incumbent digest and deployment generation it compared against.
Promote with an atomic compare-and-swap against that incumbent, current policy, compatible
runtime versions and operating state. Persist the verdict and pointer change together.
If another candidate was promoted first, the stale comparison cannot overwrite it; re-evaluate
against the new incumbent under a new frozen contract.
An active operational blocker prevents automatic promotion from clearing or bypassing it.

## Evaluation integrity

Use the same published live observations for independent accounts. Each account may choose
different candidates and quantities using its fixed strategy. Preserve its own reservations,
holdings and outcomes. Never share consumed paper depth or cash across counterfactual accounts.
Do not sum alternative paper fills and claim they were simultaneously achievable in one account.

The strategy-development record identifies all data and results available to the optimizer.
The final evaluation segment must begin after candidate and contract freeze and must not have
been used to choose that candidate. Recorded experiments may use a sealed, previously unseen
segment only with an explicit data-visibility manifest.
If AI sees interim results and changes the strategy, the changed artifact gets a new future
evaluation. The old segment becomes development evidence, not a fresh final test.

Record all proposed/evaluated candidates, failures, rejections, withdrawals and incomplete runs.
Do not report only winning candidates or reset ledgers to hide losing periods.
The contract declares how repeated candidate selection and correlated observations affect
uncertainty. Treat independent sessions/periods explicitly; trades are not automatically
independent samples. If the declared method cannot establish improvement, report inconclusive.

Metadata, candles, costs and universe eligibility use their observed availability times.
Future corrections and later delisting knowledge cannot rewrite earlier decisions.
Record scanned versus subscribed versus usable-symbol coverage. The absence of observed
opportunities outside subscriptions is missing coverage, not proof of no opportunities.
Historical simulation remains a supporting screen; it does not waive final qualification.

## AI authority and artifact execution

The optimizer can propose rule/selection/assignment/sizing artifacts and analyze supplied results.
It cannot edit evaluation contracts, evidence, verdicts, risk limits, operational controls,
execution accounting, order adapters or the active-artifact pointer.
Test-engineering workers that fix infrastructure use separate reviewed authority; do not reuse
their broad editable-file list as optimizer permissions.

Candidate code, if executable, runs in an isolated strategy process with no broker credentials,
network, account mutation or control-state writes. Its output is validated intent data.
Broker/execution processes never import untrusted candidate modules for direct execution.
Treat manifests and model output as data; paths and code cannot nominate arbitrary executables.
Use existing local isolation facilities rather than adding a new remote execution platform.
Detect artifact/evidence mutation before accepting results or changing the active pointer.

AI is triggered by new meaningful evidence or a concrete failed acceptance check.
Deduplicate the evidence fingerprint; do not call AI again just because a timer elapsed.
No application-level arbitrary AI-call, token, candidate or holding caps are introduced.

## Rollback and acceptance

Strategy rollback changes the artifact for new entries through a durable operating command.
It does not rewind real executions, cash, taxes or position ownership.
Existing positions and dispatched requests keep their pinned version until resolved.
An infrastructure bug in an old management version requires separately reviewed repair and an
audited migration; do not silently substitute new rules for its open positions.

- Promotion is idempotent and cannot erase prior losses or duplicate new-entry activation.
- Existing positions and late fills retain original ownership across promotion and restart.
- Two eligible candidates racing against one incumbent cannot both overwrite its generation.
- Changing evidence, runtime assumptions or policy invalidates stale promotion authority.
- Sparse, incomplete or reused evaluation data yields the declared non-pass outcome.
- Candidate attempts to change limits, evidence or execution state fail before promotion.
