# Explicit broker-constrained paper profiles

The first runtime slice implements fresh KR whole-share accounts and a US regular-session
trend baseline. It does not complete the full [market design](kr-us-strategy-design.md).
Both commands require `--profile broker-constrained`. Omitting it preserves legacy behavior.
Only market-data and stock-information endpoints are used. Real orders remain forbidden.

## KR diagnostic comparison

```sh
python -m tools.run_kr_paper_comparison \
  --profile broker-constrained \
  --output state/validation/kr-constrained-unique/run \
  --until YYYY-MM-DDT15:30:00+09:00
```

This profile always creates four independent KRW 100,000 accounts: fixed and AI-approved
signals at each of the 5% and 10% allocations. Accounts start empty, without historical replay
seeds. Decisions, AI inputs, sizing and reporting use the same fee-inclusive account. Pending
buys reserve cash, duplicate intents cannot reserve it twice, and entry includes commission.
Whole-share purchases that cannot afford one share are skipped. Historical files are untouched.

The provider's `today.integrated.regularMarket` establishes the regular opening and closing
auction boundary. A missing boundary or mismatched date fails initialization. New decisions and
assumed fills require source and receipt times inside the continuous window. The chosen policy
stops before the closing auction, even though the observation deadline may be later. Positions
remain open and are not automatically liquidated. The window is fixed for this single-day run.

The universe remains observed, but only verified active ordinary common shares on KOSPI/KOSDAQ
with known non-halted venue flags and no liquidation-trading flag can enter the comparison.
Unknown classifications and unresolved ETF/ETN withholding are excluded. Metadata is frozen at
initialization, not continuously certified. Venue eligibility can change after that snapshot.

The manifest records instrument and session evidence, initial account state and execution-source
hashes. KR commission 0.015% and ordinary-stock sale tax 0.20% remain dated, unrounded assumptions.
Account-specific API commission, fee rounding and settlement buying power remain unverified.

This is still the directional-signal diagnostic runner. Its fills use a newer sampled last trade,
not bid/ask depth. It is not the finished KR layered engine. AI receives the causal sampled path,
strength definition, eligibility, cash/reservations, cost assumptions and approval criteria.
Insufficient inputs are recorded as `ai_context_unavailable` without invoking a model. Same-direction
observations without a watch crossing do not generate another review merely as time passes.

## US layered baseline

```sh
python -m tools.run_async_live_validation \
  --profile broker-constrained \
  --output state/validation/us-constrained-unique/run \
  --duration 1200 --capital 100
```

Capital is USD. The existing asynchronous layered engine retains the strategy owner, reserves
fee-inclusive cash and confirms fills using a later observed quote with visible depth. Partial
fills leave the remaining position owned by its entry strategy. The profile enables only the
trend baseline. Candidate comparison and US fractional modes are not enabled by this slice.

Provider US regular-session dates, including early closes, gate quote source, publication and
decision times. Unknown or closed sessions do not execute buys or sells. The calendar is refreshed
when the US date changes. Strategy features use regular-session completed-bar closes and the
provider's minute-end timestamp, without adding another minute. Legacy fixtures retain their
explicit historical start-time convention.

Commission exemption is disabled. The 0.1% commission, SEC pass-through/minimum/rounding and
October-December 2026 TAF schedule are labelled scenarios rather than account-verified rates.
Paper sale proceeds are immediately reusable, which does not certify broker buying power.
The manifest records these assumptions and the official capability snapshot. Reverify dated costs
before a future-date campaign. The existing finite runner's controlled faults remain enabled.

## Validation and remaining work

Offline fixtures cover cash reservation, whole-share affordability, account isolation, partial
fills and retained ownership, calendar boundaries, stale/future prices, provider candle end
semantics and causal AI context. A passing fixture does not establish live profitability.

The next required implementation is the KR layered quote/depth path. It must replace the separate
diagnostic signal runner for full strategy validation. Durable account/intent recovery, final
valuation freshness, cost-aware deterministic entry, and matched live validation remain open.
Recovery is available for the adaptive US mode described below. Live-account access and numerical
real-money risk policy remain separate gates.

## US adaptive exploration

```sh
python -m tools.run_async_live_validation \
  --profile broker-constrained --adaptive \
  --end-at 2026-11-05T06:00:00+09:00 --capital 100 \
  --output state/validation/us-adaptive-unique/run
```

This finite mode creates two fresh USD accounts with identical trend rules, equal allocation,
starting capital, market observations and execution assumptions. The incumbent stays frozen.
The adaptive candidate can change new-entry policy between trend and rebound, selection
between all and active, and allocation between equal and priority. These are combinations of
existing deterministic rules. Candle-high breakouts, new executable rules, real trades and
changes to the execution contract are outside this experiment.

A fresh completed feature entering a supported rule for a symbol, or a newly recorded paper fill,
can trigger an asynchronous review. Repeated matching features do not trigger duplicate reviews.
Evidence that accumulates while a review is in flight remains available for the next review.
No clock-only review runs. Sol medium is the default. Astra
medium receives the same evidence only when Sol identifies a concrete unresolved question.
The runner validates a closed schema locally and queues approved configuration changes in
publication order. Earlier decisions, reservations and held positions preserve their original
strategy and version. No proposal can change the modeled costs, liquidity, calendar or cash.

Controlled delays and disconnects are disabled. Output includes shared input digests, AI
evidence and responses, the effective policy timeline and intent versions. The comparison is
descriptive adaptive exploration. It is not fixed-version validation or promotion evidence.

## Adaptive failure audit and recovery prerequisites

A terminal error records the exception class and traceback locations without dumping exception
arguments or local variables. A consumer failure also records the observation sequence, type,
publication time, and book symbol in `runtime_event_failed`. Use these fields to locate the
recorded provider observation and reproduce it offline against the manifest's source version.
Past runs containing only an exception class cannot recover the missing traceback.

Enable durable recovery on a new adaptive US schedule with `--recover` on
`tools.schedule_async_live_validation`, alongside its normal start, end, capital, source digest
and output arguments. The initial attempt uses `run/` and `job/`; recovery attempts use
`attempts/<number>/`. All attempts share `journal.sqlite3`. Keep that directory on reliable
local storage. Prior non-journaled runs remain separate historical evidence.

The worker commits exact ordered inputs, applied policy changes and paired account checksums
to SQLite. An exclusive lock prevents simultaneous workers from updating the same journal.
The worker checks source and settings hashes, capital, profile and cutoff, then replays the
committed events before contacting the provider. Replay makes no AI calls. A recovery gap
expires old intents under their existing rules and is reported separately from observed data.
An event interrupted before commit may appear in attempt logs without being part of the
restored account. The journal is authoritative for the committed boundary.

The scheduler can restart a killed runner or a classified transient market-data failure.
It waits for recorded provider cooldowns and keeps the original deadline. A repeated failure
without journal progress, invalid journal, unknown process identity, explicit interruption,
or incompatible source stops the schedule. Reattaching the scheduler with the same arguments
reuses its durable attempt and restart records. Preserve old attempt files when investigating.
