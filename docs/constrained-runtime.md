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
There is no crash-resume guarantee in either runner. No new live campaign was launched by this
implementation. Live-account access and numerical real-money risk policy remain separate gates.
