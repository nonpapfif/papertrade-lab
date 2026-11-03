# Papertrade Lab

## Project direction

Papertrade Lab is the validation precursor to Autotrade Lab, whose final goal is real-money
automated trading. Read `docs/project-direction.md` before proposing changes to execution,
AI responsibilities, validation, or strategy promotion. These are target requirements, not a
claim that the current implementation satisfies them or authorization to execute real trades.

## Repository workflow

- Create a dedicated branch before changing code, configuration, or documentation.
- Verify the change, commit it, and push it to GitHub.
- Open a pull request against `master` and record the change and validation results.
- Check the pull request and required checks, merge it, and fast-forward local `master`.
- The user has authorized this workflow. Do not ask for approval at every step.
- Keep unrelated changes out of the pull request. Exclude credentials, ledgers, and run logs.
- Write committed files, branch names, commit messages, and GitHub content in English.
- Communicate plans, progress, and results to the user in Korean.
- Review the complete staged text of each changed file. Run the `github-workflow` skill's
  `scripts/check_staged_language.py` before committing.
- Preserve published history. Normalize current files with ordinary commits.

## Experiment contract

The user has set two boundaries:

- Do not run AI continuously merely because time has passed. Use it for useful decisions.
- Never execute real trades.

The free simulation uses `simulation.enabled=true`.
The unrestricted fill and risk assumptions below apply to that profile. Separate layered
experiments retain their documented costs and execution constraints; neither profile's rules
silently redefine the other. Consult the relevant guide linked from `README.md`.
Its objective is to maximize virtual profit through active experiments, rather than preserve cash
or minimize losses. Frequent trades, concentration, failed experiments, and losing the entire
ledger are permitted. Do not default to waiting because of past losses or a small sample.
Stops are optional strategy tools, not mandatory loss limits.
Starting another ledger must not erase old losses or count new starting capital as profit.

- Do not require real-market hours, settlement, margin, displayed depth, whole shares,
  trading status, or corporate-action confirmation for virtual fills.
- Allow virtual shorts, borrowing, fractional quantities, additions, partial exits, and reversals.
- Do not impose entry cutoffs or forced exits based on holding time or market close.
- Assume zero costs and interest for new trades. Preserve historical costs and results.
- Count borrowing as debt. Cash inflows and short-sale proceeds are not profit.
- Distinguish observed prices from assumed fills. Record source time, assumptions, and fill time.
- Observe the full available universe. New signals can trigger review regardless of holdings,
  country, or AI watch levels. Watch crossings, exits, and user requests are additional triggers.
- Do not repeat AI calls for identical signals or time changes alone.
- Do not add arbitrary AI-call, token, reasoning-time, symbol, candidate, or holding caps.
- Restrict Toss access to OAuth and market-data or stock-information paths. Never call account,
  order, order-history, amendment, cancellation, or conditional-order paths.
- `/api/v1/orderbook` is market data. It is distinct from `/api/v1/orders`.
- Keep credentials and authentication headers out of logs and AI inputs. Treat AI output as data.
- Keep v1/v2 code and tests for comparison. Do not reinstate their constraints in the free profile.

## Agent skills

### Issue tracker

For tickets and specs, use GitHub Issues in `nonpapfif/papertrade-lab`.
Read `docs/agents/issue-tracker.md` before tracker operations.

### Triage labels

For issue triage, use the five canonical roles in `docs/agents/triage-labels.md`.

### Domain docs

Before exploring domain behavior, read `docs/agents/domain.md` for the single-context layout
and glossary/decision consumer rules.
