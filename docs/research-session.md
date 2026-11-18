# Shared-feed research sessions

Run one dated KR or US continuous session from the repository checkout on macOS. The
scheduler owns one Toss collector. Committed observations reach three independently funded
virtual accounts. No broker account or order endpoint participates.

## Install and run

Use Python 3.12 or later in a virtual environment. Install the pinned graph and WebSocket
extras, configure the existing Toss OAuth credential files in the user's private secrets
directory, and authenticate the Codex CLI configured by `ai.binary` in `settings.toml`.
Both AI and simulation must be enabled. Default research uses the logged-in Codex CLI.
The feed supplies price, volume and order-book evidence. No additional news, social or
fundamentals service key is required for this configuration.

```sh
python -m pip install -e '.[research,websocket-probe]'
python -m tools.research_session run --market KR --day YYYY-MM-DD --output state/sessions/KR-YYYY-MM-DD
python -m tools.research_session run --market US --day YYYY-MM-DD --output state/sessions/US-YYYY-MM-DD
python -m tools.research_session replay --session state/sessions/US-YYYY-MM-DD --output state/replays/US-YYYY-MM-DD
```

Replace the date with the exchange's local trading date. Use a separate directory per
session under ignored `state/`, or outside the checkout. Keep the source checkout pinned
until capture and replay finish. The first command for each market verifies the provider
calendar and uses its continuous-session cutoff, including KR's closing-auction boundary.
For an unattended session, launch the same run command with a persistent terminal or a
process supervisor. The comparison process must stay running to make timely decisions. To recover it, repeat
the exact invocation. The session lock and scheduler claim prevent duplicate collectors.
Unprocessed historical quotes are first made available at recovery time. They cannot
confirm an intent computed after they arrived. The scheduler retries supported transient
feed failures until the session cutoff. Nonrecoverable failures remain visible.

## Accounts and research

Each US account starts with USD 1,000 and each KR account with KRW 1,000,000 by default.
Override the per-account starting amount with `--capital`. Ledgers use independent cash,
reservations, positions, strategy ownership, fees and delayed whole-share fill simulations.

| Account | Eligible entries | Strategy |
| --- | --- | --- |
| existing_selection | Eligible symbols from the shared scanner | Trend |
| research_selection | Scanner symbols with published Buy or Overweight | Trend |
| research_alternative_strategy | Same published selection | Rebound |

The scanner uses batched universe prices, instrument eligibility and a single bounded
market-data subscription. Held and pending symbols remain protected across subscription
changes. All accounts use the same observations. An eligible trend or rebound transition
on fresh candles and book prices requests research. Fixed workers run the optional
TradingAgents graph outside the collector. The default concurrency is two. Each symbol holds one queued slot for its latest active
signal. Superseded generations retain their own diagnostic records. Changes of
clock alone do not schedule another analysis. A missing, late or interrupted result
cannot authorize an entry. Each completed rating applies from its recorded publication
until a later publication or the session horizon. Existing holdings follow their strategy.

The collector honors its API-group cooldowns and the provider's combined subscription
capacity. The local worker setting limits concurrent research, separate from provider
quotas. See [Toss capability evidence](toss-capability-evidence.md) for the documented
limits and data constraints.

## Evidence and interpretation

- `research/reports/<run-id>/` contains the summary, twelve agent-role files, source
  evidence and a manifest. Roles without news, sentiment or fundamentals input are
  explicitly skipped. Failed or interrupted runs preserve the roles that already finished.
  The manifest identifies the upstream graph revision and model.
- `research/attempts/` records signal, publication time, eligibility and error status.
  `status.json` is the intermediate comparison. `comparison.json` seals all three accounts.
- `schedule/journal.sqlite3` holds the common market observations and their hash chain.
  `projection.bin` records one eight-byte local delivery timestamp per shared journal entry
  so offline account decisions use the same information at the same time. These are replay
  inputs. Runtime error audits store incidents. Successful HTTP calls do not accumulate
  in the research-session HTTP audit.
- A complete capture requires a verified calendar and closed journal, at least 80% usable
  book coverage and a maximum book gap of 180 seconds. Ranking also requires recurrent
  scans, fresh features for at least 80% of eligible-symbol time after warmup, completed
  research, actual simulated activity and eligible valuation across all three accounts.
  Coverage and missing inputs are included even when a ranking is withheld.
- Replay verifies the recorded source, publication files, projection clock and shared journal,
  and compares the reconstructed result to the sealed output. It makes no broker or
  model calls. A partial capture can be verified as partial. The earlier fixed-cohort
  historical experiment retains its [own comparison procedure](research-selection-comparison.md).
