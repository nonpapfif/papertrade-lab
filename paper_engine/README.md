# Offline paper engine

This package implements version 1 of the offline market cut. A cut supplies an
expected predecessor, ordered normalized quotes, per-account proposals and
cancellations, and optional A-A and A-B branch commands. A strategy can supply
proposals through the immutable `Intent` contract. The package contains no
strategy implementation or broker client.

`execution` prices a pending whole-share request against eligible depth.
`account` applies fills, owns reservations and calculates fee-inclusive cost
basis and realized P&L. `session` applies the same accepted observations to
each account with separate paper depth. It returns the account results after
`journal` commits the complete cut. The ledger records one input per market
cut, the accepted quote identities and each account's economic outcomes.
SQLite stores the versioned genesis, cut chain and duplicate-identity indexes
in the same database.

An A-A and A-B command records both children and their shared predecessor.
Both inherit cash, positions, pending reservations, fees and realized P&L at
that cut. The candidate strategy applies to later entries. A position and its
inherited pending requests retain the original strategy owner. Historical
checkpoints let callers compare branch outcomes with the exact parent baseline.

## Execution assumptions

- KR/KRW and US/USD offline simulation uses whole shares and normalized limit
  prices. The caller supplies market-eligible quotes and requests. Venue tick
  grids, instrument and session eligibility, fractional modes, regulatory fees
  and broker rounding belong to their respective policies and adapters.
- Quotes carry unique observation IDs, source time and availability time.
  All times use epoch milliseconds. The cut checks freshness, arrival order
  and strictly post-intent source time. The default confirmation delay is one
  second and the default intent lifetime is 30 seconds.
- A quote supplies independent depth to each account. Within an account, pending
  requests consume depth in submission order. Unfilled quantities remain
  reserved until completion, cancellation or expiry. A selected one-attempt
  policy releases the remainder after the first usable confirmation.
- The configurable linear fee rates are offline paper assumptions. Historical
  accounting reconciliation can apply attributed `ConfirmedFill` records with
  their original fees through the same account transition. That verifies the
  recorded economics without rerunning historical quote execution under a
  different fill policy.

## Offline orderbook intake

`feed.books.Books` owns the latest orderbooks for all configured markets on one
event loop. Each request supplies its own freshness window. Concurrent misses
for a market and symbol share one provider request. Mark requests for held or
pending symbols as protected to give them priority over queued candidate reads.
An active provider read completes before the next request starts. Both markets
share the same read-only provider owner.

`feed.toss.TossBooks` uses the existing allowlisted `MarketClient.book` operation,
one symbol per call. It serializes the shared market-data group at the documented
15 book-operation starts per second or slower and carries the client cooldown across
markets. `TossStream` handles recorded or injected frames for one market. A
declaration requires a complete, matching acknowledgement before quotes can
enter the cache. Malformed market frames produce individual rejections. Locked,
crossed, and insufficient-depth books are unusable under the paper fill policy. Invalid
acknowledgements require disconnecting the old generation and opening a new
one. Quote identity uses the provider, market, symbol, source timestamp and
canonical book content. Equivalent REST and stream observations share that
identity and retain the first accepted availability time.

`feed.session_adapter.commit_next` takes the next quote in receipt order and an
explicit decision timestamp. A live caller must use its processing clock. A
recorded replay can supply the recorded decision time. Only the session writes
the journal and applies Phase 1 execution rules. An acknowledged duplicate
after reopen must match the recorded observation. Reconstructing an older
duplicate's receipt reads its journal prefix. This slice has no measured
throughput target. `protected_symbols` reads the union of committed held and
pending symbols for the caller's quote requests.

Offline tests use injected frames and a fake REST client. They cover malformed
frames between valid quotes on one generation, one cut per observation,
generation recovery, 429 cooldown, queue priority, concurrent reads and
per-caller freshness, stale queued inputs and idempotency after reopen.
Remaining Phase 2 work includes WebSocket transport, application PING and timed reconnects, REST
snapshot resync, automatic protection of stream subscriptions, broad price
batching, overload handling and measured backlog/latency with recorded load.
