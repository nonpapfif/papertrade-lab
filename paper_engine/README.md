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
