# Parallel live US paper accounts

This runner starts a separate market-data-only experiment. It does not start the legacy
continuous workers, call AI, or request real account or order endpoints.

```zsh
python3 -m tools.run_live_parallel \
  --date 2026-11-02 --capital 100 --prior-realized-krw 0 \
  --trade-from-regular --output state/live/2026-11-02-us-parallel-v3
python3 -m tools.run_live_parallel \
  --output state/live/2026-11-02-us-parallel-v3 --status
```

## Comparison design

The 59 policies each receive an independent virtual USD 100 in one quote-based execution model.
The ideal last-price, zero-fee comparison is excluded from live runs. Its implementation remains
available only for historical module tests.

The 59 evaluation books use long positions, integer shares and cash-funded purchases.
Policy selectors compare 2,178 configurations. They include momentum, mean reversion, sampled breakout,
deviation and acceleration signals, multiple lookbacks, exits, sizing and session settings.
Each policy owns its cash and positions. Visible depth is consumed within one account and
observation; separate research accounts independently reuse the same depth. This does not
model market impact from submitting all accounts together.

Cold/carry policies and several daily weighting methods coincide on their first day. The
`behavior_group` field identifies matching trade histories. The account count is not a count
of independent strategies or evidence for a probability of profit.

## Shared observations and causal execution

The runner freezes the available ACTIVE NYSE, NASDAQ, AMEX and US_ETC universe at startup.
It requests all symbols in batches of 200, targeting one shared snapshot every 60 seconds.
Policy decisions target five-minute boundaries. Actual receipt, provider and decision times
are recorded. Provider pacing, collection and computation can delay checks; missed intervals
are not replayed as fictional live fills.

Inputs are sampled current prices, not exchange OHLCV candles. Only received observations
can enter a lookback. Price quotes older than 90 seconds are quarantined; gaps longer than
90 seconds invalidate continuous signal windows. Volume-dependent configurations and the
four volume-only policies are excluded. Sampled highs and noise differ from historical
candle features. Do not compare live and historical returns as identical input experiments.

Actual quote accounts request depth only for needed candidates and held symbols. Initial quotes
must be USD, no more than 60 seconds old and no later than the decision time. The initial best
ask sets a buy limit; the initial best bid sets a sell limit. After an assumed minimum latency
of one second, the runner requests another quote. Both its receipt and its provider update must
follow the decision. The one-second floor is a scenario assumption, not measured Toss latency.

The local immediate-or-cancel model uses only the second quote's visible quantity within the
initial limit. A worse price, absent update, invalid quote or missing depth leaves the request
unfilled. Reduced depth can fill part of an entry or exit. Only filled shares change cash,
fees, tax basis and positions. Remaining requested shares are canceled; no resting order is
queued. Unsold holdings remain in the account for the next check. Partial exits allocate entry
fees and KRW basis proportionally; a position counts as closed/won only after its final exit.
Attempts record requested, filled and unfilled quantities separately from trades.

Stops, targets and trailing checks use the initial bid, including held symbols whose last
trade is stale. The initial affordability screen still uses the last price. There are no real
order requests or intracheck target fills. IOC describes the local model, not verified broker
order-type availability.

These are conditional fill estimates, not actual execution confirmations. Two public quote
snapshots cannot reconstruct queue priority, intervening cancellations, hidden liquidity,
market impact, broker rejections or venue routing. No calibrated fill probability is claimed.
See [SEC execution guidance](https://www.investor.gov/introduction-investing/investing-basics/how-stock-markets-work/executing-order)
and [Alpaca's simulator limitations](https://docs.alpaca.markets/us/docs/paper-trading).

Shadow learning uses the declared last-price fee proxy to avoid requesting depth for every
configuration. These selector scores are approximations, not reported realizable profits. This
remaining learning proxy does not model executable spreads or failed fills. Raw observations
and frozen source hashes make that distinction auditable. Rankings use current and prior received data, never a later
frame or end-of-session winner.

## Commissions and annual tax estimates

The manifest freezes the following schedule for October-December 2026:

- Published general US brokerage commission: 0.1% per order above USD 10; no commission at
  or below USD 10. A private account's promotional rate is not queried.
- SEC sales rate: USD 20.60 per million. Ceil-to-cent rounding and a USD 0.01 minimum are
  explicit customer pass-through estimates; exact Toss billing has not been verified.
- FINRA TAF: zero during the October 1-December 31, 2026 holiday.
- Korean resident individual: annual realized KRW profit, less KRW 2,500,000 allowance,
  multiplied by 22%. The signed `--prior-realized-krw` argument defaults to zero.

Entry and exit fees reduce trading cash. Trade net profit and win counts include both sides'
fees. Annual tax uses each purchase's fee-inclusive KRW basis and each sale's fee-net KRW
proceeds at their respective observed USD/KRW rates. Missing FX makes the tax estimate unknown.
Trade-time midrates are an estimate, not the statutory settlement-date FX basis.

Tax is a separately reported reserve, not a per-trade cash withdrawal. Unrealized gains do not
enter realized tax profit. Each research policy receives its own hypothetical annual allowance;
the accounts are not combined into one taxpayer. A separate hypothetical liquidation reserve
includes the bid sale proceeds of open positions, exit fees and their saved KRW purchase basis.
The after-tax liquidation estimate subtracts this projected reserve. Missing basis, FX or depth
makes that estimate unknown.
Dividends, ADR charges and corporate actions are not modeled. Existing dollar capital does
not incur an initial KRW conversion fee.

Sources: [Toss general fees](https://www.toss.market/service/securities),
[SEC rate advisory](https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2),
[FINRA temporary TAF change](https://www.govinfo.gov/content/pkg/FR-2026-09-23/html/2026-19392.htm),
[Toss foreign securities explanation](https://home-files.tossinvest.com/files/notice/8c287d57-0108-4a0a-987e-e91bcc90c8a1.pdf).

## Runtime and audit

Use the provider calendar. For November 2, 2026 the verified Korean-time sessions are
premarket 18:00-23:30, regular 23:30-06:00 and after-hours 06:00-09:50 on November 3.
The runner can collect warmup observations immediately. It stops at the provider's final
session end and preserves open positions. It does not force liquidation at market close.

Use `--trade-from-regular` to collect price history before the regular-session opening without
running either actual or shadow portfolios. Status is `warming_up`; account balances, trades
and evaluation count remain unchanged. The first snapshot received at or after the opening
can use that earlier observed history. The flag sets the earliest trading time, not an immediate
purchase or a regular-session-only policy. It must match when resuming the frozen experiment.
For the November 2 run, the OS timer starts collection at 23:00 KST and trading begins no earlier
than 23:30. Longer lookbacks and missing/stale observations can still require additional warmup.

Artifacts remain under the ignored output directory:

| File | Contents |
|---|---|
| `manifest.json` | Universe, policies, capital, costs, assumptions, source hashes and sessions |
| `live.sqlite3` | Compressed raw responses, HTTP audits, coverage, causal frames and checkpoint |
| `comparison.csv` | Per-policy results; `evaluation_equity_usd` uses estimated after-tax bid liquidation and is unknown without full depth/FX coverage |
| `quotes-report.json.gz` | Full attempts, partial fills, trades, choices, positions and equity curves |
| `status.json` | Process state, progress, session end and zero AI-call count |

Frames and account checkpoints commit in one SQLite transaction. A directory lock prevents
duplicate local runners. Status/report writes are atomic. Authentication failures stop with
`blocked`; other API failures record their code and respect the client's cooldown. Other
failures preserve the committed checkpoint and stop with a traceback in the process log.

An explicit `--resume` requires the same date, capital, prior realized gain and unchanged
source hashes. It restores the original universe and balances without adding capital. Local
checkpoints contain trusted Python pickle data; do not load an untrusted output directory.
Completed experiments cannot restart. Resume is explicit, not an automatic restart loop.

The runner uses only OAuth and allowlisted stock/market-data methods. `/api/v1/orderbook`
is market data. No account, order, amendment, cancellation or order-history path is used.
The monthly candle collector retains its separate shared rate gate. Live reads honor the
market client's provider headers and cooldowns and do not request historical candles.
