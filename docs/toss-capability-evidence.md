# Toss broker and Open API capability evidence

Verified on **2026-11-04**. This is a documentation audit, not an execution certification.
Only public documents were fetched. No authenticated account, order, holdings, commission,
or conditional-order endpoint was called. No trading capability was enabled.

## Sources and reproducibility

The server-owned [OpenAPI specification][rest] is version **1.2.28** and the independent
[AsyncAPI specification][ws] is version **1.2.3**. The official [LLM index][index] points to
[overview][overview] and [FAQ][faq] outside the `latest` directory. The attempted
`/openapi-docs/latest/overview.md` returned 404. This is not an API availability failure.

Raw public sources are retained locally in the Git-ignored directory
`state/validation/toss-capability-audit-20261104/`. SHA-256 values are recorded below.

| File | SHA-256 |
| --- | --- |
| `openapi.json` | `18fdc96fdf632e4869c765ed0cb169049c5310a1c48e10b40e47a71f52aa7a45` |
| `asyncapi.json` | `46669a360d4c5c6485c157624825e599f5dc55f8b9f66b9b3bcf3bfc3b9e7512` |
| `overview.md` | `668b4036865b84e6b97d25856e9aee182eaba471068299771452177ddc385d54` |
| `faq.md` | `6ad4c02b8e785f7de1a067398736e69b546069f8d1e74d89e294a1a774c1a1da` |

**Supported** below means explicitly documented. **Unsupported** means explicitly rejected or
absent from the documented request contract. **Unverified** means the available evidence does
not establish the behavior. These labels do not imply the current project implements it.

## Order and funding contract

The following findings come from `OrderCreateRequest`, `POST /api/v1/orders`, and the
[official order FAQ][faq].

| Capability | Evidence and boundary |
| --- | --- |
| KR whole shares | Supported. Positive integer quantity. KR prices in integer KRW and valid tick increments. |
| US whole shares | Supported. Quantity-based LIMIT or MARKET in USD. |
| US fractional purchase | Supported through USD `orderAmount` with MARKET. Fractional BUY `quantity` is rejected. |
| US fractional sale | Supported through MARKET SELL `quantity` with at most six decimal places. |
| US amount sale | Request schema allows BUY and SELL for `orderAmount`. Response `Order.orderAmount` prose describes BUY only. That response inconsistency remains unresolved. Prefer quantity-based full exits in the design. |
| KR fractions and fractional LIMIT orders | Unsupported by the ordinary-order contract. |
| Quantity versus amount | Exactly one of `quantity` and `orderAmount`. Never both. Both are decimal strings matching `^\d+(\.\d+)?$`, maximum length 30. No numeric `minimum` or `multipleOf` is declared. Semantic positivity and fractional restrictions are in prose. |
| Fractional acceptance window | Regular-session opening through one hour before regular-session close. Outside it, amount/fractional orders have distinct 422 errors. Compute against that day's session, including early closes. |
| Amount minimum and rounding | API schema does not specify a minimum USD amount, amount scale, allocation rounding, or complete dust-exit rules. Do not infer these from the string regex. |
| Price precision | US LIMIT supports four decimal places below USD 1 and two at USD 1 or above. Excess places are documented as truncated. KR wrong tick returns an error with tick information. |
| Order side | BUY/SELL. No explicit sell-short, borrow, credit-order, derivatives, or margin selector is documented. A SELL enum is not evidence of short-sale support. |
| Time in force | DAY default. CLS only US LIMIT (LOC). OPG only KR LIMIT/MARKET (opening auction). One error example lists only DAY/CLS, while the current schema includes OPG. Preserve this inconsistency until clarified. IOC/FOK/GTC are not ordinary-order enum values. |
| Venue | KR integrated SOR only. No KRX/NXT selector. A non-integrated investor setting can cause rejection. |
| Currency funding | US API orders do **not** automatically exchange KRW to USD. Insufficient USD buying power is rejected. A quote exchange-rate endpoint does not provide executable conversion. |
| Opposing open orders | KR ordinary orders reject an opposite-side pending order, even without crossing prices. The FAQ says this restriction does not apply to US ordinary orders or conditional orders. |
| Size restrictions | KR order amount above KRW 3 billion is rejected. Orders of at least KRW 100 million require the explicit high-value confirmation flag. This flag must not be silently treated as user consent. |

The [broker fractional-service disclosure][fractional] separately describes US amount orders
in KRW or USD, six-place holdings, and minimum KRW 1,000 or USD 1, with an exception for selling
an entire smaller remaining balance. It lists regular open through one hour before close and
excludes some small/restricted securities. This is broker-service evidence dated 2024, not a
complete API contract. In particular, its statement that fractional quantities cannot be
specified differs from the newer API's explicit fractional SELL support. Do not copy the
app's KRW amount-order route into the USD API or mark all US symbols fractional-eligible.

## Instruments, sessions, and settlement

`GET /api/v1/stocks/all` returns broker-tradable symbols by market, without pagination, sorted
by symbol. It supports status, security type, and common-share filters. The default is ACTIVE.
The documentation recommends a daily refresh because this list is updated daily. Enumeration
is not proof that a particular order is currently eligible. `StockInfo` includes security type,
listing status, currency, leverage factor, and KR liquidation/NXT support/venue halt flags.
There is **no explicit fractional-eligibility field** in this schema. Broker/account restrictions
and prerequisite errors still apply. Leveraged/inverse ETF prerequisites must be completed in
the app. The API cannot register them. See [REST][rest] and [FAQ][faq].

Market-calendar endpoints return previous/current/next business-day information in KST.
US date input uses the US local date, with nullable day/pre/regular/after sessions. KR uses
integrated KRX/NXT sessions and excludes pre/post-session closing-price trading. Calendar
sessions are unions of venue times, not per-symbol or per-venue execution guarantees. Use
current dates, holidays, and nullable sessions, rather than fixed UTC offsets or an app's
maximum advertised access window. See [REST][rest] and [FAQ][faq].

The [broker business-hours page][hours] was reachable but its downloaded HTML did not expose
readable schedule details. Therefore this audit does not certify exact current extended-session
clock times from that page. Fractional acceptance must use its narrower API window regardless.

`OrderExecution` documents cumulative filled quantity, average price, filled amount, commission,
tax, final fill time, and settlement date (nullable KST date). These are schema capabilities,
not observed account values. The [KR disclosure][kr-disclosure] describes KR T+2 and sale of
purchased quantities before settlement. Its summary also labels overseas settlement T+2.
That line alone is insufficient to establish current US exchange settlement or account cash
availability. Current US broker settlement, withdrawal, and reinvestment treatment remain
unverified in this audit. Do not equate a sale, settled cash, buying power, and withdrawable cash.

## Idempotency and reconciliation

`clientOrderId` is optional, at most 36 characters, allowing alphanumerics, hyphen, and underscore.
Without it, every submission is a separate order. With it, the same content returns the original
result for **10 minutes**. After that window the same identifier can create a new order.
Changed content produces a conflict. An in-progress request can return 409. This is bounded
idempotency, not permanent exactly-once execution. See [REST][rest] and [FAQ][faq].

Order list/detail schemas represent aggregate order state, not an individual-fill REST ledger.
Closed orders paginate by cursor (maximum 100). Open orders return the full set. App/WTS orders
can appear, while unsupported order types may be absent. CANCELED, REJECTED, and REPLACED can
retain partial fills. Cancel/replace rejection records must not erase the original order's state.
`Order` does not expose a permanent `clientOrderId` lookup field. The design must retain local
request/response mappings and reconcile ambiguous outcomes. A timeout is not a confirmed failure.
See [REST][rest] and [FAQ][faq].

Conditional orders document SINGLE/OCO/OTO, with OCO/OTO requiring LIMIT,
shared quantity, and expiry up to one year. They are not evidence of general trailing-stop,
IOC, GTC, or fractional conditional-order compatibility. Their modification creates a new ID.
This is a future capability to validate, not a mandatory baseline. See [REST][rest] and [FAQ][faq].

## Data semantics that change strategy design

| Observation | Consequence |
| --- | --- |
| Current prices accept up to 200 symbols per request. Timestamp can be null. | Batch the universe and retain per-symbol observation age. This is a request limit, not a 200-symbol universe cap. |
| Candles provide 1m/1d only, count 1-200 (default 100), newest first, inclusive `before`, and returned `nextBefore`. | Follow the supplied cursor. Deduplicate and reorder explicitly. |
| One-minute timestamp is interval end. Daily timestamp represents local midnight rendered in KST. | Do not read minute timestamps as starts or US daily KST dates as midnight sessions. |
| US one-minute history starts 2021-12-01. KR starts 2022-11-23. Delisted price history is unavailable (404). | Do not claim unlimited or survivorship-free historical validation. |
| US quote/trade/candle/stream data uses some venues, not NBBO. Venue mix can change by session. | No claim of consolidated executable best price or complete market volume. |
| US minute highs/lows can contain special/off-market prices unavailable to ordinary investors. Corrections/cancellations are not incorporated, late prints can revise old candles. | A touched candle price alone cannot prove a strategy fill. Record observation versions and use separate fill evidence. |
| US daily OHLC is regular-session official data. Daily volume can include other sessions. Minute and daily volume have different sources. | Do not reconstruct daily volume or indicators by assuming minute sums are equivalent. |
| KR no-trade minutes are zero-volume carried-price bars. US no-trade minutes are absent. | Distinguish missing minutes from observed flat prices and halted/stale observations. |
| Non-NXT KR 15:30 bar can include closing auction and post-close trades through 16:00. | A timestamp already passed does not guarantee that this bar is final. |
| KR quotes and candles combine KRX/NXT without venue identification. Crossed integrated quotes can be valid. | Do not fabricate venue routing, crossed-book arbitrage, or invalid-data labels solely from crossing. |
| Orderbook timestamp is last update, with no delay/outage flag or sequence. | Old timestamp alone cannot distinguish quiet trading from lost data. Pong only proves connectivity. |
| Adjusted prices are default. Adjustment also changes volume. Older history may remain adjusted even with `adjusted=false`. | Record adjustment settings. Do not certify point-in-time raw fills or total-return series from these bars alone. |

These findings use `PriceResponse`, `Candle`, `CandlePageResponse` in [REST][rest] and the detailed
[market-data FAQ][faq]. US adjustment rules also do not capture every small cash distribution.
Corporate-action accounting needs separate verified inputs.

## REST capacity and streaming

Published limits are per **client x API group**, and can change without notice. They are not
measured capacity or AI-call budgets. The [overview][overview] publishes the following limits.

| Group | Requests/second |
| --- | --- |
| AUTH / ACCOUNT / ASSET | 5 / 1 / 5 |
| STOCK / STOCK_ALL / STOCK_TRADING_TREND | 5 / 1 / 10 |
| MARKET_INFO / MARKET_DATA / MARKET_DATA_CHART | 3 / 15 / 20 |
| RANKING | 5 |
| MARKET_INDICATOR_PRICE / MARKET_INDICATOR / MARKET_INDICATOR_CHART | 10 / 10 / 5 |
| SECTOR / SECTOR_RANKING | 5 / 5 |
| ORDER / ORDER_HISTORY | 10 / 5 |
| ORDER_INFO | 6. 3 during 09:00-09:10 KST |
| CONDITIONAL_ORDER / CONDITIONAL_ORDER_HISTORY | 5 / 10 |

Normal and 429 responses include limit, remaining tokens, and reset estimate headers.
`X-RateLimit-Reset` means seconds until one token replenishes, **not a Unix reset timestamp**.
429 includes `Retry-After`. The guide recommends honoring that delay, exponential backoff,
and jitter. Some suspicious IP ranges are limited to one request/second. The FAQ separately
warns that repeated small orders may trigger temporary abuse restrictions. Published ORDER
TPS is not permission for unlimited economically meaningless order generation. See
[overview][overview] and [FAQ][faq].

The [AsyncAPI contract][ws] supports two simultaneous connections per account, 100 subscriptions
per connection counted as channel-symbol pairs, and five subscription declarations per second.
A declaration is a full replacement. Omitted subscriptions are removed. ACKs can partially reject
symbols. Initialize state through REST because subscriptions have no initial snapshot. Send PING within 180
seconds (60 seconds recommended), even while receiving data. Reconnect with backoff/jitter,
close the old connection, and redeclare after disconnect or token renewal.

Market streams favor latest state and may lose messages. No sequence is provided. Do not sum
trade frames into authoritative volume. Personal-order streams retain backlog only inside the
connection. Two seconds of sustained backpressure can close it, and disconnected events are
not replayed. Reconciliation through REST is prescribed by the documentation. These account
channels remain outside the project's currently authorized access boundary.

Use batched REST for broad observation and streams for selected detailed
monitoring. Subscription limits must never silently become an all-universe observation cap.

## Costs and evidence by authority

The [KR broker disclosure][kr-disclosure] lists base commission 0.015% KRX and 0.014% NXT.
It lists stock sales tax totaling 0.20%, and distinguishes ETF/ETN tax treatment, including
15.4% withholding on the smaller relevant gain/tax-base increase for applicable products.
Do not apply stock sales tax universally to ETFs. These are disclosure values, not verified
account promotions. The [API FAQ][faq] states that SOR-only orders expose **one KR commission
rate**, without a selectable venue. `/commissions` is an account-specific source in a future
separately authorized integration. It was not called here.

The [official Toss service page][service] advertises US commission 0.1% and no trading
commission when an order's total filled value is USD 10 or less. Its applicability to this
account and Open API, fee rounding, and non-commission charges were not established. Do not
silently grant app campaigns or small-order exemptions to the API cost model.

The [SEC's 2026 advisory][sec] establishes USD 20.60 per million (0.00206%) from 2026-04-04.
The 2025 zero-rate period is not a current baseline. The [FINRA filing exhibit][taf] temporarily
sets TAF to zero from 2026-10-01 through 2026-12-31, then restores USD 0.000195/share, capped
at USD 9.79/trade, from 2027-01-01. These are regulatory assessments, **not proof of Toss
customer pass-through, its minimum, rounding, or aggregation**. Those broker-specific details
remain unverified. Effective dates belong in the cost assumptions. Do not apply a generic
US broker fee table or another broker's minimum to Toss.

## Remaining evidence gates

Before calling a broker-compatible paper result execution-realistic, resolve the capabilities
that its strategy actually uses. These include fractional eligibility, minimum amounts, rounding,
dust exits, account-specific costs, dated taxes, settlement, buying power and session/order modes. Evidence may come from
updated public documentation or separately authorized account evidence. An unverified capability
must remain explicit and cannot be replaced with a zero-cost, immediate-fill assumption.

No additional runtime work or real trading is authorized by this research document.

[rest]: https://openapi.tossinvest.com/openapi-docs/latest/openapi.json
[ws]: https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json
[index]: https://developers.tossinvest.com/llms.txt
[overview]: https://openapi.tossinvest.com/openapi-docs/overview.md
[faq]: https://openapi.tossinvest.com/openapi-docs/faq.md
[fractional]: https://home-files.tossinvest.com/files/notice/ed945009-a63d-4706-a57b-ef0bb7b5f140.pdf
[kr-disclosure]: https://home-files.tossinvest.com/files/notice/f8b692a8-2b29-46c4-b279-f6bf865e64d5.pdf
[hours]: https://corp.tossinvest.com/en/business?tab=businessHours
[service]: https://toss.im/service/securities
[sec]: https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2
[taf]: https://www.sec.gov/files/rules/sro/finra/2026/34-106409-ex5.pdf
