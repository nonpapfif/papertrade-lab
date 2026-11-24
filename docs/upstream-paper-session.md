# US paper session with upstream strategies

This dated experiment compares two independent, whole-share paper accounts. The first takes
eligible Day Trader opening-range breakout plans. The second uses the same rules and also
requires a published Tauric Buy or Overweight at the decision cut. This comparison and the
local fill simulation are Papertrade Lab integration choices. The original Day Trader project
supports human selection and approval. Here the runner evaluates hypothetical plans in source
order and never contacts a broker account or order endpoint.

## Source and inputs

- [Day Trader](https://github.com/Jakeschincariol/day-trader-skill/tree/3d0f4ef6986f4984b7c6bd46f7ff4f87744c3abd)
  supplies the daily scanner, intraday scanner, entry, exit plan, sizing, risk checks and alerts.
  Its public universe comes from Nasdaq Trader. Its charts come from Yahoo. Create the universe
  and daily watchlist before the session with the source scanner.
- [TradingAgents](https://github.com/TauricResearch/TradingAgents/tree/9fa1196fffbcfd9de5146d2dd2869df84ab99838)
  supplies its native market analyst, investment debate, trader, risk debate and portfolio
  decision. The market analyst reads captured data through the upstream tool interfaces.
  The experiment selects the market analyst and records the other source analysts as skipped.
  Codex provides the model backend. Research is asynchronous and tied to dated daily candidates.
- Toss OAuth and market data supply the US calendar and executable quotes. Source chart
  timestamps describe the breakout. A paper fill requires a subsequent eligible Toss quote.
  Local fees and displayed size apply. Model a triggered exit at available prices, including
  a possible gap beyond a planned stop.

## Run

Install the repository's research and WebSocket extras in a Python 3.12 environment on macOS.
Use a signed-in Codex CLI and the existing private Toss credential files. Complete all date
substitutions in the commands below. Run from the pinned Papertrade Lab checkout.

```sh
python "$DAY_TRADER/trader-scanner/scan.py" universe --out "$PREP/universe.txt"
python "$DAY_TRADER/trader-scanner/scan.py" daily --universe "$PREP/universe.txt" --out "$PREP/watchlist.json"
python -m tools.upstream_paper_session run --day YYYY-MM-DD --source "$DAY_TRADER" \
  --output "$SESSION" --watchlist "$PREP/watchlist.json" --calendar "$PREP/calendar.json" \
  --capital 1000 --research-workers 2 --history-dir "$PREP/history"
python -m tools.upstream_paper_session verify --session "$SESSION"
```

Use the Toss market-data client to save the target day's exchange calendar. Keep the session
outputs, private reports and installed upstream dependencies outside version control. Optional
daily OHLCV history must have a recorded receipt time and contain only completed prior trading
days. Missing data and delayed publications can reduce the number of eligible plans.

## Interpretation

The source defaults use a 15-minute opening range and a completed 5-minute breakout bar.
The example exit uses the opening-range midpoint, a target of twice the entry-to-stop distance
and a planned flat time of 15:55 New York time on a regular day. The source budget and risk
modules decide affordability and eligibility, including a maximum of three trades per day.
Starting cash is an experiment input. Both accounts receive the same virtual funding.

A signal can be filtered, arrive too late, exceed available cash, or fail to get an executable
quote. Compare orders and fills separately. A session with zero research-qualified fills has
no trade sample for assessing research selection.

Requests skipped when the final entry window closes are recorded separately in the journal
and comparison counts. Those counts show which parts of the final scan lost coverage.
Earlier scan deadlines and upstream data failures still contribute to an incomplete comparison.

The session saves its source identities and frozen inputs in `plan.json`, `watchlist.json`
and `source-config.json`. Read `status.json` for progress and `research/reports/` for the
role reports. The final account comparison is `comparison.json`. The verify command
reconstructs the comparison from the sealed journal and archived research. After a process
interruption during the same session, use the original run arguments with `--resume` to
check the frozen contract and replay committed events before accepting new observations.
