# Research selection comparison

This offline US paper experiment compares a fixed strategy under the existing captured
universe and a TossTradingLab research gate. It reuses the layered replay and its paper
account implementation. It creates separate paper accounts from equal USD cash and feeds
them the same captured quote and candle data. Both use whole shares, one-second confirmation,
book depth and the same fee schedule with the US commission exemption disabled. A third
account can compare an alternative strategy under the same research gate.

The research gate only changes eligibility for new entries and the corresponding count of
candidates for equal sizing. Holdings, existing intents and exits remain subject to the
same account rules. Research inputs are frozen for the run. Only the latest snapshot can
apply, during its half-open publication window. A missing or expired snapshot admits no
new research-selected symbol. Buy and Overweight qualify; other recognized ratings do not.

## Run

Use a captured layered stream, its freeze metadata and ordered FX observations. Create
a local JSON input containing dated snapshots of completed TossTradingLab report bundle
directories (absolute paths or paths relative to the input file). Each snapshot may include
several symbols. A bundle must contain the manifest and evidence, identify a completed
Portfolio Manager result, and match a symbol in the captured US universe.

```json
{
  "snapshots": [
    {
      "published_at": "2026-11-03T16:41:48+00:00",
      "expires_at": "2026-11-03T16:44:48+00:00",
      "bundles": ["/absolute/report-bundle-id-for-first-symbol", "/absolute/report-bundle-id-for-second-symbol"]
    }
  ]
}
```

```sh
python3 -m tools.compare_research_selection \
  --capture /absolute/capture \
  --freeze /absolute/freeze.json \
  --fx-history /absolute/fx-history.json \
  --selection /absolute/selection.json \
  --out /absolute/new-output-directory \
  --strategy trend --compare-strategy rebound
```

The publication times in this comparison are operator-supplied scenario times. They
must be at or after the bundle evidence cutoff. The original bundle format does not
establish when a model result became available in a past market session. Results are
counterfactual. They do not measure actual model latency, live opportunity or profitability.

The private output includes separate account summaries, observed end time, marking
limitations, assumed costs, comparison configuration and input hashes. A single
capture only covers its recorded symbol subset. The selection gate cannot add quote
coverage. This entry point is offline and finite; it does not connect to Toss,
start a model, resume a campaign or move any existing account state.
