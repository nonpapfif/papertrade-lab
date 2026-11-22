# Docker strategy cycle

The KR and US services run independent dated paper comparisons from one pinned Linux image.
They use the existing simulation rules and approved market-data paths. Use the
[Finance dashboard](http://127.0.0.1:3000/d/finance) for the host cutover, loss review, strategy
reconsideration, frozen next plan, and market-specific execution phases.

## Build and verify

From a dedicated checkout of the approved Finance commit, stage Git bundles for that checkout
and the existing Day Trader checkout at `3d0f4ef`:

```sh
mkdir -p state/docker-build
git bundle create state/docker-build/finance.bundle HEAD
git -C /absolute/day-trader-3d0f4ef bundle create "$PWD/state/docker-build/day-trader.bundle" HEAD
```

Create a local Compose environment file with `FINANCE_COMMIT` set to the bundled Finance
revision, `FINANCE_UID` and `FINANCE_GID` set to the Mac file owner's numeric IDs,
`FINANCE_RUN_ROOT` set to the new writable run directory, `FINANCE_PRIOR_ROOT` set to its
`prior` subdirectory, and `FINANCE_TOSS_DIR` set to a dedicated private token and lock
directory owned by the chosen UID with mode `0700`. Set `FINANCE_TOSS_ID_FILE` and
`FINANCE_TOSS_SECRET_FILE` to the two existing credential file paths. The credentials mount
read-only and the services share a separate writable cache and token lock.
The environment file contains paths and numeric settings. The build context includes
only the two bundles and the Dockerfile.

```sh
docker compose --env-file /absolute/docker-cycle.env config --quiet
docker compose --env-file /absolute/docker-cycle.env build kr
docker run --rm --network none --entrypoint python finance-strategy-cycle:<commit> \
  -m unittest tests.test_strategy_review_cycle tests.test_slot_control \
  tests.test_strategy_review_handoff tests.test_upstream_paper_session
```

Offline tests should pass with the image's pinned source and Python runtime. To include the
Day Trader fixture tests add `--env PAPERTRADE_DAY_TRADER_SOURCE=/day-trader` to `docker run`.

## Cutover and ongoing operation

The Mac launches `python3.12 -m tools.start_docker_strategy_cycle` using the dedicated checkout.
Its `--docker`, `--env-file`, `--prior`, and `--output` arguments name the Docker CLI, Compose
environment file, dated Mac comparison directory, and new Docker run root. On the deployed Mac,
a LaunchAgent runs that command once per minute. It checks both Mac exit receipts and process
identities, copies the required records into `<output>/prior`, and starts each Compose service
at most once. SQLite checkpointing applies to the copies. Compose mounts the copies read-only
and puts new journals under the new writable run root.

Each service reads the preceding market's report and ledger, verifies provenance and prepares
a plan for the exchange calendar's next session. An executable plan feeds the same comparison CLI.
A hold or repair decision stops that market's cycle with a reason. The services preserve separate
plans, journals, and decisions. A new pair is a distinct experiment with equally funded accounts.
A candidate requires new validation evidence before any decision to advance it.

Inspect the existing deployment with
`docker compose --env-file /absolute/docker-cycle.env ps -a` and
`docker compose --env-file /absolute/docker-cycle.env logs kr us`. Stop an individual market
with the same Compose prefix and `stop kr` or `stop us`. The services do not restart on exit.
An interrupted date and its records are retained. Recovery uses the owning runner's evidence
and a new saved plan for a future verified date. Inspect `<output>/handoffs`, market lifecycle
files, and `<output>/<day>/<market>` before considering another launch.

The previous Mac sessions remain separate. Docker may obtain a token for the same client after the cutover gate verifies that the
previous jobs have exited. Check process participation before starting additional runners
under the same credentials.
