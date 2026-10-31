# Downloading a market day

`tools/download_market_day.py` reuses the existing `MarketClient` authentication and allowlist.
It has no account, order, or AI calls. Its process and database are separate from continuous trading.

```zsh
python3 -m tools.download_market_day --date 2026-10-30
python3 -m tools.download_market_day --date 2026-10-30 --status
```

The default directory is `state/datasets/2026-10-30/`. Keep actual data outside Git.
Rerun the same command to skip completed symbols and pages and resume the checkpoint.
A dataset file lock prevents duplicate workers. SIGTERM and Ctrl-C preserve checkpoints.

## Coverage

- Query KOSPI, KOSDAQ, KR_ETC, NYSE, NASDAQ, AMEX, and US_ETC.
- Include ACTIVE, SCHEDULED, DELISTED, and all security types. Freeze the currently supplied list.
- The current list is not a guaranteed reconstruction of the historical listing universe.
- Save the original market calendars and listing records.
- For KR, collect from the first calendar session to the final session end.
- For US, collect at least 24 hours from the first session, preserving bars beyond the calendar end.
  The 2026-10-30 example spans October 30 09:00 to October 31 09:00 KST.
- Select minute bars by end time in `(start, end]`. Request unadjusted prices.
- Preserve timestamps, OHLC, volume, and currency, including original numeric strings.
- Mark bars outside calendar sessions with `outside_calendar=1`.
- Do not generate bars or interpolate prices. Do not infer whether gaps mean no trades or missing data.
- Stop when the provider has no older page. Completion means the available API range was received,
  not that the dataset is a complete exchange transaction record.

## Pacing and stopping

The default minimum request interval is 0.6 seconds. Stay at or below half the response-header
rate limit. A one-request-per-second listing limit produces at least a two-second delay.
Wait for reset when the remaining allowance reaches zero.
For HTTP 429, wait at least 60 seconds or the server-directed delay, whichever is longer, and slow
the subsequent request interval. Retry network/5xx failures after waiting on the same page.
Persist cooldown timestamps and slower pacing across restarts.

Stop in `blocked` on 401, 403, or an unexpected client rejection. Do not repeatedly request
access after rejection. Respecting the rate limit cannot guarantee the provider will never block access.
After resolving the cause, use `--resume-blocked` for manual recovery.
Login and automatic restarts do not resume a blocked dataset without that flag.

## Storage and inspection

| Table | Contents |
|---|---|
| metadata | Date, calendars, scope, state, and cooldown |
| listings / listing_jobs | Original listing records and listing progress |
| symbols | Symbol state, next cursor, bar count, and errors |
| pages | Gzipped API result JSON, SHA-256, query cursor, and retrieval time |
| candles | Deduplicated in-scope minute bars and source-page references |
| requests / events | HTTP and execution audit without authentication values |

Save the original page, normalized bars, and next cursor in one SQLite transaction.
Preserve invalid raw pages and mark the symbol `failed` if prices, timestamps, cursors, or overlap
values conflict. Never overwrite an already saved price.
Preserve out-of-day bars in raw pages but exclude them from target-day price validation.
Use their timestamps only for pagination order, cursor progress, and stopping.

Distinguish `unavailable` for responses such as 404 from `no_data` for valid empty target days.
Keep `failed` and `unavailable` visible in `complete_with_errors`; do not hide them as successful
symbols. A partially downloaded failed symbol is not a complete market day.

Update `status.json` about once per minute. `--status` reads the current database in read-only mode.
Run SQLite `quick_check` at completion. Review gaps and failed symbols before interpreting coverage.

The macOS background job uses a separate date-specific LaunchAgent. Error exits restart after
at least five minutes; access rejection, normal completion, and handled user interruption do not.
At login, the same command resumes unfinished work. It does not resume continuous trading services.
