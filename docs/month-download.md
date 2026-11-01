# Reverse chronological market collection

`tools.download_market_month` collects one historical date at a time, newest first. Within that
date, four network workers paginate different symbols. One shared limiter spaces every actual
HTTP request, including authentication requests, by rate-limit group. Database writes remain on
the controller thread. The existing daily collector and its default pacing remain unchanged.

```zsh
python3 -m tools.download_market_month \
  --month 2026-10 --through 2026-10-29 \
  --seed state/datasets/2026-10-30/market.sqlite3 \
  --output state/downloads/2026-10-reverse --rate 5 --workers 4

python3 -m tools.download_market_month \
  --month 2026-10 --through 2026-10-29 \
  --seed state/datasets/2026-10-30/market.sqlite3 \
  --output state/downloads/2026-10-reverse --status
```

The sequence is October 29, October 28, October 27, continuing through October 1. The completed
October 30 dataset is reused as the universe snapshot and retained separately. No network workers
start the next date until the current date finishes. Completed daily datasets are inspected in
read-only mode and skipped on restart.

## Holidays and coverage

Query the KR and US calendars independently for every requested date. If one market has no
sessions, collect only the other market. If both are closed, save the calendar evidence, issue no
candle queries, and advance to the next date. Weekends are checked through the provider calendar,
not presumed to have identical coverage in both markets.

Preserve each day's ordinary `state/datasets/YYYY-MM-DD/market.sqlite3` layout and schema. Collect
all provider-supplied security types and listing statuses in the frozen October 30 universe.
Reuse that listing snapshot rather than repeat 21 listing requests for every date. Record the
snapshot path, original capture time, and its historical-universe limitation in every daily file.
It is not a reconstruction of listings that existed earlier in October.

KR covers its first through last supplied sessions. US covers at least 24 hours from its first
supplied session. Use unadjusted minute bars and preserve original numeric strings, raw result
pages, source timestamps, outside-calendar flags, hashes, and cursor checkpoints. Gaps are not
filled. Historical zero-volume prices remain source data and are not proof of executable trades.

## Pacing and recovery

The default chart dispatch target is five requests per second across all workers combined.
The aggregate limiter respects lower header limits at no more than half the reported rate.
Separate conservative intervals apply to calendars, listings, and authentication. Additional
workers overlap response latency; they do not each receive an independent request allowance.
A single month lock prevents duplicate controllers for the same run, and daily dataset locks
prevent overlapping writers. Other collectors using this account must share the allowance or
remain stopped; this in-process limiter does not coordinate unrelated programs.

- HTTP 429 globally pauses the affected group for at least 60 seconds or the server's longer
  cooldown and doubles its interval. Persist the slowdown and cooldown across restarts.
- Transport/5xx failures retain the same symbol cursor and wait at least 15 seconds.
- The first GET 401 for a revoked, expired, or invalid token permits the client's existing single
  authentication retry. A second 401, any other 401, or a 403 stops new dispatches.
- Unexpected client rejections stop for inspection. Symbol-level 404/unavailable responses remain
  visible as unavailable coverage.
- Invalid raw pages remain in the audit as failures. They are not marked as valid bars.
- `--resume-blocked` is required after inspecting and resolving a blocked job.
- SIGTERM/Ctrl-C cancels waiting workers, drains request audits, and preserves committed cursors.

Each request reads the shared token cache under the existing file lock after waiting for its
dispatch slot. Workers therefore pick up another worker's refreshed token instead of continuing
to trust their own in-memory copy. Token issuance remains serialized by that lock. In-flight
requests can still encounter revocation during a refresh; their one authentication retry adopts
the latest cache without deleting a replacement written by another worker.

At most one page per symbol is in flight. Page persistence rechecks its cursor, then reuses the
existing atomic raw-page, normalized-bar, and continuation-cursor transaction. An interruption
may require refetching an uncommitted response, but does not advance past an unsaved page.
A failed symbol does not silently become complete; the day and month retain `complete_with_errors`.

## Monitoring

The month directory contains `status.json` with the current date, daily progress, completed dates,
rate-gate state, and execution error. Daily databases retain the original `requests` and `events`
audit tables and original raw pages. Monthly progress updates approximately every ten seconds;
daily status files approximately every minute. Daily completion runs SQLite `quick_check`.

The background job uses `caffeinate -i` and a separate one-shot macOS LaunchAgent. It does not start
or resume continuous trading, make AI requests, access accounts, or call real order endpoints.
The job pauses or stops visibly on an error rather than repeatedly relaunching rejected access.

Event-source collection is separate from market-price collection. Event timestamps and
point-in-time availability must be verified before archived material becomes simulation input.
