---
id: 0014
title: Skip locked tables with SKIP_LOCKED and bound every other lock wait
date: 2026-10-13
status: Accepted
summary: Every VACUUM is sent in the parenthesized form with SKIP_LOCKED, and --lock-timeout (default 10 s, 0 disables) sets the session's lock_timeout; PostgreSQL 12 is the minimum.
---

# 0014. Skip locked tables with SKIP_LOCKED and bound every other lock wait

## Context

Record 0005 noted that with no `lock_timeout` a vacuum waiting on a lock waits
indefinitely. The time limit is tested between tables only (record 0002), so a
table held by an anti-wraparound autovacuum or a `CREATE INDEX` kept the run
queued for as long as the holder took. The table-listing queries are exposed
the same way: `pg_table_size` opens every candidate, so the whole list queues
behind a pending `ACCESS EXCLUSIVE` lock on any one of them.

## Decision

We will send every VACUUM as `VACUUM (FREEZE, ANALYZE, SKIP_LOCKED) table`,
with `FREEZE` and `ANALYZE` according to `--no-freeze` and `--no-analyze`. A
table locked by something else is passed over at once; the server reports it
with a warning and it is reported as not vacuumed through record 0010. We will
also add `--lock-timeout` (seconds, default 10, 0 means no limit), set as the
session's `lock_timeout` in every database whatever the value, so a database or
role default cannot override "no limit". It bounds the listing queries, and the
locks a VACUUM may still wait for other than its table's; a database whose
listing times out is skipped, and a table whose VACUUM times out is failed.
`SKIP_LOCKED` exists only in the parenthesized form, from PostgreSQL 12, which
is therefore the minimum version.

## Alternatives considered

- **`lock_timeout` alone around each VACUUM** (the approach of open PR #33,
  "Add an optional per-table lock timeout": a `--lock-timeout` in milliseconds
  after which the run gives up on the table and moves on) — every locked table
  costs the full timeout, is reported as a failure, and does nothing for the
  listing queries.
- **Wait indefinitely, as before** — the run sits behind any lock holder, with
  no test of the time limit.
- **Expose `PARALLEL`, `INDEX_CLEANUP` and `BUFFER_USAGE_LIMIT` at the same
  time** — they use the same parenthesized form, but are separate decisions
  with their own version minimums; deferred.

## Consequences

A table skipped because it is locked makes the run exit 1 (record 0009) and is
selected again next time. The server skips a locked TOAST table without any
message, so the parent still counts as vacuumed (record 0010). A default of
ten seconds means a listing that is merely slow behind a lock is given up on.
The oldest supported server is PostgreSQL 12, but CI covers 14 through 18, so
12 and 13 are untested. `--lock-timeout` is in seconds; a patch written for
PR #33's millisecond option will not apply as it stands.

## Addendum (2026-10-14)

`--no-truncate` adds `TRUNCATE false` to the option list, in both modes, so
the statement becomes `VACUUM (FREEZE, ANALYZE, TRUNCATE false, SKIP_LOCKED)`
(with `FREEZE` and `ANALYZE` as before). Without the flag nothing about
truncation is sent, so the table's `vacuum_truncate` storage parameter still
decides; `TRUNCATE true` is never sent, since it would override a table set
never to be truncated. Like `SKIP_LOCKED`, the option exists from
PostgreSQL 12 and only in the parenthesized form.

## Addendum (2026-10-13)

The session that lists the databases now sets `lock_timeout` from
`--lock-timeout` as well, 0 included (record 0020), so it is no longer the one
session without the bound. The consequence above that a locked TOAST table is
skipped without a message, its parent counted as vacuumed, no longer holds for
sessions on PostgreSQL 14 or later that may use schema `pg_toast`; see record
0019. Such a session adds `PROCESS_TOAST false` to the parent's option list and
sends a second statement for the TOAST table, with `SKIP_LOCKED` on both.
