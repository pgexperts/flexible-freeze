---
id: 0015
title: Claim each database with an advisory lock while it is processed
date: 2026-10-13
status: Accepted
summary: A run takes pg_try_advisory_lock on a fixed key at session level in each database, on by default; a database already claimed is skipped and reported, and --no-advisory-lock turns it off.
---

# 0015. Claim each database with an advisory lock while it is processed

## Context

Two runs in the same database (a nightly run still going when the next starts,
a second cron entry) select the same tables in the same order. With
`SKIP_LOCKED` (record 0014) the later one no longer queues behind the earlier,
but it reports the table the other is on as locked and vacuums again every
table the other has just finished.

## Decision

We will have each run call `pg_try_advisory_lock(7380385457142659685)` in each
database, after the session settings and before the table list is read. The
key is the ASCII bytes of `flexfrze` as a big-endian 64-bit integer
(`0x666C657866727A65`) and must never change. The lock is session level and
never released explicitly: the server drops it when the connection closes,
which the loop does on every way out of a database. A database in which the key
is taken is skipped and reported as a skipped database (records 0004 and
0009). The guard is on by default; `--no-advisory-lock` turns it off for a run.
The lock is per database because an advisory lock belongs to the database it is
taken in, and with `-d` there is no database that every run connects to.

## Alternatives considered

- **An option that is off by default** (issue #22 as filed) — the protection is
  wanted by the people who do not know to ask for it, and a second run is
  usually a mistake.
- **One cluster-wide lock taken in a fixed database** — `postgres` may not be
  connectable or in the run's `-d` list, and the lock would serialise runs over
  disjoint databases that cannot collide.
- **A lock file** — local to one host, so no help when two hosts run it, and a
  stale file after a crash needs clearing.

## Consequences

Any role that can connect to a database can take the same key and make every
run skip that database. That is visible (a skipped database, exit 1) but it is
a way to block maintenance. A run killed during a VACUUM leaves its backend
holding the lock until the VACUUM ends, so the next run skips the database
until then. The key sits in the namespace shared with applications' own
advisory locks. A skipped database is exit 1 even though nothing is wrong with
the database.

## Addendum (2026-10-13)

A refused lock now names its holders (issue #61). When
`pg_try_advisory_lock()` returns false, the run sends
`ADVISORY_LOCK_HOLDER_QUERY` (`pg_locks` left-joined to `pg_stat_activity`,
for the key's two 32-bit halves with `objsubid` 1) and the skip line names each
holder by pid, role and application name; those three columns are visible to
any role, so no extra privilege is needed. The wording goes by
`application_name`: a holder named `flexible_freeze` is reported as another run,
as before; any other is reported as a session that does not appear to be
flexible_freeze, with a warning that every run will skip the database until it
lets go; a holder with no pid is a prepared transaction and is never taken for
a run; a holder whose application name cannot be found is not called either.
`application_name` is set by the client, so a session can pass itself off as a
run or the reverse; that is accepted and documented in the README, and the
distinction is a hint for the operator that decides nothing. If the holder has
let go by the time of the lookup, the line says the lock was held a moment ago;
if the lookup fails, the line says the lock is held and why its holder could
not be determined. In every case the database is skipped, the exit status is 1,
and the lock is not tried again. A run that gets the lock sends nothing new.
The key and the rest of this decision are unchanged, as is the consequence that
any role that can connect can make every run skip a database.
