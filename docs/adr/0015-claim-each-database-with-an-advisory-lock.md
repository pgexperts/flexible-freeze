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
