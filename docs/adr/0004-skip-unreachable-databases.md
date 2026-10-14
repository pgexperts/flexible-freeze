---
id: 0004
title: Skip a database that cannot be connected to
date: 2018-05-29
status: Accepted
summary: A connection failure for one database is reported and the run continues with the next database.
---

# 0004. Skip a database that cannot be connected to

## Context

The default run walks every database in the cluster. Some cannot be connected
to (for example `datallowconn = false`, or `pg_hba.conf` rules). Before this
decision a single connection failure ended the whole run.

## Decision

When connecting to a database fails, we will print
"skipping database ... (couldn't connect: ...)" and continue with the next
database (commit 3cc8703). Failing to connect to `postgres` to build the
database list remains fatal.

## Alternatives considered

- **Abort on the first connection failure** — the previous behaviour;
  replaced because one unreachable database stopped maintenance of all the
  others.

## Consequences

One bad database no longer blocks the rest. The skip is not reflected in the
exit status or the final summary, so a scheduler cannot tell that a database
was missed. Table-level failures still abort the run; the two cases are
handled inconsistently.

## Addendum (2026-10-13)

The last two consequences no longer hold (record 0009). A skipped database is
now listed in the closing summary ("Databases skipped or left unfinished") and
makes the run exit 1. Other reasons now put a database in the same list: its
table list could not be read (the message reads "couldn't list its tables" and
also covers a failure of the session settings), another run holds its advisory
lock (record 0015), or the connection was lost part-way through it. A failed
table no longer aborts the run; it is recorded and the run goes on.
