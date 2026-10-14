---
id: 0020
title: The default database list is every connectable non-template database, ordered by the greater of XID and MultiXact age
date: 2026-10-13
status: Accepted
supersedes: 0013
summary: Without -d the script works on each database with datallowconn and not datistemplate, postgres included, ordered by greatest(age(datfrozenxid), mxid_age(datminmxid)) descending; the listing session gets --lock-timeout, and a failure to connect, query or fetch the list is reported and exits 1.
---

# 0020. The default database list is every connectable non-template database, ordered by the greater of XID and MultiXact age

## Context

Record 0013 chose which databases a run without `-d` works on, and in its
decision ordered them by `age(datfrozenxid)` alone. Its own consequences note
that the order went by that one age. Record 0012 orders and thresholds tables
by the greatest of their XID and MultiXact ages, because either horizon can
force an anti-wraparound vacuum, but left the database order as it was. A
database nearest wraparound through its MultiXact IDs could therefore go
unreached in a run that has a time limit (issue #66, item b).

Two smaller things about reading the list were also open (issue #65): a
failure of the listing query left the script as an uncaught exception, with a
traceback on standard error and nothing in the log file; and the listing
session was the one session without the `--lock-timeout` bound. The script is
run unattended, typically from cron, and has to leave its failures in its own
log.

The choice of databases in record 0013 is unchanged; this record restates it
with the new order and the hardened listing so that one record describes the
list.

## Decision

Without `-d`, the script connects to `postgres`, sets `lock_timeout` from
`--lock-timeout` in that session (0 included, as in the per-database sessions
of record 0014), and reads

    SELECT datname FROM pg_database
    WHERE datallowconn AND NOT datistemplate
    ORDER BY greatest(age(datfrozenxid), mxid_age(datminmxid)) DESC

so the order is by the same measure the tables are selected by. `postgres` is
included; templates and databases that refuse connections are left out by
their flags, not by name.

Reading the list fails as a whole. A failure to connect, of the `SET`, of the
query, or of fetching its rows is reported through `failure_print` (record
0016: standard error, and the log file with `-l`) and the run exits 1. An
interrupt is tested first, so a signal during the listing still ends the run
as a signal (record 0011). An empty list is reported and exits 1.

A list given with `-d` is worked through in the order written, and no listing
is read.

## Alternatives considered

The alternatives recorded in record 0013 (excluding `postgres` by name;
filtering on `datallowconn` only) still apply to the choice of databases. For
the order, no alternative to the greatest of the two ages is recorded beyond
the previous `age(datfrozenxid)`, which is what this record replaces.

## Consequences

A database far gone on MultiXact age is reached early in a timed run. As
before, the tables of one database are finished before the next is started,
whatever their relative ages. `-d` order is the operator's.

A failed listing leaves a report in the log file instead of a traceback on
standard error only. The listing query can now fail on a lock wait longer than
`--lock-timeout`, which ends the run before any database is started. `postgres`
must still be connectable for a run without `-d`. A database in the list that
then cannot be connected to is skipped and reported (records 0004 and 0009).

## Addendum (2026-10-13)

Databases of equal age are now ordered by name: the query's secondary sort key
is `datname` (issue #94). Equal ages are common, since `CREATE DATABASE` copies
the template's `datfrozenxid` and `datminmxid`, and with no secondary key such
databases came back in the server's order, so that two runs against one
cluster could visit them differently and a time-limited run could stop in a
different place each time. The primary order is unchanged.
