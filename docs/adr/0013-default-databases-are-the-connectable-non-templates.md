---
id: 0013
title: The default database list is every connectable non-template database
date: 2026-10-13
status: Accepted
summary: Without -d the script works on each database with datallowconn and not datistemplate, postgres included, oldest datfrozenxid first.
---

# 0013. The default database list is every connectable non-template database

## Context

Without `-d` the script read `pg_database` and left out the templates and
`postgres` by name. Excluding `postgres` is wrong for a tool whose purpose is
to stay ahead of wraparound: its `datfrozenxid` holds back the cluster's
oldest transaction ID like any other database's, and it can hold tables.
Excluding by name also meant that `template0`, which refuses connections,
depended on the name list rather than on the flag that says so.

## Decision

We will build the default list with
`WHERE datallowconn AND NOT datistemplate ORDER BY age(datfrozenxid) DESC`.
The list is still read through a connection to `postgres`, and failing to
connect there is fatal (exit 1).

## Alternatives considered

- **Keep excluding `postgres` by name and document it** — leaves the one
  database every cluster has unmaintained, silently.
- **Filter on `datallowconn` only** — would include `template1` and any other
  template that accepts connections, which hold no data of the cluster's own.

## Consequences

`postgres` is now vacuumed by default, so a run touches a database it did not
before. A user-made template database is skipped, even though it accepts
connections. The order is still by `age(datfrozenxid)` only, so the tables of
one database are finished before the next is started whatever their relative
ages. A database in the list that then cannot be connected to is skipped and
reported (records 0004 and 0009).
