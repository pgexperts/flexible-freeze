---
id: 0006
title: Run every session with an empty search_path
date: 2026-10-13
status: Accepted
summary: dbconnect() empties search_path on every connection instead of schema-qualifying each catalog reference.
---

# 0006. Run every session with an empty search_path

## Context

The script is normally run as a superuser and visits every database. Its
catalog queries use unqualified function, operator and relation names, so
name resolution depended on the `search_path` in effect, which a database
owner or a role with CREATE on a schema in the path can influence. An object
that matches a call more exactly than the `pg_catalog` one, or that sits in a
schema placed ahead of `pg_catalog`, would be resolved instead and run as the
connecting role.

## Decision

We will set an empty `search_path` on every connection, in `dbconnect()`,
with `SELECT pg_catalog.set_config('search_path', '', false)`, before any
other statement. The queries themselves stay unqualified. `dbconnect()` is
the only place a connection is made.

## Alternatives considered

- **Schema-qualify every function, relation and operator in the queries** —
  a large diff with `OPERATOR(pg_catalog.x)` syntax throughout, easy to get
  wrong on the next edit, and it leaves the `regclass` text the script prints
  and reuses dependent on the session's path.
- **Do both** — the qualification becomes redundant once the path is empty,
  and costs the same readability.

## Consequences

`oid::regclass` text is now always schema-qualified, so table names in the
script's output changed from `foo` to `public.foo`, and name matching had to
stop depending on that text alone (record 0007). On PostgreSQL 16 and
earlier, ANALYZE of a table with an expression index whose function depends
on `search_path` can fail under the empty path; this is the same trade-off
`vacuumdb` makes, and PostgreSQL 17 and later restrict the path during
maintenance operations anyway. Any future code that opens a connection some
other way silently loses this protection.
