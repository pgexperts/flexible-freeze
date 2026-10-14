---
id: 0012
title: Select tables to freeze by the greatest of four ages, with no limit on the list
date: 2026-10-13
status: Accepted
supersedes: 0003
summary: Freeze mode takes ordinary tables and materialized views, orders and thresholds them on the greatest XID or MultiXact age of the table and its TOAST table, has no LIMIT, and measures size with pg_table_size.
---

# 0012. Select tables to freeze by the greatest of four ages, with no limit on the list

## Context

Record 0003 ordered tables by the greater of the heap and TOAST XID age. Its
own consequences list the gaps: MultiXact age and materialized views were not
considered, and the list was cut at `LIMIT 1000` with nothing said about the
tables past the cut, so a run that still had time stopped silently. The size
test used `pg_relation_size`, which ignores the TOAST table that the age
already counted, so `--minsizemb` could drop a small table whose large TOAST
table was the old part. The server forces a vacuum on whichever of the XID
and MultiXact horizons comes due first, so going by XID age alone leaves a
table that is far gone on the other counter unselected until it does.

## Decision

In freeze mode we will select ordinary tables and materialized views (outside
`pg_catalog`, `information_schema` and the temporary schemas) whose age,
`greatest(age(relfrozenxid), mxid_age(relminmxid))` of the table and of its
TOAST table, exceeds `--freezeage`, and order them by that age descending,
then size. The one `--freezeage` applies to both counters; a table whose age is
exactly the value is not selected. There is no `LIMIT`: the time window bounds
a run. `--minsizemb` compares against `pg_table_size`, so TOAST counts, in
both modes. The order of databases by `age(datfrozenxid)` is unchanged, as are
the `--no-freeze` thresholds; that mode's query changed only in the size
function and in leaving out other sessions' temporary schemas.

## Alternatives considered

- **A separate MultiXact threshold option** — two knobs for what the server
  treats as the same urgency, and an operator who sets one and not the other
  gets the old blind spot.
- **Keep the limit as an option** — a cap is only ever the wrong number; the
  window is already the bound, and an option invites setting a cap that
  truncates silently again.
- **Keep `pg_relation_size`** — leaves `--minsizemb` disagreeing with the age
  about what the table is.

## Consequences

`--minsizemb` changed meaning: it now includes the TOAST table, free space map
and visibility map, so a threshold chosen before selects more tables. A very
old cluster produces a long list and a first query that sizes every candidate
(bounded by `lock_timeout`, record 0014). The `pg_catalog` tables are still
never selected, and partitioned parents are not selected or analyzed, only
their partitions. Foreign and temporary tables are left out.

## Addendum (2026-10-13)

**System catalogs, on request** (issue #66, item a). The default is unchanged:
the system catalogs are left to autovacuum, and without the new option the
freeze statement is byte for byte what it was. `--system-catalogs` replaces the
condition that leaves out `pg_catalog` and `information_schema` with `true`
and changes nothing else, so a catalog is held to the same age and size
thresholds, order, exclusions and time window as any table. It applies to
freeze mode only: with `--no-freeze` (or `--vacuum`) it is refused at argument
parsing (exit 2, before the log file is opened or any connection made),
because that mode selects from `pg_stat_user_tables`, which lists no catalog.
With `--table` it is accepted and has no effect. No privilege is checked; a
role that may not vacuum a catalog draws the server's WARNING, reported under
record 0010. Shared catalogs (`pg_database`, `pg_authid`) are not
de-duplicated across databases: each database's `pg_class` holds its own
`relfrozenxid` for them, and a VACUUM moves only the copy in the database it
ran in (verified), so a shared catalog is selected, and vacuumed, in every
database in which it is old enough. The consequence above that "the
`pg_catalog` tables are still never selected" now holds only without the
option.

**Database order.** "The order of databases by `age(datfrozenxid)` is
unchanged" no longer holds: without `-d`, databases are ordered by
`greatest(age(datfrozenxid), mxid_age(datminmxid))` (record 0020, which
supersedes record 0013).

**A freeze age of exactly 0** (issue #66, item d) — declined. The comparison
stays `>`, as documented in `--help` and the README: a table whose age is
exactly `--freezeage` is not selected, so even `--freezeage 0` does not select
a table whose age is 0. Switching to `>=` would change what the test suite's
freeze-everything setting selects.
