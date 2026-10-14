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
