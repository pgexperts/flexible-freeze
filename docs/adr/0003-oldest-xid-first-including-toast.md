---
id: 0003
title: Freeze tables oldest-XID first, counting TOAST age
date: 2015-07-06
status: Superseded
superseded_by: 0012
summary: Freeze mode orders tables by the greater of the heap and TOAST relfrozenxid age, descending.
---

# 0003. Freeze tables oldest-XID first, counting TOAST age

## Context

The window is finite, so the order of work decides what gets done. The
purpose of freeze mode is to stay ahead of transaction-ID wraparound. A TOAST
table has its own `relfrozenxid` and can hold rows older than any in its
parent table.

## Decision

In freeze mode we will select ordinary tables whose age exceeds `--freezeage`
and process them in descending order of
`greatest(age(table.relfrozenxid), age(toast.relfrozenxid))`, then by size
(commit ef74340 added the TOAST term). Databases are likewise visited in
descending order of `age(datfrozenxid)`.

`--no-freeze` mode is a separate selection: tables ordered by dead-tuple
fraction.

## Alternatives considered

No alternatives are recorded in the repository history.

## Consequences

The tables closest to wraparound are handled first. A table that fails or is
cancelled sorts first again on the next run. MultiXact age and materialized
views are not considered, and the list is capped at 1000 tables.
