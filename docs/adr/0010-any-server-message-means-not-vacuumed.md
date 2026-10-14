---
id: 0010
title: Any server message during a table's VACUUM means the table is not counted as vacuumed
date: 2026-10-13
status: Accepted
summary: The session sets client_min_messages = warning, and a VACUUM that drew any message from the server is reported verbatim and exits 1 instead of being counted as done.
---

# 0010. Any server message during a table's VACUUM means the table is not counted as vacuumed

## Context

From PostgreSQL 11, a VACUUM that cannot touch a table (the role has no
permission on it, or with `SKIP_LOCKED` the lock is not available) says so
with a WARNING and returns successfully. The script took a successful return
to mean the table was vacuumed, so a run could skip everything and report
"All tables vacuumed".

## Decision

We will set `client_min_messages = warning` in each database session, empty
`conn.notices` before each VACUUM, and treat any notice found there afterwards
as "not vacuumed": the messages are printed verbatim, the table goes into
`outcome.unvacuumed_tables`, and the run exits 1 (record 0009). The message
text is never examined.

## Alternatives considered

- **Match the WARNING text** — the text is localised (`lc_messages`) and
  differs between versions, so the check would be approximate and could never
  be complete.
- **Confirm from the catalog afterwards** (`relfrozenxid` for freeze mode) —
  exact for freezing, but for `--no-freeze` the statistics are updated
  asynchronously, so there is nothing reliable to read back; and it costs a
  query per table.
- **Check privileges beforehand** — covers permission but not locks, and a
  permission can change between the check and the VACUUM.

## Consequences

The one rule covers permission, lock and any skip a later server version
adds. It also over-reports: an unrelated warning raised during the VACUUM
(about an old xmin that holds back removal, for instance) marks the table as
not vacuumed although it was. The opposite gap remains: the server passes
over a locked TOAST table with no message at all, so the parent still counts
as vacuumed. Because the setting is made by the script, a database or role
default of `error` cannot hide the warnings.

## Addendum (2026-10-13)

Record 0019 closes the locked-TOAST gap named in the consequences above for
sessions on PostgreSQL 14 or later that may use schema `pg_toast` (in practice
a superuser): such a session vacuums a table's TOAST table in a statement of
its own, so a locked TOAST table draws a WARNING, and this record's rule
reports it against the parent. The rule itself is unchanged. For every other
run the gap remains as stated. Record 0019 rejects confirming from the catalog
again, for this case.
