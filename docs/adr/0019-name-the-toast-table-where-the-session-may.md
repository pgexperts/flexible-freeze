---
id: 0019
title: Vacuum a table's TOAST table by name where the session may, so that a locked one is reported
date: 2026-10-13
status: Accepted
summary: For a session on PostgreSQL 14 or later that may use schema pg_toast, a table with a TOAST table is vacuumed as two statements (the parent with PROCESS_TOAST false, then the TOAST table by name), so a locked TOAST table draws the SKIP_LOCKED warning; every other run keeps the single statement and the gap.
---

# 0019. Vacuum a table's TOAST table by name where the session may, so that a locked one is reported

## Context

Records 0010 and 0014 both list the same gap among their consequences.
`SKIP_LOCKED` reports a skip only for a relation the statement names. A table's
VACUUM also vacuums its TOAST table, and when only the TOAST table is locked
(by an autovacuum of it, say) the server passes over it with no message at all.
The script then saw no notice, counted the parent as vacuumed, and the run
could exit 0 without advancing the table's freeze horizon. Issue #64 confirmed
this on PostgreSQL 18.6 and notes that the TOAST table is often what holds the
oldest XID.

This record does not replace record 0010. Its rule, that any message from the
server during a table's VACUUM means the table is not counted as vacuumed, is
unchanged, and it is the rule that now reports the TOAST table. Record 0010's
rejection of confirming from the catalog also stands, and is made again below
for this case.

## Decision

Once per database session, right after the session settings and before the
advisory lock, the script sends `TOAST_SPLIT_QUERY`, which returns one
boolean: `current_setting('server_version_num')::integer >= 140000 AND
has_schema_privilege('pg_toast', 'USAGE')`. Both conditions are in the one
query so that every server is sent the same statements up to the VACUUMs. A
failure of the query skips the database like any other failure at that point.

When it is true, a table that has a TOAST table is vacuumed with two
statements: the parent's, with `PROCESS_TOAST false` added before
`SKIP_LOCKED`, and then one naming the TOAST table by its regclass text
(`pg_toast.pg_toast_NNNN`) with the same options otherwise. A locked TOAST
table then draws the ordinary WARNING, which is reported against the parent
("VACUUMing the TOAST table of T (X) drew a message from the server; not
counting T as vacuumed.") and records the parent as not vacuumed (exit 1). The
two statements are one unit of work: the time limit is checked before the first
only, `statement_timeout` is set once for both, the table is counted once, and
a message from the parent's statement settles the table, so the TOAST table is
then not tried.

When it is false (a role without `USAGE` on `pg_toast`, or PostgreSQL 12 or
13), every table gets the single statement it always did, and the gap remains.

## Alternatives considered

- **Name the TOAST table for every role** — naming a relation in `pg_toast`
  needs `USAGE` on that schema, which a table's owner, a database's owner and
  members of `pg_maintain` do not have; verified on PostgreSQL 18.6, where the
  statement fails with `permission denied for schema pg_toast`. In practice
  only a superuser has it.
- **Compare `relfrozenxid` before and after the VACUUM** — gives false
  positives when the xmin horizon is pinned (the VACUUM ran but could not
  advance the horizon), and in `--no-freeze` mode there is nothing to compare.
- **Both: the split where the role allows it, the comparison elsewhere** —
  rejected for its cost.

## Consequences

Superuser runs on PostgreSQL 14 or later report a locked TOAST table. Such a
table has had its own pages vacuumed and its TOAST table's not; in freeze mode
it is selected again on the next run, since its age includes its TOAST
table's.

With `--enforce-time`, the `statement_timeout` of the remaining window plus
30 seconds is set once and applies to each of the two statements, so a split
table can overrun the window by more than a single statement could.

The gap is unchanged for every other run: a role that is not a superuser
(table owner, database owner and `pg_maintain` included) and any server older
than 14. There a locked TOAST table is still passed over in silence and its
parent counted as vacuumed.

A superuser session sends one more query per database and one more statement
per table that has a TOAST table. `-v` prints each statement as it is sent.
The test harness folds the pair into one entry per table (record 0008).
