---
id: 0007
title: Match user-supplied table names bare or schema-qualified
date: 2026-10-13
status: Accepted
summary: A table name on the command line matches the printed name, the bare table name in any schema, or plain schema.table.
---

# 0007. Match user-supplied table names bare or schema-qualified

## Context

`-T`, `--exclude-table-in-database` and `--table` compared the user's value
with the table's `oid::regclass` text. That text varies with `search_path`
and quoting, so a bare name did not match a table outside the path or a
mixed-case one. With the empty `search_path` of record 0006 the text is
always schema-qualified, and a bare name would have matched nothing at all,
breaking every existing invocation.

## Decision

We will carry each selected table as `(regclass text, schema name, table
name)` and treat a user-supplied name as matching when it equals the regclass
text, or the bare table name (in any schema), or `schema.table` written
plain without SQL quoting. One helper, `table_named_in`, applies the rule for
all three options. `--exclude-table-in-database` is split on the first dot
only, so `DATABASE.SCHEMA.TABLE` is accepted. The regclass text is used
unmodified as the VACUUM target.

## Alternatives considered

- **Require schema-qualified names** — unambiguous, but breaks every existing
  `-T`, `--table` and `--exclude-table-in-database` argument.
- **Leave `search_path` alone so bare names keep matching public tables** —
  rejected with record 0006.
- **Build the VACUUM target from schema and table name with
  `psycopg2.sql.Identifier`** — needs psycopg2 2.8 or later for a
  two-part identifier, and the regclass text is already correctly qualified
  and quoted by the server.

## Consequences

Existing command lines keep working. A bare name now also matches same-named
tables in other schemas, so `-T foo` excludes `sales.foo` as well as
`public.foo`. A dotted value is ambiguous between `schema.table` and a table
whose name contains a dot; both are matched. A database whose name contains a
dot still cannot be named in `--exclude-table-in-database`. `--table` still
chooses only among tables that pass the selection filters.

## Addendum (2026-10-13)

The last consequence no longer holds. `--table` no longer chooses among the
selected tables: it is looked up in `pg_class` with the same three name forms
(in SQL, `TABLE_LOOKUP_QUERY`, which has to stay in step with
`table_named_in`), and the selection thresholds (age, size, dead rows) do not
apply to it. The exclusions still do. A table is found if it is an ordinary
table or a materialized view outside the temporary schemas, so `--table` can
reach a system catalog. A database with no such table is mentioned only with
`-v`; the run exits 1 only if the table is found in no database of the run.

## Addendum (2026-10-14)

An empty or blank `--table` value (an unset variable in a cron entry, say) is
refused at argument parsing with exit 2, by the argparse type
`non_empty_table`, and the option is tested against `None` afterwards. A
blank value can therefore no longer mean "every table".
