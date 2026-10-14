---
id: 0021
title: Refuse at parse time the option values that can never work, and leave the rest to libpq and the server
date: 2026-10-13
status: Accepted
summary: -p takes one port or a comma-separated list, each 1-65535, refuses an empty value and passes the text to libpq unchanged; --costlimit must be at least 1; --exclude-table-in-database db..t is refused; upper bounds and host/port counts are left to the server and libpq, and the checks are approximate by design.
---

# 0021. Refuse at parse time the option values that can never work, and leave the rest to libpq and the server

## Context

Three option values were checked wrongly at argument parsing:

- `-p` had been given `type=int` (issue #62, a regression), so argparse refused
  the comma-separated port list libpq takes for a `-H` that lists several hosts
  (`-H db1,db2 -p 5432,6432`). The value is only ever handed to
  `psycopg2.connect(port=...)`, where text is correct.
- `--costlimit 0` was accepted, but the server's minimum for
  `vacuum_cost_limit` is 1, so the `SET` failed in every database and every
  database was reported as skipped (issue #68, item a).
- `--exclude-table-in-database db..t` was accepted. Its table part `.t` is a
  qualified name with an empty schema and can never match, so the table it was
  meant to protect would be vacuumed (issue #66, item e).

Other parse-time checks already existed: negative values for the numeric
options (`non_negative_int`), an empty database or table part of
`--exclude-table-in-database`, a `-d` that names no database, an empty
`--table` (record 0007) and an unsafe `--comment` (record 0017).

## Decision

- **`-p`** has the argparse type `port_list`: one port, or a comma-separated
  list, each entry an optional sign and ASCII decimal digits with a value from
  1 to 65535. An empty value or an empty entry (`5432,`) is refused with exit
  2 and is never read as the default port, so that an unset variable in
  `-p "$PORT"` cannot send a run to the default server. The text is returned
  exactly as given and passed to libpq unchanged (so `--debug` shows `dbport`
  as a string). Whether the number of ports fits the number of hosts is left to
  libpq, whose answer shows up as a failed connection.
- **`--costlimit`** has the argparse type `positive_int`: at least 1. No upper
  bound is applied to it or to any other numeric option; the server enforces
  its own on the cost settings.
- **`--exclude-table-in-database`** refuses a table part that starts with a
  dot (`db..t`), with the same exit-2 message as the other empty parts.

These checks are approximate on purpose: they refuse values that cannot work,
not every value that will not.

## Alternatives considered

- **Keep `-p` as an integer** — breaks multi-host connections, which worked
  before the type was added.
- **Read an empty `-p` as "the default port"** — an unset variable would then
  send a run to whatever server the defaults reach.
- **Convert the ports, or match them to the hosts, in the script** — libpq
  already does both when it connects; the script hands it the text.

No other alternatives are recorded for the `--costlimit` and
`--exclude-table-in-database` checks.

## Consequences

Residuals, accepted and documented here:

- An out-of-range upper value for `--costdelay` or `--costlimit` is not an
  argument error; it reaches the `SET` in each database and fails there, so
  each database is skipped and the run exits 1.
- `--exclude-table-in-database db.schema.` and `db.schema..t` are accepted.
  Like any mistyped exclusion they exclude nothing unless a table is actually
  named that way; with `-v` the end of the run lists exclusion values that
  matched no selected table.
- `-T ""` is accepted and excludes nothing.
- A `-p` list whose length does not fit the `-H` list is found only when
  libpq connects, as a connection failure, not as exit 2.

## Addendum (2026-10-13)

Three of the residuals above are now refused at parse time (exit 2): `-T` /
`--exclude-table` with an empty or blank value (it takes the same type as
`--table`), and `--exclude-table-in-database` with an empty part anywhere in
the table portion (`db.schema.`, `db.schema..t`). The `-T ""` case silently
unprotects a table, as with an unset variable in a cron entry. The dotted cases
follow the `db..t` precedent: under the bare-relname matching of ADR 0007 they
could match a table literally named `schema.` or `schema..t`, and such an odd
legal name is now unnameable in this option, as `.t` already was. The other
residuals (upper bounds, the `-p`/`-H` count) stand.

## Addendum (2026-10-14)

`-T`, `--exclude-table` and `--table` (and its older spelling `-st`) refuse a
value with leading or trailing whitespace, with exit 2, as they refuse an
empty one. The value is not stripped: matching is exact, so the stripped name
would be a different table from the one written. A table whose name really
begins or ends with whitespace is reached through its quoted form, for example
`sales." orders"`. Both parts of `--exclude-table-in-database` are refused
for surrounding whitespace in the same way (the database part since #112),
although `-d` strips its names. The database part has no quoted form, so a
database whose name really begins or ends with whitespace cannot be named
in this option; that is a residual, and `-T` still excludes its tables.

The empty-part check of `--exclude-table-in-database` is quote-aware: a dot
inside a double-quoted identifier does not split the table part, so
`db.public."a..b"` names the table `a..b` and is accepted, while `db.."a..b"`
is still refused for its empty schema part.
