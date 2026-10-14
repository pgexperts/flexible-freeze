---
id: 0008
title: Test the script as a subprocess against a throwaway cluster
date: 2026-10-13
status: Accepted
summary: Tests run the unmodified script against an initdb cluster created per session and assert on the catalog and statement log.
---

# 0008. Test the script as a subprocess against a throwaway cluster

## Context

The project had no tests. The script runs at import, rebinds `sys.stdout` and
installs a signal handler, so it cannot be imported by a test, and the
behaviour that matters (name resolution, quoting, what VACUUM did) is decided
by the server, not by Python. With no host or port the script connects to the
local default server and vacuums every database, so a careless test run is
destructive in its own way.

## Decision

We will test the script unmodified, as a subprocess, against a PostgreSQL
cluster that the suite creates with `initdb` in a temporary directory,
listening on a unix socket only, and removes afterwards. A single launcher
(`run_ff` / `start_ff`) always supplies the host and port, strips every `PG*`
environment variable, and refuses arguments that could redirect the
connection. Tests prove outcomes from the server: `relfrozenxid`, statistics,
and the statement log. CI runs the suite on PostgreSQL 14 through 18 and is
triggered manually.

## Alternatives considered

- **Restructure the script into an importable module first** — the right
  long-term shape (issue #3), but it would have meant refactoring untested
  code before any test existed.
- **A stub `psycopg2` module on `PYTHONPATH`** — runs without a server, but
  cannot show what the server does with a search path or an identifier, which
  is what the tests most need to show.
- **A PostgreSQL container** — needs Docker on every developer machine and in
  CI; server binaries are enough.

## Consequences

The suite needs PostgreSQL server binaries and takes several seconds. Line
coverage of the script is not measured, because it runs in a child process.
Paths that need password authentication, real time limits or signals are not
covered. If pytest is killed with SIGKILL the throwaway postmaster is left
running and must be stopped by hand.

## Addendum (2026-10-13)

Several consequences no longer hold. The suite now measures line coverage of
the script: coverage.py starts itself in each script subprocess
(`.coveragerc`, `COVERAGE_PROCESS_START`), and CI runs the suite a second time
with it and prints the figure, report-only. Password authentication is
covered: one role (`ff_scram_role`) is asked for a SCRAM password by
`pg_hba.conf`. Signals are covered (`tests/test_signals.py`). A cluster left
running by a pytest killed with SIGKILL is swept at the start of the next
session, but only a directory that belongs to the current user, is not
writable by others, carries the harness marker, and whose marker is over an
hour old, with a live server that reports that data directory.

## Addendum (2026-10-14)

The harness's VACUUM statement parser (`ffharness.parse_vacuum`) reads two
more forms. A parenthesised option may be an option name followed by one
unquoted word as its value (`TRUNCATE false`, `PARALLEL 2`), reported as
`NAME value` with the value in lower case. And one `/* ... */` comment is
accepted after the table, as the script's `--comment` (record 0017) adds one;
it has to be on the table's line, and it is not reported, though the
statement's `.sql` keeps it. A quoted value, a value of several words, a
comment anywhere else, a second comment, or a `/*` inside the comment is
still an error. The meaning of a value is not worked out, so `TRUNCATE`,
`TRUNCATE true` and `TRUNCATE on` stay three different entries.
