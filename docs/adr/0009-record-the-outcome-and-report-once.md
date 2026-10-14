---
id: 0009
title: Record the outcome of a run and report it once, at the end
date: 2026-10-13
status: Accepted
summary: The loops only record what was and was not done in a RunOutcome; the summary and the exit status are computed from it once, after the loops.
---

# 0009. Record the outcome of a run and report it once, at the end

## Context

The table loop kept a `time_exit` flag and two counters, and called
`sys.exit(1)` when a VACUUM raised. One failed table therefore abandoned every
later table and every later database, and the exit sites, the summary and the
counters could disagree. Record 0004 had already made a database that cannot
be connected to a silent skip, outside the exit status. A scheduler could not
tell a complete run from one that did a fraction of its work.

## Decision

We will have the loops record what happened in a small `RunOutcome` object
(failed tables, tables the server declined to vacuum, skipped or unfinished
databases, the `--table` lookups, the time-limited flag and the table cancelled
at the limit, the interrupt), and compute the summary and the exit status from
it once, after the loops. Nothing inside a loop calls `sys.exit`.

Exit status: 0 when the run completed or stopped at the time limit; 1 when any
table failed, any table was not counted as vacuumed (record 0010), any database
was skipped or left unfinished, or `--table` was found in no database (and also
when the run could not start); 2 for an argument error; 130 or 143 after
SIGINT or SIGTERM (record 0011), which outrank 0 and 1. A failed table is
reported and the run goes on. A lost connection ends that database, which is
recorded as unfinished; there is no reconnect. A stop at the time limit is 0
because it is how a nightly run is designed to end; a non-zero status would
send mail every night. A run that stops at the limit but also had a failed
table still exits 1.

## Alternatives considered

- **Patch each symptom** (`continue` instead of `sys.exit`, a guard on
  `conn.close()`) — fixes the abandoned tables but leaves the counters and the
  exit sites scattered, so the next new way to fall short is forgotten in one
  of them.
- **Restructure into `main()` and classes** — the right shape eventually
  (issue #3), but it would rewrite a script that was not under test for the
  sake of an exit status.
- **A distinct exit code (3, say) for the time limit** — lets a scheduler
  tell the cases apart, but most schedulers treat any non-zero status as a
  failure, which is the nightly mail this avoids.
- **Reconnect after a lost connection** — more code, more states, and a server
  that dropped a connection is often not ready for the next one.

## Consequences

Every way to fall short is one field, one term in `work_was_missed()`, and one
line of the summary; the cost is that each new one must be added in all three.
A database that was only partly worked is exit 1, not 0, so a flaky link
fails the job. A permanent condition (a table the role may not vacuum) makes
every run exit 1 until it is fixed. The time limit cannot be told from
completion by the exit status alone; the summary line says which.
