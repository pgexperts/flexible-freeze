---
id: 0016
title: Send failure reports to standard error, and to the log file as well
date: 2026-10-13
status: Accepted
summary: A line that by itself makes the exit status non-zero goes to stderr and, with -l, also to the log; progress stays on stdout or in the log, and the log is closed by an exit handler.
---

# 0016. Send failure reports to standard error, and to the log file as well

## Context

A run under cron is judged by what it writes to standard error, which cron
mails. With `-l` everything, failures included, went to the log and the mail
was empty; without `-l` failures were on standard output among the progress
lines, where a job that discards standard output loses them. The log was also
closed at some of the places the script ends and not others.

## Decision

We will call a line a failure report when what it says by itself makes the exit
status non-zero (a failed table, a table not counted as vacuumed, a skipped
database, a `--table` found nowhere, a run that could not start, a signal).
Those go through `failure_print` to standard error and, with `-l`, to the log
file as well; progress stays on standard output, or in the log with `-l`. A
stop at the time limit is progress. Without `-l` a failure is not repeated on
standard output. The log is closed by an `atexit` handler registered where it
is opened. Output remains `print`-based.

## Alternatives considered

- **The `logging` module** — a larger change to a script with no structure to
  hang handlers on, and the tests pin standard output exactly.
- **Repeat failures on standard output as well** — seen twice when both
  streams reach the same terminal or the same mail.

## Consequences

Failure and progress lines are on different streams, so their relative order is
visible only in the log. If standard error cannot be written (its reader has
gone away), descriptor 2 is pointed at the null device so the buffered line
and later ones have somewhere to go; otherwise the interpreter's flush at exit
would replace the exit status with 120. A failed write to standard output or
the log is not handled. The rule has to be applied to each new message by
hand, and a message put on the wrong side is a silent regression.
