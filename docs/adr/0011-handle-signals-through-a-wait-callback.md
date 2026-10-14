---
id: 0011
title: Handle SIGINT and SIGTERM through a psycopg2 wait callback
date: 2026-10-13
status: Accepted
summary: The handler only records the signal; a wait callback waits in short select() slices, cancels the running statement and waits for the server's error, and the run exits 128 plus the signal.
---

# 0011. Handle SIGINT and SIGTERM through a psycopg2 wait callback

## Context

psycopg2 blocks inside libpq while a statement runs, and the interpreter does
not run during a blocking call, so a Python signal handler could not run in
the middle of a VACUUM: Ctrl-C did nothing until the statement ended, and
SIGTERM, left at its default, ended the process while the backend carried on
vacuuming with nobody waiting. Closing the connection does not help either,
because a backend does not notice a vanished client while it vacuums.

## Decision

We will install a psycopg2 wait callback (`wait_for_server`) before the first
connection, and have the signal handler do nothing but record the first signal
(and whether a second arrived). The callback waits with `select()` in short
slices, since PEP 475 makes Python resume an interrupted `select()` for the
rest of its timeout. When a signal is recorded it sends `conn.cancel()`,
re-sends it every 2 seconds (a backend that has not read the statement yet
ignores one), and goes on waiting for the server's error. The loops test the
interrupt before any failure handling, so the cancellation is not recorded as
a failed table; the pause is sliced the same way. The run exits 128 plus the
signal number, and the first signal decides. A second signal, or a cancel that
cannot be delivered, gives up the wait; the connection is then closed and the
summary says the VACUUM may still be running. libpq does not apply
`connect_timeout` under a wait callback, so the callback applies it itself, to
each connection attempt as a whole.

## Alternatives considered

- **Run the query in a worker thread** — the main thread stays responsive, but
  it adds a thread and hand-off to a single-threaded script and still has to
  cancel through a second connection.
- **Cancel from inside the handler** — the handler can run between any two
  bytecodes, in the middle of a print or of the code that is using the
  connection; it is safe only if it does nothing.
- **Asynchronous connections throughout** — a rewrite of every call site for
  what one callback gives.
- **Leave SIGTERM unhandled** — what cron, systemd and `kill` send; it leaves
  the VACUUM running in the server.

## Consequences

The wait callback is process-wide and changes how every query and connection
attempt waits. A multi-host `-H` list no longer fails over from a host that
answers nothing: the single timeout is spent on the first. `conn.cancel()` and
name resolution still block, outside the callback. SIGHUP is not handled. After
a second signal the exit status is 128 plus the signal while a VACUUM may
still run, and finding it is left to `pg_stat_activity`. SIGKILL still leaves
the VACUUM running.
