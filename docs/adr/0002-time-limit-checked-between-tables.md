---
id: 0002
title: Check the time limit between tables; hard enforcement is opt-in
date: 2014-10-17
status: Accepted
summary: The run-time limit is tested before each table; --enforce-time adds a statement_timeout of the remaining window plus 30 seconds.
---

# 0002. Check the time limit between tables; hard enforcement is opt-in

## Context

The tool exists to run VACUUM inside a known low-traffic window. A vacuum of
a large table can take longer than what is left of the window. Cancelling a
vacuum discards the work it has done so far.

## Decision

We will test the time limit (`--minutes`) only before starting each table, so
a running vacuum is allowed to finish past the end of the window. An opt-in
flag, `--enforce-time`, sets `statement_timeout` to the time remaining plus a
30-second grace period, so the server cancels a vacuum that outlives the
window (commit fae8f7b).

## Alternatives considered

No alternatives are recorded in the repository history.

## Consequences

By default a single large table can overrun the window by an arbitrary
amount. With `--enforce-time`, a table whose vacuum is longer than the window
is cancelled on every run and never completes. The limit is not tested during
the pause between tables or during table listing.
