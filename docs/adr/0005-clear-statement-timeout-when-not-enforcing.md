---
id: 0005
title: Clear statement_timeout when the time limit is not enforced
date: 2021-02-18
status: Accepted
summary: Without --enforce-time, each vacuum is preceded by SET statement_timeout = 0.
---

# 0005. Clear statement_timeout when the time limit is not enforced

## Context

A `statement_timeout` set at the system, database or role level applies to
the tool's session and cancels long vacuums, which defeats decision 0002's
default of letting a running vacuum finish.

## Decision

When `--enforce-time` is not given, we will issue `SET statement_timeout = 0`
before each VACUUM (commit dbf70c6). When it is given, the timeout from
record 0002 is set instead.

## Alternatives considered

No alternatives are recorded in the repository history.

## Consequences

An administrator's configured timeout is deliberately overridden for this
session. With no timeout and no `lock_timeout`, a vacuum waiting on a lock
waits indefinitely.
