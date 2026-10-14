---
id: 0018
title: The minimum Python is 3.11
date: 2026-10-14
status: Accepted
summary: The script refuses to run below Python 3.11; before this the check accepted 3.6, and CI runs 3.13.
---

# 0018. The minimum Python is 3.11

## Context

The script's version check accepted Python 3.6, long past end of life, a
floor no test or CI job exercised. A contributed change (PR #58, which also
made the `argparse.__version__` debug line tolerate its absence) proposed
3.10. The owner chose 3.11.

## Decision

We will require Python 3.11 or later. The script's check
(`sys.hexversion < 0x30B0000`) prints `At least Python 3.11 required; you
have ...` to stderr and exits 1. The same minimum is stated in `README.md`
and `ARCHITECTURE.md`, and `tests/test_interpreter.py` holds it as `MINIMUM`
and runs the script with `sys.hexversion` set to just below and to exactly the
minimum. CI (`.github/workflows/ci.yml`) runs lint, tests and coverage on
Python 3.13 only.

## Alternatives considered

- **Keep 3.6** — a version nobody runs or tests; the claim cannot be backed.
- **3.10, as proposed** — the owner chose one release newer; no reason beyond
  that preference is recorded.
- **Track only what CI runs (3.13)** — a floor nobody else meets for no
  reason the script needs.

## Consequences

A host whose system Python is older needs a newer interpreter installed
beside it. The floor (3.11) is lower than what CI runs (3.13), so 3.11 and
3.12 are supported but not tested in CI. The usage block in `README.md` is a
copy of `--help` and has to be regenerated on 3.11 or 3.12, because 3.13
prints each option's value name once and its output differs.
