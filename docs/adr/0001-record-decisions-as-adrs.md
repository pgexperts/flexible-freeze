---
id: 0001
title: Record architecture decisions as per-decision files
date: 2026-10-13
status: Accepted
summary: Decisions are recorded one per file in docs/adr, with a generated README index.
---

# 0001. Record architecture decisions as per-decision files

## Context

The project had no decision log. The reasons behind its behaviour were only
recoverable from commit messages and the README, and a code review on
2026-10-13 had to reconstruct them. Records 0002-0005 capture the decisions
that could be recovered with a stated reason; they are dated by the commit
that introduced them.

## Decision

We will keep one Markdown record per decision in `docs/adr/`, named
`NNNN-slug.md`, with a generated `README.md` index. Accepted records are not
rewritten; a changed decision gets a new record that supersedes the old one.

## Alternatives considered

- **A single accreting decisions document** — update cost grows with the
  history and every edit risks altering earlier entries.
- **Commit messages only** — the status quo; reasons are scattered and
  alternatives are never recorded.

## Consequences

Changes that alter a recorded behaviour need a superseding record. Records
0002-0005 are reconstructions: where the history gives no alternatives, they
say so rather than inventing them.
