---
id: 0017
title: Reject a statement comment that could end itself, and send every statement through one helper
date: 2026-10-14
status: Accepted
summary: --comment TEXT is appended as /* TEXT */ to every statement the script sends; a value with /*, */, a NUL or a line break is refused at argument parsing (exit 2), and one execute() helper is the only caller of a cursor's execute.
---

# 0017. Reject a statement comment that could end itself, and send every statement through one helper

## Context

Monitoring tools that read comments in statements (to attribute a statement,
or to ignore it) need the script's statements to carry a marker, and the
operator needs to choose the text. The text goes between `/*` and `*/`, so a
value that can end the comment early lets the rest reach the server as SQL,
executed as the connecting role. The contributed version removed `/*` and
`*/` from the value. Removal can create the delimiter it removes: with each
`/*` and then each `*/` taken out, `**//` becomes `*/`. A comment must also
reach every statement, and the script sent them from several places.

## Decision

We will add `-c/--comment TEXT`, which appends ` /* TEXT */` to every
statement the script sends. The argparse type `comment_text` refuses a value
containing `/*`, `*/`, a NUL, `\n` or `\r`; the refusal is a command-line
error (exit 2) and nothing is sent. The value is never altered. `/*` is
refused as well as `*/` because PostgreSQL nests block comments: an opening
delimiter would leave the comment open and make every statement a syntax
error. A line break would spread each statement over several lines of the
server's log, and a NUL cannot be sent.

All statements go through one helper, `execute(cur, statement, params=None)`,
the only caller of a cursor's `execute`. It appends the comment with a space
before the closing `*/` (a text ending in `/` would otherwise run into the
delimiter as `/*/`, which opens a nested comment), and doubles `%` in the text
only when the statement has query parameters, which is when psycopg2
formats it. A statement is passed in without a closing `;`, so the comment is
part of it; the semicolons at the end of the table-listing queries were removed
for that reason.

## Alternatives considered

- **Sanitise by removal** (the contributed version) — unsafe, as above:
  removal can assemble a delimiter from what is left.
- **Escape the delimiters** — PostgreSQL's comments have no escape syntax.
- **Restrict the text to an allow-list of characters** — safe, but refuses
  text monitoring tools use (`:`, `=`, `,`, quotes) for no gain once the
  delimiters, NUL and line breaks are excluded.
- **A `--` line comment** — ends at the first line break, and the text would
  need the same checks.
- **Set `application_name` instead** — the script already sets
  `application_name='flexible_freeze'` on connect; it is per session, not per
  statement, and tools that read statement comments do not see it.

## Consequences

A value the operator expected to be cleaned up is an error instead. Every
new statement must use `execute()`; `tests/test_comment.py` counts the direct
calls. One statement is outside the helper and carries no comment: the
`SET datestyle` that psycopg2 itself sends when a session starts on a server
whose `DateStyle` is not ISO. A comment the client encoding cannot encode
fails when the first statement is sent, inside `dbconnect()`. With `-v` the
line printed for each table shows the VACUUM without the comment. The test
harness's statement parser accepts one trailing comment on the table's line
for this reason (record 0008).
