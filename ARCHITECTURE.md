# Architecture

flexible-freeze is one Python script, `scripts/flexible_freeze.py`, that runs
`VACUUM [FREEZE] [ANALYZE]` table by table inside a time window. It is a
top-to-bottom script: there is no `main()`, and argument parsing, signal
registration and the whole run execute at import. It cannot be imported.

## Component map

| Component | Responsibility | Entry points |
|---|---|---|
| Argument parsing | Options, defaults, the per-database exclusion map | module level, `parser`, `args`, `database_table_map` |
| Output | Plain, verbose and debug printing; password masking | `_print`, `verbose_print`, `debug_print`, `args_for_display` |
| Connection | The only place a connection is made; sets autocommit and an empty `search_path` | `dbconnect` |
| Name matching | Compares a user-supplied table name with a selected table | `table_named_in` |
| Database list | `-d`, else `pg_database` via the `postgres` database | module level, `dblist` |
| Table selection | Two queries: freeze mode (by XID age) and `--no-freeze` mode (by dead-tuple fraction) | module level, `tabquery` |
| Vacuum loop | Exclusions, pause, time check, `statement_timeout`, VACUUM, server messages | module level, `for db in dblist` |
| Run outcome | What was and was not done; the only input to the summary and the exit status | `RunOutcome`, `outcome`, the summary block at the end of the file |
| Test harness | Throwaway cluster, the single launcher for the script, catalog and statement-log probes, a movable clock for the script | `tests/ffharness.py`, `tests/conftest.py` |

## Invariants

- **Every connection goes through `dbconnect()`.** Breaks: a session without
  the empty `search_path` resolves the unqualified catalog queries through
  whatever schemas the database or role puts on the path.
- **The selection queries are not schema-qualified; they rely on the empty
  `search_path`.** Breaks: removing the `set_config` call re-exposes them.
  Changing one without the other is wrong either way (ADR 0006).
- **Connection values are passed to `psycopg2.connect` as keyword arguments,
  never formatted into a connection string.** Breaks: database names come
  from `pg_database` and would be parsed as connection keywords.
- **A table is a row `(regclass text, nspname, relname)`.** The regclass text
  is printed and is the VACUUM target, verbatim. Breaks: quoting or otherwise
  rewriting it produces a wrong or malformed target. `nspname` and `relname`
  are for matching only.
- **All user-supplied table names are compared with `table_named_in`.**
  Breaks: `-T`, `--exclude-table-in-database` and `--table` stop agreeing on
  what a name means (ADR 0007).
- **Connections are autocommit.** Breaks: VACUUM cannot run in a transaction
  block.
- **`args.dbpass` is never printed; print `args_for_display()` instead.**
  Breaks: the password lands in stderr or the logfile.
- **Nothing inside the database and table loops exits; it records in
  `outcome`.** The summary and the exit status are computed once, after the
  loops. Breaks: a `sys.exit` in a loop drops every later table and database
  without a summary, and a problem that is only printed never reaches the
  exit status.
- **A VACUUM that returns is counted only if `conn.notices` is empty.** The
  session sets `client_min_messages = warning` and the list is emptied before
  each VACUUM. Breaks: the server skips a table it will not vacuum with a
  WARNING and a successful return, and the table is counted as done. The
  message text is never matched; it is translated and version-dependent.
- **`dbconnect()` returns a connection or raises.** Breaks: a `None` return
  fails later, outside the handler that would have skipped the database.
- **Each database's connection is closed by the `try`/`finally` around its
  work.** Breaks: code added between `dbconnect()` and that `try` can leave a
  session open for the rest of the run.
- **Tests start the script only through `run_ff` / `start_ff`.** Breaks: a
  run without the forced host and port reaches whatever server libpq
  defaults to and vacuums it (ADR 0008).

## Landmines

- With no `-H`/`-p` the script connects to the local default server and
  works through every database. Never run it by hand to "try something".
- A failed table, a database that cannot be connected to or listed (ADR
  0004), and a `--table` that matches nothing do not stop the run; each is
  recorded and makes the exit status 1. A lost connection ends that database
  only, and there is no reconnect.
- Exit status: 0 complete or stopped at the time limit, 1 work missed (or
  the run could not start), 2 bad arguments. A stop at the time limit is 0
  on purpose, including a vacuum cancelled by `--enforce-time`; that case is
  told from a failure by the error being a query cancel, `--enforce-time`
  being set, and the deadline having passed.
- The time limit is checked before each table only (ADR 0002). It is not
  checked during the table listing. The pause is taken before a vacuum that
  follows another one, not after each, and is cut off at the limit.
- The exclusions reported as unmatched (`-v`) are worked out over each whole
  table list before the table loop, not in it; the loop stops at the first
  exclusion that fits and can be cut short by the time limit.
- The window is whole minutes, so tests reach "after the deadline" by moving
  the script's clock (`script_clock` fixture, `ffharness.ScriptClock`), not
  by waiting.
- Without `--enforce-time` the script sets `statement_timeout = 0` on
  purpose (ADR 0005); do not "fix" that by removing it.
- `-l` rebinds `sys.stdout` to the logfile. Debug output and tracebacks stay
  on stderr.
- The SIGINT handler cannot run while psycopg2 is blocked in a VACUUM.
- The default database list excludes `postgres`, `template0` and `template1`
  by name.
- Freeze mode caps the table list at 1000; `--no-freeze` mode has no cap.
- Tests assert on the parsed VACUUM statement (`ffharness.parse_vacuum`), not
  on its literal text. Keep it that way when the statement's spelling
  changes.
- `ruff` runs with default rules on `tests/` but only syntax and
  undefined-name rules on `scripts/`; the script has older style debt that is
  tracked separately.

## Flow

Parse arguments → build the database list (`-d`, or `pg_database` ordered by
`age(datfrozenxid)`) → for each database: `dbconnect` → set cost limits and
`client_min_messages` → run the selection query → apply `--table`, then
exclusions → for each table: pause if a vacuum came before → time check →
set `statement_timeout` → VACUUM → record the result in `outcome` → close the
connection → summary and exit status from `outcome`.

## Where to change X

- Add or change an option: the `parser.add_argument` block; mirror it in the
  usage block of `README.md`, which is a hand-maintained copy of `--help`.
- Change which tables are selected: the two `tabquery` strings. Keep the
  three leading columns.
- Change the VACUUM statement: where `exquery` is built in the loop.
- Change how names match: `table_named_in`, and `tests/test_table_names.py`.
- Change connection behaviour: `dbconnect`, and
  `tests/test_connection_setup.py`.
- Add a way for a run to fall short: a field on `RunOutcome`, its term in
  `work_was_missed()`, its line in the summary block, the exit-status table
  in `README.md`, and `tests/test_run_outcome.py`.
- Add a test: use the fixtures in `tests/conftest.py`; prove outcomes from
  the catalog (`Database.frozen_since`) or the statement log
  (`Database.script_vacuums`), not from the script's output alone.

---

For why decisions were made, see `docs/adr/`. For build, test and usage, see
`README.md`.
