# Architecture

flexible-freeze is one Python script, `scripts/flexible_freeze.py`, that runs
`VACUUM ([FREEZE,] [ANALYZE,] SKIP_LOCKED)` table by table inside a time
window. PostgreSQL 12 is the oldest server it supports. It is a
top-to-bottom script: there is no `main()`, and argument parsing, signal
registration and the whole run execute at import. It cannot be imported.

## Component map

| Component | Responsibility | Entry points |
|---|---|---|
| Argument parsing | Options, defaults, the per-database exclusion map | module level, `parser`, `args`, `database_table_map` |
| Output | Plain, verbose and debug printing; password masking | `_print`, `verbose_print`, `debug_print`, `args_for_display` |
| Connection | The only place a connection is made; sets autocommit and an empty `search_path` | `dbconnect` |
| Name matching | Compares a user-supplied table name with a table: in Python for the exclusions, in SQL for `--table` | `table_named_in`, `TABLE_LOOKUP_QUERY` |
| Database list | `-d`, else every connectable non-template database in `pg_database`, read via the `postgres` database | module level, `dblist` |
| Table selection | Three queries: freeze mode (by the greater of XID and MultiXact age), `--no-freeze` mode (by dead-tuple fraction), and the `--table` lookup, which replaces either | module level, `tabquery`, `FREEZE_AGE_SQL`, `TABLE_LOOKUP_QUERY` |
| Vacuum loop | Exclusions, pause, time check, `statement_timeout`, VACUUM, server messages | module level, `for db in dblist` |
| Lock handling | `SKIP_LOCKED` on every VACUUM; the session's `lock_timeout` (`--lock-timeout`); one advisory lock per database against overlapping runs (`--no-advisory-lock`) | `ADVISORY_LOCK_KEY`, the session settings and `vacuum_options` in the database loop |
| Run outcome | What was and was not done; the only input to the summary and the exit status | `RunOutcome`, `outcome`, the summary block at the end of the file |
| Interruption | SIGINT and SIGTERM: the handler records the signal; the psycopg2 wait callback cancels the running statement and applies `connect_timeout`; the pause ends early; a second signal stops waiting for the server | `signal_handler`, `interrupt_signal`, `interrupt_repeated`, `wait_for_server`, `pause`, `exit_if_interrupted` |
| Test harness | Throwaway cluster, the single launcher for the script, catalog and statement-log probes, a clock for the script that tests can move and stop, a socket that never answers | `tests/ffharness.py`, `tests/conftest.py` |

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
- **Exclusion names are compared with `table_named_in`; the `--table` name
  is matched by `TABLE_LOOKUP_QUERY`, which spells out the same three
  forms.** Breaks: change one without the other and `-T`,
  `--exclude-table-in-database` and `--table` stop agreeing on what a name
  means (ADR 0007).
- **The `--table` value reaches the server as a query parameter.** It is the
  only user-supplied text in any query. Breaks: formatted into the statement
  it is SQL injection as the connecting role. Because that statement has
  parameters, its literal percent signs are doubled.
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
- **Every VACUUM carries `SKIP_LOCKED`, and a skipped table is found
  through `conn.notices` like any other the server declines.** Breaks:
  without the option a VACUUM queues behind a conflicting lock for as long
  as its holder takes, with no time check; and a skip that is recognised by
  its message text instead stops being recognised on a server with another
  language or version.
- **Each session sets `lock_timeout`, before the table list is read and
  whatever the value (0 too).** It is what bounds the listing query, whose
  size functions wait behind an `ACCESS EXCLUSIVE` lock on any candidate,
  and every lock a VACUUM waits for other than its table's. Breaks: moved
  after the listing, or left out for 0, the listing waits without limit or
  under a database default the operator asked to override.
- **`ADVISORY_LOCK_KEY` never changes, and the lock is taken per database,
  at session level, inside the `try` that closes the connection.** Breaks:
  two copies of the script with different keys do not see each other; a
  transaction-level lock is gone at once under autocommit; a lock taken
  outside that `try` can outlive the database's work.
- **The signal handler only assigns `interrupt_signal` and
  `interrupt_repeated`.** It can run between any two bytecodes. Breaks: printing from it can re-enter a print in
  progress, exiting from it skips the summary, and closing the connection
  from it leaves the VACUUM running in the server.
- **Every wait on the server goes through `wait_for_server`,** the psycopg2
  wait callback, installed before the first connection. Breaks: inside a
  blocking libpq call the interpreter does not run, so a signal is not even
  recorded until the statement returns. The one exception is `conn.cancel()`
  itself, which blocks while it delivers the request.
- **Every wait is sliced (`WAIT_SLICE_SECS`) and tests `interrupt_signal`
  between slices.** A handler that returns does not end the `select()` or
  `sleep()` it interrupted; Python resumes them (PEP 475). Breaks: a plain
  `time.sleep(pause)` or a `select()` with no timeout sees the signal only
  when the wait is over anyway.
- **Every `except` in the database and table loops tests `interrupt_signal`
  before treating the error as a failure.** The cancel that a signal causes
  arrives as an ordinary error. Breaks: the interrupted table is recorded as
  failed (or the database as skipped) and the run goes on.
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
- A failed table and a database that cannot be connected to or listed (ADR
  0004) do not stop the run; each is recorded and makes the exit status 1.
- A database in which another run holds the advisory lock is one more
  skipped database: same list in the summary, same exit status 1, with the
  reason in the line printed when it is skipped. The lock is never released
  by the script; closing the connection does it.
- A locked table is "not vacuumed" (`outcome.unvacuumed_tables`), not
  "failed", and costs no wait. A lock timeout during a VACUUM is "failed".
  A lock timeout during the listing skips the database.
- `SKIP_LOCKED` reports a skipped table with a WARNING, but not a skipped
  TOAST table: the server passes over a locked TOAST table without a
  message and the parent still counts as vacuumed.
- `lock_timeout` is set per database session, with the cost settings. The
  connection that reads the database list does not have it.
  A lost connection ends that database only, and there is no reconnect.
- A `--table` name that no table in a database has is recorded, and
  printed for that database with `-v` only; it makes the exit status 1 only
  when the name
  was found in none of the databases examined (`table_found_nowhere`). A
  table that exists and is excluded counts as found.
- `--table` bypasses the selection: age, size and dead-row thresholds do not
  apply, and neither does the selections' exclusion of the system schemas.
  It finds ordinary tables and materialized views outside the temporary
  schemas, and its matches still go through the exclusions.
- Exit status: 0 complete or stopped at the time limit, 1 work missed (or
  the run could not start), 2 bad arguments, 130 stopped by SIGINT, 143
  stopped by SIGTERM (128 + the signal number; these outrank 0 and 1). A
  stop at the time limit is 0 on purpose, including a vacuum cancelled by
  `--enforce-time`; that case is told from a failure by the error being a
  query cancel, `--enforce-time` being set, and the deadline having passed.
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
- A signal is acted on by whatever it interrupted, within one wait slice:
  `wait_for_server` cancels the statement with `conn.cancel()` and keeps
  waiting for the server's answer, so the error surfaces from the
  `execute()` that was waiting. The cancel is not sent from the handler.
  It is repeated (`CANCEL_RETRY_SECS`) because a backend that has not yet
  read the statement ignores the request. A signal that lands between two
  statements cancels nothing at that moment; the next statement's wait
  sends the request as soon as it begins.
- The wait for the server's answer is given up when `cancel()` raises (the
  request could not be delivered) or a second signal arrives
  (`interrupt_repeated`), always after one request has been sent. The
  exception leaving the callback makes psycopg2 close the connection, and
  `conn.closed` in the table loop's handler is what marks the VACUUM as
  not confirmed cancelled (`outcome.interrupt_unconfirmed`). Without the
  second signal a server that never answers leaves a process only SIGKILL
  can end.
- `interrupt_signal` is copied to `outcome.interrupted_by` once, after the
  loops; the summary and the exit status read only the copy. The steps
  before the loops (the database list) have no summary and call
  `exit_if_interrupted()` instead.
- With a wait callback installed libpq does not apply `connect_timeout`;
  `wait_for_server` does, to the attempt as a whole and not per host.
- The wait callback is process-wide in psycopg2 and changes how every query
  and connection attempt waits, not only VACUUM.
- The default database list is every database with `datallowconn` and not
  `datistemplate`, `postgres` included. The list is still read through a
  connection to `postgres`.
- Neither selection caps its list; the time window is the only bound.
- `--freezeage` is compared with one number per table, `FREEZE_AGE_SQL`: the
  greatest of the XID age and the MultiXact age of the table and of its
  TOAST table. `--minsizemb` is compared with `pg_table_size`, in both
  modes.
- Both selections leave out `pg_catalog`, `information_schema` and the
  temporary schemas, and neither selects partitioned tables.
- Tests assert on the parsed VACUUM statement (`ffharness.parse_vacuum`), not
  on its literal text. Keep it that way when the statement's spelling
  changes.
- `ruff` runs with default rules on `tests/` but only syntax and
  undefined-name rules on `scripts/`; the script has older style debt that is
  tracked separately.

## Flow

Parse arguments → build the database list (`-d`, or `pg_database` ordered by
`age(datfrozenxid)`) → for each database: `dbconnect` → set cost limits,
`client_min_messages` and `lock_timeout` → take the advisory lock, or skip
the database if another run holds it → run the selection query, or with
`--table` the lookup of that name → apply exclusions → for each table: pause
if a vacuum came before → time check → set `statement_timeout` → VACUUM →
record the result in `outcome` → close the connection, which releases the
advisory lock → summary and exit status from `outcome`.

On SIGINT or SIGTERM: the handler records the signal → the wait in progress
notices it (`wait_for_server` cancels the statement, `pause` returns) → the
loops break at their next test of `interrupt_signal`, closing the connection
→ the same summary, led by the interruption → exit 130 or 143.

## Where to change X

- Add or change an option: the `parser.add_argument` block; mirror it in the
  usage block of `README.md`, which is a hand-maintained copy of `--help`.
- Change which tables are selected: the two `tabquery` strings. Keep the
  three leading columns. What `--table` can find is `TABLE_LOOKUP_QUERY`.
- Change the VACUUM statement: where `vacuum_options` and `exquery` are
  built in the loop. `SKIP_LOCKED` stays; see the invariant.
- Change what a run does about locks: the session settings at the top of
  the database loop, `ADVISORY_LOCK_KEY`, and `tests/test_locks.py`. Tests
  elsewhere that need a VACUUM to be running hold it up with a pinned page
  (`vacuum_held_up`), since a lock no longer holds it.
- Change how names match: `table_named_in` and the three forms in
  `TABLE_LOOKUP_QUERY`, together, and `tests/test_table_names.py`.
- Change connection behaviour: `dbconnect`, and
  `tests/test_connection_setup.py`.
- Add a way for a run to fall short: a field on `RunOutcome`, its term in
  `work_was_missed()`, its line in the summary block, the exit-status table
  in `README.md`, and `tests/test_run_outcome.py`.
- Add a step that can block: make it a psycopg2 call (covered by the wait
  callback) or a sliced wait like `pause`, test `interrupt_signal` first in
  its error handling, and add a case to `tests/test_signals.py`.
- Add a test: use the fixtures in `tests/conftest.py`; prove outcomes from
  the catalog (`Database.frozen_since`) or the statement log
  (`Database.script_vacuums`), not from the script's output alone.

---

For why decisions were made, see `docs/adr/`. For build, test and usage, see
`README.md`.
