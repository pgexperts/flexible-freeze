# Architecture

flexible-freeze is one Python script, `scripts/flexible_freeze.py`, that runs
`VACUUM ([FREEZE,] [ANALYZE,] [TRUNCATE false,] SKIP_LOCKED)` table by table inside a time
window (as a superuser on PostgreSQL 14 or later, a table's TOAST table by a
statement of its own; see the invariants and ADR 0019). PostgreSQL 12 is the oldest server it supports (CI covers 14-18) and
Python 3.11 the oldest interpreter (ADR 0018). It is a
top-to-bottom script: there is no `main()`, and argument parsing, signal
registration and the whole run execute at import. It cannot be imported.

## Component map

| Component | Responsibility | Entry points |
|---|---|---|
| Argument parsing (ADR 0021) | Options, defaults, the per-database exclusion map, the checks made after parsing (one of them between two options: `--system-catalogs` with `--no-freeze`); `--version`, which argparse answers itself before anything connects | module level, `parser`, `args`, `database_table_map`, `__version__` |
| Output | Progress, verbose and debug printing; failure reports, which go to stderr and to the log file; password masking | `_print`, `verbose_print`, `failure_print`, `debug_print`, `stamped`, `log_file`, `args_for_display` |
| Connection | The only place a connection is made; sets autocommit and an empty `search_path` | `dbconnect` |
| Statements (ADR 0017) | The only place a statement is sent; appends the `--comment` text, which `comment_text` has vetted at argument parsing | `execute`, `comment_text` |
| Name matching | Compares a user-supplied table name with a table: in Python for the exclusions, in SQL for `--table` | `table_named_in`, `TABLE_NAME_MATCH_SQL` (in `TABLE_LOOKUP_QUERY` and `TABLE_KIND_QUERY`) |
| Database list (ADR 0020) | `-d`, in the order given, else every connectable non-template database in `pg_database`, by the greater of XID and MultiXact age, read via the `postgres` database in a session with the run's `lock_timeout`; a failure to connect or to read the list is reported with `failure_print` and ends the run (exit 1) | module level, `dblist` |
| Table selection (ADR 0012) | Three queries: freeze mode (by the greater of XID and MultiXact age; with `--system-catalogs` the system schemas too), `--no-freeze` mode (by dead-tuple fraction), and the `--table` lookup, which replaces either; when that lookup finds no table, a second one asks whether the name is a partitioned table, a foreign table or a view, for the message only | module level, `tabquery`, `system_schema_filter`, `FREEZE_AGE_SQL`, `TABLE_LOOKUP_QUERY`, `TABLE_KIND_QUERY`, `UNVACUUMABLE_KINDS` |
| Vacuum loop | Exclusions, pause, time check, `statement_timeout`, VACUUM (one statement, or the table's and its TOAST table's: ADR 0019), server messages | module level, `for db in dblist`, `TOAST_SPLIT_QUERY`, `vacuums` |
| Lock handling (ADR 0014, 0015) | `SKIP_LOCKED` on every VACUUM; the session's `lock_timeout` (`--lock-timeout`); one advisory lock per database against overlapping runs (`--no-advisory-lock`), and when it is refused a lookup that names its holders | `ADVISORY_LOCK_KEY`, `ADVISORY_LOCK_HOLDER_QUERY`, `advisory_lock_holders_report`, `APPLICATION_NAME`, the session settings and `vacuum_options` in the database loop |
| Run outcome (ADR 0009) | What was and was not done; the only input to the summary and the exit status | `RunOutcome`, `outcome`, the summary block at the end of the file |
| Interruption (ADR 0011) | SIGINT, SIGTERM and SIGHUP (the last only if it was not already ignored when the script started): the handler records the signal; the psycopg2 wait callback cancels the running statement and applies `connect_timeout`; the pause ends early; a second signal stops waiting for the server | `signal_handler`, `interrupt_signal`, `interrupt_repeated`, `wait_for_server`, `pause`, `exit_if_interrupted` |
| Test harness | Throwaway cluster (and a sweep of clusters a killed session left), the single launcher for the script, catalog and statement-log probes, a clock for the script that tests can move and stop, a gate that stops the script just before one statement (`StatementGate`; `KindLookupGate`, `HolderLookupGate`), a probe for the files the script left open at exit, a socket that never answers, a start with SIGHUP ignored (`start_ff(ignore_sighup=True)`), a SCRAM role for password tests, per-subprocess coverage (`.coveragerc`), the fold of a TOAST table's VACUUM into its parent's (`fold_toast_vacuums`) | `tests/ffharness.py`, `tests/conftest.py` |

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
- **A table is a row `(regclass text, nspname, relname, TOAST regclass text
  or NULL)`.** The regclass text
  is printed and is the VACUUM target, verbatim. Breaks: quoting or otherwise
  rewriting it produces a wrong or malformed target. `nspname` and `relname`
  are for matching only; the fourth column only builds the TOAST table's
  VACUUM. All three table queries (both selections, `TABLE_LOOKUP_QUERY`)
  return all four.
- **Exclusion names are compared with `table_named_in`; the `--table` name
  is matched by `TABLE_NAME_MATCH_SQL`, which spells out the same three
  forms** and is the one copy of them in SQL: `TABLE_LOOKUP_QUERY` and
  `TABLE_KIND_QUERY` both take it. Breaks: change one without the other and
  `-T`, `--exclude-table-in-database` and `--table` stop agreeing on what a
  name means (ADR 0007); give the two queries conditions of their own and a
  name is one thing when a table is looked for and another when its kind is
  reported.
- **`TABLE_KIND_QUERY` is sent only when `TABLE_LOOKUP_QUERY` returned no
  row (with or without `-v`, though only `-v` prints what it finds), and its
  rows are never vacuumed (ADR 0007).** Breaks: sent every time, a run
  that finds its table issues a statement it did not before; fed into the
  table list, a view or a foreign table gets a VACUUM the server declines
  and a partitioned table one that vacuums every partition unasked.
- **The `--table` value reaches the server as a query parameter.** Apart
  from the `--comment` text (next invariant) it is the
  only user-supplied text in any query. Breaks: formatted into the statement
  it is SQL injection as the connecting role. Because those two statements
  have parameters, their literal percent signs are doubled.
- **Every statement is sent with `execute(cur, ...)`, never with a cursor's
  own `execute`, and is passed in without a closing semicolon.** `execute`
  appends the `--comment` text as ` /* text */`. The text cannot end the
  comment because `comment_text` refuses, at argument parsing, any value
  containing `/*`, `*/`, a NUL or a line break; nothing is stripped or
  escaped except that `%` is doubled when the statement has parameters,
  which is when psycopg2 formats it (ADR 0017). Breaks: a statement sent directly goes
  without the comment; a check loosened to "clean up" the value lets text
  such as `**//` close the comment and the rest reach the server as SQL; a
  semicolon puts the comment after the statement instead of in it.
- **Connections are autocommit.** Breaks: VACUUM cannot run in a transaction
  block.
- **`args.dbpass` is never printed; print `args_for_display()` instead.**
  Breaks: the password lands in stderr or the logfile.
- **A line about something that makes the exit status non-zero is printed
  with `failure_print`, everything else with `_print` or `verbose_print`.**
  The rule is spelled out in the comment above `log_file` (ADR 0016). Breaks: printed
  with `_print`, a failure is on stdout among the progress lines, or with
  `-l` in the log file alone, and a scheduler that mails stderr reports
  nothing.
- **Nothing inside the database and table loops exits; it records in
  `outcome` (ADR 0009).** The summary and the exit status are computed once, after the
  loops. Breaks: a `sys.exit` in a loop drops every later table and database
  without a summary, and a problem that is only printed never reaches the
  exit status.
- **A VACUUM that returns is counted only if `conn.notices` is empty (ADR
  0010).** The
  session sets `client_min_messages = warning` and the list is emptied before
  each VACUUM statement. A table vacuumed with two statements (next
  invariant but one) is counted once, only if both return with no message;
  a message on either makes it "not vacuumed", recorded and reported under
  the parent's name. Breaks: the server skips a table it will not vacuum with a
  WARNING and a successful return, and the table is counted as done. The
  message text is never matched; it is translated and version-dependent.
- **Every VACUUM carries `SKIP_LOCKED` (ADR 0014), and a skipped table is found
  through `conn.notices` like any other the server declines.** Breaks:
  without the option a VACUUM queues behind a conflicting lock for as long
  as its holder takes, with no time check; and a skip that is recognised by
  its message text instead stops being recognised on a server with another
  language or version.
- **A table with a TOAST table is vacuumed with two statements exactly when
  `TOAST_SPLIT_QUERY` (sent once per database session, after `SET
  lock_timeout`) returns true: server 14 or later and `USAGE` on schema
  `pg_toast`, in practice a superuser (ADR 0019).** The parent's statement
  gets `PROCESS_TOAST false`, and the TOAST table's (`pg_toast.pg_toast_NNNN`)
  follows it with the same options otherwise; nothing is sent between them,
  and none is sent after a parent that drew a message. Otherwise the single
  statement, unchanged. Breaks: `SKIP_LOCKED` warns only about a relation the
  statement names, so without the second statement a locked TOAST table is
  skipped in silence; split for a role without the privilege, every TOAST
  statement fails with "permission denied for schema pg_toast"; a time check
  or a count between the two makes half a table a unit of work.
- **Each session sets `lock_timeout`, before the table list is read and
  whatever the value (0 too).** It is what bounds the listing query, whose
  size functions wait behind an `ACCESS EXCLUSIVE` lock on any candidate,
  and every lock a VACUUM waits for other than its table's. The session
  that reads the database list sets it too, before that query. Breaks: moved
  after the listing, or left out for 0, the listing waits without limit or
  under a database default the operator asked to override.
- **`ADVISORY_LOCK_KEY` never changes (ADR 0015), and the lock is taken per database,
  at session level, inside the `try` that closes the connection.** Breaks:
  two copies of the script with different keys do not see each other; a
  transaction-level lock is gone at once under autocommit; a lock taken
  outside that `try` can outlive the database's work.
- **SIGHUP gets the handler only if it is not already ignored when the
  script starts** (`signal.getsignal` is not `SIG_IGN`), and only where the
  platform has the signal (ADR 0011). Breaks: registered unconditionally, a run
  started with `nohup` is stopped by the hangup it was protected from.
- **The signal handler only assigns `interrupt_signal` and
  `interrupt_repeated`.** It can run between any two bytecodes (ADR 0011).
  Breaks: printing from it can re-enter a print in progress, exiting from it skips the summary, and closing the connection
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
- **The harness pins the script's host, not its port.** `-H` is always the
  throwaway cluster's socket directory, and any argument that names a host
  or a port is refused (`UnsafeInvocation`). The `port=` keyword of
  `run_ff` / `start_ff` is the one way to choose `-p`: a number is checked
  (positive, not 5432), a string is handed to the script as it is, so that
  a port list or an invalid port can be tested (only text that reads as a
  host or port option, or carries a connection keyword, is refused). That
  is safe because libpq uses the port only to pick a socket inside that
  private directory, and every `PG*` variable is stripped; the reasoning
  is in `ff_command`'s docstring (ADR 0008). Breaks: the same freedom given to the host, or to arguments,
  lets a test reach a real server.

## Landmines

- With no `-H`/`-p` the script connects to the local default server and
  works through every database. Never run it by hand to "try something".
- A failed table and a database that cannot be connected to or listed (ADR
  0004) do not stop the run; each is recorded and makes the exit status 1.
- A database in which another session holds the advisory lock is one more
  skipped database: same list in the summary, same exit status 1, with the
  reason in the line printed when it is skipped. Any role that can connect
  can hold the key; nothing checks (ADR 0015 and its addendum). So when
  `pg_try_advisory_lock` returns false, and only then,
  `ADVISORY_LOCK_HOLDER_QUERY` asks `pg_locks` (left join
  `pg_stat_activity`) for the holders, and the line names each: pid, role,
  application name. A holder whose application name is the script's
  (`APPLICATION_NAME`) is "another flexible_freeze run"; any other name
  gets the line saying it does not appear to be flexible_freeze and that
  every run will skip the database until it lets go; one such holder
  decides the wording. The distinction is client-set and spoofable, and
  decides nothing but the words. No rows means the holder let go in
  between ("held a moment ago"); the lock is never tried twice. The lookup
  has its own `except`: a failure there is still reported as the lock
  held, with why the holder is not known, never as "couldn't list its
  tables". Any role sees pid, role and application name of every session
  in `pg_stat_activity`. A NULL pid is a prepared transaction, which is
  never a run and gets the pointed line (until committed or rolled back);
  a NULL role or application name (a session just gone) is "not known",
  and a NULL application name is never read as "not flexible_freeze". The
  query is `DISTINCT`: one session can hold the key in both modes. The lock is
  never released by the script; closing the connection does it.
- A locked table is "not vacuumed" (`outcome.unvacuumed_tables`), not
  "failed", and costs no wait. A lock timeout during a VACUUM is "failed".
  A lock timeout during the listing skips the database.
- `SKIP_LOCKED` reports a skipped table with a WARNING, but not a TOAST
  table skipped along with its parent (ADR 0019). Superuser runs on 14 or later name
  the TOAST table in a statement of its own and so see the WARNING; the
  report line names both, and the parent is what is recorded. Every other
  run (any other role, or a server before 14) still has the gap: the
  server passes over a locked TOAST table without a message and the parent
  counts as vacuumed. In a split run the parent's pages are vacuumed (its
  `relfrozenxid` moves) even when its TOAST table is then skipped, so the
  catalog alone does not show the miss. `--enforce-time`'s
  `statement_timeout` is set once and applies to each of the two
  statements.
- `LOCK TABLE` refuses a TOAST table. Tests hold one with another session's
  VACUUM of it, stopped on a pinned page (`tests/test_toast.py`,
  `toast_vacuum_running`); an open `REINDEX TABLE pg_toast...` also works,
  but only for `--table`: it takes `ACCESS EXCLUSIVE` on the TOAST index,
  which `pg_table_size()` in both selections opens, so a selection waits
  for it and the database is skipped.
- `lock_timeout` is set per database session, with the cost settings, and
  in the session that reads the database list. A lock timeout there ends
  the run before it has begun (exit 1); in a database's table listing it
  skips that database.
- Reading the database list is all or nothing (ADR 0020): a failed connection, a
  failed `SET lock_timeout`, a failed query or a failed fetch is reported
  with `failure_print` ("Could not list databases: ...") and exits 1. The
  query and the fetch share one `try`; the message for both says "query".
  Nothing there is left to an uncaught exception, whose traceback would
  reach stderr and not the log file.
- A failure in the session settings, the advisory-lock call, the listing
  query or the `--table` kind lookup (`TABLE_KIND_QUERY`) is reported as
  "couldn't list its tables" and skips the database; the message names
  only the listing. The advisory-lock holder lookup is the exception (see
  above).
- A lost connection ends that database only (recorded as unfinished, exit
  status 1), and there is no reconnect.
- A `--table` name that no table in a database has is recorded, and
  printed for that database with `-v` only; it makes the exit status 1 only
  when the name
  was found in none of the databases examined (`table_found_nowhere`). A
  table that exists and is excluded counts as found.
- A `--table` name that is a partitioned table, a foreign table or a view
  in a database, and no table there, is still a name no table has in that
  database: same record (`table_missing_in`), same summary line, same exit
  status. Only the `-v` line differs: one for each such relation, saying
  that it exists, what it is and that this script cannot vacuum it, with a
  hint to name the partitions for a partitioned table. Nothing is expanded:
  the partitions are vacuumed only when they are named. Any other kind of
  relation (index, sequence, TOAST table, composite type, partitioned
  index) and anything in another session's temporary schema is said not to
  exist, like a name nothing has.
- `--table` bypasses the selection: age, size and dead-row thresholds do not
  apply, and neither does the selections' exclusion of the system schemas
  (so `--system-catalogs` adds nothing to it).
  It finds ordinary tables and materialized views outside the temporary
  schemas, and its matches still go through the exclusions.
- Exit status: 0 complete or stopped at the time limit, 1 work missed (or
  the run could not start), 2 bad arguments, 129 stopped by SIGHUP, 130
  stopped by SIGINT, 143 stopped by SIGTERM (128 + the signal number; these
  outrank 0 and 1). A SIGHUP that the script inherited as ignored (`nohup`)
  is not handled and stops nothing. A
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
- `--comment` is vetted by refusing, never by cleaning up (ADR 0017): `**//`
  shows why removal is unsafe. The text ends in a space-padded `*/`, so a
  text ending in `/` is safe; psycopg2's own `SET datestyle` is the one
  statement without the comment; `-v` prints the VACUUM without it; a
  comment the client encoding cannot encode fails in `dbconnect()`.
- An empty or blank `--table` is refused at argument parsing (exit 2,
  `non_empty_table`, ADR 0007); the option is tested against `None`.
- `-p` is one port or a comma-separated list, each from 1 to 65535
  (`port_list`); an empty value or entry is refused (exit 2), never read
  as the default port. `args.dbport` is the text as given, not a number,
  because libpq is handed the list as text; `--debug` shows it quoted.
  The number of ports is not compared with the number of hosts; libpq
  does that when it connects. These parse-time checks, `--costlimit`'s
  floor of 1 and the refusal of an empty part in the table portion of
  `--exclude-table-in-database` (`db..t`, `db.schema.`, `db.schema..t`)
  are approximate by design; ADR 0021 lists what they let through (upper
  bounds, the `-p`/`-H` count). `-T` takes `non_empty_table` like `--table`.
- Test files may not contain the default port's number as a literal
  (`test_test_files_do_not_bypass_the_harness`); port tests use other
  numbers.
- `--no-truncate` adds `TRUNCATE false`; without it nothing about
  truncation is sent, so the table's `vacuum_truncate` still applies
  (ADR 0014). `TRUNCATE true` is never sent.
- `parse_vacuum` accepts an option with one unquoted word as its value and
  one trailing comment on the table's line, nothing more (ADR 0008).
- Without `--enforce-time` the script sets `statement_timeout = 0` on
  purpose (ADR 0005); do not "fix" that by removing it.
- `-l` rebinds `sys.stdout` to the logfile, which is also kept as
  `log_file` so that `failure_print` can write a failure report to stderr
  and to the file. Without `-l` a failure report is on stderr only. Debug
  output, tracebacks and command-line errors are on stderr only, and the
  header a run writes at the top of its log is in the log only.
- `failure_print` writes the logfile copy first. A failed write to stderr
  (a pipe whose reader has gone) does not end the run: descriptor 2 is
  pointed at the null device. Catching the error alone is not enough,
  because the line stays in the stream's buffer and the interpreter's
  flush at exit then turns the exit status into 120. That only shows when
  Python buffers stderr, so the tests for it clear `PYTHONUNBUFFERED`. A
  failed write to stdout or to the logfile is not caught anywhere.
- The logfile is closed by an `atexit` handler registered where it is
  opened, not by a close at each exit. It runs on every `sys.exit`,
  including the two for a signal, and after an uncaught exception; the
  signal handler itself still does no I/O.
- Two pipes do not preserve the order of lines written to different
  streams, so most tests assert on each stream separately;
  `tests/test_output_streams.py` checks the combined order through the
  logfile and through one pipe for both (`run_ff(merge_streams=True)`),
  and the closing of the logfile through `ffharness.OpenFilesProbe`.
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
  `exit_if_interrupted()` instead, first thing in each `except`, so that a
  cancel the signal caused is not reported as a failure to list.
- With a wait callback installed libpq does not apply `connect_timeout`;
  `wait_for_server` does, to the attempt as a whole and not per host.
- The wait callback is process-wide in psycopg2 and changes how every query
  and connection attempt waits, not only VACUUM.
- The default database list is every database with `datallowconn` and not
  `datistemplate`, `postgres` included (ADR 0020). The list is still read through a
  connection to `postgres`.
- The default list is ordered by
  `greatest(age(datfrozenxid), mxid_age(datminmxid))`, oldest first, the
  same measure of age as a table's (`FREEZE_AGE_SQL`). A `-d` list never
  goes through that query and keeps the order it was written in.
  Databases of equal age (copies of one template, until something in them
  is frozen) come in whatever order the server returns.
- Neither selection caps its list; the time window is the only bound (ADR
  0012, which superseded 0003).
- `--freezeage` is compared with one number per table, `FREEZE_AGE_SQL`: the
  greatest of the XID age and the MultiXact age of the table and of its
  TOAST table, strictly (`>`: an age equal to `--freezeage`, 0 included, is
  not selected; ADR 0012, addendum). `--minsizemb` is compared with
  `pg_table_size`, in both modes.
- Both selections leave out `pg_catalog`, `information_schema` and the
  temporary schemas, and neither selects partitioned tables.
  `--system-catalogs` lifts the first two for the freeze selection, and
  only that (ADR 0012, addendum): the condition on those two schemas becomes `true`
  (`system_schema_filter`) and the rest of the statement is untouched, so
  thresholds, order and exclusions apply to a catalog as to any table.
  Without the option the statement is byte for byte what it was before
  the option existed. The `--no-freeze` selection cannot take catalogs
  (`pg_stat_user_tables` has none), so the two options together are
  refused after parsing (exit 2); with `--table` the option is accepted
  and idle.
- No privilege is checked for `--system-catalogs`. A role that may not
  vacuum a catalog draws the server's WARNING for each one, so each is
  "not vacuumed" (`outcome.unvacuumed_tables`) and the exit status is 1.
- A shared catalog (`pg_database`, `pg_authid`) has a `relfrozenxid` in
  every database's `pg_class`, and a VACUUM moves only the one in the
  database it ran in. With `--system-catalogs` it is therefore selected,
  and vacuumed, in each database; that is not a duplicate to remove.
- A `pg_catalog` table's regclass text is its bare name even under the
  empty `search_path` (`pg_class`, not `pg_catalog.pg_class`), so that is
  what is printed and vacuumed, and `parse_vacuum` reports it in schema
  `public`. `Database.frozenxids()` leaves both system schemas out; the
  catalog tests in `tests/test_selection.py` have their own probes
  (`system_frozenxids`, `tables_named`).
- Tests assert on the parsed VACUUM statement (`ffharness.parse_vacuum`), not
  on its literal text. Keep it that way when the statement's spelling
  changes.
- `Database.script_vacuums()` is one entry per table: a split pair is
  folded (`ffharness.fold_toast_vacuums`) into the parent's entry, with
  `PROCESS_TOAST false` left out of `.options` and the TOAST statement in
  `.toast`, so the same assertion holds for a split and an unsplit run. The
  fold refuses a TOAST statement that does not directly follow a split
  parent on the same connection or has other options; it cannot tell a
  missing TOAST statement from a parent that drew a WARNING.
  `Database.script_vacuum_statements()` is every VACUUM as sent. Most
  tables the harness makes have a TOAST table, so as the superuser most
  runs split; tests that count raw statements count two per table.
- `ruff` runs with default rules on `tests/` but only syntax and
  undefined-name rules on `scripts/`; the script has older style debt that is
  tracked separately.

## Flow

Parse arguments → build the database list (`-d` as written, or `pg_database` ordered by
the greater of `age(datfrozenxid)` and `mxid_age(datminmxid)`, read after `SET lock_timeout`; a failure there exits 1) → for each database: `dbconnect` → set cost limits,
`client_min_messages` and `lock_timeout` → ask whether to vacuum TOAST tables
apart (`TOAST_SPLIT_QUERY`) → take the advisory lock, or skip
the database if any session holds it, naming the holders → run the selection query, or with
`--table` the lookup of that name → apply exclusions → for each table: pause
if a vacuum came before → time check → set `statement_timeout` → VACUUM (and,
when split and the parent drew no message, the TOAST table's VACUUM) →
record the result in `outcome` → close the connection, which releases the
advisory lock → summary and exit status from `outcome`.

On SIGINT, SIGTERM or SIGHUP: the handler records the signal → the wait in progress
notices it (`wait_for_server` cancels the statement, `pause` returns) → the
loops break at their next test of `interrupt_signal`, closing the connection
→ the same summary, led by the interruption → exit 130, 143 or 129.

## Where to change X

- Release a version (ADR 0022): change `__version__` in the script (the one place the
  number is written), add a section to `CHANGELOG.md`, and change the line
  `--version` is expected to print in `tests/test_arguments.py`. The owner
  tags; nothing here does.
- Change the runtime dependency: `scripts/requirements.txt` (`psycopg2`, built
  from source) and `requirements-dev.txt` (`psycopg2-binary`, what CI
  installs) name the same range and are kept in step by hand. The dev file
  must not `-r` the runtime one: the two packages provide the same module.
  The README's "Installing" section says the same to operators.
- Add or change an option: the `parser.add_argument` block; mirror it in the
  usage block of `README.md`, which is a pasted copy of `--help` (regenerate it with
  `COLUMNS=130 python scripts/flexible_freeze.py --help` on Python 3.11 or
  3.12; 3.13 prints each option's value name once, so its output differs).
- Change which tables are selected: the two `tabquery` strings. Keep the
  three leading columns. The schemas the freeze selection covers are its
  `WHERE`, of which `system_schema_filter` is the part `--system-catalogs`
  switches. What `--table` can find is `TABLE_LOOKUP_QUERY`;
  which other kinds of relation it names in its message instead of "does
  not exist" is `UNVACUUMABLE_KINDS`.
- Change the order of the databases: the `ORDER BY` of the query that reads
  the list. `tests/test_selection.py` builds two databases that differ in
  both ages (`databases_behind_on_different_horizons`); databases made the
  usual way are the same age and show nothing about the order.
- Change the VACUUM statement: where `vacuum_options` and `vacuums` are
  built in the loop, for both the single and the split form. `SKIP_LOCKED`
  stays; see the invariant. `tests/test_toast.py` covers the split.
- Change what a run does about locks: the session settings at the top of
  the database loop, `ADVISORY_LOCK_KEY` (and, for what a refused run
  reports, `ADVISORY_LOCK_HOLDER_QUERY` and `advisory_lock_holders_report`),
  and `tests/test_locks.py`. Tests
  elsewhere that need a VACUUM to be running hold it up with a pinned page
  (`vacuum_held_up`), since a lock no longer holds it.
- Change how names match: `table_named_in` and the three forms in
  `TABLE_NAME_MATCH_SQL`, together, and `tests/test_table_names.py`.
- Change connection behaviour: `dbconnect`, and
  `tests/test_connection_setup.py`.
- Add a statement (ADR 0017): send it with `execute(cur, statement, params)`, without
  a closing semicolon; `tests/test_comment.py` counts the direct calls.
- Add a way for a run to fall short: a field on `RunOutcome`, its term in
  `work_was_missed()`, its line in the summary block, the exit-status table
  in `README.md`, and `tests/test_run_outcome.py`. Print what reports it
  with `failure_print` and add its scenario to
  `tests/test_output_streams.py`.
- Add or change a message: decide by the rule above `log_file` whether it
  is a failure report (`failure_print`) or not (`_print`, `verbose_print`);
  the rule is restated under "Where the output goes" in `README.md`.
- Add a step that can block: make it a psycopg2 call (covered by the wait
  callback) or a sliced wait like `pause`, test `interrupt_signal` first in
  its error handling, and add a case to `tests/test_signals.py`.
- Add a test: use the fixtures in `tests/conftest.py`; prove outcomes from
  the catalog (`Database.frozen_since`) or the statement log
  (`Database.script_vacuums`), not from the script's output alone.

---

For why decisions were made, see `docs/adr/`. For build, test and usage, see
`README.md`.
