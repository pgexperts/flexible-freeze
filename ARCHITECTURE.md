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
| Vacuum loop | Exclusions, time check, `statement_timeout`, VACUUM, pause | module level, `for db in dblist` |
| Test harness | Throwaway cluster, the single launcher for the script, catalog and statement-log probes | `tests/ffharness.py`, `tests/conftest.py` |

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
- **Tests start the script only through `run_ff` / `start_ff`.** Breaks: a
  run without the forced host and port reaches whatever server libpq
  defaults to and vacuums it (ADR 0008).

## Landmines

- With no `-H`/`-p` the script connects to the local default server and
  works through every database. Never run it by hand to "try something".
- A failed VACUUM calls `sys.exit(1)`: one bad table ends the run for all
  remaining tables and databases. A failed connection only skips that
  database (ADR 0004). The two are deliberately not symmetrical yet.
- The time limit is checked before each table only (ADR 0002). It is not
  checked during the pause or the table listing.
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
`age(datfrozenxid)`) → for each database: `dbconnect` → set cost limits →
run the selection query → apply `--table`, then exclusions → for each table:
time check → set `statement_timeout` → VACUUM → pause → summary and exit.

## Where to change X

- Add or change an option: the `parser.add_argument` block; mirror it in the
  usage block of `README.md`, which is a hand-maintained copy of `--help`.
- Change which tables are selected: the two `tabquery` strings. Keep the
  three leading columns.
- Change the VACUUM statement: where `exquery` is built in the loop.
- Change how names match: `table_named_in`, and `tests/test_table_names.py`.
- Change connection behaviour: `dbconnect`, and
  `tests/test_connection_setup.py`.
- Add a test: use the fixtures in `tests/conftest.py`; prove outcomes from
  the catalog (`Database.frozen_since`) or the statement log
  (`Database.script_vacuums`), not from the script's output alone.

---

For why decisions were made, see `docs/adr/`. For build, test and usage, see
`README.md`.
