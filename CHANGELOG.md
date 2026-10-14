# Changelog

What changed for whoever runs `scripts/flexible_freeze.py`, release by release.
The version is `__version__` in the script; `--version` prints it.

## 0.7 (unreleased)

Everything below is relative to 0.6.  The reasons are in `docs/adr/`.

### Requirements

- Python 3.11 or newer; the version check had accepted 3.6.
- PostgreSQL 12 or newer, because every VACUUM now carries `SKIP_LOCKED`.  CI covers 14 through 18.
- `scripts/requirements.txt` is the one runtime declaration and names `psycopg2>=2.9,<3` (built from source); the README has an "Installing" section, including `psycopg2-binary` as a convenience.

### New options

- `--version` prints the version and exits.
- `--system-catalogs` also freezes tables in `pg_catalog` and `information_schema` (freeze mode only; refused with `--no-freeze`).
- `--lock-timeout SECONDS` bounds every lock wait other than a table's own (default 10; 0 means no limit).
- `--no-advisory-lock` turns off the per-database lock that keeps two runs from working on one database.
- `--no-freeze` does VACUUM ANALYZE by dead-row fraction instead of by age; `--vacuum` is an alias for it.
- `--no-analyze` leaves out the ANALYZE step.
- `--no-truncate` sends `TRUNCATE false`, so VACUUM does not take its brief `ACCESS EXCLUSIVE` lock to truncate.
- `-c`/`--comment` appends `/* TEXT */` to every statement sent, so monitoring tools can recognise them.
- `-s`/`--minsizemb` skips tables smaller than this.
- `--table` (older spelling `-st`) vacuums one named table.

### Changed behaviour

- A table locked by another session is passed over at once (`SKIP_LOCKED`), listed in the summary as not vacuumed, and makes the exit status 1.
- A failed table no longer ends the run; it is recorded, the run goes on, and the exit status is 1.  A lost connection ends that database only.
- Exit status: 0 complete or stopped at the time limit, 1 work missed, 2 bad arguments, 129/130/143 stopped by SIGHUP/SIGINT/SIGTERM.
- SIGINT and SIGTERM cancel the running VACUUM on the server and print the summary; SIGINT used to exit 0 and SIGTERM left the VACUUM running.  SIGHUP is handled the same way.  A SIGHUP (`nohup`) or SIGINT (a background job of a non-interactive shell) that was already ignored when the script started stays ignored.
- Failure reports go to standard error, and with `-l` to the log file as well; the log file is closed when the script ends.
- A database whose advisory lock is held is skipped, and the report names each holder by process id, role and application name.
- A failure to list the databases is reported and exits 1, instead of a traceback.
- Without `-d`, the databases are every connectable non-template database, `postgres` included, oldest first by the greater of XID and MultiXact age, and by name among equals.
- Freeze mode orders and thresholds tables by the greatest XID or MultiXact age of the table and its TOAST table, selects materialized views, has no 1000-table cap, and measures `--minsizemb` with `pg_table_size`.
- `--table` ignores the age, size and dead-row thresholds, finds a table in any schema, and refuses an empty value.
- `-T` refuses an empty or blank value with exit status 2, instead of excluding nothing (an unset variable in a cron entry left the table it was meant to protect unprotected); `--exclude-table-in-database` likewise refuses `db.schema.` and `db.schema..t`.
- `-T`, `--table` and the table part of `--exclude-table-in-database` refuse a value with leading or trailing whitespace, with exit status 2, instead of matching nothing; the value is not stripped, and a name that really has such whitespace is given in its quoted form, as in `sales." orders"` (#102).
- `--table` naming a view, foreign table or partitioned table says what it is and that the script cannot vacuum it, instead of saying it does not exist.
- Tables are vacuumed by their schema-qualified name as the server prints it; `-T`, `--exclude-table-in-database` and `--table` match a bare or a `schema.table` name.
- Without `--enforce-time` each operation is preceded by `SET statement_timeout = 0`; with it, a vacuum cancelled at the time limit is a normal stop (exit 0) that names the table.  When a table's TOAST table is vacuumed by a statement of its own, that statement gets a `statement_timeout` of its own, reckoned from the time then left, so the pair no longer gets the full remaining time twice.
- A `connect_timeout` (`PGCONNECT_TIMEOUT`, or a service file) that is not a whole number of seconds is refused once, with one message and exit status 2, instead of being reported as a failed connection to every database.
- A `connect_timeout` of any length is judged by libpq's rule: leading zeros do not count, and a value outside the range of a C int is refused as above (issue #103).
- `-p` is checked before any connection: one port or a comma-separated list for a multi-host `-H`, each an integer from 1 to 65535; an empty or malformed value is refused with exit status 2.
- `--costlimit` must be at least 1, the server's minimum; negative values for the other numeric options, and an empty part of `--exclude-table-in-database`, are refused with exit status 2.
- Sessions run with an empty `search_path`, and connection values are passed to libpq as parameters, not pasted into a connection string.
- The `-w` password is masked in `--debug` output and in the log file.

### Fixes

- An empty entry in `-d` (a doubled or trailing comma) no longer raises an exception.
- `--exclude-table-in-database` accepts a quoted table name with dots in it, such as `db.public."a..b"`; the dots inside the quotes no longer count as the empty parts that were refused (#102).
- A schema whose name merely resembles a temporary schema's (`pgxtemp`, `"pg-temp"`) is no longer left out of the selection or hidden from `--table`; the temporary schemas are matched exactly (`pg_temp_N`, `pg_toast_temp_N`).
- With `-v`, a failed write to standard output while vacuuming a TOAST table is no longer reported as that table failing.
- The dead-row percentage used by `--no-freeze` is calculated correctly.
- A TOAST table locked by another session is reported, and its table counted as not vacuumed (exit status 1), instead of being skipped in silence; this holds for runs as a superuser (a role with `USAGE` on `pg_toast`) on PostgreSQL 14 or later, which vacuum the TOAST table with a statement of its own.  Other runs still have the gap.
- Reporting details (#67): a database skipped during setup now says which step failed ("couldn't set up its session", "check its TOAST tables", "take its advisory lock", "list its tables", "check what kind of relation --table names") instead of always "couldn't list its tables"; a failed `SET statement_timeout` reads "setting statement_timeout before VACUUMing X failed" instead of blaming the VACUUM; the "working on database" line is no longer printed for the database a time-limited run stops at; the summary says "matched no table that can be vacuumed" (a view, partitioned table or foreign table of the name is not a table to it); a role name in the advisory-lock holder report is quoted, so a name with a line break cannot forge report lines.
- The `--table` kind lookup is sent only with `-v`, the only place its answer is shown, so a failure of it can no longer skip a database for an invisible line (#67).
- The line for a table left out by `--exclude-table-in-database` is a `-v` line like the one for `-T`, no longer a `--debug` line (#67).

### Development

- Part of #51: `ruff` is pinned to 0.17.0 and its rules are named in `ruff.toml`; CI lints `scripts` and `tests` with one command in place of the two narrowed checks. The one finding under those rules, an unused variable in `timestamp()`, is removed. CI runs the suite on Python 3.11 against PostgreSQL 18 as well as on 3.13 against 14 through 18.

## 0.6

The baseline: the script as it stood before this changelog began.
