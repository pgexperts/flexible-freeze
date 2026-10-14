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
- `-p` accepts a comma-separated libpq port list for a multi-host `-H`.

### Changed behaviour

- A table locked by another session is passed over at once (`SKIP_LOCKED`), listed in the summary as not vacuumed, and makes the exit status 1.
- A failed table no longer ends the run; it is recorded, the run goes on, and the exit status is 1.  A lost connection ends that database only.
- Exit status: 0 complete or stopped at the time limit, 1 work missed, 2 bad arguments, 129/130/143 stopped by SIGHUP/SIGINT/SIGTERM.
- SIGINT and SIGTERM cancel the running VACUUM on the server and print the summary; SIGINT used to exit 0 and SIGTERM left the VACUUM running.  SIGHUP is handled the same way, unless it was already ignored (`nohup`).
- Failure reports go to standard error, and with `-l` to the log file as well; the log file is closed when the script ends.
- A database whose advisory lock is held is skipped, and the report names each holder by process id, role and application name.
- A failure to list the databases is reported and exits 1, instead of a traceback.
- Without `-d`, the databases are every connectable non-template database, `postgres` included, oldest first by the greater of XID and MultiXact age.
- Freeze mode orders and thresholds tables by the greatest XID or MultiXact age of the table and its TOAST table, selects materialized views, has no 1000-table cap, and measures `--minsizemb` with `pg_table_size`.
- `--table` ignores the age, size and dead-row thresholds, finds a table in any schema, and refuses an empty value.
- `--table` naming a view, foreign table or partitioned table says what it is and that the script cannot vacuum it, instead of saying it does not exist.
- Tables are vacuumed by their schema-qualified name as the server prints it; `-T`, `--exclude-table-in-database` and `--table` match a bare or a `schema.table` name.
- Without `--enforce-time` each operation is preceded by `SET statement_timeout = 0`; with it, a vacuum cancelled at the time limit is a normal stop (exit 0) that names the table.
- `--costlimit` must be at least 1, the server's minimum; negative values for the other numeric options, and an empty part of `--exclude-table-in-database`, are refused with exit status 2.
- Sessions run with an empty `search_path`, and connection values are passed to libpq as parameters, not pasted into a connection string.
- The `-w` password is masked in `--debug` output and in the log file.

### Fixes

- `--vacuum` did nothing; it now means `--no-freeze`.
- An empty entry in `-d` (a doubled or trailing comma) no longer raises an exception.
- The dead-row percentage used by `--no-freeze` is calculated correctly.

## 0.6

The baseline: the script as it stood before this changelog began.
