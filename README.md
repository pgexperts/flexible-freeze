flexible-freeze
===============

Flexible freeze scripts for managing off-hours vacuuming and freezing of PostgreSQL databases.


/scripts/flexible_freeze.py
---------------------------

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE
runs on your database during known slow traffic periods. Takes a
timeout so that it won't overrun your slow traffic period.

Tested on Python 3.8, but should work on any supported Python 3.

Supports any non-EOL version of PostgreSQL.

Requires psycopg2

    usage: flexible_freeze.py [-h] [-m RUN_MIN] [-s MINSIZEMB] [-d DBLIST] [-T TABLES_TO_EXCLUDE]
                              [--exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE] [--no-freeze] [--no-analyze] [--vacuum]
                              [--pause PAUSE_TIME] [--freezeage FREEZEAGE] [--costdelay COSTDELAY] [--costlimit COSTLIMIT] [-t]
                              [--enforce-time] [-l LOGFILE] [-v] [--debug] [-U DBUSER] [-H DBHOST] [-p DBPORT] [-w DBPASS] [--table TABLE]

    optional arguments:
      -h, --help            show this help message and exit
      -m RUN_MIN, --minutes RUN_MIN
                            Number of minutes to run before halting. Defaults to 2 hours
      -s MINSIZEMB, --minsizemb MINSIZEMB
                            Minimum table size to vacuum/freeze (in MB). Default is 0.
      -d DBLIST, --databases DBLIST
                            Comma-separated list of databases to vacuum, if not all of them
      -T TABLES_TO_EXCLUDE, --exclude-table TABLES_TO_EXCLUDE
                            Exclude any table with this name (in any database). A bare name matches in every schema; SCHEMA.TABLE
                            matches only that table. You can pass this option multiple times to exclude multiple tables.
      --exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE
                            Argument is of form 'DATABASENAME.TABLENAME' or 'DATABASENAME.SCHEMANAME.TABLENAME': exclude the named
                            table, but only when processing the named database. You can pass this option multiple times.
      --no-freeze           Do VACUUM ANALYZE instead of VACUUM FREEZE ANALYZE. Tables are then chosen by fixed thresholds instead
                            of by age: more than 100 dead rows, more than 5% of rows dead, and not vacuumed (manually or by
                            autovacuum) in the last hour. --freezeage is ignored.
      --no-analyze          Do not do an ANALYZE as part of the VACUUM operation
      --vacuum              Same as --no-freeze (deprecated spelling; use --no-freeze instead)
      --pause PAUSE_TIME    seconds to pause between vacuums. Default is 10.
      --freezeage FREEZEAGE
                            freeze tables older than this many XIDs; a table whose age is exactly this is not selected. Default 10m
                            XIDs. Ignored with --no-freeze
      --costdelay COSTDELAY
                            vacuum_cost_delay setting in ms. Default 20
      --costlimit COSTLIMIT
                            vacuum_cost_limit setting. Default 2000
      -t, --print-timestamps
      --enforce-time        enforce time limit by terminating vacuum
      -l LOGFILE, --log LOGFILE
      -v, --verbose
      --debug
      -U DBUSER, --user DBUSER
                            database user
      -H DBHOST, --host DBHOST
                            database hostname
      -p DBPORT, --port DBPORT
                            database port
      -w DBPASS, --password DBPASS
                            database password. Visible to other local users in the process list for the whole run; prefer PGPASSWORD,
                            a .pgpass file or PGSERVICE
      --table TABLE, -st TABLE
                            only process tables with this name: a bare name matches in every schema, SCHEMA.TABLE only that table.
                            -st is an older spelling of this option

Notes:

The minutes time limit is normally only enforced at the start of vacuuming each table, allowing vacuums to continue past the end of the time window.  If you set `--enforce-time`, however, it uses statement_timeout to terminate a running vacuum at the end of the time window (plus 30 seconds grace period).

If you do not specify a `--enforce_time`, `flexible_freeze` will issue a `SET statement_timeout = 0` statement before each operation
to avoid cancellation by a system- or role-wide `statement_timeout` value.

Normally flexible_freeze.py does VACUUM FREEZE ANALYZE, starting with the tables with the oldest transaction IDs.  If you set `--no-freeze`, though, it will instead do VACUUM ANALYZE, starting with the tables with the most dead rows.  If you are doing both, do the FREEZE first.

`--freezeage` selects tables whose transaction ID age is older than the value given; a table whose age is exactly that value is not selected, so even `--freezeage 0` does not select a table whose age is 0, such as one frozen a moment ago.  It applies to freezing only and is ignored with `--no-freeze`.  In `--no-freeze` mode the tables are chosen by fixed thresholds that cannot be changed from the command line: more than 100 dead rows, more than 5% of the table's rows dead, and no vacuum, manual or by autovacuum, in the last hour.  `--vacuum` is a deprecated spelling of `--no-freeze`.

`--minutes`, `--minsizemb`, `--pause`, `--freezeage`, `--costdelay` and `--costlimit` take whole numbers and may not be negative.  Empty entries in the `-d` list are ignored, so a doubled or trailing comma is harmless, but a `-d` that names no database at all is an error.

You can specify `--no-analyze` to skip the ANALYZE step. 

Table names given to `-T`, `--exclude-table-in-database` and `--table` can be written three ways: a bare table name (`orders`), which matches a table of that name in every schema; `schema.table` (`sales.orders`), written plain without SQL quoting, which matches only that table; or the name exactly as `flexible_freeze` prints it (for example `public."Orders"`).  Names are case-sensitive.

`flexible_freeze` runs every session with an empty `search_path`, as `vacuumdb` does, so table names in its output are always schema-qualified (`public.orders`).  On PostgreSQL 16 and earlier, this means the ANALYZE of a table with an expression index can fail if the index's function depends on `search_path`; give such functions an explicit `SET search_path`, or schema-qualify the names they use.

Connection settings other than host, port, user and password are taken from the standard libpq environment variables (`PGSSLMODE`, `PGSERVICE`, `PGCONNECT_TIMEOUT` and so on).  The `-H`, `-U`, `-p`, `-w` and `-d` values are passed to the server as given; they cannot carry additional connection options.

The database user supplied is expected to have permissions on all tables (e.g. a superuser).  If they do not, `flexible_freeze` will error out.

libpq's `PGPASSWORD`, `~/.pgpass` and `PGSERVICE` are honoured, so `-w` is not needed to authenticate.  A password given with `-w` is visible to other local users in the process list for the whole run; prefer one of those instead.

Currently, `flexible_freeze` will not respond to a CTRL-C until the current vacuum is done.  If you need to halt `flexible_freeze` before then, we recommend using pg_cancel_backend() from the Postgres command line.  This will cause flexible_freeze to error out and exit.

Running the tests
-----------------

The test suite runs the script against a throwaway PostgreSQL cluster that it creates in a temporary directory and removes afterwards.  It needs `initdb` and `pg_ctl`, found through the `PG_BINDIR` environment variable, `PATH`, or `pg_config --bindir`.  It never connects to any other server.

    pip install -r requirements-dev.txt
    pytest
    ruff check tests
    ruff check --select E9,F63,F7,F82 scripts

The same checks run in GitHub Actions against PostgreSQL 14 through 18 (`.github/workflows/ci.yml`).  The workflow is started manually from the Actions tab.


