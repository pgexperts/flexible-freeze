flexible-freeze
===============

Flexible freeze scripts for managing off-hours vacuuming and freezing of PostgreSQL databases.


/scripts/flexible_freeze.py
---------------------------

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE
runs on your database during known slow traffic periods. Takes a
timeout so that it won't overrun your slow traffic period.

Requires Python 3.11 or later (the script checks).  The test suite and CI run on Python 3.13.

Requires PostgreSQL 12 or later: every `VACUUM` is sent with `SKIP_LOCKED`, which older versions do not have.  CI tests PostgreSQL 14 through 18; versions 12 and 13 are supported but not tested.

Requires psycopg2

    usage: flexible_freeze.py [-h] [-m RUN_MIN] [-s MINSIZEMB] [-d DBLIST] [-T TABLES_TO_EXCLUDE]
                              [--exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE] [--no-freeze] [--no-analyze] [--vacuum]
                              [--pause PAUSE_TIME] [--freezeage FREEZEAGE] [--costdelay COSTDELAY] [--costlimit COSTLIMIT] [-t]
                              [--enforce-time] [--lock-timeout LOCK_TIMEOUT] [--no-advisory-lock] [-l LOGFILE] [-v] [--debug]
                              [-U DBUSER] [-H DBHOST] [-p DBPORT] [-w DBPASS] [--table TABLE]

    options:
      -h, --help            show this help message and exit
      -m RUN_MIN, --minutes RUN_MIN
                            Number of minutes to run before halting. Defaults to 2 hours
      -s MINSIZEMB, --minsizemb MINSIZEMB
                            Minimum table size to vacuum/freeze (in MB). The size is the table's data including its TOAST table,
                            without its indexes. Default is 0.
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
                            freeze tables older than this many XIDs. A table's age is the greatest of its transaction ID age and its
                            MultiXact ID age, and those of its TOAST table; a table whose age is exactly this is not selected.
                            Default 10m XIDs. Ignored with --no-freeze
      --costdelay COSTDELAY
                            vacuum_cost_delay setting in ms. Default 20
      --costlimit COSTLIMIT
                            vacuum_cost_limit setting. Default 2000
      -t, --print-timestamps
      --enforce-time        enforce time limit by terminating vacuum
      --lock-timeout LOCK_TIMEOUT
                            seconds to wait for a lock before giving up (set as the session's lock_timeout). A database whose table
                            list cannot be read in that time is skipped, a table whose VACUUM runs into it is reported as failed,
                            and the run goes on. 0 means wait without limit. Default is 10.
      --no-advisory-lock    Do not guard against overlapping runs. By default a run takes an advisory lock in each database while it
                            works there, and skips a database in which another run holds that lock.
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
                            database password. Visible to other local users in the process list for the whole run; prefer
                            PGPASSWORD, a .pgpass file or PGSERVICE
      --table TABLE, -st TABLE
                            only process tables with this name: a bare name matches in every schema, SCHEMA.TABLE only that table.
                            The table is vacuumed whatever its age, size or dead rows: --freezeage, --minsizemb and the --no-freeze
                            thresholds do not apply to it; exclusions still do. -st is an older spelling of this option

Notes:

The minutes time limit is normally only enforced at the start of vacuuming each table, allowing vacuums to continue past the end of the time window.  If you set `--enforce-time`, however, it uses statement_timeout to terminate a running vacuum at the end of the time window (plus 30 seconds grace period).  A vacuum terminated that way is not a failure: the run ends as it does for any other stop at the time limit, and the summary names the table whose vacuum was cancelled.  Pages that vacuum had already frozen stay frozen, but the table's `relfrozenxid` is not advanced, so it will be selected first again on the next run, so a table that needs longer than the time window to vacuum needs a longer window.

`--pause` is a pause between vacuums.  There is none after the last table, and a pause never runs past the end of the time window.

If you do not specify `--enforce-time`, `flexible_freeze` will issue a `SET statement_timeout = 0` statement before each operation
to avoid cancellation by a system- or role-wide `statement_timeout` value.

`flexible_freeze` does not wait for a table that another session has locked.  Each statement is sent as `VACUUM (FREEZE, ANALYZE, SKIP_LOCKED)` (without `FREEZE` or `ANALYZE` according to `--no-freeze` and `--no-analyze`), so a table held by an anti-wraparound autovacuum, a `CREATE INDEX` or anything else that conflicts with `VACUUM` is passed over at once.  The server reports that with a warning, and the table is then treated like any other the server declined to vacuum: its message is printed, the table is listed in the summary as not vacuumed, and the exit status is 1.  The table is selected again on the next run.

Every other wait for a lock is limited by `--lock-timeout`, ten seconds unless you say otherwise.  That matters most for the query that lists the tables: it asks for the size of each candidate, and so waits behind a pending `ACCESS EXCLUSIVE` lock (an `ALTER TABLE`, a `TRUNCATE`, a `VACUUM FULL`) on any one of them.  A database whose tables cannot be listed in that time is skipped and listed in the summary.  A `VACUUM` that has its table but then has to wait for some other lock for that long is reported as a failed table.  `flexible_freeze` sets `lock_timeout` in every database it works on, whatever the value, so `--lock-timeout 0` means no limit even where the database or the role has a `lock_timeout` of its own.  The timeout measures time spent waiting for a lock, not time spent vacuuming.

Two runs do not work on the same database at the same time.  While it works in a database, a run holds the advisory lock with the key `7380385457142659685` there (the ASCII bytes of `flexfrze` as a 64-bit integer; in `pg_locks`, `locktype = 'advisory'`, `classid = 1718379896`, `objid = 1718778469`, `objsubid = 1`).  A run that finds the lock taken, because an earlier run is still going or a second one was started by mistake, skips that database, says so, goes on to the databases that are free, and exits with status 1.  The lock is per database because PostgreSQL's advisory locks are: runs over different databases do not get in each other's way, and no single lock could cover a cluster.  It is released when the run's session in that database ends, so a run that was killed leaves nothing to clean up; one killed in the middle of a vacuum keeps the lock until the server has finished that vacuum.  `--no-advisory-lock` turns all of this off, for that run only: it neither takes the lock nor looks for it.  Keep the key out of your own applications' use of advisory locks.

Normally flexible_freeze.py does VACUUM FREEZE ANALYZE, starting with the oldest tables.  If you set `--no-freeze`, though, it will instead do VACUUM ANALYZE, starting with the tables with the most dead rows.  If you are doing both, do the FREEZE first.

When freezing, a table's age is the greatest of four numbers: its transaction ID age (`age(relfrozenxid)`), its MultiXact ID age (`mxid_age(relminmxid)`), and the same two for its TOAST table.  Each of them can force an anti-wraparound vacuum on its own.  Ordinary tables and materialized views are frozen, in every schema except `pg_catalog` and `information_schema`; there is no limit on how many tables a run selects, other than the time window.

`--freezeage` selects tables whose age, as just defined, is older than the value given; a table whose age is exactly that value is not selected, so even `--freezeage 0` does not select a table whose age is 0, such as one frozen a moment ago.  The one value is compared with both the transaction ID age and the MultiXact ID age.  It applies to freezing only and is ignored with `--no-freeze`.  In `--no-freeze` mode the tables are chosen by fixed thresholds that cannot be changed from the command line: more than 100 dead rows, more than 5% of the table's rows dead, and no vacuum, manual or by autovacuum, in the last hour.  `--vacuum` is a deprecated spelling of `--no-freeze`.

`--minsizemb` goes by `pg_table_size()`: the table's own data together with its TOAST table, free space map and visibility map, and without its indexes.  A table with a small main part and a large TOAST table is therefore as large as its TOAST table makes it.  This is the same in both modes.

If no `-d` is given, `flexible_freeze` works on every database that accepts connections and is not a template (`datallowconn` and `NOT datistemplate` in `pg_database`), oldest `datfrozenxid` first.  That includes the `postgres` database.  It connects to `postgres` to read the list.

Limitations: partitioned tables themselves are never selected, only their partitions.  Autovacuum does not analyze a partitioned table either, so the statistics kept for the parent as a whole go stale unless you run `ANALYZE` on it yourself.  Foreign tables are not selected, and neither are temporary tables, which only the session that owns them can vacuum.

`--minutes`, `--minsizemb`, `--pause`, `--freezeage`, `--costdelay`, `--costlimit` and `--lock-timeout` take whole numbers and may not be negative.  Empty entries in the `-d` list are ignored, so a doubled or trailing comma is harmless, but a `-d` that names no database at all is an error.

You can specify `--no-analyze` to skip the ANALYZE step. 

Table names given to `-T`, `--exclude-table-in-database` and `--table` can be written three ways: a bare table name (`orders`), which matches a table of that name in every schema; `schema.table` (`sales.orders`), written plain without SQL quoting, which matches only that table; or the name exactly as `flexible_freeze` prints it (for example `public."Orders"`).  Names are case-sensitive.

`--table` does not go through the selection: the named table is vacuumed whatever its age, its size or its dead rows, in either mode, provided it is an ordinary table or a materialized view.  `-T` and `--exclude-table-in-database` still apply to it, and an excluded table is reported as excluded.  `--table` can also name a system catalog (`--table pg_catalog.pg_statistic`), which a run without `--table` never selects.  When the name is in none of the schemas of a database, `flexible_freeze` goes on to the next database, and with `-v` says that the table does not exist there.  That is an error (exit status 1) only if the table was found in none of the databases of the run; the summary then lists the databases that were looked in.

`flexible_freeze` runs every session with an empty `search_path`, as `vacuumdb` does, so table names in its output are schema-qualified (`public.orders`).  The one exception is a table in `pg_catalog`, which can only come up when `--table` names it: PostgreSQL searches that schema whatever the `search_path`, so such a table is printed, and vacuumed, by its bare name (`pg_statistic`).  On PostgreSQL 16 and earlier, this means the ANALYZE of a table with an expression index can fail if the index's function depends on `search_path`; give such functions an explicit `SET search_path`, or schema-qualify the names they use.

Connection settings other than host, port, user and password are taken from the standard libpq environment variables (`PGSSLMODE`, `PGSERVICE`, `PGCONNECT_TIMEOUT` and so on).  The `-H`, `-U`, `-p`, `-w` and `-d` values are passed to the server as given; they cannot carry additional connection options.  The connection timeout (`PGCONNECT_TIMEOUT`, or `connect_timeout` in a service file) limits each connection attempt as a whole: if `-H` lists several hosts, a host that does not answer uses up the timeout instead of being passed over for the next one.

A mistyped `-T` or `--exclude-table-in-database` value excludes nothing.  With `-v`, the end of the run lists the exclusion values that matched none of the selected tables.  That is not an error: a correct value also matches nothing when its table was not selected for vacuuming in that run.

The database user supplied is expected to have permissions on all tables (e.g. a superuser).  The server does not raise an error for a table the user may not vacuum: it skips the table with a warning.  `flexible_freeze` treats any warning the server raises during a table's VACUUM as "not vacuumed": it prints the server's message, lists the table in the summary, and exits with status 1.

One table failing does not end the run.  The failure is printed, the run continues with the next table, and the summary at the end lists every table that failed.  The same goes for a database that cannot be connected to, whose tables cannot be listed, or in which another run holds the advisory lock: it is skipped and listed in the summary.  If the connection to a database is lost during a run, the rest of that database is skipped; `flexible_freeze` does not reconnect.

Where the output goes:

    progress and information    standard output; with -l, the log file instead
    reports of a failure        standard error; with -l, the log file as well
    --debug output              standard error
    command-line errors         standard error

A report of a failure is any line about something that by itself makes the exit status other than 0: a table that failed or that the server declined to vacuum, a database that was skipped or left unfinished, a `--table` name found in no database, a run that could not start, a run stopped by a signal, and the lines of the closing summary that list those.  Everything else is progress or information and stays off standard error: the closing `All tables vacuumed.`, everything `-v` adds, the note that a table named with `--table` is excluded, and a stop at the time limit, which is how a run is meant to end.

The split is made for schedulers such as cron, which mail what a job writes to standard error.  A run with `-l` writes nothing to standard output, writes its failures to standard error, and keeps the whole run, failures included and in order, in the log file.  A run without `-l` writes a failure to standard error only; it is not repeated on standard output.  A run in which nothing went wrong writes nothing to standard error, with or without `-l`, unless `--debug` is given.

The log file is appended to, never truncated.  Each run starts its part of it with a blank line, a rule and a `flexible freeze started` line with the time; that header is written only to a log file.  Lines are written out as they are produced, and the file is closed when the script ends, a stop on SIGINT or SIGTERM included; a process that is killed outright (SIGKILL) closes nothing, and loses nothing that was written.  If standard error is itself sent to the log file (`-l FILE 2>>FILE`), each failure is in it twice.  A failure report that cannot be written to standard error, because whatever was reading it has gone away, is still written to the log file; the run goes on, writes nothing more to standard error, and ends with the exit status it would have had.  If the file cannot be opened, the run does not start: the error is on standard error and the exit status is 1.  Errors in the command line are found before the log file is opened and are never in it.

`-t` puts the time in front of each message (once, if the message runs to several lines), failure reports included; the copy of a report in the log file carries the same time as the one on standard error.  A password given with `-w` is shown as `********` wherever the arguments are printed (`--debug`, and the log file with `-v`).

Exit status:

    0   every selected table was vacuumed, or the run stopped at the time limit
    1   at least one table failed or was not vacuumed, a database was skipped
        or left unfinished, or --table matched no table in any database; also
        when the run could not start (no databases, database list or logfile
        unavailable)
    2   invalid command-line arguments
    130 stopped by SIGINT (CTRL-C)
    143 stopped by SIGTERM

130 and 143 take precedence over 0 and 1: a run that was stopped by a signal exits with that status whatever else happened in it.

libpq's `PGPASSWORD`, `~/.pgpass` and `PGSERVICE` are honoured, so `-w` is not needed to authenticate.  A password given with `-w` is visible to other local users in the process list for the whole run; prefer one of those instead.

CTRL-C (SIGINT) stops a run, and so does SIGTERM, which is what cron, systemd and `kill` send.  `flexible_freeze` cancels the vacuum in progress on the server, starts no further table or database, and prints its usual summary, which says that the run was interrupted and names the table whose vacuum was cut short.  That table is not counted as failed; as with a vacuum cancelled by `--enforce-time`, the table's `relfrozenxid` is not advanced (pages already frozen stay frozen) and it will be selected again on the next run.  A signal is also acted on while `flexible_freeze` is connecting, listing tables, or pausing between vacuums.

After a signal `flexible_freeze` waits for the server to confirm that the vacuum was cancelled.  If that does not come (the server is not responding, or something between the two drops the cancel request), a second signal ends the wait; so does a cancel request that cannot be delivered at all.  The summary then says that the vacuum was not confirmed cancelled and may still be running on the server; check `pg_stat_activity`.  SIGKILL cannot be handled: it ends `flexible_freeze` at once, and the vacuum it was running carries on in the server until it is done.

A vacuum cancelled from elsewhere, with `pg_cancel_backend()` for instance, does not stop the run: it is reported as a failed table and the run continues with the next one.

Running the tests
-----------------

The test suite runs the script against a throwaway PostgreSQL cluster that it creates in a temporary directory and removes afterwards.  It needs `initdb` and `pg_ctl`, found through the `PG_BINDIR` environment variable, `PATH`, or `pg_config --bindir`.  It never connects to any other server.

    pip install -r requirements-dev.txt
    pytest
    ruff check tests
    ruff check --select E9,F63,F7,F82 scripts

To measure the script's line coverage (it runs as a subprocess, so coverage.py starts itself in each run; needs `coverage` 7.13 or later from `requirements-dev.txt`):

    COVERAGE_PROCESS_START=$PWD/.coveragerc python -m pytest
    python -m coverage combine
    python -m coverage report

The same checks run in GitHub Actions against PostgreSQL 14 through 18 (`.github/workflows/ci.yml`), followed by the coverage run, which only reports and never fails the job.  The workflow is started manually from the Actions tab.  If a pytest run is killed outright, the next session stops and removes the throwaway cluster it left behind, provided the directory is the current user's and its marker is over an hour old.


