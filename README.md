flexible-freeze
===============

Flexible freeze scripts for managing off-hours vacuuming and freezing of PostgreSQL databases.


Installing
----------

The script needs Python 3.11 or newer (it checks) and psycopg2 2.9 or later, below 3.  Install the driver with

    pip install -r scripts/requirements.txt

which builds psycopg2 from source and so needs `pg_config` (from your PostgreSQL client development package) and a C compiler.  Where that is impractical, `pip install "psycopg2-binary>=2.9,<3"` installs a prebuilt copy of the same module as a convenience.  The psycopg2 documentation advises the source distribution for production use, because the binary package carries its own libpq and libssl, which upgrading the system's libraries does not update.  Install one of the two, not both.  The test suite uses `psycopg2-binary`; see "Running the tests", below.

The current version is 0.7 (`flexible_freeze.py --version` prints it); `CHANGELOG.md` lists what changed since 0.6, and `docs/adr/` records why.

/scripts/flexible_freeze.py
---------------------------

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE
runs on your database during known slow traffic periods. Takes a
timeout so that it won't overrun your slow traffic period.

Requires Python 3.11 or later (the script checks).  The test suite and CI run on Python 3.13, and CI also runs it on 3.11 against PostgreSQL 18.

Requires PostgreSQL 12 or later: every `VACUUM` is sent with `SKIP_LOCKED`, which older versions do not have.  CI tests PostgreSQL 14 through 18; versions 12 and 13 are supported but not tested.

    usage: flexible_freeze.py [-h] [-m RUN_MIN] [-s MINSIZEMB] [-d DBLIST] [-T TABLES_TO_EXCLUDE]
                              [--exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE] [--no-freeze] [--no-analyze] [--no-truncate]
                              [--vacuum] [--pause PAUSE_TIME] [--freezeage FREEZEAGE] [--system-catalogs] [--costdelay COSTDELAY]
                              [--costlimit COSTLIMIT] [-t] [--enforce-time] [--lock-timeout SECONDS] [--no-advisory-lock]
                              [-l LOGFILE] [-v] [--debug] [-U DBUSER] [-H DBHOST] [-p DBPORT] [-w DBPASS] [--table TABLE]
                              [-c COMMENT] [--version]

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
      --no-truncate         Do not truncate empty pages off the end of each table as part of the VACUUM operation (VACUUM's TRUNCATE
                            false), which avoids the ACCESS EXCLUSIVE lock that the truncation takes on the table
      --vacuum              Same as --no-freeze (deprecated spelling; use --no-freeze instead)
      --pause PAUSE_TIME    seconds to pause between vacuums. Default is 10.
      --freezeage FREEZEAGE
                            freeze tables older than this many XIDs. A table's age is the greatest of its transaction ID age and its
                            MultiXact ID age, and those of its TOAST table; a table whose age is exactly this is not selected.
                            Default 10m XIDs. Ignored with --no-freeze
      --system-catalogs     Also freeze the system catalogs (the tables in pg_catalog and information_schema), selected by age like
                            any other table; by default they are left to autovacuum. Not allowed with --no-freeze. Needs a role that
                            may vacuum them, normally a superuser
      --costdelay COSTDELAY
                            vacuum_cost_delay setting in ms. Default 20
      --costlimit COSTLIMIT
                            vacuum_cost_limit setting. Default 2000
      -t, --print-timestamps
                            prefix each line of output with the time it was printed
      --enforce-time        enforce time limit by terminating vacuum
      --lock-timeout SECONDS
                            seconds to wait for a lock before giving up (set as the session's lock_timeout). A database whose table
                            list cannot be read in that time is skipped, a table whose VACUUM runs into it is reported as failed,
                            and the run goes on. 0 means wait without limit. Default is 10.
      --no-advisory-lock    Do not guard against overlapping runs. By default a run takes an advisory lock in each database while it
                            works there, and skips a database in which another run holds that lock.
      -l LOGFILE, --log LOGFILE
                            append output to this file instead of standard output. A failure report goes to the file and to standard
                            error
      -v, --verbose         print progress as the run goes, including each database and table it works on and each table
                            that -T or --exclude-table-in-database leaves out
      --debug               print debugging output to standard error: the versions in use, the arguments, the --exclude-table-in-
                            database entries
      -U DBUSER, --user DBUSER
                            database user
      -H DBHOST, --host DBHOST
                            database hostname
      -p DBPORT, --port DBPORT
                            database port, from 1 to 65535; or a comma-separated list of ports, one for each host, when -H lists
                            several hosts
      -w DBPASS, --password DBPASS
                            database password. Visible to other local users in the process list for the whole run; prefer
                            PGPASSWORD, a .pgpass file or PGSERVICE
      --table TABLE, -st TABLE
                            only process tables with this name: a bare name matches in every schema, SCHEMA.TABLE only that table.
                            The table is vacuumed whatever its age, size or dead rows: --freezeage, --minsizemb and the --no-freeze
                            thresholds do not apply to it; exclusions still do. -st is an older spelling of this option
      -c COMMENT, --comment COMMENT
                            text to append as a comment, /* COMMENT */, to every statement sent to the server, so that monitoring
                            tools can recognise or ignore what this script runs. It may not contain /* or */, a NUL or a line break
      --version             print the version and exit

Notes:

The minutes time limit is normally only enforced at the start of vacuuming each table, allowing vacuums to continue past the end of the time window.  If you set `--enforce-time`, however, it uses statement_timeout to terminate a running vacuum at the end of the time window (plus 30 seconds grace period).  A vacuum terminated that way is not a failure: the run ends as it does for any other stop at the time limit, and the summary names the table whose vacuum was cancelled.  Pages that vacuum had already frozen stay frozen, but the table's `relfrozenxid` is not advanced, so it will be selected first again on the next run, so a table that needs longer than the time window to vacuum needs a longer window.  A table that never finishes inside the window is cancelled on every run that reaches it, and it is not skipped on its own; `-T` is the way to exempt it.

`--pause` is a pause between vacuums.  There is none after the last table, and a pause never runs past the end of the time window.

If you do not specify `--enforce-time`, `flexible_freeze` will issue a `SET statement_timeout = 0` statement before each operation
to avoid cancellation by a system- or role-wide `statement_timeout` value.

`flexible_freeze` does not wait for a table that another session has locked.  Each statement is sent as `VACUUM (FREEZE, ANALYZE, SKIP_LOCKED)` (without `FREEZE` or `ANALYZE` according to `--no-freeze` and `--no-analyze`, and with `TRUNCATE false` added for `--no-truncate`), so a table held by an anti-wraparound autovacuum, a `CREATE INDEX` or anything else that conflicts with `VACUUM` is passed over at once.  The server reports that with a warning, and the table is then treated like any other the server declined to vacuum: its message is printed, the table is listed in the summary as not vacuumed, and the exit status is 1.  The table is selected again on the next run.

A table's TOAST table is vacuumed along with it, and `SKIP_LOCKED` warns only about a relation the statement names, so a TOAST table that another session has locked (an autovacuum of the TOAST table, say) would be passed over without a word while its table counted as vacuumed.  So when the server is PostgreSQL 14 or later and the role the run connects as may use the schema `pg_toast`, which in practice means a superuser, each table that has a TOAST table is vacuumed with two statements: the table's, with `PROCESS_TOAST false` added, and then the TOAST table's, by its name (`pg_toast.pg_toast_NNNN`) and with the same options otherwise.  A locked TOAST table then draws the server's warning, which is printed after a line naming the table it belongs to (`VACUUMing the TOAST table of public.orders (pg_toast.pg_toast_16595) drew a message from the server; not counting public.orders as vacuumed.`), and the table is listed in the summary as not vacuumed, with exit status 1, as for a locked table.  The table's own pages have been vacuumed by then and its TOAST table's have not; in freeze mode the table is selected again on the next run, since its age includes its TOAST table's.  When the table itself is locked, its TOAST table is not tried.  The two statements are one table's vacuum: the time limit is checked before the first only.  With `--enforce-time` each statement gets a `statement_timeout` of the time then left in the window plus the 30-second grace, so the two together end within the window plus the grace; if the window has closed by the time the table's own statement is done, its TOAST table's is still sent, with the grace alone, and the pair can then end up to twice the grace after the window.  For any other run, as a role that is not a superuser (the table's owner, the database's owner and members of `pg_maintain` included: naming a TOAST table needs `USAGE` on `pg_toast`, which they do not have) or against PostgreSQL 12 or 13, each table gets its one statement as before, and there a locked TOAST table is still passed over in silence and its table counted as vacuumed.  The TOAST table's `relfrozenxid` does not move then, so its age keeps the table selected in freeze mode until a run gets to it.

Every other wait for a lock is limited by `--lock-timeout`, ten seconds unless you say otherwise.  That matters most for the query that lists the tables: it asks for the size of each candidate, and so waits behind a pending `ACCESS EXCLUSIVE` lock (an `ALTER TABLE`, a `TRUNCATE`, a `VACUUM FULL`) on any one of them.  A database whose tables cannot be listed in that time is skipped and listed in the summary.  A `VACUUM` that has its table but then has to wait for some other lock for that long is reported as a failed table.  The same limit applies to the query that reads the list of databases when no `-d` is given; if that one runs into it there is no list to work on, so the run does not start and the exit status is 1.  `flexible_freeze` sets `lock_timeout` in every session it opens, whatever the value, so `--lock-timeout 0` means no limit even where the database or the role has a `lock_timeout` of its own.  The timeout measures time spent waiting for a lock, not time spent vacuuming.

Two runs do not work on the same database at the same time.  While it works in a database, a run holds the advisory lock with the key `7380385457142659685` there (the ASCII bytes of `flexfrze` as a 64-bit integer; in `pg_locks`, `locktype = 'advisory'`, `classid = 1718379896`, `objid = 1718778469`, `objsubid = 1`).  A run that finds the lock taken, because an earlier run is still going or a second one was started by mistake, skips that database, says so, goes on to the databases that are free, and exits with status 1.  The lock is per database because PostgreSQL's advisory locks are: runs over different databases do not get in each other's way, and no single lock could cover a cluster.  It is released when the run's session in that database ends, so a run that was killed leaves nothing to clean up; one killed in the middle of a vacuum keeps the lock until the server has finished that vacuum.  `--no-advisory-lock` turns all of this off, for that run only: it neither takes the lock nor looks for it.  Keep the key out of your own applications' use of advisory locks.

Nothing checks who holds that lock.  An advisory lock needs no privilege, so any role that can connect to a database can take the key there, and every run then skips that database, with exit status 1, for as long as the lock is held.  A run that finds the lock taken therefore looks up who holds it (`pg_locks` joined to `pg_stat_activity`) and names each holder in the line that reports the skip: its process id, its role and its application name.  Any role can see those three for any session, so the lookup needs no extra privilege.  If the holder goes by the application name `flexible_freeze`, which every run sets, the line says that another flexible_freeze run is working on the database.  If it goes by any other name, the line says that the lock is held by a session that does not appear to be flexible_freeze, and that every run will skip the database until that session releases the lock or ends.  Act on the process id: look the session up in `pg_stat_activity`, and end it with `pg_terminate_backend()` if it should not be holding the lock.  The difference between the two lines rests on `application_name` alone, which every client sets for itself, so a session can pass for a run or a run for something else; take it as a hint.  The database is skipped either way, with the same exit status.  A holder with no process id is a prepared transaction: it is never a run, it keeps the lock through a restart until it is committed or rolled back (see `pg_prepared_xacts` and `ROLLBACK PREPARED`), and the line says so.  A role or application name that cannot be found (a session that ended as it was being looked up) is printed as not known, and a holder whose application name is not known is not said to be either a run or something else.  If the holder has let go by the time the run looks, the line says that the lock was held a moment ago.  If the lookup itself fails (a role that may not read `pg_locks`, a lock timeout), the line still says that the lock is held, and gives the reason its holder could not be determined.  The lock is not tried a second time in either case.  Where the lock cannot be trusted, `--no-advisory-lock` does without it, at a cost: nothing then stops two runs from working on the same database at the same time.

Normally flexible_freeze.py does VACUUM FREEZE ANALYZE, starting with the oldest tables.  If you set `--no-freeze`, though, it will instead do VACUUM ANALYZE, starting with the tables with the most dead rows.  If you are doing both, do the FREEZE first.

When freezing, a table's age is the greatest of four numbers: its transaction ID age (`age(relfrozenxid)`), its MultiXact ID age (`mxid_age(relminmxid)`), and the same two for its TOAST table.  Each of them can force an anti-wraparound vacuum on its own.  Ordinary tables and materialized views are frozen, in every schema except `pg_catalog` and `information_schema`; there is no limit on how many tables a run selects, other than the time window.

The system catalogs, which are the tables in those two schemas, are left to autovacuum by default.  A database whose oldest table is a catalog therefore keeps its `datfrozenxid` until autovacuum gets to that catalog, whatever `flexible_freeze` freezes.  `--system-catalogs` adds them to the freeze selection.  Nothing else about the selection changes, and a catalog is treated like any other table: it is selected only if it is older than `--freezeage` and at least `--minsizemb` in size, it takes its place in the order by age, `-T` and `--exclude-table-in-database` can leave it out, and the time window applies.  A shared catalog such as `pg_database` or `pg_authid` is selected in each database in which it is old enough: PostgreSQL records its age separately in every database, and a vacuum in one database does not change it in the others.

`--system-catalogs` applies to freezing only.  Together with `--no-freeze` (or `--vacuum`) it is an error in the command line (exit status 2), because that mode chooses its tables from `pg_stat_user_tables`, which does not list the catalogs.  With `--table` it is accepted and changes nothing, since `--table` does not go through the selection and can name a catalog as it is.

Run `--system-catalogs` as a superuser.  `flexible_freeze` does not check beforehand whether its role may vacuum the catalogs.  A role that may not gets a warning from the server for each catalog it is refused, and each is reported like any other table the server declined to vacuum: its message is printed, the table is listed in the summary as not vacuumed, and the exit status is 1.  The owner of a database may vacuum that database's own catalogs but not the shared ones; from PostgreSQL 17 a member of `pg_maintain` may vacuum them all.

`--freezeage` selects tables whose age, as just defined, is older than the value given; a table whose age is exactly that value is not selected, so even `--freezeage 0` does not select a table whose age is 0, such as one frozen a moment ago.  The one value is compared with both the transaction ID age and the MultiXact ID age.  It applies to freezing only and is ignored with `--no-freeze`.  In `--no-freeze` mode the tables are chosen by fixed thresholds that cannot be changed from the command line: more than 100 dead rows, more than 5% of the table's rows dead, and no vacuum, manual or by autovacuum, in the last hour.  `--vacuum` is a deprecated spelling of `--no-freeze`.

`--minsizemb` goes by `pg_table_size()`: the table's own data together with its TOAST table, free space map and visibility map, and without its indexes.  A table with a small main part and a large TOAST table is therefore as large as its TOAST table makes it.  This is the same in both modes.

If no `-d` is given, `flexible_freeze` works on every database that accepts connections and is not a template (`datallowconn` and `NOT datistemplate` in `pg_database`), the `postgres` database included, oldest first.  A database's age is the greater of its transaction ID age (`age(datfrozenxid)`) and its MultiXact ID age (`mxid_age(datminmxid)`), so a database that is behind on either one is reached early in a run that may run out of time.  To read that list, a run without `-d` first connects to the `postgres` database.  If that connection cannot be made, or the query for the list fails, the run does not start: the failure is reported like any other, on standard error and in the log file, and the exit status is 1.  With `-d` there is no list to read: the databases you name are worked on in the order you list them.

Limitations: partitioned tables themselves are never selected, only their partitions.  Autovacuum does not analyze a partitioned table either, so the statistics kept for the parent as a whole go stale unless you run `ANALYZE` on it yourself.  Foreign tables are not selected, and neither are temporary tables, which only the session that owns them can vacuum.

`--minutes`, `--minsizemb`, `--pause`, `--freezeage`, `--costdelay`, `--costlimit` and `--lock-timeout` take whole numbers and may not be negative; `--costlimit` must be at least 1, the smallest `vacuum_cost_limit` the server accepts.  Empty entries in the `-d` list are ignored, so a doubled or trailing comma is harmless, but a `-d` that names no database at all is an error.

You can specify `--no-analyze` to skip the ANALYZE step. 

`--no-truncate` adds `TRUNCATE false` to each `VACUUM`, in either mode.  A vacuum normally ends by cutting off the empty pages at the end of the table and returning them to the operating system, and takes an `ACCESS EXCLUSIVE` lock on the table to do it; with `--no-truncate` it leaves them, and never takes that lock.  The space stays allocated to the table and is used again by later rows.  Without `--no-truncate` the option is not sent at all, so a table whose `vacuum_truncate` storage parameter is off is still not truncated.

Table names given to `-T`, `--exclude-table-in-database` and `--table` can be written three ways: a bare table name (`orders`), which matches a table of that name in every schema; `schema.table` (`sales.orders`), written plain without SQL quoting, which matches only that table; or the name exactly as `flexible_freeze` prints it (for example `public."Orders"`).  Names are case-sensitive.

An empty or blank `--table` or `-T` value is an error in the command line (exit status 2); `--table` never means every table, and `-T` never silently excludes nothing.  A value with leading or trailing whitespace is refused the same way: it is not stripped, because names are matched exactly, so a table whose name really begins or ends with a space is written in its quoted form (`sales." orders"`).  A database whose name contains a dot cannot be named in `--exclude-table-in-database`, and a value with an empty part after the database (`db..t`, `db.schema.`) is refused.  A dot inside double quotes does not separate the parts, so `db.public."a..b"` names the table `a..b`; the table part is refused for surrounding whitespace too.  `--table` does not go through the selection: the named table is vacuumed whatever its age, its size or its dead rows, in either mode, provided it is an ordinary table or a materialized view.  `-T` and `--exclude-table-in-database` still apply to it, and an excluded table is reported as excluded.  `--table` can also name a system catalog (`--table pg_catalog.pg_statistic`), which a run without `--table` selects only with `--system-catalogs`.  When the name is in none of the schemas of a database, `flexible_freeze` goes on to the next database, and with `-v` says that the table does not exist there.  That is an error (exit status 1) only if the table was found in none of the databases of the run; the summary then lists the databases that were looked in.

If the name belongs to a partitioned table, a foreign table or a view, and to no table that can be vacuumed, `-v` prints for that database, instead of the line saying the table does not exist, one line for each such relation of that name (a bare name can match one in several schemas).  Each line names the relation the way table names are printed, schema-qualified (`sales.orders`; see below), and says that it exists, which of the three it is, and that `flexible_freeze` cannot vacuum it.  For a partitioned table the line adds that you can name its partitions with `--table` instead; the partitions are not vacuumed on the parent's behalf.  Nothing else about the run changes: that database still counts as one where the name matched no table, for the summary and for the exit status.  Any other kind of relation with the name (an index, a sequence) is reported as a table that does not exist.

`flexible_freeze` runs every session with an empty `search_path`, as `vacuumdb` does, so table names in its output are schema-qualified (`public.orders`).  The one exception is a table in `pg_catalog`, which can only come up when `--table` names it or `--system-catalogs` is given: PostgreSQL searches that schema whatever the `search_path`, so such a table is printed, and vacuumed, by its bare name (`pg_statistic`).  On PostgreSQL 16 and earlier, this means the ANALYZE of a table with an expression index can fail if the index's function depends on `search_path`; give such functions an explicit `SET search_path`, or schema-qualify the names they use.

Connection settings other than host, port, user and password are taken from the standard libpq environment variables (`PGSSLMODE`, `PGSERVICE`, `PGCONNECT_TIMEOUT` and so on).  The `-H`, `-U`, `-p`, `-w` and `-d` values are passed to the server as given; they cannot carry additional connection options.  The connection timeout (`PGCONNECT_TIMEOUT`, or `connect_timeout` in a service file) limits each connection attempt as a whole: if `-H` lists several hosts, a host that does not answer uses up the timeout instead of being passed over for the next one.  It must be a whole number of seconds (a negative one means no timeout), as for any libpq program; a value such as `10s` ends the run at the first connection with one message and exit status 2.

`-p` takes one port, or a comma-separated list of ports to go with a `-H` that lists several hosts (`-H db1,db2 -p 5432,6432`).  Each port must be a whole number from 1 to 65535.  An empty value, or an empty entry in the list (`5432,`), is an error in the command line (exit status 2); it is never read as the default port, so an unset variable in `-p "$PORT"` cannot send the run to the default server.  The value is passed to libpq as written, and it is libpq that matches the ports to the hosts when it connects: it takes one port for all the hosts or one port for each, and with a list of any other length every connection fails.

A mistyped `-T` or `--exclude-table-in-database` value excludes nothing.  With `-v`, the end of the run lists the exclusion values that matched none of the selected tables.  That is not an error: a correct value also matches nothing when its table was not selected for vacuuming in that run.

The database user supplied is expected to have permissions on all tables (e.g. a superuser).  The server does not raise an error for a table the user may not vacuum: it skips the table with a warning.  `flexible_freeze` treats any warning the server raises during a table's VACUUM as "not vacuumed": it prints the server's message, lists the table in the summary, and exits with status 1.  That includes a warning that is benign in itself, such as "oldest xmin is far in the past": a table whose VACUUM drew one counts as not vacuumed and makes the exit status 1, though the VACUUM ran.

One table failing does not end the run.  The failure is printed, the run continues with the next table, and the summary at the end lists every table that failed.  The same goes for a database that cannot be connected to, whose tables cannot be listed, or in which another run holds the advisory lock: it is skipped and listed in the summary.  If the connection to a database is lost during a run, the rest of that database is skipped; `flexible_freeze` does not reconnect.

Where the output goes:

    progress and information    standard output; with -l, the log file instead
    reports of a failure        standard error; with -l, the log file as well
    --debug output              standard error
    command-line errors         standard error

A report of a failure is any line about something that by itself makes the exit status other than 0: a table that failed or that the server declined to vacuum, a database that was skipped or left unfinished, a `--table` name found in no database, a run that could not start, a run stopped by a signal, and the lines of the closing summary that list those.  Everything else is progress or information and stays off standard error: the closing `All tables vacuumed.`, everything `-v` adds, the note that a table named with `--table` is excluded, and a stop at the time limit, which is how a run is meant to end.

The split is made for schedulers such as cron, which mail what a job writes to standard error.  A run with `-l` writes nothing to standard output, writes its failures to standard error, and keeps the whole run, failures included and in order, in the log file.  A run without `-l` writes a failure to standard error only; it is not repeated on standard output.  A run in which nothing went wrong writes nothing to standard error, with or without `-l`, unless `--debug` is given.

The log file is appended to, never truncated.  Each run starts its part of it with a blank line, a rule and a `flexible freeze started` line with the time; that header is written only to a log file.  Lines are written out as they are produced, and the file is closed when the script ends, a stop on SIGINT, SIGTERM or SIGHUP included; a process that is killed outright (SIGKILL) closes nothing, and loses nothing that was written.  If standard error is itself sent to the log file (`-l FILE 2>>FILE`), each failure is in it twice.  A failure report that cannot be written to standard error, because whatever was reading it has gone away, is still written to the log file; the run goes on, writes nothing more to standard error, and ends with the exit status it would have had.  If the file cannot be opened, the run does not start: the error is on standard error and the exit status is 1.  Errors in the command line are found before the log file is opened and are never in it.

`--comment TEXT` (or `-c TEXT`) appends ` /* TEXT */` to every statement `flexible_freeze` sends: the `VACUUM`s, and also the session settings, the advisory-lock call and the queries that list the databases and the tables.  The one statement without it is not `flexible_freeze`'s own: on a server whose `DateStyle` is not ISO, the database driver starts each session with a `SET datestyle`.  The comment is there so that monitoring tools which read comments in statements can pick out what this script runs, or ignore it; for example `-c application:flexible_freeze,pganalyze:no-alert`.  The text is sent as given, so it may not contain `/*` or `*/`, a NUL or a line break; such a value is an error in the command line (exit status 2) and nothing is sent.  It is not altered to make it fit.  With `-v`, the line printed for each table shows its `VACUUM` without the comment; the text is in the argument lists that `--debug` and a log file with `-v` print.

`-t` puts the time in front of each message (once, if the message runs to several lines), failure reports included; the copy of a report in the log file carries the same time as the one on standard error.  A password given with `-w` is shown as `********` wherever the arguments are printed (`--debug`, and the log file with `-v`).

Exit status:

    0   every selected table was vacuumed, or the run stopped at the time limit
    1   at least one table failed or was not vacuumed, a database was skipped
        or left unfinished, or --table matched no table in any database; also
        when the run could not start (no databases, database list or logfile
        unavailable)
    2   invalid command-line arguments, or a connect_timeout that is not a
        whole number of seconds
    129 stopped by SIGHUP
    130 stopped by SIGINT (CTRL-C)
    143 stopped by SIGTERM

129, 130 and 143 take precedence over 0 and 1: a run that was stopped by a signal exits with that status whatever else happened in it.

libpq's `PGPASSWORD`, `~/.pgpass` and `PGSERVICE` are honoured, so `-w` is not needed to authenticate.  A password given with `-w` is visible to other local users in the process list for the whole run; prefer one of those instead.

CTRL-C (SIGINT) stops a run, and so do SIGTERM, which is what cron, systemd and `kill` send, and SIGHUP, which is what a run gets when the terminal or the ssh session it was started from goes away.  `flexible_freeze` cancels the vacuum in progress on the server, starts no further table or database, and prints its usual summary, which says that the run was interrupted and names the table whose vacuum was cut short.  That table is not counted as failed; as with a vacuum cancelled by `--enforce-time`, the table's `relfrozenxid` is not advanced (pages already frozen stay frozen) and it will be selected again on the next run.  A signal is also acted on while `flexible_freeze` is connecting, listing databases or tables, or pausing between vacuums.

The one exception is a SIGHUP or SIGINT that was already being ignored when `flexible_freeze` started: it stays ignored, and does not stop that run.  A SIGHUP is ignored in this way by `nohup`, and a SIGINT by the shell for a background job (`command &`) in a script or other non-interactive shell.  SIGTERM always stops the run, and so does whichever of SIGINT and SIGHUP was not ignored.

After a signal `flexible_freeze` waits for the server to confirm that the vacuum was cancelled.  If that does not come (the server is not responding, or something between the two drops the cancel request), a second signal ends the wait; so does a cancel request that cannot be delivered at all.  The summary then says that the vacuum was not confirmed cancelled and may still be running on the server; check `pg_stat_activity`.  SIGKILL cannot be handled: it ends `flexible_freeze` at once, and the vacuum it was running carries on in the server until it is done.

A vacuum cancelled from elsewhere, with `pg_cancel_backend()` for instance, does not stop the run: it is reported as a failed table and the run continues with the next one.

Running the tests
-----------------

The test suite runs the script against a throwaway PostgreSQL cluster that it creates in a temporary directory and removes afterwards.  It needs `initdb` and `pg_ctl`, found through the `PG_BINDIR` environment variable, `PATH`, or `pg_config --bindir`.  It never connects to any other server.

    pip install -r requirements-dev.txt
    pytest
    ruff check scripts tests

To measure the script's line coverage (it runs as a subprocess, so coverage.py starts itself in each run; needs `coverage` 7.13 or later from `requirements-dev.txt`):

    COVERAGE_PROCESS_START=$PWD/.coveragerc python -m pytest
    python -m coverage combine
    python -m coverage report

The same checks run in GitHub Actions against PostgreSQL 14 through 18 on Python 3.13, and PostgreSQL 18 on Python 3.11 (`.github/workflows/ci.yml`), followed by the coverage run, which only reports and never fails the job.  The workflow is started manually from the Actions tab.  If a pytest run is killed outright, the next session stops and removes the throwaway cluster it left behind, provided the directory is the current user's and its marker is over an hour old.


