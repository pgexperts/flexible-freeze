flexible-freeze
===============

Flexible freeze scripts for managing off-hours vacuuming and freezing of PostgreSQL databases.

The directory /scripts/ contains standalone scripts to manage freezing and vacuuming.

Eventually this repository will have a more sophisticated always-on service which does opportunistic freezing.  But not today. ;-)

/scripts/flexible_freeze.py
---------------------------

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE
runs on your database during known slow traffic periods. Takes a
timeout so that it won't overrun your slow traffic period.

Requires: psycopg2
Requires: argparse (which comes preinstalled with Python 2.7 and up)
Requires: PostgreSQL 9.0 or later

    usage: flexible_freeze.py [-h] [-m RUN_MIN] [-s MINSIZEMB] [-d DBLIST] [-T TABLES_TO_EXCLUDE]
                              [--exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE] [--no-freeze] [--no-analyze] [--vacuum]
                              [--pause PAUSE_TIME] [--freezeage FREEZEAGE] [--costdelay COSTDELAY] [--costlimit COSTLIMIT] [-t]
                              [--enforce-time] [-l LOGFILE] [-v] [--debug] [-U DBUSER] [-H DBHOST] [-p DBPORT] [-w DBPASS] [-st TABLE]

    optional arguments:
      -h, --help            show this help message and exit
      -m RUN_MIN, --minutes RUN_MIN
                            Number of minutes to run before halting. Defaults to 2 hours
      -s MINSIZEMB, --minsizemb MINSIZEMB
                            Minimum table size to vacuum/freeze (in MB). Default is 0.
      -d DBLIST, --databases DBLIST
                            Comma-separated list of databases to vacuum, if not all of them
      -T TABLES_TO_EXCLUDE, --exclude-table TABLES_TO_EXCLUDE
                            Exclude any table with this name (in any database). You can pass this option multiple times to exclude
                            multiple tables.
      --exclude-table-in-database EXCLUDE_TABLE_IN_DATABASE
                            Argument is of form 'DATABASENAME.TABLENAME' exclude the named table, but only when processing the named
                            database. You can pass this option multiple times.
      --no-freeze           Do VACUUM ANALYZE instead of VACUUM ANALYZE FREEZE
      --no-analyze          Do not do an ANALYZE as part of the VACUUM operation
      --vacuum              Do VACUUM ANALYZE instead of VACUUM ANALYZE FREEZE (deprecated option; use --no-freeze instead)
      --pause PAUSE_TIME    seconds to pause between vacuums. Default is 10.
      --freezeage FREEZEAGE
                            minimum age for freezing. Default 10m XIDs
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
                            database password
      -st TABLE, --table TABLE
                            only process specified table

Notes:

The minutes time limit is normally only enforced at the start of vacuuming each table, allowing vacuums to continue past the end of the time window.  If you set --enforce-time, however, it uses statement_timeout to terminate a running vacuum at the end of the time window (plus 30 seconds grace period).

Normally flexible_freeze.py does VACUUM FREEZE, starting with the tables with the oldest transaction IDs.  If you set --vacuum, though, it will instead do VACUUM ANALYZE, starting with the tables with the most dead rows.  If you are doing both, do the FREEZE first.

The database user supplied is expected to have permissions on all tables (e.g. a superuser).  If they do not, flexible freeze will error out.

Currently, flexible_freeze will not respond to a CTRL-C until the current vacuum is done.  If you need to halt flexible_freeze before then, we recommend using pg_cancel_backend() from the Postgres command line.  This will cause flexible_freeze to error out and exit.


