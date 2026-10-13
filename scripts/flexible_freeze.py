#!/usr/bin/env python3
'''Flexible Freeze script for PostgreSQL databases
Version 0.6
(c) 2014-2021 PostgreSQL Experts Inc.
Licensed under The PostgreSQL License

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE runs
on your database during known slow traffic periods.  If doing both
vacuum freezes and vacuum analyzes, do the freezes first.

Takes a timeout so that it won't overrun your slow traffic period.
Note that this is the time to START a vacuum, so a large table
may still overrun the vacuum period, unless you use the --enforce-time switch.
'''

import time
import sys
import signal
import argparse
import psycopg2
import datetime

def timestamp():
    now = time.time()
    return time.strftime("%Y-%m-%d %H:%M:%S %Z")

if sys.hexversion < 0x3060000:
    print("At least Python 3.6 required; you have %s" % sys.version, file=sys.stderr)
    exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--minutes", dest="run_min",
                    type=int, default=120,
                    help="Number of minutes to run before halting.  Defaults to 2 hours")
parser.add_argument("-s", "--minsizemb", dest="minsizemb",
                    type=int, default=0,
                    help="Minimum table size to vacuum/freeze (in MB).  Default is 0.")
parser.add_argument("-d", "--databases", dest="dblist",
                    help="Comma-separated list of databases to vacuum, if not all of them")
parser.add_argument("-T", "--exclude-table", action="append", dest="tables_to_exclude",
                    help="Exclude any table with this name (in any database). You can pass this option multiple times to exclude multiple tables.")
parser.add_argument("--exclude-table-in-database", action="append", dest="exclude_table_in_database",
                    help="Argument is of form 'DATABASENAME.TABLENAME' exclude the named table, but only when processing the named database. You can pass this option multiple times.")
parser.add_argument("--no-freeze", dest="skip_freeze", action="store_true",
                    help="Do VACUUM ANALYZE instead of VACUUM FREEZE ANALYZE")
parser.add_argument("--no-analyze", dest="skip_analyze", action="store_true",
                    help="Do not do an ANALYZE as part of the VACUUM operation")
parser.add_argument("--vacuum", dest="vacuum", action="store_true",
                    help="Do VACUUM ANALYZE instead of VACUUM FREEZE ANALYZE (deprecated option; use --no-freeze instead)")
parser.add_argument("--pause", dest="pause_time", type=int, default=10,                    
                    help="seconds to pause between vacuums.  Default is 10.")
parser.add_argument("--freezeage", dest="freezeage",
                    type=int, default=10000000,
                    help="minimum age for freezing.  Default 10m XIDs")
parser.add_argument("--costdelay", dest="costdelay", 
                    type=int, default = 20,
                    help="vacuum_cost_delay setting in ms.  Default 20")
parser.add_argument("--costlimit", dest="costlimit",
                    type=int, default = 2000,
                    help="vacuum_cost_limit setting.  Default 2000")
parser.add_argument("-t", "--print-timestamps", action="store_true",
                    dest="print_timestamps")
parser.add_argument("--enforce-time", dest="enforcetime", action="store_true",
                    help="enforce time limit by terminating vacuum")
parser.add_argument("-l", "--log", dest="logfile")
parser.add_argument("-v", "--verbose", action="store_true",
                    dest="verbose")
parser.add_argument("--debug", action="store_true",
                    dest="debug")
parser.add_argument("-U", "--user", dest="dbuser",
                  help="database user")
parser.add_argument("-H", "--host", dest="dbhost",
                  help="database hostname")
parser.add_argument("-p", "--port", dest="dbport",
                  help="database port")
parser.add_argument("-w", "--password", dest="dbpass",
                  help="database password")
parser.add_argument("-st", "--table", dest="table",
                  help="only process specified  table", default=False)

args = parser.parse_args()

def debug_print(some_message):
    if args.debug:
        print(('DEBUG (%s): ' % timestamp()) + some_message, file=sys.stderr)

def verbose_print(some_message):
    if args.verbose:
        return _print(some_message)

def _print(some_message):
    if args.print_timestamps:
        print("{timestamp}: {some_message}".format(timestamp=timestamp(), some_message=some_message))
    else:
        print(some_message)
    sys.stdout.flush()
    return True

def dbconnect(dbname, dbuser, dbhost, dbport, dbpass):

    if not dbname:
        _print("ERROR: a target database is required.")
        return None

    # Pass each value to libpq as a parameter rather than pasting it into a
    # key=value connection string.  Database names come from pg_database when
    # -d is not given, so they are not under the operator's control; a value
    # carrying whitespace, '=' or a backslash would otherwise be reparsed as
    # further connection keywords or silently mangled.  None is passed for an
    # option left unset so libpq applies its own default.
    conn = psycopg2.connect(dbname=dbname, host=dbhost, user=dbuser,
                            password=dbpass, port=dbport,
                            application_name='flexible_freeze')
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Resolve catalog names through pg_catalog only, by running every session
    # with an empty search_path -- the same measure vacuumdb takes.  An object
    # planted in a schema on the effective search_path must not be chosen over
    # the catalog object and executed as the connecting role.  Accepted
    # trade-off: on PostgreSQL 16 and earlier, ANALYZE of a table whose
    # expression index depends on search_path can fail under the empty path.
    cur = conn.cursor()
    cur.execute("SELECT pg_catalog.set_config('search_path', '', false)")
    cur.close()

    return conn

def signal_handler(signal, frame):
    _print('exiting due to user interrupt')
    if conn:
        try:
            conn.close()
        except:
            verbose_print('could not clean up db connections')
        
    sys.exit(0)

# Tables are carried around as rows of (regclass text, schema name, table name).
# The regclass text is what gets printed and what goes into the VACUUM
# statement, but it changes with search_path and quoting (orders, sales.orders,
# "Orders"), so a name given on the command line is also compared with the
# plain catalog names.  A name matches a table if it is:
#   - the regclass text, exactly as the script prints it; or
#   - the bare table name, which matches that table in every schema; or
#   - schema.table, written plain, without any SQL quoting.
# Used for --table, --exclude-table and --exclude-table-in-database alike.
def table_named_in(names, tabrow):
    regclass_text, nspname, relname = tabrow
    forms = (regclass_text, relname, "%s.%s" % (nspname, relname))
    return any(name in forms for name in names)

# startup debugging info

debug_print("python version: %s" % sys.version)
debug_print("psycopg2 version: %s" % psycopg2.__version__)
debug_print("argparse version: %s" % argparse.__version__)
debug_print("parameters: %s" % repr(args))

# process arguments that argparse can't handle completely on its own

database_table_map = {}
if args.exclude_table_in_database:
    for elem in args.exclude_table_in_database:
        # split on the first dot only, so that the table part can be
        # schema-qualified: DATABASE.TABLE or DATABASE.SCHEMA.TABLE
        parts = elem.split(".", 1)
        if len(parts) != 2:
            print("invalid argument '{arg}' to flag --exclude-table-in-database: argument must be of the form DATABASE.TABLE".format(arg=elem), file=sys.stderr)
            exit(2)
        else:
            dat = parts[0]
            tab = parts[1]
            if dat in database_table_map:
                database_table_map[dat].append(tab)
            else:
                database_table_map[dat] = [tab]
                exit

debug_print("database_table_map: {m}".format(m=database_table_map))

# set times
halt_time = time.time() + ( args.run_min * 60 )

# get set for user interrupt
conn = None
time_exit = None
signal.signal(signal.SIGINT, signal_handler)

# start logging to log file, if used
if args.logfile:
    try:
        sys.stdout = open(args.logfile, 'a')
    except Exception as ex:
        _print('could not open logfile: %s' % str(ex))
        sys.exit(1)

    _print('')
    _print('='*40)
    _print('flexible freeze started %s' % str(datetime.datetime.now()))
    verbose_print('arguments: %s' % str(args))

# do we have a database list?
# if not, connect to "postgres" database and get a list of non-system databases
if args.dblist is None:
    conn = None
    try:
        dbname = 'postgres'
        conn = dbconnect(dbname, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    except Exception as ex:
        _print("Could not list databases: connection to database {d} failed: {e}".format(d=dbname, e=str(ex)))
        sys.exit(1)

    cur = conn.cursor()
    cur.execute("""SELECT datname FROM pg_database
        WHERE datname NOT IN ('postgres','template1','template0')
        ORDER BY age(datfrozenxid) DESC""")
    dblist = []
    for dbname in cur:
        dblist.append(dbname[0])

    conn.close()
    if not dblist:
        _print("no databases to vacuum, aborting")
        sys.exit(1)
else:
    dblist = args.dblist.split(',')

verbose_print("Flexible Freeze run starting")
n_dbs = len(dblist)
verbose_print("Processing {n} database{pl} (list of databases is {l})".format(n = n_dbs, l = ', '.join(dblist), pl = 's' if n_dbs > 1 else ''))

# connect to each database
time_exit = False
tabcount = 0
dbcount = 0
timeout_secs = 0

for db in dblist:
    verbose_print("working on database {0}".format(db))
    if time_exit:
        break
    else:
        dbcount += 1
    conn = None
    try:
        conn = dbconnect(db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    except Exception as err:
        _print("skipping database {d} (couldn't connect: {e})".format(d=db, e=str(err)))
        continue

    cur = conn.cursor()
    cur.execute("SET vacuum_cost_delay = {0}".format(args.costdelay))
    cur.execute("SET vacuum_cost_limit = {0}".format(args.costlimit))
    
    # if vacuuming, get list of top tables to vacuum
    if args.skip_freeze:
        tabquery = """WITH deadrow_tables AS (
                SELECT relid::regclass as full_table_name,
                    schemaname as nspname, relname,
                    n_dead_tup::numeric / NULLIF(n_dead_tup + n_live_tup, 0) as dead_pct,
                    pg_relation_size(relid) as table_bytes
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 100
                AND ( (now() - last_autovacuum) > INTERVAL '1 hour'
                    OR last_autovacuum IS NULL )
                AND ( (now() - last_vacuum) > INTERVAL '1 hour'
                    OR last_vacuum IS NULL )
            )
            SELECT full_table_name, nspname, relname
            FROM deadrow_tables
            WHERE dead_pct > 0.05
            AND table_bytes >= {0} * 2 ^ 20
            ORDER BY dead_pct DESC, table_bytes DESC;""".format(args.minsizemb)
    else:
    # if freezing, get list of top tables to freeze
    # includes TOAST tables in case the toast table has older rows
        tabquery = """WITH tabfreeze AS (
                SELECT pg_class.oid::regclass AS full_table_name,
                nspname, pg_class.relname,
                greatest(age(pg_class.relfrozenxid), age(toast.relfrozenxid)) as freeze_age,
                pg_relation_size(pg_class.oid) as table_bytes
            FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                LEFT OUTER JOIN pg_class as toast
                    ON pg_class.reltoastrelid = toast.oid
            WHERE nspname not in ('pg_catalog', 'information_schema')
                AND nspname NOT LIKE 'pg_temp%'
                AND pg_class.relkind = 'r'
            )
            SELECT full_table_name, nspname, relname
            FROM tabfreeze
            WHERE freeze_age > {0}
            AND table_bytes >= {1} * 2 ^ 20
            ORDER BY freeze_age DESC, table_bytes DESC
            LIMIT 1000;""".format(args.freezeage, args.minsizemb)

    cur.execute(tabquery)
    verbose_print("getting list of tables")

    # rows of (regclass text, schema name, table name); see table_named_in()
    tablist = cur.fetchall()

    #if only one table is desired
    if args.table:
      # a bare name can match in more than one schema; all of them are kept
      tablist = [tabrow for tabrow in tablist if table_named_in([args.table], tabrow)]
      if not tablist:
         print("specified single table {0} was not found (or excluded) in database {1}".format(args.table, db))
         continue

    # for each table in list
    for tabrow in tablist:
        # the regclass text: used for all output and as the VACUUM target
        table = tabrow[0]
        if db in database_table_map and table_named_in(database_table_map[db], tabrow):
            debug_print("skipping table {t} in database {d} per --exclude-table-in-database argument".format(t=table, d=db))
            continue
    # check time; if overtime, exit
        elif args.tables_to_exclude and table_named_in(args.tables_to_exclude, tabrow):
            verbose_print(
                "skipping table {t} per --exclude-table argument".format(t=table))
            continue
        else:
            verbose_print("processing table {t}".format(t=table))

        if time.time() >= halt_time:
            verbose_print("Reached time limit.  Exiting.")
            time_exit = True
            break
        else:
            tabcount += 1
            # figure out statement_timeout
            if args.enforcetime:
                timeout_secs = int(halt_time - time.time()) + 30
                timeout_query = """SET statement_timeout = '%ss'""" % timeout_secs
            
    # if not, vacuum or freeze
        exquery = "VACUUM "
        if not args.skip_freeze:
            exquery += "FREEZE "
        if not args.skip_analyze:
            exquery += "ANALYZE "
        
        # The regclass text is already valid SQL for this session: the server
        # schema-qualifies it when the schema is not on the search_path and
        # double-quotes each part that needs it.  It must go in unmodified;
        # wrapping it in quotes again turns sales.orders into a single
        # (nonexistent) name and "Orders" into an invalid identifier.
        exquery += table

        verbose_print("%s in database %s" % (exquery, db,))
        excur = conn.cursor()

        try:
            if args.enforcetime:
                excur.execute(timeout_query)
            else:
                excur.execute("SET statement_timeout = 0")
                
            excur.execute(exquery)
        except Exception as ex:
            _print("VACUUMing %s failed." % table)
            _print(str(ex))
            if time.time() >= halt_time:
                verbose_print("halted flexible_freeze due to enforced time limit")
            else:
                _print("VACUUMING %s failed." % table[0])
                _print(str(ex))
            sys.exit(1)

        time.sleep(args.pause_time)

conn.close()

# did we get through all tables?
# exit, report results
if not time_exit:
    _print("All tables vacuumed.")
    verbose_print("%d tables in %d databases" % (tabcount, dbcount))
else:
    _print("Vacuuming halted due to timeout")
    verbose_print("after vacuuming %d tables in %d databases" % (tabcount, dbcount,))

verbose_print("Flexible Freeze run complete")
sys.exit(0)
