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

# argparse type for the options that are counts, sizes or durations.  A
# negative value is rejected for all of them.  For most it would surface far
# from the command line: as a ValueError from time.sleep() after the first
# table (--pause), as a server error from SET (--costdelay, --costlimit), or
# as a run that halts at once and still exits 0 (--minutes).  For --freezeage
# and --minsizemb it is at best an obscure way of asking for every table.
# Zero stays legal for all of them (-m 0 and --pause 0 are in use).  No upper
# bounds are applied here; the server enforces its own on the cost settings.
def non_negative_int(text):
    try:
        value = int(text)
    except ValueError:
        # the wording argparse itself uses for type=int
        raise argparse.ArgumentTypeError("invalid int value: %r" % text)
    if value < 0:
        raise argparse.ArgumentTypeError("must not be negative: %r" % text)
    return value

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--minutes", dest="run_min",
                    type=non_negative_int, default=120,
                    help="Number of minutes to run before halting.  Defaults to 2 hours")
parser.add_argument("-s", "--minsizemb", dest="minsizemb",
                    type=non_negative_int, default=0,
                    help="Minimum table size to vacuum/freeze (in MB).  Default is 0.")
parser.add_argument("-d", "--databases", dest="dblist",
                    help="Comma-separated list of databases to vacuum, if not all of them")
parser.add_argument("-T", "--exclude-table", action="append", dest="tables_to_exclude",
                    help="Exclude any table with this name (in any database).  A bare name matches in every schema; SCHEMA.TABLE matches only that table. You can pass this option multiple times to exclude multiple tables.")
parser.add_argument("--exclude-table-in-database", action="append", dest="exclude_table_in_database",
                    help="Argument is of form 'DATABASENAME.TABLENAME' or 'DATABASENAME.SCHEMANAME.TABLENAME': exclude the named table, but only when processing the named database. You can pass this option multiple times.")
# argparse %-formats help strings, hence the doubled percent sign.
parser.add_argument("--no-freeze", dest="skip_freeze", action="store_true",
                    help="Do VACUUM ANALYZE instead of VACUUM FREEZE ANALYZE.  Tables are then chosen by fixed thresholds instead of by age: more than 100 dead rows, more than 5%% of rows dead, and not vacuumed (manually or by autovacuum) in the last hour.  --freezeage is ignored.")
parser.add_argument("--no-analyze", dest="skip_analyze", action="store_true",
                    help="Do not do an ANALYZE as part of the VACUUM operation")
# Deprecated spelling of --no-freeze.  It must keep that option's dest: the
# rest of the script reads skip_freeze only, so a dest of its own would make
# this option a silent no-op and the run a much heavier VACUUM FREEZE.
parser.add_argument("--vacuum", dest="skip_freeze", action="store_true",
                    help="Same as --no-freeze (deprecated spelling; use --no-freeze instead)")
parser.add_argument("--pause", dest="pause_time", type=non_negative_int, default=10,
                    help="seconds to pause between vacuums.  Default is 10.")
parser.add_argument("--freezeage", dest="freezeage",
                    type=non_negative_int, default=10000000,
                    help="freeze tables older than this many XIDs; a table whose age is exactly this is not selected.  Default 10m XIDs.  Ignored with --no-freeze")
parser.add_argument("--costdelay", dest="costdelay", 
                    type=non_negative_int, default = 20,
                    help="vacuum_cost_delay setting in ms.  Default 20")
parser.add_argument("--costlimit", dest="costlimit",
                    type=non_negative_int, default = 2000,
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
parser.add_argument("-p", "--port", dest="dbport", type=int,
                  help="database port")
parser.add_argument("-w", "--password", dest="dbpass",
                  help="database password.  Visible to other local users in the "
                       "process list for the whole run; prefer PGPASSWORD, a "
                       ".pgpass file or PGSERVICE")
# --table is listed first so that it is the spelling shown in the usage line;
# -st is kept because existing invocations use it.
parser.add_argument("--table", "-st", dest="table",
                  help="only process tables with this name: a bare name matches in every schema, SCHEMA.TABLE only that table.  -st is an older spelling of this option")

args = parser.parse_args()

# The argument namespace is printed to the debug output and the logfile, both
# of which outlive the process and may be readable by others, so a password
# given with -w is replaced by a placeholder in a copy.  args itself is left
# alone: dbconnect() needs the real value.
def args_for_display():
    shown = argparse.Namespace(**vars(args))
    if shown.dbpass is not None:
        shown.dbpass = '********'
    return shown

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

# Returns an open connection or raises; it never returns None.  Callers
# decide what a failure means: fatal while listing the databases, a skipped
# database inside the run.
def dbconnect(dbname, dbuser, dbhost, dbport, dbpass):

    if not dbname:
        raise ValueError("a target database is required")

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

# What a run did and did not get done.  The loops below only record here;
# the closing summary and the exit status are worked out from this record,
# once, at the end of the run.  Nothing inside the loops may call sys.exit:
# that is what used to let one failed table drop every later table and
# database without a word.
#
# "The statement returned" is not taken to mean "the table was vacuumed":
# the server reports some tables it declined to vacuum with a WARNING and a
# successful return, so those are kept apart from tables whose VACUUM raised.
#
# Tables are recorded as (regclass text, database name).
class RunOutcome(object):
    def __init__(self):
        # Completed work only: databases whose table list was obtained, and
        # VACUUMs that returned without an error or a server message.
        self.databases_listed = 0
        self.tables_vacuumed = 0
        # VACUUM (or its statement_timeout setting) raised an error.
        self.failed_tables = []
        # VACUUM returned, but the server had something to say about it.
        self.unvacuumed_tables = []
        # Databases not worked through: could not connect, could not list
        # the tables, or lost the connection part-way.
        self.skipped_databases = []
        # Databases in which --table matched nothing.
        self.table_missing_in = []
        # The run stopped at the time limit.  With --enforce-time that can
        # be in the middle of a vacuum, which is then the cancelled table.
        self.time_limited = False
        self.cancelled_table = None

    # Exit status 1.  Stopping at the time limit is not in this list: it is
    # the designed way for a nightly run to end, and exits 0.
    def work_was_missed(self):
        return bool(self.failed_tables or self.unvacuumed_tables
                    or self.skipped_databases or self.table_missing_in)

def tables_for_display(tables):
    return ', '.join("{t} in database {d}".format(t=t, d=d) for t, d in tables)

# startup debugging info

debug_print("python version: %s" % sys.version)
debug_print("psycopg2 version: %s" % psycopg2.__version__)
debug_print("argparse version: %s" % argparse.__version__)
debug_print("parameters: %s" % repr(args_for_display()))

# process arguments that argparse can't handle completely on its own

database_table_map = {}
if args.exclude_table_in_database:
    for elem in args.exclude_table_in_database:
        # split on the first dot only, so that the table part can be
        # schema-qualified: DATABASE.TABLE or DATABASE.SCHEMA.TABLE
        parts = elem.split(".", 1)
        # An empty database or table part ('.t', 'db.') has the right shape
        # but can never match anything, so the table it was meant to protect
        # would be vacuumed; it is rejected like a value with no dot at all.
        if len(parts) != 2 or not parts[0] or not parts[1]:
            print("invalid argument '{arg}' to flag --exclude-table-in-database: argument must be of the form DATABASE.TABLE or DATABASE.SCHEMA.TABLE".format(arg=elem), file=sys.stderr)
            exit(2)
        else:
            dat = parts[0]
            tab = parts[1]
            if dat in database_table_map:
                database_table_map[dat].append(tab)
            else:
                database_table_map[dat] = [tab]

debug_print("database_table_map: {m}".format(m=database_table_map))

# -d: surrounding whitespace and empty entries ('a,,b', a trailing comma) are
# dropped here rather than passed on as database names, because an empty name
# is not something the connection code downstream can skip cleanly.  A -d that
# names no database at all is an error, not a request for every database:
# falling back to the full list would vacuum far more than was asked for.
named_dblist = None
if args.dblist is not None:
    named_dblist = [name.strip() for name in args.dblist.split(',') if name.strip()]
    if not named_dblist:
        print("invalid argument '{arg}' to flag --databases: argument must name at least one database".format(arg=args.dblist), file=sys.stderr)
        sys.exit(2)

# set times
halt_time = time.time() + ( args.run_min * 60 )

# get set for user interrupt
conn = None
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
    verbose_print('arguments: %s' % str(args_for_display()))

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
    dblist = named_dblist

verbose_print("Flexible Freeze run starting")
n_dbs = len(dblist)
verbose_print("Processing {n} database{pl} (list of databases is {l})".format(n = n_dbs, l = ', '.join(dblist), pl = 's' if n_dbs > 1 else ''))

# connect to each database
outcome = RunOutcome()
timeout_secs = 0

# --pause is a pause between vacuums, so it is taken before a vacuum that
# follows another one, never after one: whether a further table is coming is
# not known until the next database has been listed.
pause_pending = False

# Exclusion values that matched a selected table, as (database or None for
# -T, value); the rest are reported at the end of the run.
exclusions_matched = set()

for db in dblist:
    verbose_print("working on database {0}".format(db))
    if outcome.time_limited:
        break
    conn = None
    try:
        conn = dbconnect(db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    except Exception as err:
        _print("skipping database {d} (couldn't connect: {e})".format(d=db, e=str(err)))
        outcome.skipped_databases.append(db)
        continue

    # The one cursor for everything done in this database.
    cur = conn.cursor()

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

    # From here on the connection is closed on every way out of this
    # database: the skips below, the end of its table list, a stop at the
    # time limit, and an exit raised by the signal handler.
    try:
        try:
            cur.execute("SET vacuum_cost_delay = {0}".format(args.costdelay))
            cur.execute("SET vacuum_cost_limit = {0}".format(args.costlimit))
            # Needed by the check on conn.notices after each VACUUM (see
            # there): a database or role default of ERROR would otherwise
            # keep the server's warnings from this session.
            cur.execute("SET client_min_messages = warning")

            cur.execute(tabquery)
            verbose_print("getting list of tables")

            # rows of (regclass text, schema name, table name); see table_named_in()
            tablist = cur.fetchall()
        except Exception as err:
            _print("skipping database {d} (couldn't list its tables: {e})".format(d=db, e=str(err).rstrip()))
            outcome.skipped_databases.append(db)
            continue
        outcome.databases_listed += 1

        #if only one table is desired
        if args.table:
            # a bare name can match in more than one schema; all of them are kept
            tablist = [tabrow for tabrow in tablist if table_named_in([args.table], tabrow)]
            if not tablist:
                _print("specified single table {0} was not found (or excluded) in database {1}".format(args.table, db))
                outcome.table_missing_in.append(db)
                continue

        # Note which exclusion values apply to this list.  Done for the whole
        # list here rather than in the loop below: that loop stops at the
        # first exclusion that fits a table, and can itself be stopped by the
        # time limit, and either would leave a value that does match a table
        # reported as one that matched nothing.
        for tabrow in tablist:
            for name in database_table_map.get(db, []):
                if table_named_in([name], tabrow):
                    exclusions_matched.add((db, name))
            for name in args.tables_to_exclude or []:
                if table_named_in([name], tabrow):
                    exclusions_matched.add((None, name))

        # for each table in list
        for tabrow in tablist:
            # the regclass text: used for all output and as the VACUUM target
            table = tabrow[0]
            if db in database_table_map and table_named_in(database_table_map[db], tabrow):
                debug_print("skipping table {t} in database {d} per --exclude-table-in-database argument".format(t=table, d=db))
                continue
            elif args.tables_to_exclude and table_named_in(args.tables_to_exclude, tabrow):
                verbose_print(
                    "skipping table {t} per --exclude-table argument".format(t=table))
                continue
            else:
                verbose_print("processing table {t}".format(t=table))

            # The pause owed since the previous vacuum.  It is cut off at the
            # time limit: past that point the check below ends the run, and
            # sleeping first would only hold the connection open for nothing.
            if pause_pending:
                pause_pending = False
                pause_secs = min(args.pause_time, halt_time - time.time())
                if pause_secs > 0:
                    time.sleep(pause_secs)

            # check time; if overtime, exit
            if time.time() >= halt_time:
                verbose_print("Reached time limit.  Exiting.")
                outcome.time_limited = True
                break
            else:
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

            try:
                if args.enforcetime:
                    cur.execute(timeout_query)
                else:
                    cur.execute("SET statement_timeout = 0")

                # Emptied so that whatever is in it afterwards was raised by
                # this table's VACUUM and by nothing earlier.
                del conn.notices[:]
                cur.execute(exquery)
            except Exception as ex:
                # The cancellation --enforce-time asked for, arriving when it
                # was asked for.  That ends the run, but as a stop at the
                # time limit and not as a failure.  A cancel inside the
                # window, or one in a run that set no timeout, came from
                # somewhere else and is a failed table like any other.
                if (args.enforcetime and time.time() >= halt_time
                        and isinstance(ex, psycopg2.extensions.QueryCanceledError)):
                    verbose_print("halted flexible_freeze due to enforced time limit")
                    outcome.time_limited = True
                    outcome.cancelled_table = (table, db)
                    break

                _print("VACUUMing %s failed." % table)
                _print(str(ex).rstrip())
                outcome.failed_tables.append((table, db))

                # One table failing says nothing about the next, so the run
                # goes on -- unless the failure took the connection with it.
                # No reconnect is attempted: the database is recorded as
                # unfinished and the next one is started.
                if conn.closed:
                    _print("skipping the rest of database {d} (connection lost)".format(d=db))
                    outcome.skipped_databases.append(db)
                    break
            else:
                # The server reports a table it declines to vacuum (one the
                # role has no permission on, for instance) with a WARNING and
                # then returns as if the VACUUM had run.  Any message at all
                # is therefore taken to mean "not vacuumed".  Its text is
                # passed on but never examined: it is translated, and worded
                # differently from one server version to the next.
                if conn.notices:
                    _print("VACUUMing %s drew a message from the server; not counting it as vacuumed." % table)
                    for notice in conn.notices:
                        _print(notice.rstrip())
                    outcome.unvacuumed_tables.append((table, db))
                else:
                    outcome.tables_vacuumed += 1

            pause_pending = True
    finally:
        cur.close()
        conn.close()

# did we get through all tables?
# exit, report results
if outcome.time_limited:
    _print("Vacuuming halted due to timeout")
    if outcome.cancelled_table:
        _print("Work remains: VACUUM of {t} was cancelled at the time limit.".format(
            t=tables_for_display([outcome.cancelled_table])))
elif outcome.work_was_missed():
    _print("Not all tables were vacuumed.")
else:
    _print("All tables vacuumed.")

if outcome.failed_tables:
    _print("Tables that failed: " + tables_for_display(outcome.failed_tables))
if outcome.unvacuumed_tables:
    _print("Tables not counted as vacuumed: " + tables_for_display(outcome.unvacuumed_tables))
if outcome.skipped_databases:
    _print("Databases skipped or left unfinished: " + ', '.join(outcome.skipped_databases))
if outcome.table_missing_in:
    _print("Databases where --table {t} matched no selected table: {d}".format(
        t=args.table, d=', '.join(outcome.table_missing_in)))

if outcome.time_limited:
    verbose_print("after vacuuming %d tables in %d databases" % (outcome.tables_vacuumed, outcome.databases_listed,))
else:
    verbose_print("%d tables in %d databases" % (outcome.tables_vacuumed, outcome.databases_listed))

# A mistyped exclusion protects nothing, and the table it was meant for is
# vacuumed.  It is mentioned rather than treated as an error, and only with
# -v, because a correct value also matches nothing in a run that did not
# select its table.
unmatched = ["-T %s" % name for name in args.tables_to_exclude or []
             if (None, name) not in exclusions_matched]
unmatched += ["--exclude-table-in-database %s.%s" % (dat, tab)
              for dat, tabs in database_table_map.items() for tab in tabs
              if (dat, tab) not in exclusions_matched]
if unmatched:
    verbose_print("exclusions that matched no selected table: " + ', '.join(unmatched)
                  + ("" if outcome.databases_listed == len(dblist)
                     else " (not every database was examined)"))

verbose_print("Flexible Freeze run complete")
sys.exit(1 if outcome.work_was_missed() else 0)
