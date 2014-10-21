'''Flexible Freeze script for PostgreSQL databases
Version 0.4
(c) 2014 PostgreSQL Experts Inc.
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

parser = argparse.ArgumentParser()
parser.add_argument("-m", "--minutes", dest="run_min",
                    type=int, default=120,
                    help="Number of minutes to run before halting.  Defaults to 2 hours")
parser.add_argument("-d","--databases", dest="dblist",
                    help="List of databases to vacuum, if not all of them")
parser.add_argument("--vacuum", dest="vacuum", action="store_true",
                    help="Do regular vacuum instead of VACUUM FREEZE")
parser.add_argument("--pause", dest="pause_time", default=10,
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
parser.add_argument("--enforce-time", dest="enforcetime", action="store_true",
                    help="enforce time limit by terminating vacuum")
parser.add_argument("-l", "--log", dest="logfile")
parser.add_argument("-v", "--verbose", action="store_true",
                    dest="verbose")
parser.add_argument("-U", "--user", dest="dbuser",
                  help="database user")
parser.add_argument("-H", "--host", dest="dbhost",
                  help="database hostname")
parser.add_argument("-p", "--port", dest="dbport",
                  help="database port")
parser.add_argument("-w", "--password", dest="dbpass",
                  help="database password")
args = parser.parse_args()

def dbconnect(dbname, dbuser, dbhost, dbport, dbpass):

    if dbname:
        connect_string ="dbname=%s application_name=flexible_freeze" % dbname
    else:
        print "ERROR: a target database is required."
        return None

    if dbhost:
        connect_string += " host=%s " % dbhost

    if dbuser:
        connect_string += " user=%s " % dbuser

    if dbpass:
        connect_string += " password=%s " % dbpass

    if dbport:
        connect_string += " port=%s " % dbport

    try:
        conn = psycopg2.connect( connect_string )
        conn.autocommit = True
    except Exception as ex:
        print "connection to database %s failed, aborting" % dbname
        print str(ex)
        sys.exit(1)

    return conn

def verbose_print(some_message):
    if args.verbose:
        print some_message

    return True

def signal_handler(signal, frame):
    print('exiting due to user interrupt')
    if conn:
        try:
            conn.close()
        except:
            verbose_print('could not clean up db connections')
        
    sys.exit(0)

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
        print('could not open logfile: %s' % str(ex))
        sys.exit(1)

    print('')
    print('='*40)
    print('flexible freeze started %s' % str(datetime.datetime.now()))
    verbose_print('arguments: %s' % str(args))

# do we have a database list?
# if not, connect to "postgres" database and get a list of non-system databases
if args.dblist is None:
    conn = dbconnect("postgres", args.dbuser, args.dbhost, args.dbport, args.dbpass)
    cur = conn.cursor()
    cur.execute("""SELECT datname FROM pg_database
        WHERE datname NOT IN ('postgres','template1','template0')
        ORDER BY age(datfrozenxid) DESC""")
    dblist = []
    for dbname in cur:
        dblist.append(dbname[0])

    conn.close()
    if not dblist:
        conn.close()
        print "no databases to vacuum, aborting"
        sys.exit(1)
else:
    dblist = args.dblist.split(',')

verbose_print("list of databases is %s" % (','.join(dblist)))

# connect to each database
time_exit = False
tabcount = 0
dbcount = 0
timeout_secs = 0

for db in dblist:
    if time_exit:
        break
    else:
        dbcount += 1
    conn = dbconnect(db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    cur = conn.cursor()
    cur.execute("SET vacuum_cost_delay = {0}".format(args.costdelay))
    cur.execute("SET vacuum_cost_limit = {0}".format(args.costlimit))
    
# if vacuuming, get list of top tables to vacuum
    if args.vacuum:
        tabquery = """WITH deadrow_tables AS (
                SELECT relid::regclass as full_table_name,
                    ((n_dead_tup::numeric) / ( n_live_tup + 1 )) as dead_pct,
                    pg_relation_size(relid) as table_bytes
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 100
                AND ( (now() - last_autovacuum) > INTERVAL '1 hour'
                    OR last_autovacuum IS NULL )
                AND ( (now() - last_vacuum) > INTERVAL '1 hour'
                    OR last_vacuum IS NULL )
            )
            SELECT full_table_name
            FROM deadrow_tables
            WHERE dead_pct > 0.05
            AND table_bytes > 1000000
            ORDER BY dead_pct DESC, table_bytes DESC;"""
    else:
    # if freezing, get list of top tables to freeze
        tabquery = """WITH tabfreeze AS (
                SELECT pg_class.oid::regclass AS full_table_name,
                age(relfrozenxid) as freeze_age,
                pg_relation_size(pg_class.oid)
            FROM pg_class JOIN pg_namespace ON relnamespace = pg_namespace.oid
            WHERE nspname not in ('pg_catalog', 'information_schema')
                AND nspname NOT LIKE 'pg_temp%'
                AND relkind = 'r'
            )
            SELECT full_table_name
            FROM tabfreeze
            WHERE freeze_age > {0}
            ORDER BY freeze_age DESC
            LIMIT 1000;""".format(args.freezeage)

    cur.execute(tabquery)
    verbose_print("getting list of tables")
    tablist = cur.fetchall()
    # for each table in list
    for table in tablist:
    # check time; if overtime, exit
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
        if args.vacuum:
            exquery = """VACUUM ANALYZE %s""" % table[0]
        else:
            exquery = """VACUUM FREEZE ANALYZE %s""" % table[0]

        verbose_print("vacuuming table %s in database %s" % (table[0], db,))
        excur = conn.cursor()

        try:
            if args.enforcetime:
                excur.execute(timeout_query)
                
            excur.execute(exquery)
        except Exception as ex:
            if time.time() >= halt_time:
                verbose_print("halted flexible_freeze due to enforced time limit")
            else:
                print "VACUUMING %s failed." % table[0]
                print str(ex)
            sys.exit(1)

        time.sleep(args.pause_time)

conn.close()

# did we get through all tables?
# exit, report results
if not time_exit:
    print "All tables vacuumed."
    verbose_print("%d tables in %d databases" % (tabcount, dbcount))
else:
    print "Vacuuming halted due to timeout"
    verbose_print("after vacuuming %d tables in %d databases" % (tabcount, dbcount,))

verbose_print("Flexible Freeze run complete")
sys.exit(0)




