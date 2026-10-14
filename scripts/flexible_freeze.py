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

import os
import time
import sys
import atexit
import signal
import select
import argparse
import psycopg2
import psycopg2.extensions
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
                    help="Minimum table size to vacuum/freeze (in MB).  The size is the table's data including its TOAST table, without its indexes.  Default is 0.")
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
                    help="freeze tables older than this many XIDs.  A table's age is the greatest of its transaction ID age and its MultiXact ID age, and those of its TOAST table; a table whose age is exactly this is not selected.  Default 10m XIDs.  Ignored with --no-freeze")
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
# What these two guard against is described where they are applied: at the
# session settings in the database loop and at ADVISORY_LOCK_KEY.
parser.add_argument("--lock-timeout", dest="lock_timeout",
                    type=non_negative_int, default=10,
                    help="seconds to wait for a lock before giving up (set as the session's lock_timeout).  A database whose table list cannot be read in that time is skipped, a table whose VACUUM runs into it is reported as failed, and the run goes on.  0 means wait without limit.  Default is 10.")
parser.add_argument("--no-advisory-lock", dest="advisory_lock", action="store_false",
                    help="Do not guard against overlapping runs.  By default a run takes an advisory lock in each database while it works there, and skips a database in which another run holds that lock.")
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
                  help="only process tables with this name: a bare name matches in every schema, SCHEMA.TABLE only that table.  The table is vacuumed whatever its age, size or dead rows: --freezeage, --minsizemb and the --no-freeze thresholds do not apply to it; exclusions still do.  -st is an older spelling of this option")

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

# Where output goes.  There are three kinds, and each message is one of them:
#
#   progress and information (_print, verbose_print): standard output, or
#       the log file instead when -l is given;
#   failure reports (failure_print): standard error, and the log file as
#       well when -l is given;
#   debug output (debug_print): standard error only.
#
# A failure report is a line about something that by itself makes the exit
# status other than 0: a table that failed or that the server declined to
# vacuum, a database that was skipped or left unfinished, a --table name
# found in no database, a run that could not start, a run stopped by a
# signal, and the lines of the closing summary that list any of those.
# Everything else is progress or information, including what a run chose not
# to do because it was told to (an excluded table) and a stop at the time
# limit, which is how a run is meant to end and exits 0.
#
# The split is for a run under cron, which mails what a job writes to
# standard error: with -l a failure used to be in the log alone and the mail
# was empty; without -l it was on standard output among the progress lines,
# where a job that discards standard output loses it.  With -l a failure
# report goes to both places because the log is the full record of the run,
# in order, and has to read the same whether or not anyone kept the mail.
# Without -l it is not repeated on standard output: both streams usually
# reach the same terminal or the same mail, and it would be seen twice.
#
# Errors in the command line (exit status 2) are printed straight to
# standard error where they are found, which is before the log file is
# opened; they never reach the log.

# The log file opened for -l, or None.  sys.stdout is rebound to the same
# object, so _print needs no test of its own.
log_file = None

def stamped(some_message):
    if args.print_timestamps:
        return "{timestamp}: {some_message}".format(timestamp=timestamp(), some_message=some_message)
    return some_message

def _print(some_message):
    print(stamped(some_message))
    sys.stdout.flush()
    return True

# The line is built once, so that with --print-timestamps the two copies of
# a report carry the same time and can be matched up.  The log file comes
# first: it is the record, and it should have the line even if the write to
# standard error then fails (a reader that has gone away).  Each copy is
# flushed at once, like every other line, so that the two streams read in
# the order of the run wherever they end up together.
#
# A report that cannot be written to standard error (its reader has gone
# away, most likely) must not end the run: the tables still to come matter
# more than the notice, and before failure reports went to standard error a
# run with -l did not depend on that stream at all.
#
# Passing over the error is not enough.  The line that could not be written
# stays in the stream's buffer, the interpreter tries again when it flushes
# the stream at exit, and when that fails too it replaces the exit status
# with 120: the one thing a scheduler still had to go by.  (Unless Python
# runs unbuffered, which is how that goes unnoticed.)  So descriptor 2 is
# pointed at the null device instead: what is buffered, and every later
# report, then has somewhere to go, and the exit status stays the run's own.
def failure_print(some_message):
    line = stamped(some_message)
    if log_file is not None:
        print(line, file=log_file, flush=True)
    try:
        print(line, file=sys.stderr, flush=True)
    except OSError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stderr.fileno())
        os.close(devnull)
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

# Stopping on SIGINT or SIGTERM.
#
# interrupt_signal is the first of the two to arrive, or None.  The handler
# does nothing but record it, because it can run between any two bytecodes of
# the script: printing from it can re-enter a print that is in progress,
# exiting from it skips the summary, and closing the connection from it
# abandons a VACUUM that the server then runs to completion, since a backend
# does not notice a client that has gone away while it is vacuuming.  The
# stopping is done by the code that the signal interrupted: wait_for_server()
# cancels the statement in progress, pause() ends early, the loops test the
# variable before starting anything new, and the summary and the exit status
# (128 + the signal number, the shell's convention) come from the same place
# as for any other run.
#
# interrupt_repeated: a further signal arrived after the first.  The first
# one waits for the server to confirm the cancellation, and a server that
# never answers (a frozen backend, a proxy that drops cancel requests) would
# otherwise leave a process that only SIGKILL could end.  The second one
# gives up that wait; see wait_for_server().
interrupt_signal = None
interrupt_repeated = False

def signal_handler(signum, frame):
    global interrupt_signal, interrupt_repeated
    if interrupt_signal is None:
        interrupt_signal = signum
    else:
        interrupt_repeated = True

def interruption_notice(signum):
    return "Vacuuming interrupted by {s}".format(s=signal.Signals(signum).name)

# For the steps before the first database is started.  They have nothing to
# summarise and already end the process themselves when they fail, so an
# interrupt ends it there too, with the line the summary would have led with.
def exit_if_interrupted():
    if interrupt_signal is not None:
        failure_print(interruption_notice(interrupt_signal))
        sys.exit(128 + interrupt_signal)

# The longest a wait goes without looking at interrupt_signal.  A handler
# that returns normally does not end the system call it interrupted: Python
# resumes select() and sleep() for the rest of their timeout (PEP 475).  So
# the waits below are made in slices of this length, and the variable is
# tested between slices.
WAIT_SLICE_SECS = 0.5

# How long a cancel request is given to take effect before it is repeated.
CANCEL_RETRY_SECS = 2

# psycopg2 wait callback (psycopg2.extensions.set_wait_callback): called
# whenever the driver has to wait for the server, in place of a blocking libpq
# call.  Inside a blocking call the interpreter does not run at all, so the
# handler above could not even record a signal until the VACUUM had finished;
# here the wait is a select() in Python, and each slice of it can act on
# interrupt_signal.
#
# On an interrupt the statement is cancelled on the server and the wait goes
# on until the server answers, normally with the query-cancelled error, which
# is raised from the execute() call that was waiting.  The request is repeated
# because one can be lost: it travels on a connection of its own, and a
# backend that receives it before it has read the statement ignores it.
# That is also why the test of interrupt_signal comes before the first
# select(): a signal that arrived while nothing was waiting on the server,
# between two statements, is acted on when the next statement's wait begins.
#
# The wait is given up, without the server's answer, in two cases: the
# request cannot be delivered (cancel() raises), or a second signal arrives.
# Either way an exception leaves this function, and psycopg2 then closes the
# connection, since it can no longer tell what the server will send on it.
# That closed connection is how the caller knows that the statement was not
# confirmed cancelled and may still be running.  A second signal is tested
# only after a request has been sent, so that an impatient double CTRL-C
# still cancels the VACUUM and only skips the wait for the confirmation.
#
# cancel() is itself a blocking libpq call (it opens a connection to the
# server) and is not covered by any of this: while the server's host does not
# answer at all, it blocks for as long as the operating system lets a
# connection attempt take.
#
# With a wait callback installed libpq no longer applies connect_timeout
# (PGCONNECT_TIMEOUT, or a service file's), so it is applied here.  The limit
# covers the whole attempt, where libpq's own would start afresh for each
# host of a multi-host connection.
def wait_for_server(waiting_conn):
    # psycopg2 moves a connection to STATUS_READY once it is established; a
    # statement can be cancelled from then on, and not before.
    connecting = waiting_conn.status not in (psycopg2.extensions.STATUS_READY,
                                             psycopg2.extensions.STATUS_BEGIN)
    give_up_at = None
    if connecting:
        connect_timeout = int(waiting_conn.get_dsn_parameters().get('connect_timeout') or 0)
        if connect_timeout > 0:
            give_up_at = time.monotonic() + connect_timeout
    next_cancel = 0

    while True:
        state = waiting_conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            return

        if interrupt_signal is not None:
            if connecting:
                raise psycopg2.OperationalError("connection attempt abandoned on an interrupt")
            if time.monotonic() >= next_cancel:
                waiting_conn.cancel()
                next_cancel = time.monotonic() + CANCEL_RETRY_SECS
            if interrupt_repeated:
                raise psycopg2.OperationalError("gave up waiting for the server on a second signal")
        elif give_up_at is not None and time.monotonic() >= give_up_at:
            raise psycopg2.OperationalError("timeout expired")

        if state == psycopg2.extensions.POLL_READ:
            select.select([waiting_conn], [], [], WAIT_SLICE_SECS)
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [waiting_conn], [], WAIT_SLICE_SECS)
        else:
            raise psycopg2.OperationalError("unexpected poll state: %r" % (state,))

# The pause between vacuums, ended early by an interrupt.
def pause(seconds):
    wake_at = time.monotonic() + seconds
    while interrupt_signal is None:
        remaining = wake_at - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(remaining, WAIT_SLICE_SECS))

# Tables are carried around as rows of (regclass text, schema name, table name).
# The regclass text is what gets printed and what goes into the VACUUM
# statement, but it changes with search_path and quoting (orders, sales.orders,
# "Orders"), so a name given on the command line is also compared with the
# plain catalog names.  A name matches a table if it is:
#   - the regclass text, exactly as the script prints it; or
#   - the bare table name, which matches that table in every schema; or
#   - schema.table, written plain, without any SQL quoting.
# Used for --exclude-table and --exclude-table-in-database.  --table goes by
# the same three forms, but in SQL: see TABLE_LOOKUP_QUERY.
def table_named_in(names, tabrow):
    regclass_text, nspname, relname = tabrow
    forms = (regclass_text, relname, "%s.%s" % (nspname, relname))
    return any(name in forms for name in names)

# The age a freezing run goes by: the greatest of the transaction ID age and
# the MultiXact ID age, of the table and of its TOAST table.  Each of the four
# is a wraparound horizon in its own right.  The server forces a vacuum on
# whichever comes due first (autovacuum_freeze_max_age for one counter,
# autovacuum_multixact_freeze_max_age for the other), and a TOAST table has
# its own relfrozenxid and relminmxid, which can be older than its parent's.
# Going by transaction ID age alone leaves a table that is quiet on that
# counter and far gone on the other unselected until the server steps in.
# greatest() ignores NULLs, which is what a table without a TOAST table
# supplies for the last two.
#
# Written for the aliases the queries below use: pg_class is the table, toast
# its TOAST table.
FREEZE_AGE_SQL = """greatest(age(pg_class.relfrozenxid), mxid_age(pg_class.relminmxid),
                    age(toast.relfrozenxid), mxid_age(toast.relminmxid))"""

# --table: the table is looked up by name instead of being picked out of the
# mode's selection.  Naming a table is a request to vacuum that table; the
# age, size and dead-row thresholds are there to choose tables when nobody
# has, and applied to a named table they turned "below a threshold" into
# "not found".  The result goes through the exclusions like any other list.
#
# pg_class and not to_regclass(): to_regclass resolves a bare name through
# search_path, which is empty in every session here (see dbconnect), so it
# returns NULL for the very form most command lines use.  It also returns
# one relation, where a bare name means that table in every schema.
#
# The three alternatives are the three forms table_named_in() accepts and
# have to stay in step with it: this is the one place a user-supplied name
# is matched in SQL.  Everything is compared as text because the name type
# cuts a longer value down to 63 bytes, and would match on what was left.
#
# What can be found is what either mode can vacuum: ordinary tables and
# materialized views.  The temporary schemas are left out because they hold
# other sessions' tables (this one creates none), which the server passes
# over without saying so.  The system schemas are not left out, unlike in
# the two selections: a catalog named outright is vacuumed like any table.
#
# The name travels as a query parameter and never as text pasted into the
# statement.  psycopg2 %-formats a statement that has parameters, hence the
# doubled percent signs in the LIKE patterns.
#
# Oldest first, as in a freezing run, so that a name present in many schemas
# is worked through in order of need inside the time window.
TABLE_LOOKUP_QUERY = """SELECT pg_class.oid::regclass AS full_table_name,
                nspname, pg_class.relname
            FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                LEFT OUTER JOIN pg_class as toast
                    ON pg_class.reltoastrelid = toast.oid
            WHERE pg_class.relkind IN ('r', 'm')
                AND nspname NOT LIKE 'pg_temp%%'
                AND nspname NOT LIKE 'pg_toast_temp%%'
                AND %(name)s::text IN (pg_class.relname::text,
                    nspname::text || '.' || pg_class.relname::text,
                    pg_class.oid::regclass::text)
            ORDER BY {freeze_age} DESC, nspname, pg_class.relname;""".format(
                freeze_age=FREEZE_AGE_SQL)

# One run per database at a time.  Two runs in the same database (a nightly
# run still going when the next one starts, a second cron entry) select the
# same tables in the same order.  With SKIP_LOCKED on the VACUUM the later
# one no longer queues behind the earlier, but it reports the table the
# other is on as locked and vacuums again every table the other has just
# finished.  So a run claims each database, for as long as it has it open,
# with an advisory lock on this key, and a run that finds the key taken
# skips that database and says why.
#
# Per database, and not one lock for the whole cluster: an advisory lock
# belongs to the database it is taken in (the same key in two databases is
# two locks), and with -d a run connects to the named databases only, so two
# runs need not have any database in common in which to meet.  It is also
# the finer grain: runs over different databases leave each other alone.
#
# Session level and never released explicitly.  The server drops it when
# the database's connection closes, which the loop does on every way out of
# a database and which the server does for it if the process dies, so there
# is no stale lock to clear after a crash.  pg_try_advisory_lock() does not
# wait, so it needs neither a timeout nor handling of its own for an
# interrupt.
#
# The key is the eight ASCII bytes of "flexfrze" read as one big-endian
# 64-bit integer.  It is fixed, and must stay so, because every copy and
# every version of the script has to agree on it.  It is spelled from a name
# because the key space is shared with everything else that uses the
# database, and small numbers are what applications pick.  In pg_locks it
# shows as locktype 'advisory', classid 1718379896, objid 1718778469,
# objsubid 1.
ADVISORY_LOCK_KEY = 0x666C657866727A65  # 7380385457142659685

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
        # Databases not worked through: could not connect, held by another
        # run, could not list the tables, or lost the connection part-way.
        self.skipped_databases = []
        # --table: the databases in which no table has that name, and
        # whether one has it anywhere.  A miss in one database is expected
        # when the name is given for a list of databases (by default every
        # database), so it is only a name found in none of them that counts
        # as missed work.  A table that exists but is excluded was found.
        self.table_missing_in = []
        self.table_found = False
        # The run stopped at the time limit.  With --enforce-time that can
        # be in the middle of a vacuum, which is then the cancelled table.
        self.time_limited = False
        self.cancelled_table = None
        # The run was stopped by a signal (its number), and the table whose
        # VACUUM that cut short, if there was one.  Neither is a failure:
        # an interrupted table is kept out of failed_tables, and the exit
        # status for a signal takes precedence over 0 and 1.
        self.interrupted_by = None
        self.interrupted_table = None
        # The wait for that VACUUM was given up without the server's answer,
        # so it is not known to have been cancelled.
        self.interrupt_unconfirmed = False

    # Exit status 1.  Stopping at the time limit is not in this list: it is
    # the designed way for a nightly run to end, and exits 0.
    def work_was_missed(self):
        return bool(self.failed_tables or self.unvacuumed_tables
                    or self.skipped_databases or self.table_found_nowhere())

    # False when no database was examined at all; those are in
    # skipped_databases, and nothing is known about the table.
    def table_found_nowhere(self):
        return bool(self.table_missing_in) and not self.table_found

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

# Get set for an interrupt.  SIGTERM is what cron, systemd and a plain kill
# send; left at its default it ends the process at once, and the VACUUM in
# progress carries on in the server with nobody waiting for it.  The wait
# callback goes in before the first connection is made: it is what lets
# either handler run while the driver waits for the server.
psycopg2.extensions.set_wait_callback(wait_for_server)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# start logging to log file, if used
#
# The file is closed by an exit handler rather than at each place the script
# ends.  There are several of those from here on (the exits before the first
# database, the two at the bottom, an error nobody caught), and a close
# written at each would be missing from the next one added.  The handler runs
# on all of them, the exits for a signal included: a signal is only recorded
# by its handler, and the run then ends through sys.exit like any other.  It
# does not run if the process is killed outright, which loses nothing that
# was printed, because every line is flushed as it is written.
if args.logfile:
    try:
        log_file = open(args.logfile, 'a')
    except Exception as ex:
        failure_print('could not open logfile: %s' % str(ex))
        sys.exit(1)
    sys.stdout = log_file
    atexit.register(log_file.close)

    _print('')
    _print('='*40)
    _print('flexible freeze started %s' % str(datetime.datetime.now()))
    verbose_print('arguments: %s' % str(args_for_display()))

# do we have a database list?
# if not, connect to "postgres" database and get a list of every database
# that can be vacuumed.  That includes postgres itself: its datfrozenxid
# holds back the cluster's oldest transaction ID like any other database's.
# The two catalog flags, rather than names, decide what is left out: a
# database that refuses connections (template0, and any other marked so)
# could only ever be reported as skipped, on every run, and a template is
# by its flag not a database that holds the cluster's data.
if args.dblist is None:
    conn = None
    try:
        dbname = 'postgres'
        conn = dbconnect(dbname, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    except Exception as ex:
        # A connection attempt given up on a signal is not a failed one.
        exit_if_interrupted()
        failure_print("Could not list databases: connection to database {d} failed: {e}".format(d=dbname, e=str(ex)))
        sys.exit(1)

    cur = conn.cursor()
    try:
        cur.execute("""SELECT datname FROM pg_database
            WHERE datallowconn AND NOT datistemplate
            ORDER BY age(datfrozenxid) DESC""")
    except Exception:
        # Only the cancellation an interrupt asked for is dealt with here;
        # any other failure of this query is left as it was.
        exit_if_interrupted()
        raise
    dblist = []
    for dbname in cur:
        dblist.append(dbname[0])

    conn.close()
    # Before the test for an empty list: a signal outranks that exit status.
    exit_if_interrupted()
    if not dblist:
        failure_print("no databases to vacuum, aborting")
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
    # An interrupted run starts no further database.  This is also where the
    # table loop's break for an interrupt ends up.
    if interrupt_signal is not None:
        break
    verbose_print("working on database {0}".format(db))
    if outcome.time_limited:
        break
    conn = None
    try:
        conn = dbconnect(db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
    except Exception as err:
        # In each handler from here to the end of the loop the interrupt is
        # tested first.  What stops a statement or a connection attempt on a
        # signal reaches the script as an ordinary error (a cancelled query,
        # a failed connection), and taken at face value it would be recorded
        # as a skipped database or a failed table, and the run would go on.
        if interrupt_signal is not None:
            break
        failure_print("skipping database {d} (couldn't connect: {e})".format(d=db, e=str(err)))
        outcome.skipped_databases.append(db)
        continue

    # The one cursor for everything done in this database.
    cur = conn.cursor()

    # Parameters for tabquery.  None for the two selections, which are
    # complete statements: psycopg2 leaves a statement without parameters
    # alone, single percent signs included.
    tabparams = None

    # if only one table is desired, look it up; see TABLE_LOOKUP_QUERY
    if args.table:
        tabquery = TABLE_LOOKUP_QUERY
        tabparams = {'name': args.table}
    # if vacuuming, get list of top tables to vacuum
    #
    # pg_stat_user_tables lists the temporary tables of every session.  The
    # server passes over another session's without a word, so one selected
    # here would be counted as vacuumed and selected again on every run.
    #
    # table_bytes: see the note on pg_table_size at the freeze query.
    elif args.skip_freeze:
        tabquery = """WITH deadrow_tables AS (
                SELECT relid::regclass as full_table_name,
                    schemaname as nspname, relname,
                    n_dead_tup::numeric / NULLIF(n_dead_tup + n_live_tup, 0) as dead_pct,
                    pg_table_size(relid) as table_bytes
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 100
                AND schemaname NOT LIKE 'pg_temp%'
                AND schemaname NOT LIKE 'pg_toast_temp%'
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
    # if freezing, get list of top tables to freeze, oldest first; what
    # "oldest" means is at FREEZE_AGE_SQL.
    #
    # Materialized views ('m') are taken with the ordinary tables: they have
    # a relfrozenxid and a relminmxid that age in the same way.
    #
    # table_bytes is pg_table_size, the table with its TOAST table and
    # without its indexes.  The age above counts the TOAST table, so the
    # size has to as well: measured on the heap alone, --minsizemb dropped
    # a small table whose large TOAST table was the old part.
    #
    # There is no LIMIT.  The time window is what bounds a run; a cap on the
    # list ended a run that still had time, with the tables past the cap
    # never mentioned.
        tabquery = """WITH tabfreeze AS (
                SELECT pg_class.oid::regclass AS full_table_name,
                nspname, pg_class.relname,
                {2} as freeze_age,
                pg_table_size(pg_class.oid) as table_bytes
            FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                LEFT OUTER JOIN pg_class as toast
                    ON pg_class.reltoastrelid = toast.oid
            WHERE nspname not in ('pg_catalog', 'information_schema')
                AND nspname NOT LIKE 'pg_temp%'
                AND pg_class.relkind IN ('r', 'm')
            )
            SELECT full_table_name, nspname, relname
            FROM tabfreeze
            WHERE freeze_age > {0}
            AND table_bytes >= {1} * 2 ^ 20
            ORDER BY freeze_age DESC, table_bytes DESC;""".format(
                args.freezeage, args.minsizemb, FREEZE_AGE_SQL)

    # From here on the connection is closed on every way out of this
    # database: the skips below, the end of its table list, a stop at the
    # time limit, and a stop on a signal.
    try:
        try:
            cur.execute("SET vacuum_cost_delay = {0}".format(args.costdelay))
            cur.execute("SET vacuum_cost_limit = {0}".format(args.costlimit))
            # Needed by the check on conn.notices after each VACUUM (see
            # there): a database or role default of ERROR would otherwise
            # keep the server's warnings from this session.
            cur.execute("SET client_min_messages = warning")
            # Ends every wait for a lock in this session after this long,
            # with an error that the handlers here record like any other:
            # against the database when it was the table listing, against
            # the table when it was a VACUUM.  It is what bounds the
            # listing: the size functions in those queries open every
            # candidate table, so the whole list queues behind a pending
            # ACCESS EXCLUSIVE lock on any one of them.  For a VACUUM it is
            # the fallback behind SKIP_LOCKED (see where the statement is
            # built), which passes over a locked table but does not cover
            # any other lock the statement may come to wait for.  Unlike
            # statement_timeout it measures the wait and not the work, so
            # it never cuts short a VACUUM that is running.  Sent in every
            # case, 0 included: that replaces a database or role default,
            # which is what "without limit" has to mean.
            cur.execute("SET lock_timeout = '{0}s'".format(args.lock_timeout))

            # Claim the database; see ADVISORY_LOCK_KEY.  Before the table
            # list is read, so that a run that is turned away has done no
            # work here, and after the settings, so that nothing is left to
            # fail between the claim and the first table.
            if args.advisory_lock:
                cur.execute("SELECT pg_catalog.pg_try_advisory_lock({0})".format(ADVISORY_LOCK_KEY))
                if not cur.fetchone()[0]:
                    failure_print("skipping database {d} (another flexible_freeze run is working on it: advisory lock {k} is held there)".format(d=db, k=ADVISORY_LOCK_KEY))
                    outcome.skipped_databases.append(db)
                    continue

            cur.execute(tabquery, tabparams)
            verbose_print("getting list of tables")

            # rows of (regclass text, schema name, table name); see table_named_in()
            tablist = cur.fetchall()
        except Exception as err:
            if interrupt_signal is not None:
                break
            failure_print("skipping database {d} (couldn't list its tables: {e})".format(d=db, e=str(err).rstrip()))
            outcome.skipped_databases.append(db)
            continue
        outcome.databases_listed += 1

        # --table: the list is that table, in every schema that has one of
        # the name.  An empty list says only that this database has no such
        # table; whether that matters is decided at the end of the run, when
        # it is known whether any database had it.  So it is said with -v
        # only: a name given for every database is expected to be missing
        # from most of them, and a line for each, on every run, is noise in
        # a scheduler's mail.  A name found nowhere is in the summary at any
        # verbosity, with the databases that were looked in.
        if args.table:
            if not tablist:
                verbose_print("specified single table {0} does not exist in database {1}".format(args.table, db))
                outcome.table_missing_in.append(db)
                continue
            outcome.table_found = True

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
            # An exclusion outranks --table.  In a --table run it is said
            # at every verbosity, and in words of its own: the table was
            # asked for by name, and leaving it out quietly reads as a run
            # that did what it was asked.
            if db in database_table_map and table_named_in(database_table_map[db], tabrow):
                if args.table:
                    _print("specified single table {t} is excluded in database {d} by --exclude-table-in-database; not vacuuming it".format(t=table, d=db))
                else:
                    debug_print("skipping table {t} in database {d} per --exclude-table-in-database argument".format(t=table, d=db))
                continue
            elif args.tables_to_exclude and table_named_in(args.tables_to_exclude, tabrow):
                if args.table:
                    _print("specified single table {t} is excluded in database {d} by --exclude-table; not vacuuming it".format(t=table, d=db))
                else:
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
                    pause(pause_secs)

            # An interrupt that arrived during the pause, or during a
            # statement that completed all the same, stops the run before
            # the next table.  Tested ahead of the time limit so that a run
            # stopped by a signal is never reported as one that ran out of
            # time.
            if interrupt_signal is not None:
                break

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
            #
            # SKIP_LOCKED, always: a table on which another session holds a
            # conflicting lock (an anti-wraparound autovacuum, a CREATE
            # INDEX) is passed over at once.  Without it the VACUUM joined
            # the queue for that lock, and the run sat there for as long as
            # the other session took, with no test of the time limit, which
            # is made between tables only.  The server reports the skip
            # with a WARNING and a successful return; the check on
            # conn.notices below is what records the table as not vacuumed.
            # lock_timeout alone would not do: every locked table would
            # cost the full timeout and be reported as a failure.
            #
            # SKIP_LOCKED exists only in the parenthesized option list, and
            # only from PostgreSQL 12, the oldest version supported.
            vacuum_options = []
            if not args.skip_freeze:
                vacuum_options.append("FREEZE")
            if not args.skip_analyze:
                vacuum_options.append("ANALYZE")
            vacuum_options.append("SKIP_LOCKED")

            # The regclass text is already valid SQL for this session: the server
            # schema-qualifies it when the schema is not on the search_path and
            # double-quotes each part that needs it.  It must go in unmodified;
            # wrapping it in quotes again turns sales.orders into a single
            # (nonexistent) name and "Orders" into an invalid identifier.
            exquery = "VACUUM ({0}) {1}".format(', '.join(vacuum_options), table)

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
                # An interrupt first (see the note at the connection attempt
                # above).  The error is then almost always the cancellation
                # that wait_for_server() asked for, and whatever else it may
                # be, the table was not vacuumed and the run is stopping.
                # This has to come before the --enforce-time test, which
                # would take a cancel after the deadline for a stop at the
                # time limit, and before the failure handling, which would
                # report the table as failed and go on to the next one.
                #
                # A connection that is closed at this point was given up on
                # before the server answered (see wait_for_server), or was
                # lost; in both cases nothing says the VACUUM has stopped.
                if interrupt_signal is not None:
                    outcome.interrupted_table = (table, db)
                    outcome.interrupt_unconfirmed = bool(conn.closed)
                    break

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

                failure_print("VACUUMing %s failed." % table)
                failure_print(str(ex).rstrip())
                outcome.failed_tables.append((table, db))

                # One table failing says nothing about the next, so the run
                # goes on -- unless the failure took the connection with it.
                # No reconnect is attempted: the database is recorded as
                # unfinished and the next one is started.
                if conn.closed:
                    failure_print("skipping the rest of database {d} (connection lost)".format(d=db))
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
                    failure_print("VACUUMing %s drew a message from the server; not counting it as vacuumed." % table)
                    for notice in conn.notices:
                        failure_print(notice.rstrip())
                    outcome.unvacuumed_tables.append((table, db))
                else:
                    outcome.tables_vacuumed += 1

            pause_pending = True
    finally:
        cur.close()
        conn.close()

# Read once.  A signal that arrives from here on finds the work finished and
# changes nothing; reading the variable again below could give a summary and
# an exit status that disagree.
outcome.interrupted_by = interrupt_signal

# did we get through all tables?
# exit, report results
#
# Which of these lines are failure reports follows the rule at
# failure_print.  The headline of a run that stopped at the time limit is
# not one, nor is the table that --enforce-time cancelled: that run exits 0
# unless something else went wrong in it, and what did has a line of its own
# below.
if outcome.interrupted_by is not None:
    failure_print(interruption_notice(outcome.interrupted_by))
    if outcome.interrupted_table and outcome.interrupt_unconfirmed:
        failure_print("Work remains: VACUUM of {t} was interrupted; the server did not confirm that it was cancelled, and it may still be running there.".format(
            t=tables_for_display([outcome.interrupted_table])))
    elif outcome.interrupted_table:
        failure_print("Work remains: VACUUM of {t} was interrupted.".format(
            t=tables_for_display([outcome.interrupted_table])))
elif outcome.time_limited:
    _print("Vacuuming halted due to timeout")
    if outcome.cancelled_table:
        _print("Work remains: VACUUM of {t} was cancelled at the time limit.".format(
            t=tables_for_display([outcome.cancelled_table])))
elif outcome.work_was_missed():
    failure_print("Not all tables were vacuumed.")
else:
    _print("All tables vacuumed.")

if outcome.failed_tables:
    failure_print("Tables that failed: " + tables_for_display(outcome.failed_tables))
if outcome.unvacuumed_tables:
    failure_print("Tables not counted as vacuumed: " + tables_for_display(outcome.unvacuumed_tables))
if outcome.skipped_databases:
    failure_print("Databases skipped or left unfinished: " + ', '.join(outcome.skipped_databases))
if outcome.table_found_nowhere():
    failure_print("Databases where --table {t} matched no table: {d}".format(
        t=args.table, d=', '.join(outcome.table_missing_in)))

if outcome.time_limited or outcome.interrupted_by is not None:
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
if outcome.interrupted_by is not None:
    sys.exit(128 + outcome.interrupted_by)
sys.exit(1 if outcome.work_was_missed() else 0)
