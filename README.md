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

Usage example:

::
    flexible_freeze.py -m 120 --dblist="prod,queue" --pause 5 -U postgres

Arguments:

* -m, --minutes : number of minutes to run for (see note below)
* -d, --databases : comma-delimited list of databases to vacuum
* --vacuum : do a VACUUM ANALYZE instead of a VACUUM FREEZE
* --pause : seconds to pause between vacuums (10)
* --freezeage : minimum XID age for freezing (10000)
* --costdelay : vacuum_cost_delay in ms (20)
* --costlimit : vacuum_cost_limit (2000)
* --enforce-time : enforce ending time through statement_timeout
* -l, --log : log file for all output (optional)
* -v, --verbose
* -U, --user : database user
* -H, --host : database host
* -p, --port : database port
* -w, --password : database password

Notes:

The minutes time limit is normally only enforced at the start of vacuuming each table, allowing vacuums to continue past the end of the time window.  If you set --enforce-time, however, it uses statement_timeout to terminate a running vacuum at the end of the time window (plus 30 seconds grace period).

Normally flexible_freeze.py does VACUUM FREEZE, starting with the tables with the oldest transaction IDs.  If you set --vacuum, though, it will instead do VACUUM ANALYZE, starting with the tables with the most dead rows.  If you are doing both, do the FREEZE first.

The database user supplied is expected to have permissions on all tables (e.g. a superuser).  If they do not, flexible freeze will error out.

Currently, flexible_freeze will not respond to a CTRL-C until the current vacuum is done.  If you need to halt flexible_freeze before then, we recommend using pg_cancel_backend() from the Postgres command line.  This will cause flexible_freeze to error out and exit.


