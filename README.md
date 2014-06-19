flexible-freeze
===============

Flexible freeze scripts for managing off-hours vacuuming and freezing of PostgreSQL databases.

The directory /scripts/ contains standalone scripts to manage freezing and vacuuming.

Eventually this repository will have a more sophisticated always-on service which does opportunistic freezing.  But not today. ;-)

/scripts/flexible_freeze.py
---------------------------

This script is designed for doing VACUUM FREEZE or VACUUM ANALYZE runs
on your database during known slow traffic periods.  If doing both
vacuum freezes and vacuum analyzes, do the freezes first.

Takes a timeout so that it won't overrun your slow traffic period.  
Note that this is the time to START a vacuum, so a large table may still overrun the vacuum period.

Requires: psycopg2

Usage example:

::
    flexible_freeze.py -m 120 --dblist="prod,queue" --pause 5 -U postgres

