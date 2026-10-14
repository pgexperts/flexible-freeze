# Characterization tests for how a run is bounded and reported: connection
# options and failures, the time limit, the log file and debug output.
#
# Which stream a message goes to is settled (issue #40): a report of a
# failure is on stderr, everything else on stdout or in the log file.  The
# tests here assert on the stream each message belongs on; the rule itself,
# and the log file's share of the failure reports, is in
# test_output_streams.py.
#
# The exit status is settled too: 0 for a complete run and for one that stopped
# at its time limit, 1 when work was missed.  The cases where work is missed
# are in test_run_outcome.py.

import contextlib

import pytest
from ffharness import FREEZE_ALL, assert_lines_in_order, wait_for

T1 = ("public", "t1")

# How long a script whose statement was ended from outside is given to exit.
PROMPT = 15.0

# A database name that is not valid UTF-8.  It can only be given from a
# SQL_ASCII database, where the server takes the bytes as they come.
ASCII_DB = "ff_sql_ascii"
UNDECODABLE_NAME = b"ff_undecodable\xff"


def output(result):
    return result.stdout + result.stderr


@contextlib.contextmanager
def database_listing_held_up(cluster):
    """Keep the query that lists the databases from finishing.

    The query waits for a lock on pg_statistic in the postgres database,
    which planning it needs and opening a session does not
    (test_signals.py has the longer account).  Every other new query in
    that database waits there too, so the script is watched, and acted on,
    from the session that holds the lock.

    Yields a cursor of that session.
    """
    holder = cluster.connect(autocommit=False)
    try:
        with holder.cursor() as cur:
            cur.execute("LOCK TABLE pg_catalog.pg_statistic IN ACCESS EXCLUSIVE MODE")
            yield cur
    finally:
        holder.close()


def backend_waiting_to_list(cur):
    """The process id of the backend waiting for the lock that `cur`'s
    session holds on pg_statistic, or None.  Nothing but the script is
    running, so that backend is its own.  pg_locks, unlike pg_stat_activity,
    is read afresh by each query of a transaction."""
    cur.execute(
        "SELECT pid FROM pg_locks WHERE NOT granted "
        "AND relation = 'pg_catalog.pg_statistic'::regclass"
    )
    row = cur.fetchone()
    return row[0] if row else None


def test_unreachable_database_is_skipped_and_the_run_continues(db, run_ff):
    # ADR 0004.  The missing database comes first: the run must get past it
    # to reach the real one.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", f"ff_no_such_database,{db.name}")

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == {T1}
    # The driver's message ends in a line break.  It is taken off, so that
    # the report is one line and closes its own parenthesis.
    skipped, *summary = result.stderr.splitlines()
    assert skipped.startswith(
        "skipping database ff_no_such_database (couldn't connect: "
    )
    assert skipped.endswith('database "ff_no_such_database" does not exist)')
    assert summary == [
        "Not all tables were vacuumed.",
        "Databases skipped or left unfinished: ff_no_such_database",
    ]


def test_zero_minute_limit_halts_before_the_first_table(db, run_ff):
    # ADR 0002: the limit is checked before each table, so a window that is
    # already over stops the run without vacuuming anything.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-m", "0", "-d", db.name)

    # Stopping at the limit is how a run is meant to end, not a failure.
    assert result.returncode == 0
    assert_lines_in_order(
        result.stdout,
        ["Reached time limit.  Exiting.", "Vacuuming halted due to timeout"],
    )
    assert "All tables vacuumed." not in result.stdout
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == set()
    assert db.script_vacuums() == []


def test_zero_minute_limit_without_verbose_prints_only_the_halt(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-m", "0", "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "Vacuuming halted due to timeout\n"


def test_log_option_writes_to_the_file_instead_of_stdout(db, run_ff, tmp_path):
    db.create_table("t1")
    logfile = tmp_path / "flexible_freeze.log"
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-l", str(logfile), "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == ""
    assert db.frozen_since(before) == {T1}
    log = logfile.read_text()
    assert_lines_in_order(
        log,
        [
            "=" * 40,
            f"{db.script_vacuums()[0].sql} in database {db.name}",
            "All tables vacuumed.",
            "Flexible Freeze run complete",
        ],
    )
    assert log.count("flexible freeze started ") == 1

    # The file is appended to, not truncated, by the next run.
    result = run_ff(*FREEZE_ALL, "-l", str(logfile), "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == ""
    log = logfile.read_text()
    assert log.count("flexible freeze started ") == 2
    assert log.count("All tables vacuumed.") == 2


def test_unwritable_logfile_is_reported_and_nothing_runs(db, run_ff, tmp_path):
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "-l", str(tmp_path / "missing-dir" / "ff.log"), "-d", db.name
    )

    assert result.returncode == 1
    assert "could not open logfile: " in result.stderr
    assert result.stdout == ""
    assert db.frozen_since(before) == set()
    assert db.script_statements() == []


def test_debug_goes_to_stderr(db, run_ff):
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--debug", "-d", db.name)

    assert result.returncode == 0
    # Debug output does not mix into the normal report on stdout.
    assert result.stdout == "All tables vacuumed.\n"
    debug = [line for line in result.stderr.splitlines() if line.startswith("DEBUG (")]
    for topic in (
        "python version: ",
        "psycopg2 version: ",
        "parameters: Namespace(",
        "database_table_map: {}",
    ):
        assert any(topic in line for line in debug), topic
    assert db.frozen_since(before) == {T1}


def test_no_debug_output_by_default(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    assert "DEBUG" not in output(result)


def test_user_option_selects_the_connecting_role(db, make_role, run_ff):
    # The role owns the table, so its VACUUM is permitted; what happens to
    # tables a role may not vacuum is a separate matter (issue #41).
    role = make_role("owner")
    db.create_table("t1", owner=role)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-U", role, "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {T1}
    assert {record.user for record in db.cluster.statements(dbname=db.name)} == {role}


def test_failure_to_list_databases_is_fatal(cluster, run_ff):
    # With no -d the list comes from the postgres database; if that
    # connection fails there is nothing to work on.
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "-U", "ff_no_such_role")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert cluster.statements(since=mark) == []
    # One line, and no blank one after it for the line break that the
    # driver's message ends in.
    (report,) = result.stderr.splitlines()
    assert report.startswith(
        "Could not list databases: connection to database postgres failed: "
    )
    assert report.endswith('role "ff_no_such_role" does not exist')


@pytest.mark.parametrize(
    "ended_by, reason",
    [
        ("pg_cancel_backend", "canceling statement due to user request"),
        ("pg_terminate_backend", "terminating connection due to administrator command"),
    ],
    ids=["query cancelled", "connection lost"],
)
def test_failed_database_listing_query_is_fatal(
    cluster, db, start_ff, tmp_path, ended_by, reason
):
    # The connection was made and the query for the list then fails.  That
    # is the end of the run, like a connection that could not be made, and
    # it is reported the same way: by the script, on stderr and in the log
    # file, not by a traceback that only stderr gets.
    #
    # --lock-timeout 0, so that on a slow machine the wait cannot end by
    # itself before the test has ended it.
    db.create_table("t1")
    before = db.frozenxids()
    logfile = tmp_path / "ff.log"

    with database_listing_held_up(cluster) as cur:
        proc = start_ff(*FREEZE_ALL, "--lock-timeout", "0", "-l", str(logfile))
        backend = wait_for(
            lambda: backend_waiting_to_list(cur),
            what="the script to wait in its database list",
        )
        cur.execute(f"SELECT {ended_by}(%s)", (backend,))
        stdout, stderr = proc.communicate(timeout=PROMPT)

    assert proc.returncode == 1
    assert "Traceback" not in stderr
    assert stdout == ""
    report = stderr.splitlines()
    # Beginning and end, because for a connection that is lost the driver
    # puts the server's severity in front of the message.
    assert report[0].startswith(
        "Could not list databases: query in database postgres failed: "
    )
    assert report[0].endswith(reason)
    # Whatever lines the driver adds after the server's message, the report
    # does not end in a blank one.
    assert report[-1].strip()
    # The log file has the header of the run and then the same report.
    assert logfile.read_text().endswith("\n" + stderr)
    assert db.script_statements() == []
    assert db.frozen_since(before) == set()


def test_database_list_that_cannot_be_fetched_is_fatal(cluster, db, run_ff, tmp_path):
    # The query succeeds and the failure is in the loop that fetches its
    # rows: pg_database is shared, so the session in postgres (UTF8) is sent
    # the undecodable name's bytes unconverted, and the driver fails to
    # decode that row when the script iterates over the cursor.
    #
    # That database is copied from one that was just frozen, so both of its
    # horizons, datfrozenxid and datminmxid, are younger than those of a
    # copy of template0, and the script's order, oldest first by the greater
    # of the two ages, puts at least one good row before it.  The loop has
    # therefore run before it fails, which is the case in which the loop
    # variable must not be the name the message uses.
    logfile = tmp_path / "ff.log"
    db.create_table("t1")
    db.execute("VACUUM (FREEZE)")
    cluster.execute(
        "postgres",
        f"CREATE DATABASE {ASCII_DB} TEMPLATE template0 ENCODING 'SQL_ASCII'",
    )
    maker = cluster.connect(ASCII_DB)
    try:
        with maker.cursor() as cur:
            cur.execute(
                b'CREATE DATABASE "' + UNDECODABLE_NAME + b'" TEMPLATE '
                + db.name.encode()
            )
            try:
                # The premise: a database that can be listed is older, by
                # the age the script orders its list by.
                cur.execute(
                    "SELECT datname::text::bytea = %s, "
                    "greatest(age(datfrozenxid), mxid_age(datminmxid)) "
                    "FROM pg_database WHERE datallowconn AND NOT datistemplate",
                    (UNDECODABLE_NAME,),
                )
                ages = cur.fetchall()
                (bad_age,) = [age for is_bad, age in ages if is_bad]
                assert bad_age < max(age for is_bad, age in ages if not is_bad)

                result = run_ff(*FREEZE_ALL, "-l", str(logfile), timeout=PROMPT)
            finally:
                cur.execute(
                    b'DROP DATABASE "' + UNDECODABLE_NAME + b'" WITH (FORCE)'
                )
    finally:
        maker.close()
        cluster.drop_database(ASCII_DB)

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert result.stdout == ""
    (report,) = result.stderr.splitlines()
    assert report.startswith(
        "Could not list databases: query in database postgres failed: "
    )
    assert "codec can't decode byte 0xff" in report
    # The log file has the header of the run and then the same report.
    assert logfile.read_text().endswith("\n" + result.stderr)
