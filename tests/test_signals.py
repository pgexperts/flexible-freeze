# Tests for stopping a run with a signal.  SIGINT (Ctrl-C) and SIGTERM (cron,
# systemd, kill) must each, wherever in the run they arrive:
#
#   * cancel the statement in progress on the server, so that no VACUUM is
#     left running there with nobody waiting for it;
#   * start nothing further, in this database or a later one;
#   * print the usual summary, saying that the run was interrupted, and report
#     the table that was cut short as interrupted, not as failed;
#   * exit with status 130 (SIGINT) or 143 (SIGTERM), whatever status the run
#     would otherwise have ended with.
#
# A second signal stops the wait for a server that does not confirm the
# cancellation, and the summary then says that the VACUUM may still be
# running.
#
# How a signal is made to arrive at a known point.  Each test waits for a
# state it can observe before signalling, never for a length of time:
#
# * during a VACUUM or the table-listing query: another session holds a lock
#   the statement needs, and the test waits for the script's backend to show
#   up as waiting for it;
# * during a connection attempt: the script is pointed at a socket that
#   accepts and never answers (silent_server), and the test waits for the
#   accept;
# * between two statements: the script is stopped where it reads the clock
#   before a table (script_clock.hold);
# * during the pause: the test waits for the line the script logs just before
#   it pauses.
#
# Every wait on the script is bounded, so a signal that goes unanswered fails
# the test instead of hanging the suite.

import contextlib
import os
import signal
import time

import pytest
from ffharness import FREEZE_ALL, SCRIPT_APP, assert_lines_in_order, wait_for

# How long an interrupted script is given to exit.  Generous for a loaded
# machine; a script that does not act on the signal sits behind a lock, a
# silent server or a long pause for far longer than this.
PROMPT = 15.0

BOTH_SIGNALS = pytest.mark.parametrize(
    "signum, status",
    [(signal.SIGINT, 130), (signal.SIGTERM, 143)],
    ids=["SIGINT", "SIGTERM"],
)

# Blocks VACUUM (which takes the same lock) but not the table-listing query.
BLOCKS_VACUUM = "SHARE UPDATE EXCLUSIVE"
# Blocks everything, the listing query's pg_relation_size() included.
BLOCKS_LISTING = "ACCESS EXCLUSIVE"


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


@contextlib.contextmanager
def lock_held(db, table, mode=BLOCKS_VACUUM):
    """Hold a lock on public.<table> in an open transaction.

    Yields the connection; rolling it back releases the lock early.
    """
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(f'LOCK TABLE public."{table}" IN {mode} MODE')
        yield conn
    finally:
        conn.close()


def script_backends(cluster):
    """The script's sessions on the server, as (database name, state, wait
    event type, backend process id) rows."""
    return cluster.execute(
        "postgres",
        "SELECT datname, state, wait_event_type, pid FROM pg_stat_activity "
        "WHERE application_name = %s",
        (SCRIPT_APP,),
    )


def wait_until_blocked(db):
    """Return, once the script's backend in `db` is waiting for a lock, the
    process id of that backend."""

    def probe():
        for datname, state, waiting_on, pid in script_backends(db.cluster):
            if (datname, state, waiting_on) == (db.name, "active", "Lock"):
                return pid
        return None

    return wait_for(probe, what=f"the script to wait for a lock in {db.name}")


def assert_no_backend_left(cluster):
    """The script's sessions are gone from the server.

    Called while the lock that blocked the statement is still held: a
    statement that was merely abandoned by its client is then still there,
    waiting, and would run as soon as the lock went away.
    """
    wait_for(
        lambda: not script_backends(cluster),
        timeout=10.0,
        what="the script's backend to be gone from pg_stat_activity",
    )


def finish(proc, timeout=PROMPT):
    """Wait for a script started with start_ff; return (status, stdout, stderr)."""
    stdout, stderr = proc.communicate(timeout=timeout)
    return proc.returncode, stdout, stderr


# -- during a VACUUM ----------------------------------------------------------


@BOTH_SIGNALS
def test_signal_during_a_vacuum_cancels_it_and_ends_the_run(
    make_db, start_ff, signum, status
):
    # t_locked is created first, so it is the oldest table and is taken
    # first: everything else in the run would come after it.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    one.create_table("t_next")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "t_locked"):
        proc = start_ff(*FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}")
        wait_until_blocked(one)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == status
    assert "Traceback" not in stderr
    # Nothing was vacuumed, then or after the lock went away, and nothing
    # was started after the interrupted table: not the next table, and not
    # so much as a connection to the next database.
    assert targets(one.script_vacuums()) == [("public", "t_locked")]
    assert one.frozen_since(before_one) == set()
    assert two.script_statements() == []
    assert two.frozen_since(before_two) == set()

    name = signal.Signals(signum).name
    lines = stdout.splitlines()
    assert_lines_in_order(
        stdout,
        [
            "processing table public.t_locked",
            f"Vacuuming interrupted by {name}",
            (f"Work remains: VACUUM of public.t_locked in database {one.name} "
             "was interrupted."),
            "after vacuuming 0 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )
    # Interrupted is not failed, and the run is not described as complete,
    # as merely incomplete, or as out of time.
    assert "VACUUMing public.t_locked failed." not in lines
    assert not [line for line in lines if line.startswith("Tables that failed")]
    assert "All tables vacuumed." not in lines
    assert "Not all tables were vacuumed." not in lines
    assert "Vacuuming halted due to timeout" not in lines
    assert f"working on database {two.name}" not in lines


@BOTH_SIGNALS
def test_interrupt_report_without_verbose(db, start_ff, signum, status):
    db.create_table("t_locked")

    with lock_held(db, "t_locked"):
        proc = start_ff(*FREEZE_ALL, "-d", db.name)
        wait_until_blocked(db)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)

    assert code == status
    assert stderr == ""
    assert stdout.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}",
        f"Work remains: VACUUM of public.t_locked in database {db.name} was interrupted.",
    ]


def test_interrupt_outranks_the_enforced_time_limit(db, start_ff, script_clock):
    # After the deadline, with --enforce-time, a cancelled VACUUM normally
    # means "stopped at the time limit" and exit status 0.  Not when the
    # cancel was the script's own, sent because of a signal.
    db.create_table("t_big")
    before = db.frozenxids()

    with lock_held(db, "t_big"):
        proc = start_ff(
            *FREEZE_ALL, "--enforce-time", "-m", "1", "-d", db.name,
            env=script_clock.env,
        )
        wait_until_blocked(db)
        script_clock.advance(3600)
        proc.send_signal(signal.SIGINT)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(db.cluster)

    assert code == 130
    assert "Traceback" not in stderr
    assert db.frozen_since(before) == set()
    assert stdout.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        f"Work remains: VACUUM of public.t_big in database {db.name} was interrupted.",
    ]


def test_interrupt_outranks_a_failure_and_the_failure_is_still_reported(
    make_db, start_ff
):
    # A table has already failed when the signal arrives.  Left alone the
    # run would exit 1; interrupted it exits 130, and the summary still
    # lists what failed and counts what was done.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_fails")
    one.create_table("t_done")
    two.create_table("t_locked")
    two.create_table("t_never")
    # In `one` a lock wait is an error after a moment; in `two` it is a wait.
    one.execute(f'ALTER DATABASE "{one.name}" SET lock_timeout = \'200ms\'')
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "t_fails"), lock_held(two, "t_locked"):
        proc = start_ff(*FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}")
        wait_until_blocked(two)
        proc.send_signal(signal.SIGTERM)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == 143
    assert "Traceback" not in stderr
    assert one.frozen_since(before_one) == {("public", "t_done")}
    assert two.frozen_since(before_two) == set()
    assert targets(two.script_vacuums()) == [("public", "t_locked")]
    assert_lines_in_order(
        stdout,
        [
            "VACUUMing public.t_fails failed.",
            "Vacuuming interrupted by SIGTERM",
            (f"Work remains: VACUUM of public.t_locked in database {two.name} "
             "was interrupted."),
            f"Tables that failed: public.t_fails in database {one.name}",
            "after vacuuming 1 tables in 2 databases",
            "Flexible Freeze run complete",
        ],
    )
    assert "VACUUMing public.t_locked failed." not in stdout


def test_second_signal_gives_up_on_a_server_that_does_not_answer(db, start_ff):
    # The backend is stopped (SIGSTOP), so it can neither act on the cancel
    # request nor answer the script, which therefore cannot exit on the
    # first signal however long it waits.  The second one ends the wait.
    # The first still decides the exit status, and the summary no longer
    # claims that the VACUUM was cancelled.
    db.create_table("t_locked")
    before = db.frozenxids()

    with lock_held(db, "t_locked"):
        proc = start_ff(*FREEZE_ALL, "-d", db.name)
        backend = wait_until_blocked(db)
        os.kill(backend, signal.SIGSTOP)
        try:
            proc.send_signal(signal.SIGINT)
            proc.send_signal(signal.SIGTERM)
            code, stdout, stderr = finish(proc)
        finally:
            os.kill(backend, signal.SIGCONT)
        # The cancel request was sent before the script gave up: once the
        # backend runs again it acts on it, and goes away without the lock
        # having been released.
        assert_no_backend_left(db.cluster)

    assert code == 130
    assert stderr == ""
    assert stdout.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        (f"Work remains: VACUUM of public.t_locked in database {db.name} was "
         "interrupted; the server did not confirm that it was cancelled, and it "
         "may still be running there."),
    ]
    assert db.frozen_since(before) == set()


# -- between two statements ---------------------------------------------------


def test_signal_while_idle_between_statements_stops_the_next_vacuum(
    db, start_ff, script_clock
):
    # The signal arrives while the script has nothing running on the server:
    # after the table list, before anything is sent for the first table.
    # Nothing is waiting on the server at that moment, so nothing is there to
    # cancel; the VACUUM that follows has to be cancelled once it is under
    # way.  Held up behind a lock, it would otherwise wait for good.
    #
    # Getting the script to that point: t_gate's lock holds up the listing
    # query (but t_locked is listed first, being older); the clock is put on
    # hold while the script waits there; with the gate opened the script
    # runs on to its next reading of the clock, which is the time check
    # before the first table.
    db.create_table("t_locked")
    db.create_table("t_gate")
    before = db.frozenxids()

    with lock_held(db, "t_locked"), lock_held(db, "t_gate", BLOCKS_LISTING) as gate:
        proc = start_ff(*FREEZE_ALL, "-d", db.name, env=script_clock.env)
        wait_until_blocked(db)
        script_clock.hold()
        gate.rollback()
        script_clock.wait_until_held()
        wait_for(
            lambda: [row[1] for row in script_backends(db.cluster)] == ["idle"],
            what="the script's backend to be idle",
        )

        proc.send_signal(signal.SIGINT)
        script_clock.release()
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(db.cluster)

    assert code == 130
    assert "Traceback" not in stderr
    assert db.frozen_since(before) == set()
    assert stdout.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        f"Work remains: VACUUM of public.t_locked in database {db.name} was interrupted.",
    ]


# -- during the pause between vacuums -----------------------------------------


@BOTH_SIGNALS
def test_signal_during_the_pause_ends_the_run(db, start_ff, tmp_path, signum, status):
    # With -v the script logs "processing table ..." and then takes the pause
    # owed since the previous vacuum, so that line for the second table says
    # it is at the pause.  The log goes to a file so that it can be read
    # while the script runs.
    db.create_table("t_first")
    db.create_table("t_second")
    before = db.frozenxids()
    log = tmp_path / "ff.log"

    proc = start_ff(*FREEZE_ALL, "-v", "-l", str(log), "--pause", "600", "-d", db.name)
    wait_for(
        lambda: log.exists() and "processing table public.t_second" in log.read_text(),
        what="the script to reach the pause before the second table",
    )
    signalled = time.monotonic()
    proc.send_signal(signum)
    code, _, stderr = finish(proc)
    elapsed = time.monotonic() - signalled

    assert code == status
    assert elapsed < PROMPT
    assert "Traceback" not in stderr
    assert targets(db.script_vacuums()) == [("public", "t_first")]
    assert db.frozen_since(before) == {("public", "t_first")}
    logged = log.read_text()
    assert_lines_in_order(
        logged,
        [
            f"Vacuuming interrupted by {signal.Signals(signum).name}",
            "after vacuuming 1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )
    # No VACUUM was cut short, so no table is named as unfinished.
    assert "Work remains" not in logged


# -- during the table-listing query -------------------------------------------


@BOTH_SIGNALS
def test_signal_during_the_table_listing_ends_the_run(
    make_db, start_ff, signum, status
):
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "t_locked", BLOCKS_LISTING):
        proc = start_ff(*FREEZE_ALL, "-d", f"{one.name},{two.name}")
        wait_until_blocked(one)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == status
    assert stderr == ""
    # Not "skipping database ... (couldn't list its tables ...)": the query
    # was cancelled on request, and the run does not go on to the next one.
    assert stdout.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]
    assert one.script_vacuums() == []
    assert two.script_statements() == []
    assert one.frozen_since(before_one) == set()
    assert two.frozen_since(before_two) == set()


# -- during a connection attempt ----------------------------------------------


@BOTH_SIGNALS
def test_signal_while_connecting_to_a_database_ends_the_run(
    start_ff, silent_server, signum, status
):
    proc = start_ff("-d", "ff_one,ff_two", port=silent_server.port)
    silent_server.wait_for_client()
    proc.send_signal(signum)
    code, stdout, stderr = finish(proc)

    assert code == status
    assert stderr == ""
    # Not "skipping database ff_one (couldn't connect ...)", and no attempt
    # on the second database.
    assert stdout.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]


@BOTH_SIGNALS
def test_signal_while_connecting_for_the_database_list_ends_the_run(
    start_ff, silent_server, signum, status
):
    # Without -d the first connection is to the postgres database, for the
    # list of databases.
    proc = start_ff(port=silent_server.port)
    silent_server.wait_for_client()
    proc.send_signal(signum)
    code, stdout, stderr = finish(proc)

    assert code == status
    assert stderr == ""
    assert stdout.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]


def test_signal_during_the_database_listing_query_ends_the_run(
    cluster, db, start_ff
):
    # The query that lists the databases cannot be held up through
    # pg_database itself: a new session needs that catalog just to start.
    # Planning the query, though, looks up column statistics, which opening
    # a session does not.  With pg_statistic locked the script connects and
    # then waits in that query.  The lock makes every other new query wait
    # as well, so the script's wait is observed from the session that holds
    # the lock, and in pg_locks, which unlike pg_stat_activity is read afresh
    # by each query of a transaction.  Nothing but the script is running, so
    # a lock request on pg_statistic that has not been granted is its own.
    db.create_table("t1")
    before = db.frozenxids()
    holder = cluster.connect(autocommit=False)
    try:
        with holder.cursor() as cur:
            cur.execute("LOCK TABLE pg_catalog.pg_statistic IN ACCESS EXCLUSIVE MODE")

            def script_is_waiting():
                cur.execute(
                    "SELECT 1 FROM pg_locks WHERE NOT granted "
                    "AND relation = 'pg_catalog.pg_statistic'::regclass"
                )
                return cur.fetchall()

            proc = start_ff(*FREEZE_ALL)
            wait_for(script_is_waiting, what="the script to wait in its database list")
            proc.send_signal(signal.SIGTERM)
            code, stdout, stderr = finish(proc)
    finally:
        holder.close()

    assert code == 143
    assert stderr == ""
    assert stdout.splitlines() == ["Vacuuming interrupted by SIGTERM"]
    assert db.script_statements() == []
    assert db.frozen_since(before) == set()


# -- the connection timeout ---------------------------------------------------


def test_connect_timeout_still_applies(run_ff, silent_server):
    # Not a signal test, but owed to the same mechanism: the script waits
    # for the server itself so that a signal can be acted on, and libpq
    # then leaves connect_timeout to the script.  A server that accepts and
    # never answers must still be given up on, as an unreachable database.
    started = time.monotonic()
    result = run_ff(
        "-d", "ff_one", env={"PGCONNECT_TIMEOUT": "2"}, port=silent_server.port
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 1
    assert 2 <= elapsed < 20
    lines = result.stdout.splitlines()
    assert lines[0].startswith("skipping database ff_one (couldn't connect: ")
    assert "timeout expired" in lines[0]
    assert "Databases skipped or left unfinished: ff_one" in lines
