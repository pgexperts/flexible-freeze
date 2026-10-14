# Tests for stopping a run with a signal.  SIGINT (Ctrl-C), SIGTERM (cron,
# systemd, kill) and SIGHUP (a terminal or an ssh session that goes away)
# must each, wherever in the run they arrive:
#
#   * cancel the statement in progress on the server, so that no VACUUM is
#     left running there with nobody waiting for it;
#   * start nothing further, in this database or a later one;
#   * print the usual summary, saying that the run was interrupted, and report
#     the table that was cut short as interrupted, not as failed;
#   * exit with status 130 (SIGINT), 143 (SIGTERM) or 129 (SIGHUP), whatever
#     status the run would otherwise have ended with.
#
# A second signal stops the wait for a server that does not confirm the
# cancellation, and the summary then says that the VACUUM may still be
# running.
#
# SIGHUP and SIGINT have one exception: a run that was started with the
# signal ignored (SIGHUP by nohup, SIGINT by a non-interactive shell that
# starts a background job) leaves it ignored and is not stopped by it; SIGTERM
# has none.  And a platform that has no SIGHUP must not keep the script from
# running.
#
# How a signal is made to arrive at a known point.  Each test waits for a
# state it can observe before signalling, never for a length of time:
#
# * during a VACUUM: another session keeps a pin on the table's only page,
#   which a freezing VACUUM has to wait for, and the test waits for the
#   script's backend to show up as waiting for it.  A lock on the table
#   would not hold the VACUUM: the script's VACUUM skips a locked table and
#   is over before a signal could arrive;
# * during the table-listing query: another session holds a lock the query
#   needs, the script is run with --lock-timeout 0 so that the wait does not
#   end by itself, and the test waits for the backend to show up as waiting;
#   for the --table kind lookup, which no lock can hold up without holding
#   up the lookup before it, the script is first stopped just before the
#   statement (kind_lookup_gate) and let go once the lock is in place; the
#   lookup of who holds the advisory lock is reached the same way
#   (holder_lookup_gate);
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
import json
import os
import signal
import time

import pytest
from ffharness import FREEZE_ALL, SCRIPT_APP, assert_lines_in_order, wait_for

# How long an interrupted script is given to exit.  Generous for a loaded
# machine; a script that does not act on the signal sits behind a lock, a
# silent server or a long pause for far longer than this.
PROMPT = 15.0

# The script inherits the test process's disposition of SIGHUP, and leaves
# an ignored one ignored.  So under a pytest that was itself started with
# the signal ignored (nohup pytest) a SIGHUP cannot stop the script, and the
# cases that send one have nothing to observe.
SIGHUP_REACHES_THE_SCRIPT = pytest.mark.skipif(
    signal.getsignal(signal.SIGHUP) is signal.SIG_IGN,
    reason="SIGHUP is ignored in this process, and so in the script it starts",
)

EACH_SIGNAL = pytest.mark.parametrize(
    "signum, status",
    [
        pytest.param(signal.SIGINT, 130, id="SIGINT"),
        pytest.param(signal.SIGTERM, 143, id="SIGTERM"),
        pytest.param(signal.SIGHUP, 129, id="SIGHUP", marks=SIGHUP_REACHES_THE_SCRIPT),
    ],
)

# Blocks everything, the listing query's pg_table_size() included.
BLOCKS_LISTING = "ACCESS EXCLUSIVE"

# Without this the script gives up a lock wait after ten seconds.  The tests
# that signal a script waiting for a lock do so well inside that, but on a
# loaded machine the wait must not be able to end first and be taken for
# the effect of the signal.
WAIT_FOR_LOCKS = ("--lock-timeout", "0")

# What a backend is waiting for, as pg_stat_activity.wait_event_type has it.
A_LOCK = "Lock"
A_PINNED_PAGE = "BufferPin"


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


@contextlib.contextmanager
def lock_held(db, table, mode):
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


@contextlib.contextmanager
def vacuum_held_up(db, table):
    """Keep a freezing VACUUM of public.<table> from finishing.

    A cursor that has returned one row and is still open keeps a pin on the
    page that row is on.  VACUUM (FREEZE) must freeze the rows of that page
    and needs the page to itself for it, so it waits for the pin to go:
    past the table's lock (the cursor's ACCESS SHARE does not conflict, so
    SKIP_LOCKED does not come into it) and not waiting for any lock (so
    lock_timeout does not either).  The wait can be cancelled like any
    other.  Only a freezing VACUUM waits; a plain one leaves a pinned page
    for next time.  The table has to fit in one page, as the tables
    create_table builds by default do.

    Yields the connection; rolling it back closes the cursor, which drops
    the pin and lets the VACUUM finish.
    """
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'DECLARE ff_pin NO SCROLL CURSOR FOR SELECT id FROM public."{table}"'
            )
            cur.execute("FETCH 1 FROM ff_pin")
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


def wait_until_blocked(db, on=A_PINNED_PAGE):
    """Return, once the script's backend in `db` is waiting for `on`, the
    process id of that backend.

    The default is the wait of a VACUUM held up by vacuum_held_up(); the
    table-listing query behind a lock is A_LOCK.
    """

    def probe():
        for datname, state, waiting_on, pid in script_backends(db.cluster):
            if (datname, state, waiting_on) == (db.name, "active", on):
                return pid
        return None

    return wait_for(probe, what=f"the script to wait ({on}) in {db.name}")


def assert_no_backend_left(cluster):
    """The script's sessions are gone from the server.

    Called while the lock or pin that blocked the statement is still held:
    a statement that was merely abandoned by its client is then still there,
    waiting, and would run as soon as the obstacle went away.
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


@EACH_SIGNAL
def test_signal_during_a_vacuum_cancels_it_and_ends_the_run(
    make_db, start_ff, tmp_path, signum, status
):
    # t_held is created first, so it is the oldest table and is taken
    # first: everything else in the run would come after it.
    #
    # Run with a log file: it is where the report of the interrupt and the
    # progress lines are found in one sequence, so that the place of the
    # report in the run can be asserted on.  The report is on stderr too.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_held")
    one.create_table("t_next")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()
    log = tmp_path / "ff.log"

    with vacuum_held_up(one, "t_held"):
        proc = start_ff(
            *FREEZE_ALL, "-v", "-l", str(log), "-d", f"{one.name},{two.name}"
        )
        wait_until_blocked(one)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == status
    assert "Traceback" not in stderr
    # Nothing was vacuumed, then or after the pin went away, and nothing
    # was started after the interrupted table: not the next table, and not
    # so much as a connection to the next database.
    assert targets(one.script_vacuums()) == [("public", "t_held")]
    assert one.frozen_since(before_one) == set()
    assert two.script_statements() == []
    assert two.frozen_since(before_two) == set()

    name = signal.Signals(signum).name
    # The report of the interrupt is a failure report and so on stderr; the
    # progress lines are in the log file, which has both
    # (test_output_streams.py has the rule).
    assert stdout == ""
    lines = log.read_text().splitlines()
    failures = stderr.splitlines()
    assert_lines_in_order(
        stderr,
        [
            f"Vacuuming interrupted by {name}",
            (f"Work remains: VACUUM of public.t_held in database {one.name} "
             "was interrupted."),
        ],
    )
    assert_lines_in_order(
        log.read_text(),
        [
            "processing table public.t_held",
            f"Vacuuming interrupted by {name}",
            (f"Work remains: VACUUM of public.t_held in database {one.name} "
             "was interrupted."),
            "after vacuuming 0 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )
    # Interrupted is not failed, and the run is not described as complete,
    # as merely incomplete, or as out of time.
    assert "VACUUMing public.t_held failed." not in lines + failures
    assert not [
        line for line in lines + failures if line.startswith("Tables that failed")
    ]
    assert "All tables vacuumed." not in lines
    assert "Not all tables were vacuumed." not in lines + failures
    assert "Vacuuming halted due to timeout" not in lines
    assert f"working on database {two.name}" not in lines


@EACH_SIGNAL
def test_interrupt_report_without_verbose(db, start_ff, signum, status):
    db.create_table("t_held")

    with vacuum_held_up(db, "t_held"):
        proc = start_ff(*FREEZE_ALL, "-d", db.name)
        wait_until_blocked(db)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)

    assert code == status
    assert stdout == ""
    assert stderr.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]


def test_signal_cancels_a_vacuum_sent_with_a_comment(db, start_ff):
    # --comment changes the text of every statement and nothing about how
    # one is sent or waited for.
    db.create_table("t_held")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"):
        proc = start_ff(*FREEZE_ALL, "-c", "app:nightly 100%", "-d", db.name)
        wait_until_blocked(db)
        proc.send_signal(signal.SIGTERM)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(db.cluster)

    assert code == 143
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGTERM",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("public", "t_held")]
    assert vacuums[0].sql.endswith(" /* app:nightly 100% */")
    assert db.frozen_since(before) == set()


def test_interrupt_outranks_the_enforced_time_limit(db, start_ff, script_clock):
    # After the deadline, with --enforce-time, a cancelled VACUUM normally
    # means "stopped at the time limit" and exit status 0.  Not when the
    # cancel was the script's own, sent because of a signal.
    db.create_table("t_big")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_big"):
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
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        f"Work remains: VACUUM of public.t_big in database {db.name} was interrupted.",
    ]


def test_interrupt_outranks_a_failure_and_the_failure_is_still_reported(
    make_db, make_role, start_ff
):
    # A table has already failed when the signal arrives.  Left alone the
    # run would exit 1; interrupted it exits 130, and the summary still
    # lists what failed and counts what was done.
    #
    # The failure: the run is made as a role that has no USAGE on the schema
    # of t_fails, so the server refuses that VACUUM with an error.  The role
    # owns the other tables and vacuums them like anyone.
    role = make_role("owner")
    one, two = make_db("one"), make_db("two")
    one.create_schema("vault")
    one.create_table("t_fails", schema="vault")
    one.create_table("t_done", owner=role)
    two.create_table("t_held", owner=role)
    two.create_table("t_never", owner=role)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with vacuum_held_up(two, "t_held"):
        proc = start_ff(
            *FREEZE_ALL, "-v", "-U", role, "-d", f"{one.name},{two.name}"
        )
        wait_until_blocked(two)
        proc.send_signal(signal.SIGTERM)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == 143
    assert "Traceback" not in stderr
    assert one.frozen_since(before_one) == {("public", "t_done")}
    assert two.frozen_since(before_two) == set()
    assert targets(two.script_vacuums()) == [("public", "t_held")]
    assert_lines_in_order(
        stderr,
        [
            "VACUUMing vault.t_fails failed.",
            "permission denied for schema vault",
            "Vacuuming interrupted by SIGTERM",
            (f"Work remains: VACUUM of public.t_held in database {two.name} "
             "was interrupted."),
            f"Tables that failed: vault.t_fails in database {one.name}",
        ],
    )
    assert_lines_in_order(
        stdout,
        ["after vacuuming 1 tables in 2 databases", "Flexible Freeze run complete"],
    )
    assert "VACUUMing public.t_held failed." not in stdout + stderr


def test_second_signal_gives_up_on_a_server_that_does_not_answer(db, start_ff):
    # The backend is stopped (SIGSTOP), so it can neither act on the cancel
    # request nor answer the script, which therefore cannot exit on the
    # first signal however long it waits.  The second one ends the wait.
    # The first still decides the exit status, and the summary no longer
    # claims that the VACUUM was cancelled.
    db.create_table("t_held")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"):
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
        # backend runs again it acts on it, and goes away without the pin
        # having been released.
        assert_no_backend_left(db.cluster)

    assert code == 130
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        (f"Work remains: VACUUM of public.t_held in database {db.name} was "
         "interrupted; the server did not confirm that it was cancelled, and it "
         "may still be running there."),
    ]
    assert db.frozen_since(before) == set()


def test_sighup_ignored_when_the_run_started_stays_ignored(db, start_ff):
    # nohup starts a command with SIGHUP ignored, so that it outlives the
    # terminal it was started from.  The script must not undo that by
    # installing its handler.
    #
    # The signal is sent while the first VACUUM is held up, and the pin is
    # then let go.  A signal that is ignored is discarded as it is sent, and
    # one that is not is pending from that moment on, so there is nothing to
    # wait for in between: a run that had taken any notice of it could not
    # go on to vacuum both tables and exit with status 0.
    db.create_table("t_held")
    db.create_table("t_next")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held") as pin:
        proc = start_ff(*FREEZE_ALL, "-d", db.name, ignore_signals=(signal.SIGHUP,))
        wait_until_blocked(db)
        proc.send_signal(signal.SIGHUP)
        pin.rollback()
        code, stdout, stderr = finish(proc)

    assert code == 0
    assert stderr == ""
    assert stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {("public", "t_held"), ("public", "t_next")}


def test_run_started_with_sighup_ignored_still_stops_on_sigterm(db, start_ff):
    # Leaving SIGHUP alone must not cost the run its other two signals.
    db.create_table("t_held")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"):
        proc = start_ff(*FREEZE_ALL, "-d", db.name, ignore_signals=(signal.SIGHUP,))
        wait_until_blocked(db)
        proc.send_signal(signal.SIGHUP)
        proc.send_signal(signal.SIGTERM)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(db.cluster)

    assert code == 143
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGTERM",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]
    assert db.frozen_since(before) == set()


def test_sigint_ignored_when_the_run_started_stays_ignored(db, start_ff):
    # A background job of a non-interactive shell (`cmd &` in a script) is
    # started with SIGINT ignored, so that the Ctrl-C meant for the script
    # does not reach it.  The script must not undo that by installing its
    # handler.  Same reasoning as for SIGHUP above: the signal is sent while
    # the first VACUUM is held up, and a run that took any notice of it
    # could not go on to vacuum both tables and exit with status 0.
    db.create_table("t_held")
    db.create_table("t_next")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held") as pin:
        proc = start_ff(*FREEZE_ALL, "-d", db.name, ignore_signals=(signal.SIGINT,))
        wait_until_blocked(db)
        proc.send_signal(signal.SIGINT)
        pin.rollback()
        code, stdout, stderr = finish(proc)

    assert code == 0
    assert stderr == ""
    assert stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {("public", "t_held"), ("public", "t_next")}


def test_run_started_with_sigint_ignored_still_stops_on_sigterm(db, start_ff):
    # SIGTERM is deliberately not given the check: an inherited ignore of it
    # is rare, and the operator's kill must work.
    db.create_table("t_held")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"):
        proc = start_ff(*FREEZE_ALL, "-d", db.name, ignore_signals=(signal.SIGINT,))
        wait_until_blocked(db)
        proc.send_signal(signal.SIGINT)
        proc.send_signal(signal.SIGTERM)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(db.cluster)

    assert code == 143
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGTERM",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]
    assert db.frozen_since(before) == set()


def test_run_on_a_platform_without_sighup(db, run_ff, tmp_path):
    # The other case in which SIGHUP is left alone.  The script is started
    # through a sitecustomize module that takes SIGHUP out of the signal
    # module, which is what the script finds on a platform that has no such
    # signal.  (Found through PYTHONPATH, as in test_interpreter.py.)
    (tmp_path / "sitecustomize.py").write_text("import signal\ndel signal.SIGHUP\n")
    inherited = os.environ.get("PYTHONPATH")
    path = [str(tmp_path)] + ([inherited] if inherited else [])
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "-d", db.name, env={"PYTHONPATH": os.pathsep.join(path)}
    )

    assert result.stderr == ""
    assert result.stdout == "All tables vacuumed.\n"
    assert result.returncode == 0
    assert db.frozen_since(before) == {("public", "t1")}


# -- between two statements ---------------------------------------------------


def test_signal_while_idle_between_statements_stops_the_next_vacuum(
    db, start_ff, script_clock
):
    # The signal arrives while the script has nothing running on the server:
    # after the table list, before anything is sent for the first table.
    # Nothing is waiting on the server at that moment, so nothing is there to
    # cancel; the VACUUM that follows has to be cancelled once it is under
    # way.  Held up by a pinned page, it would otherwise wait for good.
    #
    # Getting the script to that point: t_gate's lock holds up the listing
    # query (but t_held is listed first, being older); the clock is put on
    # hold while the script waits there; with the gate opened the script
    # runs on to its next reading of the clock, which is the time check
    # before the first table.
    db.create_table("t_held")
    db.create_table("t_gate")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"), lock_held(db, "t_gate", BLOCKS_LISTING) as gate:
        proc = start_ff(
            *FREEZE_ALL, *WAIT_FOR_LOCKS, "-d", db.name, env=script_clock.env
        )
        wait_until_blocked(db, A_LOCK)
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
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGINT",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]


# -- during the pause between vacuums -----------------------------------------


@EACH_SIGNAL
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


@EACH_SIGNAL
def test_signal_during_the_table_listing_ends_the_run(
    make_db, start_ff, signum, status
):
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "t_locked", BLOCKS_LISTING):
        proc = start_ff(
            *FREEZE_ALL, *WAIT_FOR_LOCKS, "-d", f"{one.name},{two.name}"
        )
        wait_until_blocked(one, A_LOCK)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)
        assert_no_backend_left(one.cluster)

    assert code == status
    assert stdout == ""
    # Not "skipping database ... (couldn't list its tables ...)": the query
    # was cancelled on request, and the run does not go on to the next one.
    assert stderr.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]
    assert one.script_vacuums() == []
    assert two.script_statements() == []
    assert one.frozen_since(before_one) == set()
    assert two.frozen_since(before_two) == set()


@EACH_SIGNAL
def test_signal_during_the_kind_lookup_ends_the_run(
    make_db, start_ff, kind_lookup_gate, signum, status
):
    # The --table lookup's second statement, which asks what a name that
    # is no table is (a view, here).  It reads the same catalogs as the
    # first, so the script is stopped just before it (kind_lookup_gate),
    # pg_class of that database is locked, and then the script is let go
    # into the wait.
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        database.create_schema("sales")
    one.create_table("items", schema="sales")
    one.execute("CREATE VIEW sales.orders AS SELECT * FROM sales.items")
    two.create_table("orders", schema="sales")
    before_two = two.frozenxids()

    proc = start_ff(
        *FREEZE_ALL, "-v", *WAIT_FOR_LOCKS, "--table", "orders",
        "-d", f"{one.name},{two.name}", env=kind_lookup_gate.env,
    )
    kind_lookup_gate.wait_until_held()
    blocker = one.connect(autocommit=False)
    try:
        with blocker.cursor() as cur:
            cur.execute(f"LOCK TABLE pg_catalog.pg_class IN {BLOCKS_LISTING} MODE")
        kind_lookup_gate.release()
        wait_until_blocked(one, A_LOCK)
        proc.send_signal(signum)
        code, stdout, stderr = finish(proc)
        # While the lock is still held: an abandoned statement would be there.
        assert_no_backend_left(one.cluster)
    finally:
        blocker.close()

    assert code == status, stdout + stderr
    # Not a skipped database, not a missing table: the statement was
    # cancelled on request and the run does not go on.
    assert stderr.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]
    assert "specified single table" not in stdout
    assert "after vacuuming 0 tables in 0 databases" in stdout.splitlines()
    assert one.script_vacuums() == []
    assert two.script_statements() == []
    assert two.frozen_since(before_two) == set()


# -- while looking up who holds the advisory lock -----------------------------


@EACH_SIGNAL
def test_signal_during_the_advisory_lock_holder_lookup_ends_the_run(
    make_db, start_ff, holder_lookup_gate, signum, status
):
    # A run refused the advisory lock asks who holds it.  The script is
    # stopped just before it asks (holder_lookup_gate), the pg_locks view of
    # that database is locked, and the script is let go into the wait.  The
    # signal must end the run as anywhere else: not the database reported
    # as skipped, with or without a holder, and no further database.
    one, two = make_db("one"), make_db("two")
    one.create_table("t1")
    two.create_table("t_other")
    before_two = two.frozenxids()

    holder = one.connect()
    try:
        with holder.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(7380385457142659685)")
        proc = start_ff(
            *FREEZE_ALL, "-v", *WAIT_FOR_LOCKS, "-d", f"{one.name},{two.name}",
            env=holder_lookup_gate.env,
        )
        holder_lookup_gate.wait_until_held()
        blocker = one.connect(autocommit=False)
        try:
            with blocker.cursor() as cur:
                cur.execute(f"LOCK TABLE pg_catalog.pg_locks IN {BLOCKS_LISTING} MODE")
            holder_lookup_gate.release()
            wait_until_blocked(one, A_LOCK)
            proc.send_signal(signum)
            code, stdout, stderr = finish(proc)
            # While the lock is still held: an abandoned statement would be there.
            assert_no_backend_left(one.cluster)
        finally:
            blocker.close()
    finally:
        holder.close()

    assert code == status, stdout + stderr
    assert stderr.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]
    assert "after vacuuming 0 tables in 0 databases" in stdout.splitlines()
    assert one.script_vacuums() == []
    assert two.script_statements() == []
    assert two.frozen_since(before_two) == set()


# -- during a connection attempt ----------------------------------------------


@EACH_SIGNAL
def test_signal_while_connecting_to_a_database_ends_the_run(
    start_ff, silent_server, signum, status
):
    proc = start_ff("-d", "ff_one,ff_two", port=silent_server.port)
    silent_server.wait_for_client()
    proc.send_signal(signum)
    code, stdout, stderr = finish(proc)

    assert code == status
    assert stdout == ""
    # Not "skipping database ff_one (couldn't connect ...)", and no attempt
    # on the second database.
    assert stderr.splitlines() == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}"
    ]


@EACH_SIGNAL
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
    assert stdout == ""
    assert stderr.splitlines() == [
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

            # The listing session has a lock timeout too; it must not end
            # the wait (exit 1) before the signal does.
            proc = start_ff(*FREEZE_ALL, *WAIT_FOR_LOCKS)
            wait_for(script_is_waiting, what="the script to wait in its database list")
            proc.send_signal(signal.SIGTERM)
            code, stdout, stderr = finish(proc)
    finally:
        holder.close()

    assert code == 143
    assert stdout == ""
    assert stderr.splitlines() == ["Vacuuming interrupted by SIGTERM"]
    assert db.script_statements() == []
    assert db.frozen_since(before) == set()


# -- the connection timeout ---------------------------------------------------


def test_connect_timeout_still_applies(run_ff, silent_server):
    # Not a signal test, but owed to the same mechanism: the script waits
    # for the server itself so that a signal can be acted on, and libpq
    # then leaves connect_timeout to the script.  A server that accepts and
    # never answers must still be given up on, as an unreachable database.
    # Only PGCONNECT_TIMEOUT is exercised here: a connect_timeout in a
    # service file reaches the same read, but the harness refuses
    # PGSERVICEFILE, so that source is untested by design (ADR 0011).
    started = time.monotonic()
    result = run_ff(
        "-d", "ff_one", env={"PGCONNECT_TIMEOUT": "2"}, port=silent_server.port
    )
    elapsed = time.monotonic() - started

    assert result.returncode == 1
    assert 2 <= elapsed < 20
    lines = result.stderr.splitlines()
    assert lines[0].startswith("skipping database ff_one (couldn't connect: ")
    assert "timeout expired" in lines[0]
    assert "Databases skipped or left unfinished: ff_one" in lines


# What libpq accepts as connect_timeout, and the script has to accept too:
# an optional sign and ASCII digits, with white space around them.  A
# negative value means no timeout.
@pytest.mark.parametrize(
    "value",
    [" 10", "10 ", "+5", "-5", "010",
     # libpq reads this with strtol, which has no digit limit: 5
     pytest.param("0" * 4400 + "5", id="leading-zeros-4400")],
)
def test_connect_timeout_libpq_accepts_is_accepted(db, run_ff, value):
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name, env={"PGCONNECT_TIMEOUT": value})

    assert result.stderr == ""
    assert result.stdout == "All tables vacuumed.\n"
    assert result.returncode == 0
    assert db.frozen_since(before) == {("public", "t1")}


# The check is the script's own, since libpq does not validate the value on a
# connection driven by a wait callback.  Every value here is refused by libpq
# itself when it connects over TCP.  The run ends at the first connection
# with one message (the value shown as JSON, so that it is plain ASCII on one
# line), instead of blaming each database for it.
@pytest.mark.parametrize(
    "value",
    ["10s", "abc", "10.5", "1_0", "1e1", "0x10", "+", "", " ", "\u0663",
     "2147483648", "-2147483649", "99999999999", "-99999999999",
     pytest.param("9" * 4301, id="digits-4301")],
)
@pytest.mark.parametrize("named", [False, True], ids=["listing", "named"])
def test_connect_timeout_that_is_not_a_whole_number_ends_the_run(
    db, run_ff, value, named
):
    db.create_table("t1")
    before = db.frozenxids()
    args = ["-d", f"{db.name},postgres"] if named else []

    result = run_ff(*FREEZE_ALL, *args, env={"PGCONNECT_TIMEOUT": value})

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.splitlines() == [
        f"connect_timeout is {json.dumps(value)}, not a whole number of seconds"
    ]
    assert db.script_statements() == []
    assert db.frozen_since(before) == set()
