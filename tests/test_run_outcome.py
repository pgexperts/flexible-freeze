# Tests for what a run reports about itself when not everything went to plan:
# a VACUUM that fails, a table the server declines to vacuum, a database that
# cannot be reached or listed, a connection lost half-way, a vacuum cancelled
# at the time limit.  In each case the run has to carry on with whatever is
# still possible, say what was left undone, and exit with a status a
# scheduler can act on:
#
#   0  every selected table was vacuumed, or the run stopped at its time limit
#   1  a table failed or was not vacuumed, a database was skipped or left
#      unfinished, or --table matched nothing in any database
#
# Also here, because they decide the same summary: the pause between vacuums
# and the report of exclusions that matched nothing.
#
# The lines that report a failure are on stderr and the rest of the output is
# on stdout, so each is asserted on where it belongs; the rule for which is
# which, and the order of the two taken together, is in test_output_streams.py.
#
# How the failures are produced:
#
# * A VACUUM fails when it names a table in a schema the connecting role may
#   not use: the server refuses the statement with an error before it does
#   anything to the table.  A lock on the table no longer produces a failure:
#   the script's VACUUM passes over a locked table, which is a different
#   outcome with a report of its own (test_locks.py).
# * A VACUUM is kept from finishing by another session's pin on the table's
#   only page.  A freezing VACUUM has to have that page to itself, so it
#   waits, already holding the table's lock and genuinely under way, until
#   the pin goes.  That gives the test time to cancel or terminate the
#   script's backend, or to move its clock.  A lock would not do here either:
#   the VACUUM would be over, skipped, before the test could act.
# * The table-listing query fails behind an ACCESS EXCLUSIVE lock, once the
#   script's --lock-timeout has run out: it calls pg_table_size() on every
#   candidate.
# * Deadlines are reached through the script_clock fixture; the script's
#   window cannot be shorter than a minute otherwise.

import contextlib
import re
import time

import pytest
from ffharness import FREEZE_ALL, SCRIPT_APP, assert_lines_in_order, wait_for

NO_SUCH_DB = "ff_no_such_database"
TIMESTAMPED = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^:]*: .+")

# Blocks everything, the listing query's pg_table_size() included.
BLOCKS_LISTING = "ACCESS EXCLUSIVE"

# A schema the roles made by make_role have no USAGE on, and what the server
# says to a VACUUM that names a table in it.
CLOSED_SCHEMA = "vault"
SCHEMA_DENIED = f"permission denied for schema {CLOSED_SCHEMA}"


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def table_the_role_cannot_reach(db, name):
    """Create <CLOSED_SCHEMA>.<name>, whose VACUUM is an error for any role
    but a superuser.

    The table is still listed: the selection reads pg_class and the size
    functions check no privilege.  It is the VACUUM, which looks the table up
    by name, that is refused, and for the lack of USAGE on the schema that is
    an ERROR.  (For a table the role can reach and does not own it is only a
    WARNING; that case is further down.)
    """
    db.create_schema(CLOSED_SCHEMA)
    return db.create_table(name, schema=CLOSED_SCHEMA)


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
    lock_timeout does not either).  Only a freezing VACUUM waits; a plain
    one leaves a pinned page for next time.  The table has to fit in one
    page, as the tables create_table builds by default do.

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


def waiting_backend(db):
    """The pid of the script's backend in `db`, once its VACUUM is waiting
    for the page that vacuum_held_up() has pinned."""

    def probe():
        rows = db.cluster.execute(
            "postgres",
            "SELECT pid FROM pg_stat_activity WHERE application_name = %s "
            "AND datname = %s AND wait_event_type = 'BufferPin'",
            (SCRIPT_APP, db.name),
        )
        return rows[0][0] if rows else None

    return wait_for(
        probe, what=f"the script's VACUUM to wait for a pinned page in {db.name}"
    )


def finish(proc, timeout=30.0):
    """Wait for a script started with start_ff; return (status, stdout, stderr)."""
    stdout, stderr = proc.communicate(timeout=timeout)
    return proc.returncode, stdout, stderr


# -- a VACUUM that fails ------------------------------------------------------


def test_failed_table_is_reported_once_and_the_run_continues(
    make_db, make_role, run_ff
):
    # t_denied is created first, so it is the oldest table and is taken
    # first: everything else in the run comes after the failure.
    role = make_role("owner")
    one, two = make_db("one"), make_db("two")
    denied = table_the_role_cannot_reach(one, "t_denied")
    one.create_table("t_next", owner=role)
    two.create_table("t_other", owner=role)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-U", role, "-d", f"{one.name},{two.name}")

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    # Later tables of the same database, and later databases, are still done.
    assert targets(one.script_vacuums()) == [denied, ("public", "t_next")]
    assert one.frozen_since(before_one) == {("public", "t_next")}
    assert two.frozen_since(before_two) == {("public", "t_other")}

    lines = result.stdout.splitlines()
    failures = result.stderr.splitlines()
    # Once, and with the table's whole name.
    assert failures.count(f"VACUUMing {CLOSED_SCHEMA}.t_denied failed.") == 1
    assert not [line for line in lines + failures if line.startswith("VACUUMING ")]
    assert result.stderr.count(SCHEMA_DENIED) == 1
    assert SCHEMA_DENIED not in result.stdout
    assert "All tables vacuumed." not in lines
    assert_lines_in_order(
        result.stderr,
        [
            f"VACUUMing {CLOSED_SCHEMA}.t_denied failed.",
            "Not all tables were vacuumed.",
            f"Tables that failed: {CLOSED_SCHEMA}.t_denied in database {one.name}",
        ],
    )
    assert_lines_in_order(
        result.stdout,
        [
            "processing table public.t_next",
            f"working on database {two.name}",
            # The failed table is not counted; the two that worked are.
            "2 tables in 2 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_failure_report_without_verbose(db, make_role, run_ff):
    role = make_role("owner")
    table_the_role_cannot_reach(db, "t_denied")

    result = run_ff(*FREEZE_ALL, "-U", role, "-d", db.name)

    assert result.returncode == 1
    assert result.stderr.splitlines() == [
        f"VACUUMing {CLOSED_SCHEMA}.t_denied failed.",
        SCHEMA_DENIED,
        "Not all tables were vacuumed.",
        f"Tables that failed: {CLOSED_SCHEMA}.t_denied in database {db.name}",
    ]
    assert result.stdout == ""


def test_cancelled_vacuum_inside_the_window_is_a_failure(db, start_ff):
    # A cancel that the time limit did not ask for (pg_cancel_backend by an
    # operator, say) is a failed table like any other, with or without
    # --enforce-time, and the run goes on to the next table.
    db.create_table("t_held")
    db.create_table("t_next")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_held"):
        proc = start_ff(*FREEZE_ALL, "--enforce-time", "-d", db.name)
        pid = waiting_backend(db)
        db.cluster.execute("postgres", "SELECT pg_cancel_backend(%s)", (pid,))
        status, stdout, stderr = finish(proc)

    assert status == 1
    assert "Traceback" not in stderr
    assert db.frozen_since(before) == {("public", "t_next")}
    failures = stderr.splitlines()
    assert failures.count("VACUUMing public.t_held failed.") == 1
    assert "Vacuuming halted due to timeout" not in stdout.splitlines()
    assert f"Tables that failed: public.t_held in database {db.name}" in failures


def test_lost_connection_ends_that_database_and_the_run_continues(
    make_db, start_ff, tmp_path
):
    # Run with a log file: it is where the failure reports and the progress
    # lines are found in one sequence, so that the place of each report in
    # the run can be asserted on.  The reports are on stderr as well.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_held")
    one.create_table("t_next")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()
    logfile = tmp_path / "ff.log"

    with vacuum_held_up(one, "t_held"):
        proc = start_ff(
            *FREEZE_ALL, "-v", "-l", str(logfile), "-d", f"{one.name},{two.name}"
        )
        pid = waiting_backend(one)
        one.cluster.execute("postgres", "SELECT pg_terminate_backend(%s)", (pid,))
        status, stdout, stderr = finish(proc)

    assert status == 1
    assert "Traceback" not in stderr
    # Nothing more is attempted in the database whose connection is gone, and
    # no second connection to it is made.
    assert targets(one.script_vacuums()) == [("public", "t_held")]
    assert one.frozen_since(before_one) == set()
    assert len({record.pid for record in one.cluster.statements(dbname=one.name)}) == 1
    assert two.frozen_since(before_two) == {("public", "t_other")}
    assert_lines_in_order(
        stderr,
        [
            "VACUUMing public.t_held failed.",
            f"skipping the rest of database {one.name} (connection lost)",
            "Not all tables were vacuumed.",
            f"Tables that failed: public.t_held in database {one.name}",
            f"Databases skipped or left unfinished: {one.name}",
        ],
    )
    assert stdout == ""
    assert_lines_in_order(
        logfile.read_text(),
        [
            "VACUUMing public.t_held failed.",
            f"skipping the rest of database {one.name} (connection lost)",
            f"working on database {two.name}",
            "Not all tables were vacuumed.",
            f"Tables that failed: public.t_held in database {one.name}",
            f"Databases skipped or left unfinished: {one.name}",
            "1 tables in 2 databases",
        ],
    )


# -- a table the server declines to vacuum ------------------------------------


@pytest.mark.parametrize(
    "hide_warnings",
    [False, True],
    ids=["default-message-level", "database-hides-warnings"],
)
def test_table_the_role_may_not_vacuum_is_reported_and_not_counted(
    db, make_role, run_ff, hide_warnings
):
    # For a table the role may not vacuum the server raises a WARNING and
    # reports success.  The wording differs between server versions and is
    # translated, so only its presence is asserted on.  A database that
    # hides warnings from its clients must not hide the skip as well.
    role = make_role("owner")
    db.create_table("t_own", owner=role)
    db.create_table("t_other")
    if hide_warnings:
        db.execute(f'ALTER DATABASE "{db.name}" SET client_min_messages = error')
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-U", role, "-d", db.name)

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    # Both were attempted; only the role's own table was vacuumed.
    assert set(targets(db.script_vacuums())) == {("public", "t_own"), ("public", "t_other")}
    assert db.frozen_since(before) == {("public", "t_own")}

    lines = result.stdout.splitlines()
    failures = result.stderr.splitlines()
    assert "All tables vacuumed." not in lines
    report = failures.index(
        "VACUUMing public.t_other drew a message from the server; "
        "not counting it as vacuumed."
    )
    # The server's own text follows, as received.
    assert failures[report + 1].startswith("WARNING:")
    assert '"t_other"' in failures[report + 1]
    assert_lines_in_order(
        result.stderr,
        [
            "Not all tables were vacuumed.",
            f"Tables not counted as vacuumed: public.t_other in database {db.name}",
        ],
    )
    assert "1 tables in 1 databases" in lines
    assert "VACUUMing public.t_own" not in result.stdout + result.stderr


def test_messages_of_one_table_are_not_held_against_the_next(db, make_role, run_ff):
    # t_other is the older table and is taken first; the warning it draws
    # must be gone before t_own is vacuumed.
    role = make_role("owner")
    db.create_table("t_other")
    db.create_table("t_own", owner=role)

    result = run_ff(*FREEZE_ALL, "-U", role, "-d", db.name)

    assert result.returncode == 1
    assert targets(db.script_vacuums()) == [("public", "t_other"), ("public", "t_own")]
    lines = result.stderr.splitlines()
    assert lines[-1] == (
        f"Tables not counted as vacuumed: public.t_other in database {db.name}"
    )


# -- databases that cannot be worked on ---------------------------------------


@pytest.mark.parametrize("position", ["first", "last"])
def test_unreachable_database_is_reported_and_fails_the_run(db, run_ff, position):
    # ADR 0004: the run goes on without it.  Being skipped is still not
    # success, wherever in the list the database is.
    db.create_table("t1")
    before = db.frozenxids()
    names = [NO_SUCH_DB, db.name] if position == "first" else [db.name, NO_SUCH_DB]

    result = run_ff(*FREEZE_ALL, "-v", "-d", ",".join(names))

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == {("public", "t1")}
    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr
    lines = result.stdout.splitlines()
    assert "All tables vacuumed." not in lines
    assert_lines_in_order(
        result.stderr,
        [
            "Not all tables were vacuumed.",
            f"Databases skipped or left unfinished: {NO_SUCH_DB}",
        ],
    )
    assert_lines_in_order(
        result.stdout,
        [
            # Only the database that was worked on is counted.
            "1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_table_listing_failure_skips_that_database_only(make_db, run_ff):
    one, two = make_db("one"), make_db("two")
    # The timeout is the script's own (--lock-timeout); the database sets
    # none.  One second keeps the test short; the default and the timing are
    # in test_locks.py.
    one.create_table("t_locked")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "t_locked", BLOCKS_LISTING):
        result = run_ff(
            *FREEZE_ALL, "-v", "--lock-timeout", "1", "-d", f"{one.name},{two.name}"
        )

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert one.script_vacuums() == []
    assert one.frozen_since(before_one) == set()
    assert two.frozen_since(before_two) == {("public", "t_other")}
    assert (
        f"skipping database {one.name} (couldn't list its tables: "
        "canceling statement due to lock timeout"
    ) in result.stderr
    assert_lines_in_order(
        result.stderr,
        [
            "Not all tables were vacuumed.",
            f"Databases skipped or left unfinished: {one.name}",
        ],
    )
    assert_lines_in_order(
        result.stdout, [f"working on database {two.name}", "1 tables in 1 databases"]
    )


def test_rejected_session_setting_skips_the_database(db, run_ff):
    # The server refuses a vacuum_cost_delay above 100 ms.  That is found out
    # per database, when the session is set up, and reported like any other
    # database that could not be worked on.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--costdelay", "1000", "-d", db.name)

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == set()
    assert f"skipping database {db.name} (couldn't list its tables: " in result.stderr
    assert result.stderr.splitlines()[-2:] == [
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]


def test_table_option_that_matches_in_no_database_fails_the_run(make_db, run_ff):
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        database.create_table("items")

    result = run_ff(
        *FREEZE_ALL, "-t", "--table", "orders", "-d", f"{one.name},{two.name}"
    )

    assert result.returncode == 1
    assert one.script_vacuums() == two.script_vacuums() == []
    assert result.stdout == ""
    lines = result.stderr.splitlines()
    for line in lines:
        assert TIMESTAMPED.fullmatch(line), line
    messages = [line.split(": ", 1)[1] for line in lines]
    # Without -v the summary is all there is, and it names the databases;
    # the line for each database is a -v message (test_table_names.py).
    assert messages == [
        "Not all tables were vacuumed.",
        f"Databases where --table orders matched no table: {one.name}, {two.name}",
    ]


def test_table_option_missing_from_one_database_is_not_a_failure(make_db, run_ff):
    # A name given for several databases need not be in each of them.  The
    # miss is kept out of the summary and of the exit status, and without -v
    # out of the output altogether: this is a complete, clean run.
    one, two = make_db("one"), make_db("two")
    one.create_table("items")
    two.create_table("orders")
    before_two = two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-d", f"{one.name},{two.name}"
    )

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    assert two.frozen_since(before_two) == {("public", "orders")}


def test_table_option_excluded_where_it_exists_is_not_a_failure(make_db, run_ff):
    # The table was found, so the name is not a mistake; that an exclusion
    # then kept it from being vacuumed is reported, and nothing more.
    one, two = make_db("one"), make_db("two")
    one.create_table("items")
    two.create_table("orders")

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-T", "orders",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0
    assert one.script_vacuums() == two.script_vacuums() == []
    assert result.stdout.splitlines() == [
        (f"specified single table public.orders is excluded in database {two.name} "
         "by --exclude-table; not vacuuming it"),
        "All tables vacuumed.",
    ]


def test_table_option_missing_and_a_skipped_database_reports_both(db, run_ff):
    # The database that could not be reached might have held the table, but
    # nothing is known about it; the name counts as found nowhere among the
    # databases that were examined.
    db.create_table("items")

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-d", f"{db.name},{NO_SUCH_DB}"
    )

    assert result.returncode == 1
    assert result.stderr.splitlines()[-3:] == [
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {NO_SUCH_DB}",
        f"Databases where --table orders matched no table: {db.name}",
    ]


# -- the time limit -----------------------------------------------------------


def test_time_limit_ends_the_run_for_the_remaining_databases(make_db, run_ff):
    # Characterization: the limit is met at the first table of the first
    # database, and the second database is then not even connected to.
    one, two = make_db("one"), make_db("two")
    one.create_table("t1")
    two.create_table("t2")

    result = run_ff(*FREEZE_ALL, "-v", "-m", "0", "-d", f"{one.name},{two.name}")

    assert result.returncode == 0
    assert one.script_vacuums() == []
    assert two.cluster.statements(dbname=two.name) == []
    assert_lines_in_order(
        result.stdout,
        [
            "Reached time limit.  Exiting.",
            "Vacuuming halted due to timeout",
            "after vacuuming 0 tables in 1 databases",
        ],
    )


def test_vacuum_cancelled_at_the_enforced_limit_is_a_time_limited_stop(
    db, start_ff, script_clock
):
    # With --enforce-time the server cancels a vacuum that outlives the
    # window.  Here the test does the cancelling, after moving the script's
    # clock past the deadline: the script sees the same error, at a time
    # when it expects it.  That is the designed way for a run to end, not a
    # failure.
    db.create_table("t_big")
    db.create_table("t_later")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_big"):
        proc = start_ff(
            *FREEZE_ALL, "--enforce-time", "-m", "1", "-d", db.name,
            env=script_clock.env,
        )
        pid = waiting_backend(db)
        script_clock.advance(3600)
        db.cluster.execute("postgres", "SELECT pg_cancel_backend(%s)", (pid,))
        status, stdout, stderr = finish(proc)

    assert status == 0
    assert "Traceback" not in stderr
    assert stdout.splitlines() == [
        "Vacuuming halted due to timeout",
        (f"Work remains: VACUUM of public.t_big in database {db.name} "
        "was cancelled at the time limit."),
    ]
    # The run stopped there.
    assert targets(db.script_vacuums()) == [("public", "t_big")]
    assert db.frozen_since(before) == set()


def test_time_limited_stop_with_verbose_counts_only_finished_tables(
    db, start_ff, script_clock
):
    db.create_table("t_big")

    with vacuum_held_up(db, "t_big"):
        proc = start_ff(
            *FREEZE_ALL, "-v", "--enforce-time", "-m", "1", "-d", db.name,
            env=script_clock.env,
        )
        pid = waiting_backend(db)
        script_clock.advance(3600)
        db.cluster.execute("postgres", "SELECT pg_cancel_backend(%s)", (pid,))
        status, stdout, stderr = finish(proc)

    assert status == 0
    assert "VACUUMing public.t_big failed." not in stdout + stderr
    assert_lines_in_order(
        stdout,
        [
            "halted flexible_freeze due to enforced time limit",
            "Vacuuming halted due to timeout",
            (f"Work remains: VACUUM of public.t_big in database {db.name} "
            "was cancelled at the time limit."),
            "after vacuuming 0 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


# -- the pause between vacuums ------------------------------------------------


def test_pause_comes_between_vacuums_and_not_after_the_last(make_db, run_ff):
    # One table in each of two databases: two vacuums, so one pause.  A pause
    # after the second as well would double the run time.
    one, two = make_db("one"), make_db("two")
    one.create_table("t1")
    two.create_table("t2")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    started = time.monotonic()
    result = run_ff(*FREEZE_ALL, "--pause", "3", "-d", f"{one.name},{two.name}")
    elapsed = time.monotonic() - started

    assert result.returncode == 0
    assert one.frozen_since(before_one) == {("public", "t1")}
    assert two.frozen_since(before_two) == {("public", "t2")}
    assert 3 <= elapsed < 6


def test_no_pause_once_the_deadline_has_passed(db, start_ff, script_clock):
    # The first vacuum is held up until the window is over (not enforced, so
    # it is allowed to finish).  The run must then stop at once instead of
    # sleeping through the pause first.
    db.create_table("t_first")
    db.create_table("t_second")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_first") as holder:
        proc = start_ff(
            *FREEZE_ALL, "-v", "-m", "1", "--pause", "20", "-d", db.name,
            env=script_clock.env,
        )
        waiting_backend(db)
        script_clock.advance(3600)
        holder.rollback()
        released = time.monotonic()
        status, stdout, _ = finish(proc, timeout=60)
        elapsed = time.monotonic() - released

    assert status == 0
    assert elapsed < 10
    assert db.frozen_since(before) == {("public", "t_first")}
    assert_lines_in_order(
        stdout,
        [
            "processing table public.t_second",
            "Reached time limit.  Exiting.",
            "Vacuuming halted due to timeout",
            "after vacuuming 1 tables in 1 databases",
        ],
    )


def test_pause_is_cut_short_at_the_deadline(db, start_ff, script_clock):
    # As above, but the clock is moved to a few seconds before the deadline:
    # the pause lasts until the deadline and no longer.
    db.create_table("t_first")
    db.create_table("t_second")
    before = db.frozenxids()

    with vacuum_held_up(db, "t_first") as holder:
        proc = start_ff(
            *FREEZE_ALL, "-v", "-m", "1", "--pause", "30", "-d", db.name,
            env=script_clock.env,
        )
        waiting_backend(db)
        moved = time.monotonic()
        script_clock.advance(56)
        holder.rollback()
        status, stdout, _ = finish(proc, timeout=60)
        elapsed = time.monotonic() - moved

    assert status == 0
    # Four seconds of the window were left, less the time the script took to
    # reach its first table.
    assert 1 <= elapsed < 15
    assert db.frozen_since(before) == {("public", "t_first")}
    assert "Reached time limit.  Exiting." in stdout.splitlines()


# -- exclusions that matched nothing ------------------------------------------


def test_exclusions_that_matched_nothing_are_listed_with_verbose(db, run_ff):
    for name in ("t_kept", "t_global", "t_local", "t_both"):
        db.create_table(name)
    before = db.frozenxids()
    exclusions = (
        "-T", "t_global",
        "-T", "t_both",
        "-T", "t_mistyped",
        "--exclude-table-in-database", f"{db.name}.t_local",
        "--exclude-table-in-database", f"{db.name}.t_both",
        "--exclude-table-in-database", f"{db.name}.public.t_wrong",
        "--exclude-table-in-database", "ff_other_database.t_kept",
    )

    result = run_ff(*FREEZE_ALL, "-v", *exclusions, "-d", db.name)

    # It is information, not a failure.
    assert result.returncode == 0
    assert db.frozen_since(before) == {("public", "t_kept")}
    lines = result.stdout.splitlines()
    assert [line for line in lines if line.startswith("exclusions that")] == [
        ("exclusions that matched no selected table: -T t_mistyped, "
        f"--exclude-table-in-database {db.name}.public.t_wrong, "
        "--exclude-table-in-database ff_other_database.t_kept")
    ]
    assert_lines_in_order(
        result.stdout, ["All tables vacuumed.", lines[-2], "Flexible Freeze run complete"]
    )
    assert lines[-2].startswith("exclusions that")

    # Without -v the run's output is what it always was.
    db.execute("SELECT txid_current()")
    result = run_ff(*FREEZE_ALL, *exclusions, "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"


def test_exclusions_that_all_matched_are_not_mentioned(db, run_ff):
    db.create_table("t_kept")
    db.create_table("t_global")

    result = run_ff(*FREEZE_ALL, "-v", "-T", "t_global", "-d", db.name)

    assert result.returncode == 0
    assert "exclusions that" not in result.stdout


def test_unmatched_exclusions_in_an_unfinished_run_say_so(db, run_ff):
    # The database that might have held the table was never listed, so "no
    # match" is not the whole story.
    db.create_table("t1")

    result = run_ff(
        *FREEZE_ALL, "-v", "-T", "t_elsewhere", "-d", f"{db.name},{NO_SUCH_DB}"
    )

    assert result.returncode == 1
    assert (
        "exclusions that matched no selected table: -T t_elsewhere "
        "(not every database was examined)"
    ) in result.stdout.splitlines()
