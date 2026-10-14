# Tests for what keeps a run from being held up by locks, and from running
# on top of another run:
#
#   * the VACUUM is sent with SKIP_LOCKED, in the parenthesized form that
#     option needs, so a table somebody else has locked is passed over and
#     reported instead of waited for;
#   * --lock-timeout bounds every other wait for a lock, the table-listing
#     query's above all, and the wait of the query that lists the databases;
#   * a run takes an advisory lock in each database while it works there,
#     and a second run skips a database it finds taken (--no-advisory-lock
#     turns that off), naming whoever holds the lock and saying whether
#     that looks like another run or something else (issue #61).
#
# The first two are deliberately tested apart, because each covers what the
# other cannot: SKIP_LOCKED says nothing about the listing query or about a
# lock on something other than the table, and a lock timeout alone would
# make every locked table a ten-second wait and a failure.
#
# Nothing here waits for a length of time.  A test either runs the script to
# completion while the obstacle is still in place (so the run can only have
# finished by not waiting for it), or waits for a state the server reports
# (pg_stat_activity, pg_locks) before it acts.  Every run of the script is
# bounded, so a script that does wait fails the test instead of hanging the
# suite.

import contextlib
import json
import threading
import time

import pytest
from ffharness import (
    FREEZE_ALL,
    HARNESS_APP,
    SCRIPT_APP,
    assert_lines_in_order,
    wait_for,
)

# The key of the advisory lock, written out rather than imported or computed:
# it is part of what the script documents to its users, who look for it in
# pg_locks and may have to avoid it in their own applications.  A change to
# it also lets an old and a new copy of the script run side by side unseen,
# so it should not be possible to change it without changing this.
ADVISORY_KEY = 7380385457142659685
# How the bigint above appears in pg_locks, split into two halves.
ADVISORY_CLASSID, ADVISORY_OBJID = 1718379896, 1718778469
TRY_ADVISORY_LOCK = f"SELECT pg_catalog.pg_try_advisory_lock({ADVISORY_KEY})"
# Text that only the statement asking who holds the lock contains (it is
# also what ffharness.HolderLookupGate stops the script in front of).
HOLDER_LOOKUP = "pg_catalog.pg_locks"
# An application name that is not the script's.
OTHER_APP = "nightly_report"

# How long a run that is expected not to wait is given.  Far more than it
# needs, and far less than the harness's default: a run that does wait
# should fail soon.
PROMPT = 30.0


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def named(pid, role, application):
    """How the skip line names one session that holds the lock."""
    return (
        f'pid {pid}, role {json.dumps(role)}, application name "{application}"'
    )


def skipped_as_held_by_a_run(db, *holders):
    """The line for a database that another flexible_freeze run is working
    on, going by the application name of every holder."""
    return (
        f"skipping database {db.name} (another flexible_freeze run is working "
        f"on it: advisory lock {ADVISORY_KEY} is held there by "
        + "; ".join(holders) + ")"
    )


def skipped_as_held_by_another_session(db, *holders):
    """The line for a database in which a holder of the lock does not go by
    the script's application name."""
    if len(holders) == 1:
        who = ("a session that does not appear to be flexible_freeze, and "
               "every run will skip this database until that session releases "
               "the lock or ends")
    else:
        who = ("holders not all of which appear to be flexible_freeze, and "
               "every run will skip this database until they let go of the "
               "lock")
    return (
        f"skipping database {db.name} (advisory lock {ADVISORY_KEY} is held "
        f"there by {who}: " + "; ".join(holders) + ")"
    )


def skipped_as_held_a_moment_ago(db):
    """The line for a database whose lock was gone again by the time the
    script asked who held it."""
    return (
        f"skipping database {db.name} (advisory lock {ADVISORY_KEY} was held "
        "there a moment ago, and its holder had let it go by the time it was "
        "looked up; the lock is not tried again in this run)"
    )


def skipped_holder_unknown(db, error):
    """The start of the line for a database whose lock was held and whose
    holder could not be looked up; `error` is the start of the server's."""
    return (
        f"skipping database {db.name} (advisory lock {ADVISORY_KEY} is held "
        f"there; who holds it could not be determined: {error}"
    )


@contextlib.contextmanager
def lock_held(db, statement):
    """Run `statement` in a transaction that stays open, so that the lock it
    takes is held until the block ends."""
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def advisory_lock_held(db, *, user=None, application=None, shared=False):
    """Hold the script's advisory lock in `db`, as a run working there does.

    Session level, like the script's: it goes when the connection closes.
    The session is the harness's, as `user` (the superuser by default),
    under the harness's application name unless `application` is given.
    That is set last, so that no statement of this session is logged under
    the script's name, which the statement log goes by.  `shared` takes the
    lock in share mode, which several sessions can hold at once.

    Yields the process id of the session's backend.
    """
    conn = db.connect(user=user)
    try:
        with conn.cursor() as cur:
            take = "pg_advisory_lock_shared" if shared else "pg_advisory_lock"
            cur.execute(f"SELECT {take}(%s)", (ADVISORY_KEY,))
            cur.execute("SELECT pg_backend_pid()")
            pid = cur.fetchone()[0]
            if application is not None:
                cur.execute("SELECT set_config('application_name', %s, false)",
                            (application,))
        yield pid
    finally:
        conn.close()


def holder_lookups(db, since=0):
    """The script's statements in `db` that ask who holds the lock."""
    return [sql for sql in db.script_statements(since=since) if HOLDER_LOOKUP in sql]


def advisory_lock_holders(db):
    """Process ids of the sessions holding the script's advisory lock in
    `db`.  objsubid 1 is what marks a key given as one bigint."""
    rows = db.cluster.execute(
        "postgres",
        "SELECT l.pid FROM pg_locks l JOIN pg_database d ON d.oid = l.database "
        "WHERE l.locktype = 'advisory' AND l.granted AND d.datname = %s "
        "AND l.classid = %s AND l.objid = %s AND l.objsubid = 1",
        (db.name, ADVISORY_CLASSID, ADVISORY_OBJID),
    )
    return [row[0] for row in rows]


@contextlib.contextmanager
def vacuum_held_up(db, table):
    """Keep a freezing VACUUM of public.<table> from finishing: an open
    cursor's pin on the table's only page, which the VACUUM has to wait for.
    It is no lock, so neither SKIP_LOCKED nor the lock timeout ends the wait
    (test_run_outcome.py has the longer account).

    Yields the connection; rolling it back drops the pin.
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


def script_backend_waiting(db, on, at_least="0 seconds"):
    """Return the process id of the script's backend in `db` once it has been
    waiting for `on` (pg_stat_activity.wait_event_type) for `at_least`.

    The statement that waits is the one the backend is running, so the time
    since it began is an upper bound on the wait, and that is the side the
    callers need: "has not been given up on after this long".
    """

    def probe():
        rows = db.cluster.execute(
            "postgres",
            "SELECT pid FROM pg_stat_activity WHERE application_name = %s "
            "AND datname = %s AND state = 'active' AND wait_event_type = %s "
            "AND clock_timestamp() - query_start >= %s::interval",
            (SCRIPT_APP, db.name, on, at_least),
        )
        return rows[0][0] if rows else None

    return wait_for(probe, what=f"the script to wait ({on}) in {db.name}")


# -- the VACUUM statement -----------------------------------------------------


@pytest.mark.parametrize(
    "flags, options",
    [
        ((), {"FREEZE", "ANALYZE", "SKIP_LOCKED"}),
        (("--no-freeze",), {"ANALYZE", "SKIP_LOCKED"}),
        (("--no-analyze",), {"FREEZE", "SKIP_LOCKED"}),
        (("--no-freeze", "--no-analyze"), {"SKIP_LOCKED"}),
    ],
    ids=["freeze", "no-freeze", "no-analyze", "plain"],
)
def test_vacuum_is_sent_with_an_option_list_that_includes_skip_locked(
    db, run_ff, flags, options
):
    # --table, so that the same table is taken in either mode whatever the
    # mode's selection would make of it.
    db.create_table("t1")

    result = run_ff(*flags, "--table", "t1", "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    (vacuum,) = db.script_vacuums()
    assert vacuum.target == ("public", "t1")
    assert vacuum.options == options
    # SKIP_LOCKED has no keyword spelling: only the parenthesized list takes
    # it, and the parser the tests use accepts either form.
    assert vacuum.sql.startswith("VACUUM (")


# -- a table somebody else has locked -----------------------------------------


@pytest.mark.parametrize(
    "mode",
    ["SHARE UPDATE EXCLUSIVE", "SHARE"],
    ids=["as-another-vacuum-holds", "as-create-index-holds"],
)
def test_locked_table_is_skipped_and_reported_and_the_run_continues(
    make_db, run_ff, mode
):
    # t_locked is created first, so it is the oldest table and is taken
    # first.  The lock is still held when the run returns: it got past the
    # table without waiting for it.  No --lock-timeout is given, and the
    # outcome shows that none was needed: a VACUUM that had waited for the
    # default one would be reported as failed, with the timeout's error.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    one.create_table("t_next")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, f"LOCK TABLE public.t_locked IN {mode} MODE"):
        result = run_ff(
            *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", timeout=PROMPT
        )

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    # The locked table was attempted and left alone; the next table of the
    # same database and the next database were done.
    assert targets(one.script_vacuums()) == [("public", "t_locked"), ("public", "t_next")]
    assert one.frozen_since(before_one) == {("public", "t_next")}
    assert two.frozen_since(before_two) == {("public", "t_other")}

    # The report is a failure report and so on stderr; the progress lines
    # stay on stdout (test_output_streams.py has the rule).
    lines = result.stdout.splitlines()
    failures = result.stderr.splitlines()
    report = failures.index(
        "VACUUMing public.t_locked drew a message from the server; "
        "not counting it as vacuumed."
    )
    # The server's own text follows, as received.  Only its presence is
    # asserted on: the wording is translated and differs between versions.
    assert failures[report + 1].startswith("WARNING:")
    assert '"t_locked"' in failures[report + 1]
    # Skipped is not failed.
    assert "VACUUMing public.t_locked failed." not in lines + failures
    assert "lock timeout" not in result.stdout + result.stderr
    assert not [
        line for line in lines + failures if line.startswith("Tables that failed")
    ]
    assert "All tables vacuumed." not in lines
    assert_lines_in_order(
        result.stderr,
        [
            "Not all tables were vacuumed.",
            f"Tables not counted as vacuumed: public.t_locked in database {one.name}",
        ],
    )
    assert_lines_in_order(
        result.stdout,
        [
            "processing table public.t_locked",
            "processing table public.t_next",
            f"working on database {two.name}",
            "2 tables in 2 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_locked_table_is_reported_where_it_happened(make_db, run_ff):
    # The same run with both streams on one pipe.  Two pipes cannot show
    # where in the run the report was written: after the line for its own
    # table and before the line for the next, and in the summary before the
    # count.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    one.create_table("t_next")
    two.create_table("t_other")

    with lock_held(one, "LOCK TABLE public.t_locked IN SHARE UPDATE EXCLUSIVE MODE"):
        result = run_ff(
            *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", timeout=PROMPT,
            merge_streams=True,
        )

    assert result.returncode == 1
    assert_lines_in_order(
        result.stdout,
        [
            "processing table public.t_locked",
            ("VACUUMing public.t_locked drew a message from the server; "
             "not counting it as vacuumed."),
            "processing table public.t_next",
            f"working on database {two.name}",
            "Not all tables were vacuumed.",
            f"Tables not counted as vacuumed: public.t_locked in database {one.name}",
            "2 tables in 2 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_locked_table_report_without_verbose(db, run_ff):
    db.create_table("t_locked")

    with lock_held(db, "LOCK TABLE public.t_locked IN SHARE UPDATE EXCLUSIVE MODE"):
        result = run_ff(*FREEZE_ALL, "--no-analyze", "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1
    assert result.stdout == ""
    lines = result.stderr.splitlines()
    # One message from the server: with ANALYZE in the statement there would
    # be a second, for the analyze it skips as well.
    assert len(lines) == 4
    assert lines[0] == (
        "VACUUMing public.t_locked drew a message from the server; "
        "not counting it as vacuumed."
    )
    assert lines[1].startswith("WARNING:")
    assert lines[2:] == [
        "Not all tables were vacuumed.",
        f"Tables not counted as vacuumed: public.t_locked in database {db.name}",
    ]


def test_vacuum_waiting_for_a_lock_other_than_the_tables_fails_at_the_timeout(
    db, run_ff
):
    # What SKIP_LOCKED does not cover.  The table is free, so the VACUUM is
    # not skipped; it then has to open the table's index, on which another
    # session holds a lock that conflicts.  (ALTER INDEX ... SET TABLESPACE
    # locks the index and not its table, and does so even when the index is
    # already there.)  Only the lock timeout ends that wait.  The table list
    # is not held up: the size it asks for leaves indexes out.
    db.create_table("t_indexed")
    db.execute("CREATE INDEX t_indexed_id ON public.t_indexed (id)")
    db.create_table("t_next")
    before = db.frozenxids()

    with lock_held(db, "ALTER INDEX public.t_indexed_id SET TABLESPACE pg_default"):
        started = time.monotonic()
        result = run_ff(*FREEZE_ALL, "--lock-timeout", "2", "-d", db.name, timeout=PROMPT)
        elapsed = time.monotonic() - started

    assert result.returncode == 1
    # It did wait, for as long as it was told to.
    assert elapsed >= 2
    assert db.frozen_since(before) == {("public", "t_next")}
    assert result.stderr.splitlines() == [
        "VACUUMing public.t_indexed failed.",
        "canceling statement due to lock timeout",
        "Not all tables were vacuumed.",
        f"Tables that failed: public.t_indexed in database {db.name}",
    ]
    assert result.stdout == ""


# -- the lock timeout ---------------------------------------------------------


@pytest.mark.parametrize(
    "flags, seconds",
    [((), 10), (("--lock-timeout", "45"), 45), (("--lock-timeout", "0"), 0)],
    ids=["default", "given", "zero"],
)
def test_lock_timeout_is_set_before_the_table_list_is_read(db, run_ff, flags, seconds):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, *flags, "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    statements = db.script_statements()
    # Zero is sent too, not left out: see the test of what it overrides.
    setting = statements.index(f"SET lock_timeout = '{seconds}s'")
    # The listing queries are the ones that ask for a size.
    listing = [n for n, sql in enumerate(statements) if "pg_table_size" in sql]
    assert listing and setting < listing[0]


def test_blocked_table_listing_ends_at_the_lock_timeout(make_db, run_ff):
    # An ACCESS EXCLUSIVE lock on one table holds up the listing of them
    # all: the query asks for the size of every candidate.  The database
    # sets no lock timeout of its own; what ends the wait is the script's.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_locked")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with lock_held(one, "LOCK TABLE public.t_locked IN ACCESS EXCLUSIVE MODE"):
        started = time.monotonic()
        result = run_ff(
            *FREEZE_ALL, "-v", "--lock-timeout", "2",
            "-d", f"{one.name},{two.name}", timeout=PROMPT,
        )
        elapsed = time.monotonic() - started

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    # It waited the two seconds it was given, and no more than the run was
    # allowed in all.
    assert elapsed >= 2
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


def test_lock_timeout_of_zero_waits_for_as_long_as_it_takes(db, start_ff):
    # Zero means no limit, and that has to hold in a database that sets a
    # limit of its own: the script sends its value in every case, so its 0
    # replaces the database's 200 ms.  The listing query is left waiting for
    # five times that before the lock is let go.
    db.create_table("t_locked")
    db.execute(f'ALTER DATABASE "{db.name}" SET lock_timeout = \'200ms\'')
    before = db.frozenxids()

    with lock_held(db, "LOCK TABLE public.t_locked IN ACCESS EXCLUSIVE MODE") as holder:
        proc = start_ff(*FREEZE_ALL, "--lock-timeout", "0", "-d", db.name)
        try:
            script_backend_waiting(db, "Lock", at_least="1 second")
            holder.rollback()
            stdout, stderr = proc.communicate(timeout=PROMPT)
        finally:
            proc.kill()

    assert proc.returncode == 0
    assert stderr == ""
    assert stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {("public", "t_locked")}


@pytest.mark.parametrize(
    "flags, seconds",
    [((), 10), (("--lock-timeout", "45"), 45), (("--lock-timeout", "0"), 0)],
    ids=["default", "given", "zero"],
)
def test_lock_timeout_is_set_before_the_database_list_is_read(
    cluster, db, run_ff, flags, seconds
):
    # Without -d the run begins with a session in the postgres database
    # that reads the list of databases.  It is the first session of the
    # run, so the statements up to that query are all its own.
    db.create_table("t1")
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, *flags)

    assert result.returncode == 0
    statements = [
        record.sql for record in cluster.statements(dbname="postgres", since=mark)
    ]
    listing = next(n for n, sql in enumerate(statements) if "FROM pg_database" in sql)
    # Zero is sent too, as in the sessions that vacuum.
    assert statements[:listing] == [
        "SELECT pg_catalog.set_config('search_path', '', false)",
        f"SET lock_timeout = '{seconds}s'",
    ]


def test_blocked_database_listing_ends_at_the_lock_timeout(cluster, db, run_ff):
    # The query that lists the databases waits for a lock too: here one on
    # pg_statistic, which planning it reads (test_signals.py has the account
    # of why that catalog).  Without a limit of its own that session would
    # wait for as long as the lock is held, before the run had begun.  When
    # the limit ends the wait there is no list and so no run, and that is
    # reported by the script and not by a traceback.
    db.create_table("t1")
    before = db.frozenxids()

    with lock_held(
        cluster.database("postgres"),
        "LOCK TABLE pg_catalog.pg_statistic IN ACCESS EXCLUSIVE MODE",
    ):
        started = time.monotonic()
        result = run_ff(*FREEZE_ALL, "--lock-timeout", "2", timeout=PROMPT)
        elapsed = time.monotonic() - started

    assert result.returncode == 1
    assert elapsed >= 2
    assert result.stdout == ""
    assert result.stderr == (
        "Could not list databases: query in database postgres failed: "
        "canceling statement due to lock timeout\n"
    )
    assert db.script_statements() == []
    assert db.frozen_since(before) == set()


# -- overlapping runs ---------------------------------------------------------


def test_second_run_skips_the_database_the_first_is_working_on(
    cluster, make_db, start_ff, run_ff
):
    # The first run is kept in the middle of `one`, in a VACUUM that cannot
    # finish.  The second is given both databases: it must leave `one` alone,
    # say why, do `two`, and exit 1.  The first then finishes as if nothing
    # had happened, and once it has gone a third run gets into `one`.
    one, two = make_db("one"), make_db("two")
    one.create_table("t_held")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    with vacuum_held_up(one, "t_held") as holder:
        first = start_ff(*FREEZE_ALL, "-d", one.name)
        try:
            backend = script_backend_waiting(one, "BufferPin")
            assert advisory_lock_holders(one) == [backend]

            mark = cluster.log_offset()
            second = run_ff(
                *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", timeout=PROMPT
            )

            assert second.returncode == 1
            assert "Traceback" not in second.stderr
            # In `one` the second run asked for the lock, then who had it,
            # and went no further.
            statements = one.script_statements(since=mark)
            assert statements[-2] == TRY_ADVISORY_LOCK
            assert HOLDER_LOOKUP in statements[-1]
            assert one.script_vacuums(since=mark) == []
            assert two.frozen_since(before_two) == {("public", "t_other")}
            # The holder is the first run: its backend, its role, and the
            # application name every run of the script connects with.
            assert_lines_in_order(
                second.stderr,
                [
                    skipped_as_held_by_a_run(
                        one, named(backend, cluster.superuser, SCRIPT_APP)
                    ),
                    "Not all tables were vacuumed.",
                    f"Databases skipped or left unfinished: {one.name}",
                ],
            )
            assert_lines_in_order(
                second.stdout,
                [
                    f"working on database {one.name}",
                    f"working on database {two.name}",
                    "processing table public.t_other",
                    # `one` was not listed, so it is not counted.
                    "1 tables in 1 databases",
                    "Flexible Freeze run complete",
                ],
            )
            assert "getting list of tables" not in second.stdout.split(
                f"working on database {two.name}"
            )[0]

            # The first run was not disturbed, and still holds the lock.
            assert advisory_lock_holders(one) == [backend]
            holder.rollback()
            stdout, stderr = first.communicate(timeout=PROMPT)
        finally:
            first.kill()

    assert first.returncode == 0
    assert stderr == ""
    assert stdout == "All tables vacuumed.\n"
    assert one.frozen_since(before_one) == {("public", "t_held")}

    # The lock went with the first run's connection; nothing released it by
    # name.  The server drops it when the backend exits, which is a moment
    # after the client has gone, hence the wait.
    wait_for(
        lambda: not advisory_lock_holders(one),
        what=f"the advisory lock in {one.name} to be released",
    )
    one.execute("SELECT txid_current()")  # so t_held's age is above zero again
    mark = cluster.log_offset()

    third = run_ff(*FREEZE_ALL, "-d", one.name)

    assert third.returncode == 0
    assert third.stdout == "All tables vacuumed.\n"
    assert targets(one.script_vacuums(since=mark)) == [("public", "t_held")]


def test_database_whose_advisory_lock_is_held_is_skipped(db, run_ff):
    # The same, with the test's own session holding the lock, which pins the
    # key and the form of the lock: a session-level lock on the documented
    # key, taken as one bigint.  That session goes by the harness's
    # application name, not the script's, so it is not reported as another
    # run; the next test has more on that.
    db.create_table("t1")
    before = db.frozenxids()

    with advisory_lock_held(db) as pid:
        result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.splitlines() == [
        skipped_as_held_by_another_session(
            db, named(pid, db.cluster.superuser, HARNESS_APP)
        ),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]
    statements = db.script_statements()
    assert statements[-2] == TRY_ADVISORY_LOCK
    assert HOLDER_LOOKUP in statements[-1]
    assert db.frozen_since(before) == set()


# -- who holds the lock (issue #61) --------------------------------------------
#
# The key is fixed and documented, and an advisory lock is checked against
# no privilege, so any role that can connect can hold it and make every run
# skip the database.  A run that is refused the lock therefore looks the
# holder up and names it, and tells a holder that goes by the script's
# application name (another run, as far as can be seen) from any other.


def test_lock_held_by_something_else_is_named_and_reported_as_such(
    make_db, make_role, run_ff
):
    # Another role, another application: not a run of the script.  The
    # database is skipped as before, with the same exit status, and the
    # other database is worked through.
    one, two = make_db("one"), make_db("two")
    one.create_table("t1")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()
    other = make_role("other")

    with advisory_lock_held(one, user=other, application=OTHER_APP) as pid:
        result = run_ff(
            *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", timeout=PROMPT
        )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    assert result.stderr.splitlines() == [
        skipped_as_held_by_another_session(one, named(pid, other, OTHER_APP)),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {one.name}",
    ]
    assert one.script_vacuums() == []
    assert one.frozen_since(before_one) == set()
    assert two.frozen_since(before_two) == {("public", "t_other")}
    assert_lines_in_order(
        result.stdout,
        [
            f"working on database {one.name}",
            f"working on database {two.name}",
            "processing table public.t_other",
            "1 tables in 1 databases",
        ],
    )


def test_holder_is_named_to_a_role_without_the_right_to_read_all_statistics(
    db, make_role, run_ff
):
    # pg_stat_activity hides most of another role's session from a role
    # that is neither that role nor a member of pg_read_all_stats (its
    # query, state and wait event), but not its process id, role and
    # application name, which are all the lookup asks for.  So a run as an
    # ordinary role names the holder as fully as a superuser's does.
    runner, other = make_role("runner"), make_role("other")

    with advisory_lock_held(db, user=other, application=OTHER_APP) as pid:
        result = run_ff(*FREEZE_ALL, "-U", runner, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stderr.splitlines() == [
        skipped_as_held_by_another_session(db, named(pid, other, OTHER_APP)),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]
    statements = db.script_statements(user=runner)
    assert statements[-2] == TRY_ADVISORY_LOCK
    assert HOLDER_LOOKUP in statements[-1]


def test_holder_role_with_a_line_break_in_its_name_cannot_forge_a_report_line(
    db, make_role, run_ff
):
    # The role is quoted like the application name beside it, so a name
    # with a line break in it stays inside the one line that names the
    # holder instead of starting lines of its own.
    other = make_role("a\nflexible_freeze: forged")

    with advisory_lock_held(db, user=other, application=OTHER_APP) as pid:
        result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stderr.splitlines() == [
        skipped_as_held_by_another_session(db, named(pid, other, OTHER_APP)),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]
    assert "\\n" in result.stderr.splitlines()[0]


def test_every_holder_is_named_and_one_that_is_not_a_run_decides_the_wording(
    db, run_ff
):
    # Several sessions can hold the key at once, in share mode.  One of
    # them goes by the script's application name, the other does not; the
    # report names both, and is the one for a holder that is not a run.
    with contextlib.ExitStack() as stack:
        as_run = stack.enter_context(
            advisory_lock_held(db, application=SCRIPT_APP, shared=True)
        )
        as_other = stack.enter_context(
            advisory_lock_held(db, application=OTHER_APP, shared=True)
        )
        # In the order of the process ids, which is the report's.
        holders = sorted([(as_run, SCRIPT_APP), (as_other, OTHER_APP)])
        mark = db.cluster.log_offset()
        result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stderr.splitlines()[0] == skipped_as_held_by_another_session(
        db, *(named(pid, db.cluster.superuser, app) for pid, app in holders)
    )
    assert db.script_vacuums(since=mark) == []


def test_lock_held_by_a_prepared_transaction(db, run_ff):
    # A prepared transaction can hold the key (at transaction level) and
    # outlive the session that prepared it, and a restart.  pg_locks then
    # has no process id for the holder and pg_stat_activity no row.  It is
    # never a run (the script holds the key at session level and prepares
    # nothing), so the report is the pointed one, with what ends it.
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_KEY,))
            cur.execute("PREPARE TRANSACTION 'ff_holds_the_key'")
        conn.commit()
        result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)
    finally:
        # Out of any transaction a failed statement above left open.
        conn.rollback()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT gid FROM pg_prepared_xacts WHERE gid = 'ff_holds_the_key'"
            )
            if cur.fetchall():
                cur.execute("ROLLBACK PREPARED 'ff_holds_the_key'")
        conn.close()

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stderr.splitlines() == [
        (f"skipping database {db.name} (advisory lock {ADVISORY_KEY} is held "
         "there by a prepared transaction (see pg_prepared_xacts), which is not "
         "flexible_freeze, and every run will skip this database until that "
         "transaction is committed or rolled back)"),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]


def test_only_holders_of_the_key_in_this_database_are_named(
    make_db, run_ff
):
    # What the lookup must leave out: the same key held in another
    # database, a session still waiting for the key, and a lock on two
    # integer keys that happen to be the key's halves (objsubid 2, not 1).
    # And what it must name once: a session holding the key in both modes,
    # which pg_locks lists twice.
    one, two = make_db("one"), make_db("two")
    sessions = []

    def session(database):
        conn = database.connect()
        sessions.append(conn)
        return conn.cursor()

    waiter = None
    try:
        both_modes = session(one)
        both_modes.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_KEY,))
        both_modes.execute("SELECT pg_advisory_lock_shared(%s)", (ADVISORY_KEY,))
        both_modes.execute("SELECT pg_backend_pid()")
        (pid,) = both_modes.fetchone()
        session(two).execute("SELECT pg_advisory_lock(%s)", (ADVISORY_KEY,))
        session(one).execute(
            "SELECT pg_advisory_lock(%s, %s)", (ADVISORY_CLASSID, ADVISORY_OBJID)
        )
        waiting = session(one)
        waiter = threading.Thread(
            target=waiting.execute,
            args=("SELECT pg_advisory_lock(%s)", (ADVISORY_KEY,)),
        )
        waiter.start()
        wait_for(
            lambda: one.cluster.execute(
                "postgres",
                "SELECT count(*) FROM pg_locks l JOIN pg_database d "
                "ON d.oid = l.database WHERE d.datname = %s "
                "AND l.locktype = 'advisory' AND NOT l.granted",
                (one.name,),
            )[0][0] == 1,
            what="a second session to wait for the key",
        )

        result = run_ff(*FREEZE_ALL, "-d", one.name, timeout=PROMPT)
    finally:
        # The holders first, so that the waiter gets the key and returns;
        # its own connection only once it has.
        for conn in sessions[:-1]:
            conn.close()
        if waiter is not None:
            waiter.join(timeout=PROMPT)
        sessions[-1].close()

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stderr.splitlines()[0] == skipped_as_held_by_another_session(
        one, named(pid, one.cluster.superuser, HARNESS_APP)
    )


def test_lock_released_before_the_holder_is_looked_up(db, start_ff, holder_lookup_gate):
    # The script is refused the lock, and stopped before it asks who has it
    # (holder_lookup_gate); the holder lets go, and only then is the script
    # let on.  It finds nobody, says so, and skips the database all the
    # same: it does not try the lock a second time.
    db.create_table("t1")
    before = db.frozenxids()

    with advisory_lock_held(db) as pid:
        proc = start_ff(*FREEZE_ALL, "-d", db.name, env=holder_lookup_gate.env)
        holder_lookup_gate.wait_until_held()
        assert advisory_lock_holders(db) == [pid]
    # The holder's connection is closed; the server drops the lock when the
    # backend has gone, a moment later.
    wait_for(
        lambda: not advisory_lock_holders(db),
        what=f"the advisory lock in {db.name} to be released",
    )
    holder_lookup_gate.release()
    stdout, stderr = proc.communicate(timeout=PROMPT)

    assert proc.returncode == 1, stdout + stderr
    assert stdout == ""
    assert stderr.splitlines() == [
        skipped_as_held_a_moment_ago(db),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]
    statements = db.script_statements()
    assert statements.count(TRY_ADVISORY_LOCK) == 1
    assert statements[-2] == TRY_ADVISORY_LOCK
    assert HOLDER_LOOKUP in statements[-1]
    assert db.frozen_since(before) == set()


def test_holder_that_cannot_be_looked_up_still_skips_the_database_as_held(
    make_db, make_role, run_ff
):
    # A lookup that fails is not a failure to list the tables: the lock is
    # known to be held, and that is what is reported, with why the holder
    # is not named.  Here the role the script runs as may not read pg_locks
    # in `one` (the view's privileges are per database); in `two` it may,
    # and the run goes on there.
    runner = make_role("runner")
    one, two = make_db("one"), make_db("two")
    one.execute("REVOKE SELECT ON pg_catalog.pg_locks FROM PUBLIC")
    two.create_table("t_mine", owner=runner)
    before_two = two.frozenxids()

    with advisory_lock_held(one):
        result = run_ff(
            *FREEZE_ALL, "-v", "-U", runner, "-d", f"{one.name},{two.name}",
            timeout=PROMPT,
        )

    assert result.returncode == 1, result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    assert "couldn't list its tables" not in result.stderr
    assert result.stderr.splitlines() == [
        skipped_holder_unknown(one, "permission denied for view pg_locks") + ")",
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {one.name}",
    ]
    assert one.script_vacuums() == []
    assert two.frozen_since(before_two) == {("public", "t_mine")}
    assert "1 tables in 1 databases" in result.stdout.splitlines()


def test_holder_lookup_that_runs_into_the_lock_timeout(
    make_db, start_ff, holder_lookup_gate
):
    # The other way a lookup fails here: a lock on the pg_locks view of the
    # one database (a relation of that database, so nothing else is held
    # up), put in place while the script is stopped in front of the lookup.
    one, two = make_db("one"), make_db("two")
    two.create_table("t_other")
    before_two = two.frozenxids()

    with advisory_lock_held(one):
        proc = start_ff(
            *FREEZE_ALL, "--lock-timeout", "1", "-d", f"{one.name},{two.name}",
            env=holder_lookup_gate.env,
        )
        try:
            holder_lookup_gate.wait_until_held()
            with lock_held(one, "LOCK TABLE pg_catalog.pg_locks IN ACCESS EXCLUSIVE MODE"):
                holder_lookup_gate.release()
                stdout, stderr = proc.communicate(timeout=PROMPT)
        finally:
            proc.kill()

    assert proc.returncode == 1, stdout + stderr
    assert "Traceback" not in stderr
    assert "couldn't list its tables" not in stderr
    assert stderr.startswith(
        skipped_holder_unknown(one, "canceling statement due to lock timeout")
    )
    assert_lines_in_order(
        stderr,
        [
            "Not all tables were vacuumed.",
            f"Databases skipped or left unfinished: {one.name}",
        ],
    )
    assert one.script_vacuums() == []
    assert two.frozen_since(before_two) == {("public", "t_other")}


def test_holder_lookup_carries_the_comment_and_the_key_as_values(db, run_ff):
    # The lookup has parameters, so a percent sign in the comment is
    # doubled for the driver and has to arrive single.  The key travels as
    # the two halves pg_locks shows it in.
    with advisory_lock_held(db):
        result = run_ff(
            *FREEZE_ALL, "--comment", "50% of nightly", "-d", db.name, timeout=PROMPT
        )

    assert result.returncode == 1, result.stdout + result.stderr
    (lookup,) = holder_lookups(db)
    assert lookup.endswith(" /* 50% of nightly */")
    assert f"l.classid = {ADVISORY_CLASSID}::oid" in lookup
    assert f"l.objid = {ADVISORY_OBJID}::oid" in lookup
    assert "%%" not in lookup


def test_no_holder_lookup_when_the_lock_is_free(db, run_ff):
    # A run that gets the lock sends what it always sent.
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 0, result.stdout + result.stderr
    assert TRY_ADVISORY_LOCK in db.script_statements()
    assert holder_lookups(db) == []


def test_advisory_lock_held_in_another_database_does_not_matter(make_db, run_ff):
    # An advisory lock belongs to the database it was taken in, which is why
    # the script takes one per database and cannot take one for the cluster.
    one, two = make_db("one"), make_db("two")
    one.create_table("t1")
    before = one.frozenxids()

    with advisory_lock_held(two):
        result = run_ff(*FREEZE_ALL, "-d", one.name, timeout=PROMPT)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert one.frozen_since(before) == {("public", "t1")}


def test_no_advisory_lock_runs_in_a_database_that_is_taken(db, run_ff):
    db.create_table("t1")
    before = db.frozenxids()

    with advisory_lock_held(db):
        result = run_ff(*FREEZE_ALL, "--no-advisory-lock", "-d", db.name, timeout=PROMPT)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {("public", "t1")}
    # It neither asks for the lock nor takes it.
    assert not [sql for sql in db.script_statements() if "advisory" in sql]


def test_no_advisory_lock_leaves_the_database_open_to_other_runs(db, start_ff):
    # The other half of the opt-out: a run made with it does not turn other
    # runs away either.  Seen while the run is in the middle of the database.
    db.create_table("t_held")

    with vacuum_held_up(db, "t_held") as holder:
        proc = start_ff(*FREEZE_ALL, "--no-advisory-lock", "-d", db.name)
        try:
            script_backend_waiting(db, "BufferPin")
            assert advisory_lock_holders(db) == []
            holder.rollback()
            stdout, _ = proc.communicate(timeout=PROMPT)
        finally:
            proc.kill()

    assert proc.returncode == 0
    assert stdout == "All tables vacuumed.\n"
