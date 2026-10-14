# Tests for a table's TOAST table (issue #64).
#
# SKIP_LOCKED reports a skip with a WARNING only for a relation the VACUUM
# names.  A TOAST table that is vacuumed along with its parent, and on which
# another session holds a conflicting lock, is passed over without a word,
# and the parent used to be counted as vacuumed.  Where the session may name
# a TOAST table (a superuser on PostgreSQL 14 or later; CI runs 14 to 18),
# the script now sends the parent's VACUUM with PROCESS_TOAST false and then
# one of the TOAST table by name, so that the skip draws the WARNING and the
# parent is reported as not vacuumed.  Everywhere else (another role, or a
# table without a TOAST table) the single statement is sent as before.
#
# How the TOAST table is kept locked.  LOCK TABLE refuses a TOAST table, so
# the lock is one that another statement takes on it:
#
# * a VACUUM of the TOAST table that cannot finish (toast_vacuum_running):
#   SHARE UPDATE EXCLUSIVE, what an autovacuum of the TOAST table holds, and
#   the case that happens in practice.  The VACUUM is kept from finishing by
#   a pinned page, as in test_signals.py.  This is the one the selections
#   can be tested with;
# * an open REINDEX TABLE of the TOAST table: SHARE on it, but also ACCESS
#   EXCLUSIVE on its index, which pg_table_size() in both selections has to
#   open, so a run that selects its tables would wait at the listing and
#   skip the database.  The --table lookup does not ask for sizes, so it is
#   used there, as a second kind of lock.
#
# Tables are made with wide rows of hex digits, which do not compress, so
# that their TOAST tables have data (and pages to pin).  The harness's
# create_table pads with one repeated letter, which compresses to nothing.

import contextlib
import signal
import threading

import pytest
from ffharness import (
    FREEZE_ALL,
    SCRIPT_APP,
    StatementGate,
    assert_lines_in_order,
    wait_for,
)

PROMPT = 30.0

# The text of the statement that decides whether the session splits its
# VACUUMs; only that statement contains it.
SPLIT_QUERY = "has_schema_privilege('pg_toast', 'USAGE')"
COMMENT = "app:nightly 100%"


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def create_toasted_table(db, name, rows=20):
    """public.<name> (id integer, pad text) whose every row is stored in the
    TOAST table: 3,200 hex digits each, in a later transaction than the
    CREATE (see Database.create_table).  Returns the TOAST table's name as
    the script writes it, pg_toast.pg_toast_<oid>."""
    db.create_table(name, rows=0)
    db.execute(
        f'INSERT INTO public."{name}" SELECT g, (SELECT string_agg('
        "md5(g::text || ':' || i::text), '') FROM generate_series(1, 100) AS i) "
        "FROM generate_series(1, %s) AS g",
        (rows,),
    )
    toast = toast_table_of(db, name)
    assert db.execute(f"SELECT count(*) FROM {toast}")[0][0] > 0
    return toast


def toast_table_of(db, name):
    rows = db.execute(
        "SELECT c.reltoastrelid::regclass::text FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = %s AND c.reltoastrelid <> 0",
        (name,),
    )
    return rows[0][0] if rows else None


def toast_relfrozenxid(db, name):
    return db.execute(
        "SELECT t.relfrozenxid::text::bigint FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_class t ON t.oid = c.reltoastrelid "
        "WHERE n.nspname = 'public' AND c.relname = %s",
        (name,),
    )[0][0]


@contextlib.contextmanager
def toast_page_pinned(db, toast):
    """Keep a freezing VACUUM of `toast` from finishing: an open cursor's
    pin on the page of its first row (see vacuum_held_up in
    test_signals.py)."""
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DECLARE ff_pin NO SCROLL CURSOR FOR SELECT chunk_id FROM {toast}")
            cur.execute("FETCH 1 FROM ff_pin")
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def toast_vacuum_running(db, toast):
    """Hold SHARE UPDATE EXCLUSIVE on `toast` as an autovacuum of it would:
    another session's VACUUM of it, kept from finishing by a pinned page,
    for as long as the block lasts.  It finishes when the block ends."""
    holder = db.connect()
    pid = holder.get_backend_pid()
    thread = None
    try:
        with toast_page_pinned(db, toast):

            def vacuum():
                with contextlib.suppress(Exception), holder.cursor() as cur:
                    cur.execute(f"VACUUM (FREEZE) {toast}")

            thread = threading.Thread(target=vacuum, daemon=True)
            thread.start()
            wait_for(
                lambda: db.execute(
                    "SELECT 1 FROM pg_stat_activity WHERE pid = %s "
                    "AND wait_event_type = 'BufferPin'",
                    (pid,),
                ),
                what=f"the VACUUM of {toast} to wait for the pinned page",
            )
            yield
    finally:
        if thread is not None:
            thread.join(timeout=PROMPT)
        holder.close()


@contextlib.contextmanager
def lock_held(db, statement):
    """Run `statement` in a transaction that stays open."""
    conn = db.connect(autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute(statement)
        yield conn
    finally:
        conn.close()


def toast_report(table, toast):
    return (
        f"VACUUMing the TOAST table of {table} ({toast}) drew a message from "
        f"the server; not counting {table} as vacuumed."
    )


def assert_toast_skip_reported(result, db, table, toast):
    """The run's stderr reports the TOAST table's skip against `table`, in
    the words used for a locked table, and nothing as failed."""
    failures = result.stderr.splitlines()
    report = failures.index(toast_report(table, toast))
    # The server's WARNING follows; it names the TOAST table (bare).
    assert failures[report + 1].startswith("WARNING:")
    assert f'"{toast.removeprefix("pg_toast.")}"' in failures[report + 1]
    assert not [line for line in failures if line.startswith("Tables that failed")]
    assert "failed." not in result.stderr
    assert_lines_in_order(
        result.stderr,
        [
            "Not all tables were vacuumed.",
            f"Tables not counted as vacuumed: {table} in database {db.name}",
        ],
    )


# -- a locked TOAST table -------------------------------------------------------------


def test_locked_toast_table_is_reported_against_its_parent_and_the_run_continues(
    make_db, run_ff
):
    # Mirrors test_locks.py's test for a locked table.  t_locked is created
    # first, so it is the oldest table and is taken first; its TOAST table
    # is still locked when the run returns.
    one, two = make_db("one"), make_db("two")
    toast = create_toasted_table(one, "t_locked")
    one.create_table("t_next")
    two.create_table("t_other")
    before_one, before_two = one.frozenxids(), two.frozenxids()
    toast_before = toast_relfrozenxid(one, "t_locked")

    with toast_vacuum_running(one, toast):
        result = run_ff(
            *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", timeout=PROMPT
        )
        # Read while the other VACUUM still has the TOAST table: it moves
        # the value itself once it is let go.
        toast_after = toast_relfrozenxid(one, "t_locked")

    assert result.returncode == 1, result.stdout + result.stderr
    assert "Traceback" not in result.stderr
    # The TOAST table was not vacuumed.  Its parent's own pages were (the
    # parent's statement left the TOAST table out and was not locked out),
    # which is why the catalog alone does not show the table as missed.
    assert toast_after == toast_before
    assert one.frozen_since(before_one) == {("public", "t_locked"), ("public", "t_next")}
    assert two.frozen_since(before_two) == {("public", "t_other")}
    statements = one.script_vacuum_statements()
    assert targets(statements) == [
        ("public", "t_locked"),
        ("pg_toast", toast.removeprefix("pg_toast.")),
        ("public", "t_next"),
        ("pg_toast", toast_table_of(one, "t_next").removeprefix("pg_toast.")),
    ]

    assert_toast_skip_reported(result, one, "public.t_locked", toast)
    assert "All tables vacuumed." not in result.stdout
    assert_lines_in_order(
        result.stdout,
        [
            "processing table public.t_locked",
            f"VACUUM (FREEZE, ANALYZE, SKIP_LOCKED) {toast} in database {one.name}",
            "processing table public.t_next",
            f"working on database {two.name}",
            "2 tables in 2 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_locked_toast_table_report_without_verbose(db, run_ff):
    toast = create_toasted_table(db, "t_locked")

    with toast_vacuum_running(db, toast):
        result = run_ff(*FREEZE_ALL, "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1
    assert result.stdout == ""
    lines = result.stderr.splitlines()
    assert lines[0] == toast_report("public.t_locked", toast)
    assert lines[1].startswith("WARNING:")
    assert lines[-2:] == [
        "Not all tables were vacuumed.",
        f"Tables not counted as vacuumed: public.t_locked in database {db.name}",
    ]


def test_locked_toast_table_in_a_no_freeze_run(db, run_ff):
    toast = create_toasted_table(db, "t_locked", rows=300)
    db.make_dead_tuples("t_locked")
    toast_before = toast_relfrozenxid(db, "t_locked")

    with toast_vacuum_running(db, toast):
        result = run_ff("--no-freeze", "-d", db.name, timeout=PROMPT)
        toast_after = toast_relfrozenxid(db, "t_locked")

    assert result.returncode == 1, result.stdout + result.stderr
    assert toast_after == toast_before
    (parent, toast_vacuum) = db.script_vacuum_statements()
    assert parent.options == {"ANALYZE", "PROCESS_TOAST false", "SKIP_LOCKED"}
    assert toast_vacuum.options == {"ANALYZE", "SKIP_LOCKED"}
    assert_toast_skip_reported(result, db, "public.t_locked", toast)


@pytest.mark.parametrize(
    "holder", ["vacuum", "reindex"], ids=["as-autovacuum-holds", "as-reindex-holds"]
)
def test_locked_toast_table_of_the_table_named_by_table(db, run_ff, holder):
    toast = create_toasted_table(db, "t_locked")
    toast_before = toast_relfrozenxid(db, "t_locked")
    if holder == "vacuum":
        held = toast_vacuum_running(db, toast)
    else:
        held = lock_held(db, f"REINDEX TABLE {toast}")

    with held:
        result = run_ff("--table", "t_locked", "-d", db.name, timeout=PROMPT)
        toast_after = toast_relfrozenxid(db, "t_locked")

    assert result.returncode == 1, result.stdout + result.stderr
    assert toast_after == toast_before
    assert_toast_skip_reported(result, db, "public.t_locked", toast)


def test_locked_parent_is_reported_and_its_toast_table_is_not_vacuumed(db, run_ff):
    # The parent's skip settles the table; nothing is sent for its TOAST
    # table, and the report is the one for any locked table.
    toast = create_toasted_table(db, "t_locked")

    with lock_held(db, "LOCK TABLE public.t_locked IN SHARE UPDATE EXCLUSIVE MODE"):
        result = run_ff(*FREEZE_ALL, "--no-analyze", "-d", db.name, timeout=PROMPT)

    assert result.returncode == 1
    (vacuum,) = db.script_vacuum_statements()
    assert vacuum.target == ("public", "t_locked")
    assert vacuum.options == {"FREEZE", "PROCESS_TOAST false", "SKIP_LOCKED"}
    lines = result.stderr.splitlines()
    assert lines[0] == (
        "VACUUMing public.t_locked drew a message from the server; "
        "not counting it as vacuumed."
    )
    assert lines[1].startswith("WARNING:")
    assert toast not in result.stderr
    assert lines[2:] == [
        "Not all tables were vacuumed.",
        f"Tables not counted as vacuumed: public.t_locked in database {db.name}",
    ]


# -- the statements -------------------------------------------------------------------


def test_toast_table_is_vacuumed_by_a_statement_of_its_own(db, run_ff):
    toast = create_toasted_table(db, "t_wide")
    before = db.frozenxids()
    toast_before = toast_relfrozenxid(db, "t_wide")

    result = run_ff(*FREEZE_ALL, "-v", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stderr == ""
    assert db.frozen_since(before) == {("public", "t_wide")}
    assert toast_relfrozenxid(db, "t_wide") > toast_before
    statements = db.script_statements()
    parent, toast_vacuum = db.script_vacuum_statements()
    # The two, one after the other, after the one statement_timeout.
    assert statements[-3:] == ["SET statement_timeout = 0", parent.sql, toast_vacuum.sql]
    assert parent.target == ("public", "t_wide")
    assert parent.options == {"FREEZE", "ANALYZE", "PROCESS_TOAST false", "SKIP_LOCKED"}
    assert toast_vacuum.target == ("pg_toast", toast.removeprefix("pg_toast."))
    assert toast_vacuum.options == {"FREEZE", "ANALYZE", "SKIP_LOCKED"}
    # The session asked whether to split before it claimed the database.
    split = [i for i, sql in enumerate(statements) if SPLIT_QUERY in sql]
    assert split == [5]
    # Both are printed with -v, and the table is counted once.
    assert_lines_in_order(
        result.stdout,
        [
            f"{parent.sql} in database {db.name}",
            f"{toast_vacuum.sql} in database {db.name}",
            "All tables vacuumed.",
            "1 tables in 1 databases",
        ],
    )


def test_toast_statement_has_the_parents_options(db, run_ff):
    toast = create_toasted_table(db, "t_wide")

    result = run_ff(
        "--no-freeze", "--no-analyze", "--no-truncate", "--table", "t_wide",
        "-d", db.name,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    parent, toast_vacuum = db.script_vacuum_statements()
    assert parent.options == {"TRUNCATE false", "PROCESS_TOAST false", "SKIP_LOCKED"}
    assert toast_vacuum.target == ("pg_toast", toast.removeprefix("pg_toast."))
    assert toast_vacuum.options == {"TRUNCATE false", "SKIP_LOCKED"}


def test_table_without_a_toast_table_gets_the_single_statement(db, run_ff):
    # As the superuser, in a session that splits: the decision is per table.
    db.execute("CREATE TABLE public.t_fixed (id integer, n bigint)")
    db.execute("INSERT INTO public.t_fixed SELECT g, g FROM generate_series(1, 100) g")
    assert toast_table_of(db, "t_fixed") is None
    create_toasted_table(db, "t_wide")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("public", "t_fixed"), ("public", "t_wide")}
    statements = db.script_vacuum_statements()
    (fixed,) = [vacuum for vacuum in statements if vacuum.table == "t_fixed"]
    assert fixed.options == {"FREEZE", "ANALYZE", "SKIP_LOCKED"}
    assert "PROCESS_TOAST" not in fixed.sql
    assert len(statements) == 3


def test_role_that_may_not_use_pg_toast_gets_the_single_statement(db, make_role, run_ff):
    # The table's owner may vacuum the table and, through it, its TOAST
    # table, but not name the TOAST table: that needs USAGE on pg_toast.
    # Its run sends what every run sent before the split, and the TOAST
    # table is still vacuumed, with its parent.
    owner = make_role("owner")
    create_toasted_table(db, "t_wide")
    db.execute(f'ALTER TABLE public.t_wide OWNER TO "{owner}"')
    toast_before = toast_relfrozenxid(db, "t_wide")

    result = run_ff(*FREEZE_ALL, "-U", owner, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    (vacuum,) = db.script_vacuum_statements()
    assert vacuum.target == ("public", "t_wide")
    assert vacuum.options == {"FREEZE", "ANALYZE", "SKIP_LOCKED"}
    assert "PROCESS_TOAST" not in vacuum.sql
    # The question was asked, and its answer was no.
    assert [sql for sql in db.script_statements(user=owner) if SPLIT_QUERY in sql]
    assert toast_relfrozenxid(db, "t_wide") > toast_before


def test_comment_is_on_the_split_query_and_both_statements(db, run_ff):
    create_toasted_table(db, "t_wide")

    result = run_ff(*FREEZE_ALL, "-c", COMMENT, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    (split,) = [sql for sql in db.script_statements() if SPLIT_QUERY in sql]
    assert split.endswith(f" /* {COMMENT} */")
    vacuums = db.script_vacuum_statements()
    assert len(vacuums) == 2
    for vacuum in vacuums:
        assert vacuum.sql.endswith(f" /* {COMMENT} */")


def test_system_catalog_toast_tables_are_vacuumed_by_statements_of_their_own(
    db, run_ff
):
    # pg_rewrite has a TOAST table in every supported version.
    rewrite_toast = db.execute(
        "SELECT reltoastrelid::regclass::text FROM pg_class "
        "WHERE oid = 'pg_catalog.pg_rewrite'::regclass"
    )[0][0]

    result = run_ff(*FREEZE_ALL, "--system-catalogs", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    statements = db.script_vacuum_statements()
    (at,) = [i for i, vacuum in enumerate(statements) if vacuum.table == "pg_rewrite"]
    assert "PROCESS_TOAST false" in statements[at].options
    assert statements[at + 1].target == (
        "pg_toast", rewrite_toast.removeprefix("pg_toast.")
    )
    # Every pair folds: each TOAST statement directly follows its parent's.
    assert len(db.script_vacuums()) < len(statements)


def test_error_on_the_toast_statement_is_a_failed_table(db, start_ff, tmp_path):
    # Handled like an error on any VACUUM: the table failed, under its own
    # name, and the run goes on.  The script is stopped just before the
    # TOAST table's statement, and the table is dropped meanwhile, so that
    # the statement names a relation that no longer exists.
    toast = create_toasted_table(db, "t_gone")
    db.create_table("t_next")
    before = db.frozenxids()
    gate = StatementGate(tmp_path / "gate", f") {toast}")

    proc = start_ff(*FREEZE_ALL, "-d", db.name, env=gate.env)
    try:
        gate.wait_until_held()
        db.execute("DROP TABLE public.t_gone")
    finally:
        gate.release()
    stdout, stderr = proc.communicate(timeout=PROMPT)

    assert proc.returncode == 1, stdout + stderr
    assert stdout == ""
    lines = stderr.splitlines()
    assert lines[0] == f"VACUUMing the TOAST table of public.t_gone ({toast}) failed."
    assert toast in lines[1]
    assert lines[2:] == [
        "Not all tables were vacuumed.",
        f"Tables that failed: public.t_gone in database {db.name}",
    ]
    assert db.frozen_since(before) == {("public", "t_next")}


# -- an interrupt ---------------------------------------------------------------------


def test_signal_during_the_toast_statement_cancels_it_and_names_the_parent(
    db, start_ff
):
    # The parent's statement leaves the TOAST table out and finishes; the
    # TOAST table's waits for the pinned page and is cancelled.
    toast = create_toasted_table(db, "t_held")
    toast_before = toast_relfrozenxid(db, "t_held")

    with toast_page_pinned(db, toast):
        proc = start_ff(*FREEZE_ALL, "-d", db.name)

        def script_waiting():
            return db.execute(
                "SELECT 1 FROM pg_stat_activity WHERE application_name = %s "
                "AND wait_event_type = 'BufferPin' AND query LIKE %s",
                (SCRIPT_APP, f"VACUUM % {toast}%"),
            )

        wait_for(script_waiting, what="the TOAST table's VACUUM to wait")
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=PROMPT)
        wait_for(
            lambda: not db.execute(
                "SELECT 1 FROM pg_stat_activity WHERE application_name = %s",
                (SCRIPT_APP,),
            ),
            what="the script's backend to be gone",
        )
        toast_after = toast_relfrozenxid(db, "t_held")

    assert proc.returncode == 143
    assert stdout == ""
    assert stderr.splitlines() == [
        "Vacuuming interrupted by SIGTERM",
        f"Work remains: VACUUM of public.t_held in database {db.name} was interrupted.",
    ]
    assert toast_after == toast_before
    assert len(db.script_vacuum_statements()) == 2
