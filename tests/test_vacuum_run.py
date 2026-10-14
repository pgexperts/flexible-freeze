# Characterization tests for what a run does to the tables it selects: the
# VACUUM it sends (FREEZE / ANALYZE variants, session settings around it),
# the order tables are taken in, and what it prints with -v and -t.
#
# These record behaviour that works today, for lowercase tables in schema
# public.  Schema-qualified and mixed-case names are deliberately absent:
# they currently fail (issue #31) and belong to the branch that fixes them.
#
# Assertions are on parsed statements (target and options), not on the
# literal VACUUM text, so they keep holding if the statement is respelled.

import re

import pytest
from ffharness import FREEZE_ALL, assert_lines_in_order

T1 = ("public", "t1")
SET_SEARCH_PATH = "SELECT pg_catalog.set_config('search_path', '', false)"
# The key is part of what the script documents; test_locks.py says why.
ADVISORY_LOCK = "SELECT pg_catalog.pg_try_advisory_lock(7380385457142659685)"
# What only the statement asking whether to vacuum TOAST tables apart has.
TOAST_SPLIT = "pg_catalog.has_schema_privilege('pg_toast', 'USAGE')"
TIMESTAMPED = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^:]*: .+")


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def table_bytes(db):
    # The main fork of t1 only, which is what truncation shortens.
    return db.execute("SELECT pg_relation_size('public.t1')")[0][0]


def test_default_run_freezes_and_analyzes(db, run_ff):
    db.create_table("t1")
    before = db.frozenxids()

    # No -d: the database is found through the default database list.
    result = run_ff(*FREEZE_ALL)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {T1}
    assert db.is_analyzed("t1")
    db.wait_for_vacuum_count("t1", 1)
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [T1]
    assert vacuums[0].options == {"FREEZE", "ANALYZE", "SKIP_LOCKED"}


def test_default_freezeage_leaves_young_tables_alone(db, run_ff):
    # Without FREEZE_ALL the threshold is ten million transactions, which a
    # new table is nowhere near.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff("-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == set()
    assert db.script_vacuums() == []


def test_verbose_names_the_statement_run(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-v", "-d", db.name)

    assert result.returncode == 0
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [T1]
    assert_lines_in_order(
        result.stdout,
        [
            "Flexible Freeze run starting",
            f"Processing 1 database (list of databases is {db.name})",
            f"working on database {db.name}",
            "getting list of tables",
            "processing table public.t1",
            # The statement printed is the one the server received.
            f"{vacuums[0].sql} in database {db.name}",
            "All tables vacuumed.",
            "1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_no_analyze_drops_analyze(db, run_ff):
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--no-analyze", "-v", "-d", db.name)

    assert result.returncode == 0
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [T1]
    assert vacuums[0].options == {"FREEZE", "SKIP_LOCKED"}
    assert f"{vacuums[0].sql} in database {db.name}" in result.stdout.splitlines()
    assert "ANALYZE" not in result.stdout
    assert db.frozen_since(before) == {T1}
    assert not db.is_analyzed("t1")


def test_no_freeze_vacuums_a_table_with_dead_rows_and_skips_a_clean_one(db, run_ff):
    # --no-freeze selects from the statistics views instead of by XID age,
    # so --freezeage plays no part.  Autovacuum is off in the test cluster,
    # which keeps the dead rows in place and last_autovacuum NULL.  Half of
    # t_dead's rows are deleted, far past the script's thresholds (more than
    # 100 dead tuples, more than 5% dead); the thresholds themselves are not
    # probed here.
    db.create_table("t_dead", rows=1000)
    db.create_table("t_clean", rows=1000)
    db.make_dead_tuples("t_dead")

    result = run_ff("--no-freeze", "-v", "-d", db.name)

    assert result.returncode == 0
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("public", "t_dead")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    assert f"{vacuums[0].sql} in database {db.name}" in result.stdout.splitlines()
    assert "FREEZE" not in result.stdout
    db.wait_for_vacuum_count("t_dead", 1)
    assert db.is_analyzed("t_dead")
    assert not db.is_analyzed("t_clean")


@pytest.mark.parametrize(
    "mode, mode_options",
    [(FREEZE_ALL, {"FREEZE", "ANALYZE"}), (("--no-freeze",), {"ANALYZE"})],
    ids=["freeze", "no-freeze"],
)
@pytest.mark.parametrize("no_truncate", [False, True], ids=["default", "no-truncate"])
def test_no_truncate_keeps_the_empty_pages_at_the_end_of_a_table(
    db, run_ff, mode, mode_options, no_truncate
):
    # Every row is deleted, so the whole table is empty pages that VACUUM
    # gives back to the operating system unless it is told not to.  Half of
    # the rows go first, through make_dead_tuples, which waits for the
    # statistics the --no-freeze selection reads.
    db.create_table("t1", rows=1000)
    db.make_dead_tuples("t1")
    db.execute("DELETE FROM public.t1")
    size_before = table_bytes(db)
    assert size_before > 0

    result = run_ff(*mode, *(["--no-truncate"] if no_truncate else []), "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [T1]
    # The option is sent only when it was asked for.
    assert vacuums[0].options == mode_options | {"SKIP_LOCKED"} | (
        {"TRUNCATE false"} if no_truncate else set()
    )
    # And the server acted on it.
    assert table_bytes(db) == (size_before if no_truncate else 0)


def test_verbose_shows_truncate_false_in_the_statement(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "--no-truncate", "-v", "-d", db.name)

    assert result.returncode == 0
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [T1]
    assert "TRUNCATE false" in vacuums[0].options
    assert f"{vacuums[0].sql} in database {db.name}" in result.stdout.splitlines()


def test_table_with_the_oldest_xid_is_frozen_first(db, run_ff):
    # ADR 0003, heap age only (the TOAST term is not exercised).  t_first is
    # created first, then frozen by hand so that it becomes the younger of
    # the two: age order is now the reverse of both creation order and name
    # order.
    first = db.create_table("t_first")
    second = db.create_table("t_second")
    db.execute('VACUUM FREEZE "t_first"')
    db.execute("SELECT txid_current()")  # so t_first's age is above zero again
    xids = db.frozenxids()
    assert xids[second] < xids[first]

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    assert targets(db.script_vacuums()) == [second, first]
    assert db.frozen_since(xids) == {first, second}


def test_session_settings_precede_the_work(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    # script_statements() holds the script's own statements only: anything
    # the driver sends for itself at session start is filtered out by
    # content, so the positions below do not depend on the server's settings
    # (test_statement_log_leaves_out_the_drivers_session_setup).
    statements = db.script_statements()
    # The empty search_path is set first, before any other statement.
    assert statements[0] == SET_SEARCH_PATH
    # Then the session settings, all of them before the table list is read.
    # Warnings are asked for so that a table the server declines to vacuum
    # is noticed (test_run_outcome.py).  The lock timeout has to be in place
    # before the table list is read, which is the statement it is mainly
    # there for, and the database is claimed for this run before any work is
    # done in it (test_locks.py for both).  Between the two, the session
    # asks whether it vacuums TOAST tables apart (test_toast.py).
    assert statements[1:5] == [
        "SET vacuum_cost_delay = 20",
        "SET vacuum_cost_limit = 2000",
        "SET client_min_messages = warning",
        "SET lock_timeout = '10s'",
    ]
    assert TOAST_SPLIT in statements[5]
    assert statements[6] == ADVISORY_LOCK
    # ADR 0005: without --enforce-time any inherited statement_timeout is
    # cleared immediately before the vacuum: here two statements, t1's and
    # its TOAST table's, since the harness's superuser splits them.
    (vacuum,) = db.script_vacuums()
    assert statements[-3:] == ["SET statement_timeout = 0", vacuum.sql, vacuum.toast.sql]


def test_session_settings_precede_the_work_on_a_non_iso_datestyle(db, run_ff):
    # The same order, in a database where psycopg2 opens every session with
    # a SET datestyle of its own.  That statement is not the script's and
    # must not be taken for its first.
    db.create_table("t1")
    db.execute(f'ALTER DATABASE "{db.name}" SET datestyle = \'German, DMY\'')

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    statements = db.script_statements()
    assert statements[:5] == [
        SET_SEARCH_PATH,
        "SET vacuum_cost_delay = 20",
        "SET vacuum_cost_limit = 2000",
        "SET client_min_messages = warning",
        "SET lock_timeout = '10s'",
    ]
    assert TOAST_SPLIT in statements[5]
    assert statements[6] == ADVISORY_LOCK
    (vacuum,) = db.script_vacuums()
    assert statements[-3:] == ["SET statement_timeout = 0", vacuum.sql, vacuum.toast.sql]


def test_cost_options_are_applied(db, run_ff):
    db.create_table("t1")

    result = run_ff(
        *FREEZE_ALL, "--costdelay", "3", "--costlimit", "700", "-d", db.name
    )

    assert result.returncode == 0
    # The script's statements only; see test_session_settings_precede_the_work.
    statements = db.script_statements()
    assert statements[0] == SET_SEARCH_PATH
    assert statements[1:3] == [
        "SET vacuum_cost_delay = 3",
        "SET vacuum_cost_limit = 700",
    ]


def test_enforce_time_sets_statement_timeout_to_window_plus_grace(db, run_ff):
    # ADR 0002: the timeout is the time left in the window plus 30 seconds.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--enforce-time", "-m", "10", "-d", db.name)

    assert result.returncode == 0
    # The script's statements only; see test_session_settings_precede_the_work.
    # One timeout for the table's two statements (test_toast.py).
    statements = db.script_statements()
    (vacuum,) = db.script_vacuums()
    assert statements[-2:] == [vacuum.sql, vacuum.toast.sql]
    match = re.fullmatch(r"SET statement_timeout = '(\d+)s'", statements[-3])
    assert match, statements[-3]
    # Ten minutes, minus however long the run took to reach the table, plus
    # the grace period.  Anything above 600 can only come from the grace
    # period; the lower bound holds as long as the script reaches its first
    # table within 30 seconds of starting, which normally takes well under
    # one.
    assert 600 < int(match.group(1)) <= 630
    assert db.frozen_since(before) == {T1}


def test_print_timestamps_prefixes_every_line(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-t", "-v", "-d", db.name)

    assert result.returncode == 0
    lines = result.stdout.splitlines()
    assert len(lines) > 1
    for line in lines:
        assert TIMESTAMPED.fullmatch(line), line
    assert lines[-1].endswith(": Flexible Freeze run complete")


def test_no_timestamps_by_default(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-v", "-d", db.name)

    assert result.returncode == 0
    assert not [line for line in result.stdout.splitlines() if TIMESTAMPED.fullmatch(line)]
