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

from ffharness import FREEZE_ALL, assert_lines_in_order

T1 = ("public", "t1")
TIMESTAMPED = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^:]*: .+")


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


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
    assert vacuums[0].options == {"FREEZE", "ANALYZE"}


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
            "processing table t1",
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
    assert vacuums[0].options == {"FREEZE"}
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
    assert vacuums[0].options == {"ANALYZE"}
    assert f"{vacuums[0].sql} in database {db.name}" in result.stdout.splitlines()
    assert "FREEZE" not in result.stdout
    db.wait_for_vacuum_count("t_dead", 1)
    assert db.is_analyzed("t_dead")
    assert not db.is_analyzed("t_clean")


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
    statements = db.script_statements()
    assert statements[:2] == [
        "SET vacuum_cost_delay = 20",
        "SET vacuum_cost_limit = 2000",
    ]
    # ADR 0005: without --enforce-time any inherited statement_timeout is
    # cleared immediately before the vacuum.
    assert statements[-2] == "SET statement_timeout = 0"
    assert statements[-1] == db.script_vacuums()[0].sql


def test_cost_options_are_applied(db, run_ff):
    db.create_table("t1")

    result = run_ff(
        *FREEZE_ALL, "--costdelay", "3", "--costlimit", "700", "-d", db.name
    )

    assert result.returncode == 0
    assert db.script_statements()[:2] == [
        "SET vacuum_cost_delay = 3",
        "SET vacuum_cost_limit = 700",
    ]


def test_enforce_time_sets_statement_timeout_to_window_plus_grace(db, run_ff):
    # ADR 0002: the timeout is the time left in the window plus 30 seconds.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--enforce-time", "-m", "10", "-d", db.name)

    assert result.returncode == 0
    statements = db.script_statements()
    assert statements[-1] == db.script_vacuums()[0].sql
    match = re.fullmatch(r"SET statement_timeout = '(\d+)s'", statements[-2])
    assert match, statements[-2]
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
