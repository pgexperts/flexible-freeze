# Characterization tests for how a run is bounded and reported: connection
# options and failures, the time limit, the log file and debug output.
#
# Two things are deliberately left unpinned because they are open questions
# (issue #40), not settled behaviour:
#
# * exit status after skipping a database or stopping at the time limit:
#   those tests assert the message and the effect on the tables only;
# * which stream an error message goes to: such messages are looked for in
#   stdout and stderr together (see `output`).

from ffharness import FREEZE_ALL, assert_lines_in_order

T1 = ("public", "t1")


def output(result):
    return result.stdout + result.stderr


def test_unreachable_database_is_skipped_and_the_run_continues(db, run_ff):
    # ADR 0004.  The missing database comes first: the run must get past it
    # to reach the real one.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", f"ff_no_such_database,{db.name}")

    assert "skipping database ff_no_such_database (couldn't connect: " in output(result)
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == {T1}


def test_zero_minute_limit_halts_before_the_first_table(db, run_ff):
    # ADR 0002: the limit is checked before each table, so a window that is
    # already over stops the run without vacuuming anything.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-m", "0", "-d", db.name)

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
    assert "could not open logfile: " in output(result)
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
    assert (
        "Could not list databases: connection to database postgres failed: "
        in output(result)
    )
    assert "Traceback" not in result.stderr
    assert cluster.statements(since=mark) == []
