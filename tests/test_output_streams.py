# Tests for where the script's output goes (issue #40, item 8).
#
# The rule under test: a line is a failure report if what it says is by
# itself a reason for the exit status not to be 0 -- a table that failed or
# that the server declined to vacuum, a database that was skipped, a --table
# name found nowhere, a run that could not start or was stopped by a signal,
# and the summary lines that list those.  Failure reports go to stderr, and
# to the log file as well when -l is given.  Everything else (progress, what
# a run was told to leave out, a stop at the time limit) goes to stdout, or
# to the log file instead when -l is given.  Debug output is stderr only.
#
# Why it matters is cron: it mails what a job writes to stderr.  Before this
# rule a failure was on stdout among the progress lines, or with -l in the
# log file alone, and the mail was empty.  The log file still gets a copy
# because it is the one record of the whole run in order.
#
# What each failure looks like is the subject of test_run_outcome.py,
# test_locks.py and test_signals.py, which assert on these same streams; this
# file is about the streams themselves, the order of the two taken together,
# the log file's share, and the log file being closed on every way out.
#
# A run without -v prints nothing but its closing headline and its failure
# reports.  That is what lets the tests below say "stdout is empty", and
# "the log holds exactly what stderr got", instead of picking lines out.

import contextlib
import re
import signal

import pytest
from ffharness import FREEZE_ALL, HARNESS_APP, assert_lines_in_order, wait_for

NO_SUCH_DB = "ff_no_such_database"
TIMESTAMPED = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[^:]*: .+")

# Written out, as in test_locks.py: the key is part of what the script
# documents.
ADVISORY_KEY = 7380385457142659685

# A schema the roles made by make_role have no USAGE on; a VACUUM that names
# a table in it is refused with an error.
CLOSED_SCHEMA = "vault"

# Distinctive enough that a substring search cannot hit anything else.
SECRET = "s3cr3t-Zq7XvK-pw"
MASKED = "dbpass='********'"

# How long an interrupted script is given to exit.
PROMPT = 15.0

# The SIGHUP cases need a script the signal can reach: see the same
# parametrization in test_signals.py.
EACH_SIGNAL = pytest.mark.parametrize(
    "signum, status",
    [
        pytest.param(signal.SIGINT, 130, id="SIGINT"),
        pytest.param(signal.SIGTERM, 143, id="SIGTERM"),
        pytest.param(
            signal.SIGHUP, 129, id="SIGHUP",
            marks=pytest.mark.skipif(
                signal.getsignal(signal.SIGHUP) is signal.SIG_IGN,
                reason="SIGHUP is ignored in this process, and so in the script it starts",
            ),
        ),
    ],
)


def assert_reported(text, starts):
    """Assert that each of `starts` begins a line of `text`, in that order.

    Beginnings and not whole lines, because several reports end with the
    server's or the driver's own wording, which is theirs to change.
    """
    lines = text.splitlines()
    position = 0
    for want in starts:
        for n in range(position, len(lines)):
            if lines[n].startswith(want):
                position = n + 1
                break
        else:
            raise AssertionError(
                f"no line starting {want!r}, or out of order, in:\n{text}"
            )


def log_body(logfile):
    """The lines a run wrote to its log file after the header.

    The header (a blank line, a rule, "flexible freeze started ...") is
    written only to a log file and is left out so that what remains can be
    compared with what the same run wrote elsewhere.
    """
    lines = logfile.read_text().splitlines()
    started = [n for n, line in enumerate(lines) if "flexible freeze started " in line]
    assert len(started) == 1, lines
    return lines[started[0] + 1:]


def finish(proc, timeout=PROMPT):
    """Wait for a script started with start_ff; return (status, stdout, stderr)."""
    stdout, stderr = proc.communicate(timeout=timeout)
    return proc.returncode, stdout, stderr


# -- one scenario for each kind of failure ------------------------------------
#
# Each sets the failure up and returns the script's arguments and the
# beginnings of the lines that report it.  `stack` closes whatever has to
# stay open while the script runs.


def failed_table(db, make_role, stack):
    role = make_role("owner")
    db.create_schema(CLOSED_SCHEMA)
    db.create_table("t_fails", schema=CLOSED_SCHEMA)
    return ("-U", role, "-d", db.name), failed_table_report(db)


def failed_table_report(db):
    return [
        f"VACUUMing {CLOSED_SCHEMA}.t_fails failed.",
        f"permission denied for schema {CLOSED_SCHEMA}",
        "Not all tables were vacuumed.",
        f"Tables that failed: {CLOSED_SCHEMA}.t_fails in database {db.name}",
    ]


def table_the_server_skipped(db, make_role, stack):
    # The role can reach the table and does not own it: the server skips it
    # with a WARNING and reports success.  Without ANALYZE in the statement
    # there is one message and not two.
    role = make_role("owner")
    db.create_table("t_other")
    return ("--no-analyze", "-U", role, "-d", db.name), [
        ("VACUUMing public.t_other drew a message from the server; "
        "not counting it as vacuumed."),
        "WARNING:",
        "Not all tables were vacuumed.",
        f"Tables not counted as vacuumed: public.t_other in database {db.name}",
    ]


def unreachable_database(db, make_role, stack):
    return ("-d", NO_SUCH_DB), [
        f"skipping database {NO_SUCH_DB} (couldn't connect: ",
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {NO_SUCH_DB}",
    ]


def database_held_by_another_run(db, make_role, stack):
    # The holder is the harness's own session, which the report names and
    # does not take for a run of the script (test_locks.py has the cases).
    conn = db.connect()
    stack.callback(conn.close)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_KEY,))
        cur.execute("SELECT pg_backend_pid()")
        pid = cur.fetchone()[0]
    return ("-d", db.name), [
        (f"skipping database {db.name} (advisory lock {ADVISORY_KEY} is held "
         "there by a session that does not appear to be flexible_freeze, and "
         "every run will skip this database until that session releases the "
         f"lock or ends: pid {pid}, role "
         f'"{db.cluster.superuser}", application name "{HARNESS_APP}")'),
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]


def table_listing_failure(db, make_role, stack):
    # The server refuses a vacuum_cost_delay above 100 ms, which is reported
    # as a database whose session could not be set up.
    return ("--costdelay", "1000", "-d", db.name), [
        f"skipping database {db.name} (couldn't set up its session: ",
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {db.name}",
    ]


def table_found_nowhere(db, make_role, stack):
    db.create_table("items")
    return ("--table", "orders", "-d", db.name), [
        "Not all tables were vacuumed.",
        f"Databases where --table orders matched no table that can be vacuumed: {db.name}",
    ]


def database_list_failure(db, make_role, stack):
    # No -d: the list of databases is read first, and a run that cannot read
    # it ends there, before there is anything to summarise.
    return ("-U", "ff_no_such_role"), [
        "Could not list databases: connection to database postgres failed: ",
    ]


EVERY_FAILURE = pytest.mark.parametrize(
    "scenario",
    [
        failed_table,
        table_the_server_skipped,
        unreachable_database,
        database_held_by_another_run,
        table_listing_failure,
        table_found_nowhere,
        database_list_failure,
    ],
    ids=lambda scenario: scenario.__name__,
)


# -- failure reports ----------------------------------------------------------


@EVERY_FAILURE
def test_failure_is_reported_on_stderr_and_not_on_stdout(
    db, make_role, run_ff, scenario
):
    with contextlib.ExitStack() as stack:
        args, report = scenario(db, make_role, stack)
        result = run_ff(*FREEZE_ALL, *args)

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert_reported(result.stderr, report)
    # Not repeated there: the two streams usually end up in the same place.
    assert result.stdout == ""


@EVERY_FAILURE
def test_failure_is_reported_on_stderr_and_in_the_log_file(
    db, make_role, run_ff, tmp_path, scenario
):
    logfile = tmp_path / "ff.log"

    with contextlib.ExitStack() as stack:
        args, report = scenario(db, make_role, stack)
        result = run_ff(*FREEZE_ALL, "-l", str(logfile), *args)

    assert result.returncode == 1
    assert result.stdout == ""
    assert_reported(result.stderr, report)
    # The log file has the same report, whole and in the same order.
    assert log_body(logfile) == result.stderr.splitlines()


def test_could_not_open_the_log_file_is_reported_on_stderr(db, run_ff, tmp_path):
    result = run_ff(
        *FREEZE_ALL, "-l", str(tmp_path / "missing-dir" / "ff.log"), "-d", db.name
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert_reported(result.stderr, ["could not open logfile: "])


# -- progress and failures in one run -----------------------------------------


def a_table_that_fails_and_one_that_works(db, make_role):
    """t_fails is created first, so it is the oldest table and taken first."""
    role = make_role("owner")
    db.create_schema(CLOSED_SCHEMA)
    db.create_table("t_fails", schema=CLOSED_SCHEMA)
    db.create_table("t_next", owner=role)
    return role


def whole_run_in_order(db):
    """Every line of that run with -v, as the script writes them."""
    return [
        f"working on database {db.name}",
        f"processing table {CLOSED_SCHEMA}.t_fails",
        f"VACUUMing {CLOSED_SCHEMA}.t_fails failed.",
        f"permission denied for schema {CLOSED_SCHEMA}",
        "processing table public.t_next",
        "Not all tables were vacuumed.",
        f"Tables that failed: {CLOSED_SCHEMA}.t_fails in database {db.name}",
        "1 tables in 1 databases",
        "Flexible Freeze run complete",
    ]


def test_progress_is_on_stdout_and_failures_are_on_stderr(db, make_role, run_ff):
    role = a_table_that_fails_and_one_that_works(db, make_role)

    result = run_ff(*FREEZE_ALL, "-v", "-U", role, "-d", db.name)

    assert result.returncode == 1
    failures = failed_table_report(db)
    # Each line is on one stream and one only.
    assert result.stderr.splitlines() == failures
    progress = [line for line in whole_run_in_order(db) if line not in failures]
    assert_lines_in_order(result.stdout, progress)
    assert not set(result.stdout.splitlines()) & set(failures)


def test_the_two_streams_together_are_in_the_order_of_the_run(db, make_role, run_ff):
    # Both streams are flushed line by line, so a terminal, or a file that
    # both are sent to, shows a failure where it happened and not after the
    # progress lines that followed it.
    role = a_table_that_fails_and_one_that_works(db, make_role)

    result = run_ff(*FREEZE_ALL, "-v", "-U", role, "-d", db.name, merge_streams=True)

    assert result.returncode == 1
    assert_lines_in_order(result.stdout, whole_run_in_order(db))


# The same for the other kinds of failure, one test each: every report has
# its own place in the run, and a line written late (after the lines that
# followed it, or at the very end) is what these would catch.  Two more are
# beside the tests whose set-up they need: a table skipped for a lock
# (test_locks.py), and a lost connection and an interrupted VACUUM, which
# are read from the log file (test_run_outcome.py, test_signals.py).


def test_skipped_database_is_reported_where_it_happened(db, run_ff):
    # A failure that belongs to a database and not to a table: its line
    # comes between the progress lines of the two databases.
    db.create_table("t1")

    result = run_ff(
        *FREEZE_ALL, "-v", "-d", f"{NO_SUCH_DB},{db.name}", merge_streams=True
    )

    assert result.returncode == 1
    assert_reported(
        result.stdout,
        [
            f"working on database {NO_SUCH_DB}",
            f"skipping database {NO_SUCH_DB} (couldn't connect: ",
            f"working on database {db.name}",
            "processing table public.t1",
            "Not all tables were vacuumed.",
            f"Databases skipped or left unfinished: {NO_SUCH_DB}",
            "1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_server_skipped_table_is_reported_where_it_happened(db, make_role, run_ff):
    # t_other is created first, so it is the older table and taken first.
    role = make_role("owner")
    db.create_table("t_other")
    db.create_table("t_own", owner=role)

    result = run_ff(
        *FREEZE_ALL, "-v", "--no-analyze", "-U", role, "-d", db.name,
        merge_streams=True,
    )

    assert result.returncode == 1
    assert_reported(
        result.stdout,
        [
            "processing table public.t_other",
            ("VACUUMing public.t_other drew a message from the server; "
             "not counting it as vacuumed."),
            "WARNING:",
            "processing table public.t_own",
            "Not all tables were vacuumed.",
            f"Tables not counted as vacuumed: public.t_other in database {db.name}",
            "1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_database_held_by_another_run_is_reported_where_it_happened(make_db, run_ff):
    one, two = make_db("one"), make_db("two")
    two.create_table("t_other")

    with contextlib.ExitStack() as stack:
        _, report = database_held_by_another_run(one, None, stack)
        result = run_ff(
            *FREEZE_ALL, "-v", "-d", f"{one.name},{two.name}", merge_streams=True
        )

    assert result.returncode == 1
    assert_reported(
        result.stdout,
        [
            f"working on database {one.name}",
            report[0],
            f"working on database {two.name}",
            "processing table public.t_other",
            *report[1:],
            "1 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_table_found_nowhere_is_reported_where_it_happened(db, run_ff):
    db.create_table("items")

    result = run_ff(
        *FREEZE_ALL, "-v", "--table", "orders", "-d", db.name, merge_streams=True
    )

    assert result.returncode == 1
    assert_reported(
        result.stdout,
        [
            f"specified single table orders does not exist in database {db.name}",
            "Not all tables were vacuumed.",
            f"Databases where --table orders matched no table that can be vacuumed: {db.name}",
            "0 tables in 1 databases",
            "Flexible Freeze run complete",
        ],
    )


def test_log_file_holds_the_whole_run_in_order(db, make_role, run_ff, tmp_path):
    role = a_table_that_fails_and_one_that_works(db, make_role)
    logfile = tmp_path / "ff.log"

    result = run_ff(*FREEZE_ALL, "-v", "-l", str(logfile), "-U", role, "-d", db.name)

    assert result.returncode == 1
    assert result.stdout == ""
    # stderr gets the failure reports and nothing else: with -l the progress
    # lines are in the log file only.
    assert result.stderr.splitlines() == failed_table_report(db)
    assert_lines_in_order("\n".join(log_body(logfile)), whole_run_in_order(db))
    # Once each: the copy for stderr is not also written to the log twice.
    for line in failed_table_report(db):
        assert log_body(logfile).count(line) == 1


def test_clean_run_writes_nothing_to_stderr(db, run_ff, tmp_path):
    db.create_table("t1")
    logfile = tmp_path / "ff.log"

    result = run_ff(*FREEZE_ALL, "-v", "-d", db.name)

    assert result.returncode == 0
    assert result.stderr == ""
    assert_lines_in_order(
        result.stdout,
        [
            "processing table public.t1",
            "All tables vacuumed.",
            "Flexible Freeze run complete",
        ],
    )

    db.execute("SELECT txid_current()")  # so t1's age is above zero again
    result = run_ff(*FREEZE_ALL, "-v", "-l", str(logfile), "-d", db.name)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == ""
    assert_lines_in_order(
        "\n".join(log_body(logfile)),
        [
            "processing table public.t1",
            "All tables vacuumed.",
            "Flexible Freeze run complete",
        ],
    )


def test_timestamps_apply_to_failure_reports_on_both_streams(
    db, make_role, run_ff, tmp_path
):
    logfile = tmp_path / "ff.log"

    with contextlib.ExitStack() as stack:
        args, report = failed_table(db, make_role, stack)
        result = run_ff(*FREEZE_ALL, "-t", "-l", str(logfile), *args)

    assert result.returncode == 1
    lines = result.stderr.splitlines()
    for line in lines:
        assert TIMESTAMPED.fullmatch(line), line
    assert [line.split(": ", 1)[1] for line in lines] == report
    # The log file's copies are stamped too.  (They are the very lines that
    # went to stderr, built once; with stamps a second apart at the finest,
    # this comparison could not tell if they were built twice.)
    assert log_body(logfile) == lines


# -- what is not a failure report ---------------------------------------------


@pytest.mark.parametrize(
    "option, value",
    [("--exclude-table", "orders"), ("--exclude-table-in-database", "{db}.orders")],
    ids=["exclude-table", "exclude-table-in-database"],
)
def test_excluded_named_table_is_information(db, run_ff, option, value):
    # The run did what it was told, and exits 0.
    db.create_table("orders")

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", option, value.format(db=db.name),
        "-d", db.name,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        (f"specified single table public.orders is excluded in database {db.name} "
        f"by {option}; not vacuuming it"),
        "All tables vacuumed.",
    ]


def test_verbose_notes_are_information(make_db, run_ff):
    # A --table name missing from one database of several, and an exclusion
    # that matched nothing: both are said with -v, neither changes the exit
    # status, and neither belongs in a scheduler's mail.
    one, two = make_db("one"), make_db("two")
    one.create_table("items")
    two.create_table("orders")

    result = run_ff(
        *FREEZE_ALL, "-v", "--table", "orders", "-T", "t_mistyped",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert_lines_in_order(
        result.stdout,
        [
            f"specified single table orders does not exist in database {one.name}",
            "All tables vacuumed.",
            "exclusions that matched no selected table: -T t_mistyped",
        ],
    )


def test_stop_at_the_time_limit_is_information(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-m", "0", "-d", db.name)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == "Vacuuming halted due to timeout\n"


def test_time_limited_run_that_also_missed_work_reports_each_on_its_stream(
    db, run_ff
):
    # The stop at the limit is still not a failure; the database that was
    # skipped before it is, and is what makes the exit status 1.
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-m", "0", "-d", f"{NO_SUCH_DB},{db.name}")

    assert result.returncode == 1
    assert result.stdout == "Vacuuming halted due to timeout\n"
    assert_reported(
        result.stderr,
        [
            f"skipping database {NO_SUCH_DB} (couldn't connect: ",
            f"Databases skipped or left unfinished: {NO_SUCH_DB}",
        ],
    )
    assert "Not all tables were vacuumed." not in result.stdout + result.stderr


# -- the log file is complete and closed on every way out ----------------------
#
# The content is read after the process has ended.  That alone would not
# show a file left open, since every line is flushed as it is written, so
# each test also asks the process itself (open_files_probe).  The standard
# streams are always among the files it names; that they are is what shows
# the probe ran and could see an open file.


def assert_closed(probe, logfile):
    left_open = probe.files_left_open()
    assert "<stderr>" in left_open
    assert str(logfile) not in left_open


def test_log_file_is_complete_and_closed_after_a_clean_run(
    db, run_ff, tmp_path, open_files_probe
):
    db.create_table("t1")
    logfile = tmp_path / "ff.log"

    result = run_ff(
        *FREEZE_ALL, "-l", str(logfile), "-d", db.name, env=open_files_probe.env
    )

    assert result.returncode == 0
    assert log_body(logfile) == ["All tables vacuumed."]
    assert_closed(open_files_probe, logfile)


def test_log_file_is_complete_and_closed_after_a_failing_run(
    db, run_ff, tmp_path, open_files_probe
):
    db.create_table("t1")
    logfile = tmp_path / "ff.log"

    result = run_ff(
        *FREEZE_ALL, "-l", str(logfile), "-d", f"{NO_SUCH_DB},{db.name}",
        env=open_files_probe.env,
    )

    assert result.returncode == 1
    assert log_body(logfile)[-2:] == [
        "Not all tables were vacuumed.",
        f"Databases skipped or left unfinished: {NO_SUCH_DB}",
    ]
    assert_closed(open_files_probe, logfile)


def test_log_file_is_complete_and_closed_when_the_run_cannot_start(
    run_ff, tmp_path, open_files_probe
):
    # One of the exits that come before the first database, after the log
    # file has been opened.
    logfile = tmp_path / "ff.log"

    result = run_ff(
        *FREEZE_ALL, "-l", str(logfile), "-U", "ff_no_such_role",
        env=open_files_probe.env,
    )

    assert result.returncode == 1
    assert_reported(
        "\n".join(log_body(logfile)),
        ["Could not list databases: connection to database postgres failed: "],
    )
    assert log_body(logfile) == result.stderr.splitlines()
    assert_closed(open_files_probe, logfile)


@EACH_SIGNAL
def test_log_file_is_complete_and_closed_after_a_signal(
    db, start_ff, tmp_path, open_files_probe, signum, status
):
    # With -v the script logs "processing table ..." and then takes the pause
    # owed since the previous vacuum; that line for the second table says it
    # is at the pause, which is where the signal finds it.
    db.create_table("t_first")
    db.create_table("t_second")
    logfile = tmp_path / "ff.log"
    interrupted = f"Vacuuming interrupted by {signal.Signals(signum).name}"

    proc = start_ff(
        *FREEZE_ALL, "-v", "-l", str(logfile), "--pause", "600", "-d", db.name,
        env=open_files_probe.env,
    )
    wait_for(
        lambda: logfile.exists()
        and "processing table public.t_second" in logfile.read_text(),
        what="the script to reach the pause before the second table",
    )
    proc.send_signal(signum)
    code, stdout, stderr = finish(proc)

    assert code == status
    assert stdout == ""
    assert stderr.splitlines() == [interrupted]
    assert log_body(logfile)[-3:] == [
        interrupted,
        "after vacuuming 1 tables in 1 databases",
        "Flexible Freeze run complete",
    ]
    assert_closed(open_files_probe, logfile)


def test_log_file_is_complete_and_closed_after_a_signal_before_the_first_database(
    start_ff, silent_server, tmp_path, open_files_probe
):
    # Without -d the first connection is for the list of databases; a signal
    # there ends the run from a different place than one during the run.
    logfile = tmp_path / "ff.log"

    proc = start_ff(
        "-l", str(logfile), port=silent_server.port, env=open_files_probe.env
    )
    silent_server.wait_for_client()
    proc.send_signal(signal.SIGTERM)
    code, stdout, stderr = finish(proc)

    assert code == 143
    assert stdout == ""
    assert stderr.splitlines() == ["Vacuuming interrupted by SIGTERM"]
    assert log_body(logfile) == ["Vacuuming interrupted by SIGTERM"]
    assert_closed(open_files_probe, logfile)


# -- a standard error that cannot be written to -------------------------------


#
# Before failure reports went to stderr, a run with -l did not write to that
# stream at all, so a reader that had gone away could not hurt it.  It still
# must not: the report is in the log file, the rest of the run is carried
# out, and the exit status is the run's own.
#
# The exit status is the delicate part.  Python buffers stderr, and a line
# left in that buffer by a failed write makes the interpreter exit with 120
# when it cannot flush it either.  An unbuffered interpreter has no such
# line and hides the whole problem, so these runs are made buffered whatever
# the environment of the test run says: to Python an empty PYTHONUNBUFFERED
# is an unset one.
#
# In both tests the pipe is closed at a moment when the script is known not
# to have written a failure report yet.

BUFFERED = {"PYTHONUNBUFFERED": ""}


def test_failing_run_goes_on_and_exits_1_when_nobody_is_reading_stderr(
    db, make_role, start_ff, script_clock, tmp_path
):
    # The script is held at its first reading of the clock, which comes
    # before it has opened the log file or connected to anything.
    role = a_table_that_fails_and_one_that_works(db, make_role)
    logfile = tmp_path / "ff.log"
    script_clock.hold()

    proc = start_ff(
        *FREEZE_ALL, "-v", "-l", str(logfile), "-U", role, "-d", db.name,
        env={**script_clock.env, **BUFFERED},
    )
    script_clock.wait_until_held()
    proc.stderr.close()
    script_clock.release()

    assert proc.wait(timeout=PROMPT) == 1
    # The table after the one that failed was still processed, and the
    # summary is all there.
    assert_lines_in_order("\n".join(log_body(logfile)), whole_run_in_order(db))


@EACH_SIGNAL
def test_interrupted_run_keeps_its_exit_status_when_nobody_is_reading_stderr(
    db, start_ff, tmp_path, open_files_probe, signum, status
):
    # The script is caught at the pause before the second table (see the
    # signal test above); the signal then supplies the failure report.
    db.create_table("t_first")
    db.create_table("t_second")
    logfile = tmp_path / "ff.log"

    proc = start_ff(
        *FREEZE_ALL, "-v", "-l", str(logfile), "--pause", "600", "-d", db.name,
        env={**open_files_probe.env, **BUFFERED},
    )
    wait_for(
        lambda: logfile.exists()
        and "processing table public.t_second" in logfile.read_text(),
        what="the script to reach the pause before the second table",
    )
    proc.stderr.close()
    proc.send_signal(signum)

    assert proc.wait(timeout=PROMPT) == status
    assert log_body(logfile)[-3:] == [
        f"Vacuuming interrupted by {signal.Signals(signum).name}",
        "after vacuuming 1 tables in 1 databases",
        "Flexible Freeze run complete",
    ]
    assert_closed(open_files_probe, logfile)


# -- the password -------------------------------------------------------------


def test_password_is_on_no_stream_and_not_in_the_log_file_of_a_failing_run(
    db, run_ff, tmp_path
):
    # The failure reports are a second path to stderr and to the log file;
    # the masking of -w (test_password_output.py) has to hold on it too.
    db.create_table("t1")
    logfile = tmp_path / "ff.log"

    result = run_ff(
        *FREEZE_ALL, "-w", SECRET, "--debug", "-v", "-l", str(logfile),
        "-d", f"{NO_SUCH_DB},{db.name}",
    )

    assert result.returncode == 1
    log = logfile.read_text()
    assert_reported(result.stderr, [f"skipping database {NO_SUCH_DB} (couldn't connect: "])
    assert_reported(log, [f"skipping database {NO_SUCH_DB} (couldn't connect: "])
    for text in (result.stdout, result.stderr, log):
        assert SECRET not in text
    assert MASKED in result.stderr
    assert MASKED in log
