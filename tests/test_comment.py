# Tests for --comment / -c: the text is appended, as /* TEXT */, to every
# statement the script sends and to nothing else, and no value of it can
# reach the server as anything but a comment.
#
# The text goes into the statement itself (it cannot be a query parameter),
# so the second half is the part that matters: a value that could end the
# comment is refused when the arguments are parsed, with exit status 2 and
# before any connection is made, and a percent sign in it must come through
# unchanged both in the statements the driver formats (those with
# parameters: the --table lookup) and in those it does not.
#
# What the server received is read from its statement log, as everywhere
# else in the suite.

import ast
import os

import pytest
from ffharness import FREEZE_ALL, SCRAM_ROLE, SCRIPT

T1 = ("public", "t1")
COMMENT = "application:flexible_freeze,pganalyze:no-alert"
SET_SEARCH_PATH = "SELECT pg_catalog.set_config('search_path', '', false)"
ADVISORY_LOCK = "SELECT pg_catalog.pg_try_advisory_lock(7380385457142659685)"

# Each selects t1 by a different statement: the freeze selection, the
# dead-row selection, and the --table lookup, which is the one statement
# sent with parameters.
MODES = pytest.mark.parametrize(
    "mode",
    [FREEZE_ALL, ("--no-freeze",), ("--table", "t1")],
    ids=["freeze", "no-freeze", "table"],
)


def as_sent(statement, comment=COMMENT):
    return f"{statement} /* {comment} */"


def table_with_dead_rows(db):
    # Selected in every mode: by its age with FREEZE_ALL, by its dead rows
    # with --no-freeze, by its name with --table.
    db.create_table("t1", rows=1000)
    db.make_dead_tuples("t1")


def assert_rejected(result, cluster, mark):
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert cluster.statements(since=mark) == []


# -- on every statement, and only when asked for ---------------------------------


@MODES
def test_comment_is_appended_to_every_statement(cluster, db, run_ff, mode):
    table_with_dead_rows(db)
    mark = cluster.log_offset()

    # No -d, so that the query for the database list is among the statements.
    result = run_ff(*mode, "-c", COMMENT)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    everything = [record.sql for record in cluster.statements(since=mark)]
    for statement in everything:
        assert statement.endswith(f" /* {COMMENT} */"), statement
        # Once, and inside the statement: not after a closing semicolon.
        assert statement.count("/*") == 1, statement
        assert ";" not in statement, statement
    # Each kind of statement was among them: the database list, ...
    assert [sql for sql in everything if "FROM pg_database" in sql]
    # ... and in the database, the search_path, the session settings and the
    # claim on the database, the table list, and the timeout and the VACUUM.
    statements = db.script_statements()
    assert statements[:6] == [
        as_sent(SET_SEARCH_PATH),
        as_sent("SET vacuum_cost_delay = 20"),
        as_sent("SET vacuum_cost_limit = 2000"),
        as_sent("SET client_min_messages = warning"),
        as_sent("SET lock_timeout = '10s'"),
        as_sent(ADVISORY_LOCK),
    ]
    assert len(statements) == 9
    assert "full_table_name" in statements[6]
    assert statements[7] == as_sent("SET statement_timeout = 0")
    vacuums = db.script_vacuums()
    assert [vacuum.target for vacuum in vacuums] == [T1]
    assert statements[8] == vacuums[0].sql


def test_comment_is_on_the_timeout_of_an_enforced_time_limit(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "--enforce-time", "--comment", COMMENT, "-d", db.name)

    assert result.returncode == 0
    timeout = db.script_statements()[-2]
    assert timeout.startswith("SET statement_timeout = '")
    assert timeout.endswith(f"s' /* {COMMENT} */")


@MODES
def test_without_the_option_no_statement_has_a_comment(cluster, db, run_ff, mode):
    table_with_dead_rows(db)
    mark = cluster.log_offset()

    result = run_ff(*mode)

    assert result.returncode == 0
    everything = [record.sql for record in cluster.statements(since=mark)]
    assert [vacuum.target for vacuum in db.script_vacuums()] == [T1]
    assert not [sql for sql in everything if "/*" in sql or "*/" in sql]


def test_verbose_prints_the_vacuum_without_the_comment(db, run_ff):
    # The comment is the same for the whole run and is in the argument dump;
    # the line for each table stays as it is without the option.
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-v", "-c", COMMENT, "-d", db.name)

    assert result.returncode == 0
    sent = db.script_vacuums()[0].sql
    assert sent.endswith(f" /* {COMMENT} */")
    printed = sent.removesuffix(f" /* {COMMENT} */")
    assert f"{printed} in database {db.name}" in result.stdout.splitlines()
    assert "/*" not in result.stdout


# -- values that are refused ------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    ["*/", "/*", "a */ b", "a /* b", "x*/ SELECT 1 /*", "**//", "/**/", "//**"],
)
def test_comment_with_a_comment_delimiter_is_rejected(cluster, run_ff, value):
    # "**//" is the value that defeats taking the delimiters out: with each
    # "/*" and then each "*/" removed, what is left of it is "*/".
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "-c", value)

    assert_rejected(result, cluster, mark)
    assert (
        f"argument -c/--comment: must not contain /* or */: {value!r}"
    ) in result.stderr


@pytest.mark.parametrize(
    "value",
    ["two\nlines", "ends with one\n", "carriage\rreturn"],
    ids=["newline", "trailing-newline", "carriage-return"],
)
def test_comment_with_a_line_break_is_rejected(cluster, run_ff, value):
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, f"--comment={value}")

    assert_rejected(result, cluster, mark)
    assert (
        f"argument -c/--comment: must not contain a NUL or a line break: {value!r}"
    ) in result.stderr


def test_comment_with_a_nul_is_rejected(cluster, run_ff, tmp_path):
    # A NUL cannot be passed on a command line, so it is put into the
    # script's arguments from inside the process, by a sitecustomize module
    # found through PYTHONPATH (as in test_interpreter.py).
    (tmp_path / "sitecustomize.py").write_text(
        "import sys\nsys.argv = [arg.replace('<NUL>', chr(0)) for arg in sys.argv]\n"
    )
    inherited = os.environ.get("PYTHONPATH")
    path = [str(tmp_path)] + ([inherited] if inherited else [])
    mark = cluster.log_offset()

    result = run_ff(
        *FREEZE_ALL, "-c", "a<NUL>b", env={"PYTHONPATH": os.pathsep.join(path)}
    )

    assert_rejected(result, cluster, mark)
    assert (
        "argument -c/--comment: must not contain a NUL or a line break: 'a\\x00b'"
    ) in result.stderr


# -- values that are accepted, and stay a comment -----------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "/", "*", "* /", "/ *", "x*", "/x", "*x/", "",
        "'; DROP TABLE public.t1; --", "x -- y", '"', "\\", "$$", "{0} {name}",
        "café ✓",
    ],
)
def test_comment_cannot_be_anything_but_a_comment(db, run_ff, value):
    # The text is padded with a space on each side, so a "/" or "*" at
    # either end of it cannot join up with a delimiter, and inside a comment
    # nothing else means anything to the server.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, f"--comment={value}", "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    assert db.frozen_since(before) == {T1}
    statements = db.script_statements()
    assert len(statements) == 9
    for statement in statements:
        assert statement.endswith(f" /* {value} */"), statement


# -- percent signs ----------------------------------------------------------------


@MODES
@pytest.mark.parametrize("value", ["100%", "%s", "%(name)s", "%%", "%", "%d %(x)s %%s"])
def test_percent_signs_in_the_comment_arrive_unchanged(db, run_ff, mode, value):
    # The driver formats a statement that has parameters and leaves one
    # without them alone.  A run with --table sends both kinds.
    table_with_dead_rows(db)

    result = run_ff(*mode, "-c", value, "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert result.stderr == ""
    assert [vacuum.target for vacuum in db.script_vacuums()] == [T1]
    statements = db.script_statements()
    assert len(statements) == 9
    for statement in statements:
        assert statement.endswith(f" /* {value} */"), statement


# -- where the comment is shown ----------------------------------------------------


def test_comment_is_shown_in_the_argument_dumps_and_the_password_is_not(
    db, make_role, run_ff, tmp_path
):
    secret = "s3cr3t-Zq7XvK-pw"
    role = make_role(name=SCRAM_ROLE, password=secret, superuser=True)
    db.create_table("t1")
    logfile = tmp_path / "flexible_freeze.log"

    result = run_ff(
        *FREEZE_ALL, "-U", role, "-w", secret, "-c", COMMENT,
        "--debug", "-v", "-l", str(logfile), "-d", db.name,
    )

    assert result.returncode == 0
    log = logfile.read_text()
    parameters = [line for line in result.stderr.splitlines() if "parameters: " in line]
    arguments = [line for line in log.splitlines() if line.startswith("arguments: ")]
    assert len(parameters) == 1 and len(arguments) == 1
    for line in parameters + arguments:
        assert f"comment={COMMENT!r}" in line
        assert "dbpass='********'" in line
    for text in (log, result.stdout, result.stderr):
        assert secret not in text


def test_comment_defaults_to_none(run_ff):
    result = run_ff("--debug", "-d", "ff_no_such_database")

    assert "comment=None" in result.stderr


# -- the script source --------------------------------------------------------------


def test_script_sends_every_statement_through_one_function():
    # A statement sent with a cursor's execute() directly would go without
    # the comment, and a run cannot show that for a statement it happens not
    # to send, so the calls are counted in the source.
    tree = ast.parse(SCRIPT.read_text())
    calls = {}
    for function in ast.walk(tree):
        if isinstance(function, ast.FunctionDef):
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "execute"
                ):
                    calls[node.lineno] = function.name
    everywhere = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
    ]

    assert len(everywhere) == 1
    assert calls == {everywhere[0]: "execute"}
