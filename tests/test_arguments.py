# Tests for the command-line boundary: which option values are accepted,
# which are rejected, and what the options that are only spellings of one
# another do.  The contract for a rejected value is the same throughout:
# exit status 2, a message on stderr, nothing on stdout, no traceback, and no
# connection to the server.
#
# The port option cannot be given a bad value here: the harness supplies -p
# itself and refuses any argument that names a port, so its type is observed
# through the argument dump of --debug instead.
#
# The tests for rejected values give no -d on purpose.  A value that slipped
# through would then make the script connect and list the databases, which is
# what the check on the statement log is there to catch.

import ast
import re

import pytest
from ffharness import FREEZE_ALL, SCRIPT

T = ("public", "t")

# A database that does not exist.  The connection fails, the database is
# skipped (ADR 0004) and the run ends without a statement being sent, which
# makes it the cheapest way to get past argument parsing.
NO_SUCH_DB = "ff_no_such_database"

NON_NEGATIVE_OPTIONS = [
    "-m", "--minutes", "-s", "--minsizemb", "--pause",
    "--freezeage", "--costdelay", "--costlimit", "--lock-timeout",
]


def output(result):
    return result.stdout + result.stderr


def assert_rejected(result, cluster, mark):
    assert result.returncode == 2
    assert result.stdout == ""
    assert "Traceback" not in result.stderr
    assert cluster.statements(since=mark) == []


# -- --vacuum -----------------------------------------------------------------


def test_vacuum_option_is_the_same_as_no_freeze(db, run_ff):
    # The table has dead rows but is far younger than the default --freezeage,
    # so it is selected only if --vacuum really switches to the dead-row
    # selection, and the statement shows whether FREEZE was dropped.
    db.create_table("t_dead", rows=1000)
    db.make_dead_tuples("t_dead")

    result = run_ff("--vacuum", "-v", "-d", db.name)

    assert result.returncode == 0
    vacuums = db.script_vacuums()
    assert [vacuum.target for vacuum in vacuums] == [("public", "t_dead")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    assert "FREEZE" not in result.stdout


# -- -d -----------------------------------------------------------------------


@pytest.mark.parametrize(
    "dblist",
    ["{name},,", ",{name}", "{name},", " {name} , "],
    ids=["doubled-comma", "leading-comma", "trailing-comma", "padded"],
)
def test_empty_database_entries_are_dropped(db, run_ff, dblist):
    db.create_table("t")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", dblist.format(name=db.name))

    assert result.returncode == 0
    assert "Traceback" not in result.stderr
    assert "a target database is required" not in output(result)
    assert db.frozen_since(before) == {T}


@pytest.mark.parametrize("dblist", ["", ",", " , "], ids=["empty", "comma", "blank"])
def test_database_list_that_names_nothing_is_rejected(db, cluster, run_ff, dblist):
    # It must not be read as "no -d given": that would vacuum every database,
    # including the one created here.
    db.create_table("t")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "-d", dblist)

    assert_rejected(result, cluster, mark)
    assert (
        f"invalid argument '{dblist}' to flag --databases: "
        "argument must name at least one database"
    ) in result.stderr
    assert db.frozen_since(before) == set()


# -- numeric options ----------------------------------------------------------


@pytest.mark.parametrize("option", NON_NEGATIVE_OPTIONS)
def test_negative_value_is_rejected(cluster, run_ff, option):
    mark = cluster.log_offset()

    result = run_ff(option, "-1")

    assert_rejected(result, cluster, mark)
    assert "must not be negative: '-1'" in result.stderr


def test_negative_value_attached_with_an_equals_sign_is_rejected(cluster, run_ff):
    mark = cluster.log_offset()

    result = run_ff("--pause=-1")

    assert_rejected(result, cluster, mark)
    assert "must not be negative: '-1'" in result.stderr


@pytest.mark.parametrize("option", NON_NEGATIVE_OPTIONS)
def test_zero_is_accepted(run_ff, option):
    # Only acceptance by the parser is checked: the database is skipped, so
    # the value is never used.  How a run ends after skipping its last
    # database is not this file's subject, so neither the exit status nor the
    # rest of stderr is asserted on.
    result = run_ff(option, "0", "-d", NO_SUCH_DB)

    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr
    assert "usage:" not in result.stderr


def test_non_integer_value_keeps_the_usual_argparse_message(cluster, run_ff):
    mark = cluster.log_offset()

    result = run_ff("--freezeage", "old")

    assert_rejected(result, cluster, mark)
    assert "argument --freezeage: invalid int value: 'old'" in result.stderr


# -- option types, seen in the --debug argument dump --------------------------
#
# The dump is the repr of the argparse namespace, whose field order differs
# between Python versions, so each field is looked for on its own.


def test_port_is_parsed_as_an_integer(cluster, run_ff):
    result = run_ff("--debug", "-d", NO_SUCH_DB)

    assert re.search(rf"\bdbport={cluster.port}\b", result.stderr)


def test_table_option_defaults_to_none(run_ff):
    result = run_ff("--debug", "-d", NO_SUCH_DB)

    assert re.search(r"\btable=None\b", result.stderr)


# -- --table / -st ------------------------------------------------------------


def test_st_is_still_accepted_for_table(db, run_ff):
    db.create_table("t")
    db.create_table("t_other")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-st", "t", "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {T}


# -- --exclude-table-in-database ----------------------------------------------


@pytest.mark.parametrize(
    "value",
    [".t", "somedb.", ".", ".public.t"],
    ids=["no-database", "no-table", "neither", "no-database-qualified"],
)
def test_exclude_table_in_database_rejects_an_empty_part(cluster, run_ff, value):
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", value)

    assert_rejected(result, cluster, mark)
    assert (
        f"invalid argument '{value}' to flag --exclude-table-in-database: "
        "argument must be of the form DATABASE.TABLE"
    ) in result.stderr


# -- the script source --------------------------------------------------------


def test_script_has_no_statement_that_is_only_a_name():
    # A name on a line of its own (`exit` without parentheses) is evaluated
    # and thrown away.  It reads as an action and does nothing, and nothing a
    # run prints can show that, so it is looked for in the source.
    tree = ast.parse(SCRIPT.read_text())
    bare_names = [
        (node.lineno, node.value.id)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Name)
    ]

    assert bare_names == []


# -- help text ----------------------------------------------------------------


def test_help_describes_the_options_as_they_behave(run_ff):
    # argparse wraps help to the terminal width, breaking option names at
    # their hyphens, and newer versions colour it on request.  Both are pinned
    # so that the phrases below arrive whole.
    result = run_ff("--help", env={"COLUMNS": "1000", "NO_COLOR": "1"})

    assert result.returncode == 0
    text = " ".join(result.stdout.split())
    # --table is the documented spelling, -st is still listed
    assert "[--table TABLE]" in text
    assert "[-st TABLE]" not in text
    assert "-st is an older spelling" in text
    # --freezeage: strictly older than, and unused with --no-freeze
    assert "freeze tables older than this many XIDs" in text
    assert "Ignored with --no-freeze" in text
    # the fixed thresholds of --no-freeze
    assert "more than 100 dead rows" in text
    assert "more than 5% of rows dead" in text
    assert "in the last hour" in text
    assert "--vacuum Same as --no-freeze" in text
    # --no-truncate: what it leaves out and what that is for
    assert "--no-truncate Do not truncate empty pages off the end of each table" in text
    assert "ACCESS EXCLUSIVE lock" in text
