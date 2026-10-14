# Tests for the command-line boundary: which option values are accepted,
# which are rejected, and what the options that are only spellings of one
# another do.  The contract for a rejected value is the same throughout:
# exit status 2, a message on stderr, nothing on stdout, no traceback, and no
# connection to the server.
#
# The harness supplies -H and -p itself and refuses any argument that names a
# host or a port.  A -p value is tested through the launcher's port keyword
# instead: a string given there becomes the script's -p value as it is, with
# the host still the throwaway cluster's socket directory.
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


# --costlimit is the one numeric option that zero is not legal for: the
# server's minimum for vacuum_cost_limit is 1, and a 0 that got past the parser
# would make every database look unlistable.  Upper bounds are left to the
# server for both cost options, so none is tested here.
ZERO_ACCEPTED_OPTIONS = [o for o in NON_NEGATIVE_OPTIONS if o != "--costlimit"]


@pytest.mark.parametrize("option", ZERO_ACCEPTED_OPTIONS)
def test_zero_is_accepted(run_ff, option):
    # Only acceptance by the parser is checked: the database is skipped, so
    # the value is never used.  How a run ends after skipping its last
    # database is not this file's subject, so neither the exit status nor the
    # rest of stderr is asserted on.
    result = run_ff(option, "0", "-d", NO_SUCH_DB)

    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr
    assert "usage:" not in result.stderr


def test_zero_costlimit_is_rejected_before_connecting(cluster, run_ff):
    mark = cluster.log_offset()

    result = run_ff("--costlimit", "0")

    assert_rejected(result, cluster, mark)
    assert "argument --costlimit: must be at least 1: '0'" in result.stderr


def test_costlimit_of_one_is_accepted(run_ff):
    # The smallest value the server takes; 0 is the first one refused.
    result = run_ff("--costlimit", "1", "-d", NO_SUCH_DB)

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


def test_port_is_kept_as_the_string_given(cluster, run_ff):
    # The value goes to libpq as text, so that it can be a list (see below);
    # it is not turned into a number on the way.
    result = run_ff("--debug", "-d", NO_SUCH_DB)

    assert re.search(rf"\bdbport='{cluster.port}'", result.stderr)


def test_table_option_defaults_to_none(run_ff):
    result = run_ff("--debug", "-d", NO_SUCH_DB)

    assert re.search(r"\btable=None\b", result.stderr)


# -- -p / --port --------------------------------------------------------------
#
# One port, or a comma-separated list of them for a -H that lists several
# hosts; each a decimal integer from 1 to 65535.  The script checks that much
# and passes the text on untouched.  Whether the number of ports fits the
# number of hosts is libpq's to say, when it connects.


def test_single_port_reaches_the_server(db, cluster, run_ff):
    db.create_table("t")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name, port=str(cluster.port))

    assert result.returncode == 0
    assert db.frozen_since(before) == {T}


def test_port_text_is_passed_on_without_being_normalised(db, cluster, run_ff):
    # A leading zero would not survive a conversion to a number and back.
    # libpq reads the text as the cluster's port all the same.
    port = f"0{cluster.port}"
    db.create_table("t")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--debug", "-d", db.name, port=port)

    assert result.returncode == 0
    assert re.search(rf"\bdbport='{port}'", result.stderr)
    assert db.frozen_since(before) == {T}


def test_port_list_is_accepted_and_passed_on_unchanged(cluster, run_ff):
    # Only the parser's part is checked.  The harness gives the script one
    # host, and libpq will not connect with two ports for one host; that
    # refusal comes after parsing, as a database that could not be connected
    # to, and its wording is libpq's own.
    ports = f"{cluster.port},{cluster.port}"

    result = run_ff("--debug", "-d", NO_SUCH_DB, port=ports)

    assert result.returncode != 2
    assert "usage:" not in result.stderr
    assert re.search(rf"\bdbport='{ports}'", result.stderr)
    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr


@pytest.mark.parametrize("port", ["1", "65535", "1,65535"])
def test_port_at_either_end_of_the_range_is_accepted(run_ff, port):
    # Nothing listens on these in the cluster's socket directory, so the run
    # gets no further than the parser and a failed connection.
    result = run_ff("-d", NO_SUCH_DB, port=port)

    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr
    assert "usage:" not in result.stderr


def test_empty_port_is_rejected_before_connecting(cluster, run_ff):
    # It must not be read as "no -p given": an unset variable in
    # `-p "$PORT"` would then send the run to the default server.
    mark = cluster.log_offset()

    result = run_ff(port="")

    assert_rejected(result, cluster, mark)
    assert "argument -p/--port: must name a port: ''" in result.stderr


@pytest.mark.parametrize("port", ["6543,", ",6543", "6543,,6544", ","])
def test_empty_entry_in_a_port_list_is_rejected(cluster, run_ff, port):
    mark = cluster.log_offset()

    result = run_ff(port=port)

    assert_rejected(result, cluster, mark)
    assert f"argument -p/--port: empty entry in the port list: '{port}'" in result.stderr


@pytest.mark.parametrize(
    "port, entry",
    [
        ("old", "old"),
        ("6543,old", "old"),
        ("6543x", "6543x"),
        ("6543.0", "6543.0"),
        ("0x1538", "0x1538"),
        # Forms that Python's int() takes; libpq does not take the first.
        ("6_543", "6_543"),
        (" 6543", " 6543"),
        ("6543, 6544", " 6544"),
        (" ", " "),
    ],
)
def test_port_that_is_not_an_integer_is_rejected(cluster, run_ff, port, entry):
    mark = cluster.log_offset()

    result = run_ff(port=port)

    assert_rejected(result, cluster, mark)
    assert f"argument -p/--port: invalid int value: {entry!r}" in result.stderr


def test_port_in_digits_that_are_not_ascii_is_rejected(cluster, run_ff):
    # Fullwidth digits: int() reads them as 6543 and libpq does not.  The
    # value is left out of the comparison because how it is echoed depends
    # on the encoding of the script's stderr.
    mark = cluster.log_offset()

    result = run_ff(port="".join(chr(0xFF10 + int(digit)) for digit in "6543"))

    assert_rejected(result, cluster, mark)
    assert "argument -p/--port: invalid int value: " in result.stderr


@pytest.mark.parametrize(
    "port, entry",
    [
        ("0", "0"),
        ("-1", "-1"),
        ("65536", "65536"),
        ("6543,0", "0"),
        ("65536,6543", "65536"),
        ("00000", "00000"),
        # longer than Python's int() will convert at all
        ("9" * 5000, "9" * 5000),
    ],
    ids=["zero", "negative", "too-large", "zero-in-list", "too-large-in-list",
         "zeros", "enormous"],
)
def test_port_out_of_range_is_rejected(cluster, run_ff, port, entry):
    mark = cluster.log_offset()

    result = run_ff(port=port)

    assert_rejected(result, cluster, mark)
    assert f"argument -p/--port: must be from 1 to 65535: {entry!r}" in result.stderr


# -- --table / -st ------------------------------------------------------------


@pytest.mark.parametrize("option", ["--table", "-st"])
@pytest.mark.parametrize("value", ["", " \t "])
def test_empty_table_name_is_rejected_before_connecting(db, cluster, run_ff, option, value):
    db.create_table("t")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, option, value)

    assert_rejected(result, cluster, mark)
    assert "argument must name a table" in result.stderr
    assert db.frozen_since(before) == set()


@pytest.mark.parametrize("option", ["-T", "--exclude-table"])
@pytest.mark.parametrize("value", ["", " \t "])
def test_empty_exclude_table_is_rejected_before_connecting(db, cluster, run_ff, option, value):
    # '-T "$VAR"' with VAR unset would otherwise protect nothing, silently.
    db.create_table("t")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, option, value)

    assert_rejected(result, cluster, mark)
    assert "argument must name a table" in result.stderr
    assert db.frozen_since(before) == set()


@pytest.mark.parametrize("option", ["-T", "--exclude-table", "--table", "-st"])
@pytest.mark.parametrize("value", [" orders", "orders ", "\torders\n"])
def test_table_name_with_surrounding_whitespace_is_rejected(
    db, cluster, run_ff, option, value
):
    # Matching is exact, so a padded name would match nothing and be vacuumed
    # without a word.  It is refused, not stripped: a table really named
    # " orders" stays reachable through its quoted form (ADR 0021).
    db.create_table("orders")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, option, value)

    assert_rejected(result, cluster, mark)
    assert (
        "argument must not have leading or trailing whitespace"
    ) in result.stderr
    assert db.frozen_since(before) == set()


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


def test_exclude_table_in_database_rejects_an_empty_schema_part(cluster, run_ff):
    # 'db..t' splits into ['db', '.t']: a table part that begins with a dot is
    # a schema-qualified name with no schema, which can never match anything.
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", "somedb..t")

    assert_rejected(result, cluster, mark)
    assert (
        "invalid argument 'somedb..t' to flag --exclude-table-in-database: "
        "argument must be of the form DATABASE.TABLE"
    ) in result.stderr


@pytest.mark.parametrize("value", ["somedb.sch.", "somedb.sch..t"], ids=["trailing", "middle"])
def test_exclude_table_in_database_rejects_an_empty_qualified_part(cluster, run_ff, value):
    # Like 'db..t', these could match a table whose relname contains a dot;
    # they are refused for consistency (ADR 0021).
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", value)

    assert_rejected(result, cluster, mark)
    assert (
        f"invalid argument '{value}' to flag --exclude-table-in-database: "
        "argument must be of the form DATABASE.TABLE"
    ) in result.stderr


@pytest.mark.parametrize(
    "value",
    ["somedb. orders", "somedb.orders ", "somedb.\torders", "somedb.orders\n"],
    ids=["leading", "trailing", "tab", "newline"],
)
def test_exclude_table_in_database_rejects_a_padded_table_part(
    cluster, run_ff, value
):
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", value)

    assert_rejected(result, cluster, mark)
    assert (
        f"invalid argument '{value}' to flag --exclude-table-in-database: "
        "table name must not have leading or trailing whitespace"
    ) in result.stderr


@pytest.mark.parametrize(
    "value",
    ['somedb.."a..b"', 'somedb."a..b".'],
    ids=["empty-schema", "trailing"],
)
def test_quoting_does_not_hide_an_empty_part(cluster, run_ff, value):
    # The dots inside the quotes are not separators, but the dot outside
    # them still splits off an empty part, which is refused as before.
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", value)

    assert_rejected(result, cluster, mark)
    assert (
        f"invalid argument '{value}' to flag --exclude-table-in-database: "
        "argument must be of the form DATABASE.TABLE"
    ) in result.stderr


# -- --system-catalogs ----------------------------------------------------------


@pytest.mark.parametrize("option", ["--no-freeze", "--vacuum"])
@pytest.mark.parametrize("order", ["before", "after"])
def test_system_catalogs_is_rejected_with_no_freeze(cluster, run_ff, option, order):
    # The option widens the freeze selection and nothing else.  Taken
    # silently with --no-freeze it would read as "the catalogs too" and do
    # nothing; --vacuum is that option under its older name.
    arguments = ["--system-catalogs", option]
    if order == "after":
        arguments.reverse()
    mark = cluster.log_offset()

    result = run_ff(*arguments)

    assert_rejected(result, cluster, mark)
    assert result.stderr == (
        "invalid combination of flags --system-catalogs and --no-freeze: "
        "system catalogs are selected only when freezing\n"
    )


def test_system_catalogs_and_no_freeze_are_rejected_with_table_too(cluster, run_ff):
    # --table makes the option idle, not the combination acceptable.
    mark = cluster.log_offset()

    result = run_ff("--system-catalogs", "--no-freeze", "--table", "t")

    assert_rejected(result, cluster, mark)
    assert "invalid combination of flags --system-catalogs and --no-freeze" in result.stderr


@pytest.mark.parametrize("option", ["--no-freeze", "--vacuum"])
def test_system_catalogs_with_no_freeze_is_rejected_before_the_log_file(
    run_ff, tmp_path, option
):
    # Refused after parsing, but before the log file is opened and before
    # any connection is tried.  Nothing listens on port 1, so a connection
    # attempt would end the run with exit status 1.
    log = tmp_path / "ff.log"

    result = run_ff("--system-catalogs", option, "-l", str(log), "-d", "x", port="1")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == (
        "invalid combination of flags --system-catalogs and --no-freeze: "
        "system catalogs are selected only when freezing\n"
    )
    assert not log.exists()
    # The same command without --system-catalogs gets as far as both.
    control = run_ff(option, "-l", str(log), "-d", "x", port="1")
    assert control.returncode == 1
    assert log.exists()


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


# -- --version ----------------------------------------------------------------


def test_version_prints_the_version_and_exits_before_connecting(cluster, run_ff):
    # The expected line is spelled out on purpose: a version bump is a
    # deliberate edit here and in CHANGELOG.md, not something to follow along.
    mark = cluster.log_offset()

    result = run_ff("--version")

    assert result.returncode == 0
    assert result.stdout == "flexible_freeze 0.7\n"
    assert result.stderr == ""
    assert cluster.statements(since=mark) == []


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
    # --system-catalogs: a flag, what it adds, the default, and its limits
    assert "[--system-catalogs]" in text
    assert (
        "--system-catalogs Also freeze the system catalogs (the tables in "
        "pg_catalog and information_schema)"
    ) in text
    assert "by default they are left to autovacuum" in text
    assert "Not allowed with --no-freeze" in text
    assert "normally a superuser" in text
    # the fixed thresholds of --no-freeze
    assert "more than 100 dead rows" in text
    assert "more than 5% of rows dead" in text
    assert "in the last hour" in text
    assert "--vacuum Same as --no-freeze" in text
    # --no-truncate: what it leaves out and what that is for
    assert "--no-truncate Do not truncate empty pages off the end of each table" in text
    assert "ACCESS EXCLUSIVE lock" in text
    # --comment: where the text goes and what it may not contain
    # (how the two spellings are listed differs between Python versions)
    assert "[-c COMMENT]" in text
    assert "--comment COMMENT text to append as a comment" in text
    assert "/* COMMENT */, to every statement sent to the server" in text
    assert "It may not contain /* or */, a NUL or a line break" in text
    # --version: listed, with its help
    assert "[--version]" in text
    assert "--version print the version and exit" in text


def test_help_gives_text_for_every_option_and_names_the_lock_timeout_value(run_ff):
    # The options that had no help string at all, each checked by a phrase
    # from its new help text; --lock-timeout's value is shown by its metavar.
    result = run_ff("--help", env={"COLUMNS": "1000", "NO_COLOR": "1"})

    assert result.returncode == 0
    text = " ".join(result.stdout.split())
    assert "prefix each line of output with the time it was printed" in text
    assert "append output to this file instead of standard output" in text
    assert "print progress as the run goes" in text
    assert "print debugging output to standard error" in text
    assert "[--lock-timeout SECONDS]" in text
    assert "--lock-timeout SECONDS seconds to wait for a lock" in text
