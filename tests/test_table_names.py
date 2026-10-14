# Tests for table names beyond a bare lowercase name in schema public: tables
# in schemas that are not on the search_path, names that need quoting (mixed
# case, a dot, a double quote), and how -T, --exclude-table-in-database and
# --table match such tables (issues #31 and #38).
#
# The script identifies a table by its regclass text, which depends on the
# session's search_path: a table in public prints bare while public is on the
# path and as public.name when it is not.  These tests hold either way.  They
# prove outcomes from relfrozenxid and from the parsed VACUUM target, and they
# quote the script's own output only for tables outside public, whose text is
# the same under any search_path.

import pytest
from ffharness import FREEZE_ALL, assert_lines_in_order

T_KEEP = ("public", "t_keep")


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def build(database, *tables):
    """Create each (schema, name) table, and its schema on first use.

    Call it once per database: it does not know about schemas made earlier.
    """
    schemas = {"public"}
    for schema, name in tables:
        if schema not in schemas:
            database.create_schema(schema)
            schemas.add(schema)
        database.create_table(name, schema=schema)
    return set(tables)


# -- the VACUUM target --------------------------------------------------------


@pytest.mark.parametrize(
    "table",
    [
        pytest.param(("sales", "orders"), id="schema-off-search-path"),
        pytest.param(("public", "Orders"), id="mixed-case-in-public"),
        pytest.param(("Sales", "OrderItems"), id="mixed-case-off-search-path"),
        pytest.param(("sales", "order.items"), id="dot-in-name"),
        pytest.param(("sales", 'order"items'), id="quote-in-name"),
        pytest.param(("sales", "order items"), id="space-in-name"),
    ],
)
def test_table_is_frozen_whatever_its_name(db, run_ff, table):
    # t_keep is there to show that the run carries on to the other tables
    # rather than stopping at the awkward one.
    expected = build(db, table, T_KEEP)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == expected
    assert set(targets(db.script_vacuums())) == expected
    assert db.is_analyzed(table[1], schema=table[0])


def test_no_freeze_vacuums_a_quoted_table_off_the_search_path(db, run_ff):
    # --no-freeze takes its table list from a different query (the
    # statistics views), so the same naming rules are checked there.
    db.create_schema("Sales")
    db.create_table("OrderItems", schema="Sales", rows=1000)
    db.create_table("skipped", schema="Sales", rows=1000)
    db.make_dead_tuples("OrderItems", schema="Sales")
    db.make_dead_tuples("skipped", schema="Sales")

    result = run_ff("--no-freeze", "-T", "skipped", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("Sales", "OrderItems")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    db.wait_for_vacuum_count("OrderItems", 1, schema="Sales")
    assert db.is_analyzed("OrderItems", schema="Sales")
    assert not db.is_analyzed("skipped", schema="Sales")


# -- -T / --exclude-table -----------------------------------------------------


def test_exclude_table_bare_name_matches_in_every_schema(db, run_ff):
    build(db, ("sales", "orders"), ("archive", "orders"), ("public", "orders"), T_KEEP)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-T", "orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    lines = result.stdout.splitlines()
    assert "skipping table sales.orders per --exclude-table argument" in lines
    assert "skipping table archive.orders per --exclude-table argument" in lines
    assert db.frozen_since(before) == {T_KEEP}
    assert targets(db.script_vacuums()) == [T_KEEP]


def test_exclude_table_bare_name_matches_a_mixed_case_table(db, run_ff):
    # The name is given as it is stored, without quotes.  Matching is exact,
    # so the lowercase table of the same spelling is a different table.
    build(db, ("public", "Orders"), ("sales", "Orders"), ("public", "orders"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-T", "Orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("public", "orders")}


def test_exclude_table_qualified_name_matches_that_schema_only(db, run_ff):
    kept = {("archive", "orders"), ("public", "orders")}
    build(db, *kept, ("sales", "orders"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-T", "sales.orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == kept
    assert set(targets(db.script_vacuums())) == kept


def test_exclude_table_qualified_name_matches_a_public_table(db, run_ff):
    # Only public tables here, so this depends on name matching alone and
    # not on how the VACUUM target is written.
    build(db, ("public", "t_skip"), T_KEEP)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-T", "public.t_skip", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {T_KEEP}


def test_exclude_table_qualified_name_needs_no_quotes(db, run_ff):
    # schema.table is compared as plain text against the catalog names, so a
    # name that SQL would have to quote is written without quotes.
    build(db, ("Sales", "Order Items"), ("Sales", "kept"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-T", "Sales.Order Items", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("Sales", "kept")}


def test_exclude_table_still_matches_the_printed_name(db, run_ff):
    # What the script prints for a table (its regclass text, with SQL
    # quoting) has always been accepted, and still is.
    build(db, ("Sales", "Order Items"), ("Sales", "kept"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "-T", '"Sales"."Order Items"', "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        'skipping table "Sales"."Order Items" per --exclude-table argument'
        in result.stdout.splitlines()
    )
    assert db.frozen_since(before) == {("Sales", "kept")}


# -- --exclude-table-in-database ----------------------------------------------


@pytest.mark.parametrize("name", ["sales.orders", "orders"])
def test_exclude_table_in_database_matches_a_table_off_the_search_path(
    make_db, run_ff, name
):
    one, two = make_db("one"), make_db("two")
    tables = [("sales", "orders"), ("archive", "items"), T_KEEP]
    for database in (one, two):
        build(database, *tables)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--debug",
        "--exclude-table-in-database", f"{one.name}.{name}",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        f"skipping table sales.orders in database {one.name} "
        "per --exclude-table-in-database argument"
    ) in result.stderr
    assert one.frozen_since(before_one) == {("archive", "items"), T_KEEP}
    assert two.frozen_since(before_two) == set(tables)


def test_exclude_table_in_database_qualified_name_matches_that_schema_only(
    make_db, run_ff
):
    one, two = make_db("one"), make_db("two")
    tables = [("sales", "orders"), ("archive", "orders"), ("public", "orders")]
    for database in (one, two):
        build(database, *tables)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL,
        "--exclude-table-in-database", f"{one.name}.sales.orders",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert one.frozen_since(before_one) == {("archive", "orders"), ("public", "orders")}
    assert two.frozen_since(before_two) == set(tables)


def test_exclude_table_in_database_accepts_a_qualified_public_table(make_db, run_ff):
    # Only public tables: this is the argument parsing and the matching, with
    # no dependence on how the VACUUM target is written.
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        build(database, ("public", "t"), T_KEEP)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL,
        "--exclude-table-in-database", f"{one.name}.public.t",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert one.frozen_since(before_one) == {T_KEEP}
    assert two.frozen_since(before_two) == {("public", "t"), T_KEEP}


def test_exclude_table_in_database_still_matches_the_printed_name(make_db, run_ff):
    # The table part may be the regclass text the script prints, quotes and
    # all.  Only the first dot of the argument separates the database, so
    # the dot inside the printed name stays part of the table.
    one, two = make_db("one"), make_db("two")
    tables = [("Sales", "Order Items"), ("Sales", "kept")]
    for database in (one, two):
        build(database, *tables)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--debug",
        "--exclude-table-in-database", f'{one.name}."Sales"."Order Items"',
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        f'skipping table "Sales"."Order Items" in database {one.name} '
        "per --exclude-table-in-database argument"
    ) in result.stderr
    assert one.frozen_since(before_one) == {("Sales", "kept")}
    assert two.frozen_since(before_two) == set(tables)


def test_exclude_table_in_database_given_twice_for_one_database(make_db, run_ff):
    # Each occurrence adds to the database's list rather than replacing it;
    # one is qualified and one bare, to show the forms can be mixed.
    one, two = make_db("one"), make_db("two")
    tables = [("sales", "orders"), ("archive", "items"), T_KEEP]
    for database in (one, two):
        build(database, *tables)
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL,
        "--exclude-table-in-database", f"{one.name}.sales.orders",
        "--exclude-table-in-database", f"{one.name}.items",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert one.frozen_since(before_one) == {T_KEEP}
    assert targets(one.script_vacuums()) == [T_KEEP]
    assert two.frozen_since(before_two) == set(tables)


def test_no_freeze_honours_exclude_table_in_database(make_db, run_ff):
    # The exclusion is applied to the --no-freeze table list (from the
    # statistics views) exactly as to the freeze list.
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        database.create_schema("Sales")
        for name in ("OrderItems", "skipped"):
            database.create_table(name, schema="Sales", rows=1000)
            database.make_dead_tuples(name, schema="Sales")

    result = run_ff(
        "--no-freeze",
        "--exclude-table-in-database", f"{one.name}.Sales.skipped",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0, result.stdout + result.stderr
    vacuums = one.script_vacuums()
    assert targets(vacuums) == [("Sales", "OrderItems")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    assert set(targets(two.script_vacuums())) == {
        ("Sales", "OrderItems"), ("Sales", "skipped"),
    }
    one.wait_for_vacuum_count("OrderItems", 1, schema="Sales")
    assert not one.is_analyzed("skipped", schema="Sales")
    assert two.is_analyzed("skipped", schema="Sales")


# -- --table ------------------------------------------------------------------


def test_table_option_bare_name_selects_the_name_in_every_schema(db, run_ff):
    wanted = {("sales", "orders"), ("archive", "orders"), ("public", "orders")}
    build(db, *wanted, ("sales", "items"), T_KEEP)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--table", "orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == wanted
    assert set(targets(db.script_vacuums())) == wanted


def test_table_option_qualified_name_selects_that_schema_only(db, run_ff):
    build(db, ("sales", "orders"), ("archive", "orders"), ("public", "orders"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--table", "sales.orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("sales", "orders")}
    assert targets(db.script_vacuums()) == [("sales", "orders")]


def test_table_option_qualified_name_selects_a_public_table(db, run_ff):
    # Only public tables, so this depends on name matching alone.
    build(db, ("public", "t_one"), ("public", "t_two"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--table", "public.t_one", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {("public", "t_one")}
    assert targets(db.script_vacuums()) == [("public", "t_one")]


def test_table_option_bare_name_selects_a_mixed_case_table(db, run_ff):
    build(db, ("public", "Orders"), ("public", "orders"))
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--table", "Orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("public", "Orders")}
    assert targets(db.script_vacuums()) == [("public", "Orders")]


def test_table_option_honours_exclusions_among_its_matches(db, run_ff):
    build(db, ("sales", "orders"), ("archive", "orders"))
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-T", "archive.orders", "-d", db.name
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("sales", "orders")}


def test_table_option_reports_a_database_without_the_table(make_db, run_ff):
    # The run moves on to the next database, where the name does match, and
    # exits 0: the table was found (see test_run_outcome.py for the status,
    # and for the same run without -v, which does not mention the miss).
    one, two = make_db("one"), make_db("two")
    build(one, ("sales", "items"))
    build(two, ("sales", "orders"))
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "-v", "--table", "orders", "-d", f"{one.name},{two.name}"
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        f"specified single table orders does not exist in database {one.name}"
    ) in result.stdout.splitlines()
    assert one.frozen_since(before_one) == set()
    assert one.script_vacuums() == []
    assert two.frozen_since(before_two) == {("sales", "orders")}


def test_table_option_says_when_the_table_does_not_exist(db, run_ff):
    build(db, T_KEEP)
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-v", "--table", "t_absent", "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert_lines_in_order(
        result.stdout,
        [
            f"specified single table t_absent does not exist in database {db.name}",
            "Not all tables were vacuumed.",
            f"Databases where --table t_absent matched no table: {db.name}",
        ],
    )
    assert db.frozen_since(before) == set()

    # The line for the database is a -v message; the summary is not.
    result = run_ff(*FREEZE_ALL, "--table", "t_absent", "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stdout.splitlines() == [
        "Not all tables were vacuumed.",
        f"Databases where --table t_absent matched no table: {db.name}",
    ]


@pytest.mark.parametrize(
    "exclusion, option",
    [
        pytest.param(("-T", "orders"), "--exclude-table", id="exclude-table"),
        pytest.param(
            ("--exclude-table-in-database", "{db}.sales.orders"),
            "--exclude-table-in-database",
            id="exclude-table-in-database",
        ),
    ],
)
def test_table_option_says_when_the_table_is_excluded(db, run_ff, exclusion, option):
    # Not the same thing as a table that does not exist, and said without
    # -v or --debug: the table was asked for by name.  It is not a failure.
    build(db, ("sales", "orders"))
    before = db.frozenxids()
    exclusion = [part.format(db=db.name) for part in exclusion]

    result = run_ff(*FREEZE_ALL, "--table", "orders", *exclusion, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [
        (f"specified single table sales.orders is excluded in database {db.name} "
         f"by {option}; not vacuuming it"),
        "All tables vacuumed.",
    ]
    assert result.stderr == ""
    assert db.frozen_since(before) == set()
    assert db.script_vacuums() == []


def test_table_option_reports_each_excluded_match(db, run_ff):
    # A bare name with a match in two schemas, one of them excluded: the
    # other is vacuumed and the excluded one is named.
    build(db, ("sales", "orders"), ("archive", "orders"))
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-T", "archive.orders", "-d", db.name
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines() == [
        (f"specified single table archive.orders is excluded in database {db.name} "
         "by --exclude-table; not vacuuming it"),
        "All tables vacuumed.",
    ]
    assert db.frozen_since(before) == {("sales", "orders")}


def test_table_option_still_matches_the_printed_name(db, run_ff):
    # As for -T: the regclass text the script prints, with its SQL quoting,
    # is accepted as the table's name.
    build(db, ("Sales", "Order Items"), ("Sales", "kept"))
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "-v", "--table", '"Sales"."Order Items"', "-d", db.name
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert 'processing table "Sales"."Order Items"' in result.stdout.splitlines()
    assert db.frozen_since(before) == {("Sales", "Order Items")}
    assert targets(db.script_vacuums()) == [("Sales", "Order Items")]


def test_no_freeze_table_option_selects_among_the_tables_with_dead_rows(db, run_ff):
    # Both tables would be on the --no-freeze list; only the named one is
    # vacuumed.
    db.create_schema("Sales")
    for name in ("OrderItems", "other"):
        db.create_table(name, schema="Sales", rows=1000)
        db.make_dead_tuples(name, schema="Sales")

    result = run_ff("--no-freeze", "--table", "OrderItems", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("Sales", "OrderItems")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    db.wait_for_vacuum_count("OrderItems", 1, schema="Sales")
    assert db.is_analyzed("OrderItems", schema="Sales")
    assert not db.is_analyzed("other", schema="Sales")


@pytest.mark.parametrize(
    "selection",
    [
        # The default --freezeage is ten million; the table is a few
        # transactions old.
        pytest.param((), id="below-freezeage"),
        pytest.param((*FREEZE_ALL, "--minsizemb", "1"), id="below-minsizemb"),
        pytest.param(("--no-freeze",), id="no-dead-rows"),
    ],
)
def test_table_option_vacuums_a_table_the_selection_would_leave_out(
    db, run_ff, selection
):
    # --table looks the table up by name (issue #38 item 3): the mode's
    # thresholds choose tables when none is named, and do not apply to one
    # that is.  t_keep is as far below them and is left alone.
    small = ("public", "t_small")
    build(db, small, T_KEEP)
    before = db.frozenxids()

    result = run_ff(*selection, "--table", "t_small", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [small]
    if "--no-freeze" in selection:
        assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
        db.wait_for_vacuum_count("t_small", 1)
    else:
        assert vacuums[0].options == {"FREEZE", "ANALYZE", "SKIP_LOCKED"}
        assert db.frozen_since(before) == {small}


@pytest.mark.parametrize(
    "mode",
    [pytest.param((), id="freeze"), pytest.param(("--no-freeze",), id="no-freeze")],
)
def test_table_option_bare_name_below_the_thresholds_is_vacuumed_in_every_schema(
    db, run_ff, mode
):
    wanted = {("sales", "orders"), ("archive", "orders")}
    build(db, *wanted, ("sales", "items"))

    result = run_ff(*mode, "--table", "orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert set(targets(db.script_vacuums())) == wanted


def test_table_option_qualified_name_below_the_thresholds_is_that_table_only(
    db, run_ff
):
    build(db, ("sales", "orders"), ("archive", "orders"))

    result = run_ff("--table", "sales.orders", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert targets(db.script_vacuums()) == [("sales", "orders")]


def test_table_option_does_not_match_on_a_truncated_name(db, run_ff):
    # A table name is at most 63 bytes.  A longer value must not be cut down
    # to that and matched; -T would not match it either.
    longest = "t" * 63
    build(db, ("public", longest))

    result = run_ff(*FREEZE_ALL, "--table", longest + "x", "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert db.script_vacuums() == []


def test_table_option_value_is_never_read_as_sql(db, run_ff):
    # The value reaches the server as a parameter of the lookup.  One that
    # would end a string literal and start a statement is just a name that
    # no table has.
    build(db, T_KEEP)
    value = "t_keep'; DROP TABLE public.t_keep; --"

    result = run_ff(*FREEZE_ALL, "-v", "--table", value, "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert (
        f"specified single table {value} does not exist in database {db.name}"
    ) in result.stdout.splitlines()
    assert db.execute("SELECT count(*) FROM public.t_keep") == [(100,)]
    assert db.script_vacuums() == []


def test_table_option_finds_a_materialized_view_but_not_a_view(db, run_ff):
    # What can be named is what VACUUM can be pointed at in either mode.
    build(db, ("public", "t_base"))
    db.execute("CREATE MATERIALIZED VIEW mv_base AS SELECT * FROM t_base")
    db.execute("CREATE VIEW v_base AS SELECT * FROM t_base")

    result = run_ff("--table", "mv_base", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert targets(db.script_vacuums()) == [("public", "mv_base")]

    mark = db.cluster.log_offset()
    result = run_ff("-v", "--table", "v_base", "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert (
        f"specified single table v_base does not exist in database {db.name}"
    ) in result.stdout.splitlines()
    assert db.script_vacuums(since=mark) == []


def test_table_option_does_not_find_another_sessions_temp_table(db, run_ff):
    # The server would pass over it without a word, and the run would count
    # it as vacuumed.
    build(db, T_KEEP)
    owner = db.connect()
    try:
        with owner.cursor() as cur:
            cur.execute("CREATE TEMP TABLE ff_temp (id integer)")

        result = run_ff("--table", "ff_temp", "-d", db.name)
    finally:
        owner.close()

    assert result.returncode == 1, result.stdout + result.stderr
    assert db.script_vacuums() == []


def test_table_option_reaches_a_system_catalog(db, run_ff):
    # The selections leave the system schemas out; a catalog named outright
    # is looked up like any table.  Its printed name carries no schema:
    # pg_catalog is searched even under the empty search_path.
    result = run_ff("-v", "--table", "pg_catalog.pg_am", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "processing table pg_am" in result.stdout.splitlines()
    assert [vacuum.table for vacuum in db.script_vacuums()] == ["pg_am"]
