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
from ffharness import FREEZE_ALL

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
    assert vacuums[0].options == {"ANALYZE"}
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
    assert vacuums[0].options == {"ANALYZE"}
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


def test_table_option_reports_a_name_that_matches_nothing(make_db, run_ff):
    # The run moves on to the next database, where the name does match.  It
    # still exits 1: the name matched nothing in one of the databases it was
    # asked to work on (see test_run_outcome.py).
    one, two = make_db("one"), make_db("two")
    build(one, ("sales", "items"))
    build(two, ("sales", "orders"))
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--table", "orders", "-d", f"{one.name},{two.name}"
    )

    assert result.returncode == 1, result.stdout + result.stderr
    assert (
        f"specified single table orders was not found (or excluded) "
        f"in database {one.name}"
    ) in result.stdout.splitlines()
    assert one.frozen_since(before_one) == set()
    assert one.script_vacuums() == []
    assert two.frozen_since(before_two) == {("sales", "orders")}


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
    # --table narrows whichever list the mode produced; here that is the
    # --no-freeze list, and both tables are on it.
    db.create_schema("Sales")
    for name in ("OrderItems", "other"):
        db.create_table(name, schema="Sales", rows=1000)
        db.make_dead_tuples(name, schema="Sales")

    result = run_ff("--no-freeze", "--table", "OrderItems", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("Sales", "OrderItems")]
    assert vacuums[0].options == {"ANALYZE"}
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
def test_table_option_reports_an_existing_table_the_selection_left_out(
    db, run_ff, selection
):
    # Characterization of what the script does today, not of what it should
    # do.  --table is looked up in the list the selection query returned, so
    # a table that exists but did not pass the mode's filters gets the same
    # message as a name that matches nothing, although it is neither missing
    # nor excluded.  Issue #38 item 3 changes this (resolve the name directly
    # and vacuum the table whatever the filters say); this test is expected
    # to be rewritten with that fix.  Until then the run also exits 1, as it
    # does for any --table that matched nothing.
    build(db, ("public", "t_small"))
    before = db.frozenxids()

    result = run_ff(*selection, "--table", "t_small", "-d", db.name)

    assert result.returncode == 1, result.stdout + result.stderr
    assert (
        f"specified single table t_small was not found (or excluded) "
        f"in database {db.name}"
    ) in result.stdout.splitlines()
    assert db.frozen_since(before) == set()
    assert db.script_vacuums() == []
