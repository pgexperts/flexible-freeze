# Characterization tests for which databases and tables a run covers: the
# default database list, -d, -T, --exclude-table-in-database, --table and
# --minsizemb.
#
# Every test proves the selection from relfrozenxid, which moves for exactly
# the tables that were frozen.  Table names are bare lowercase names in
# schema public; other schemas, qualified names and names that need quoting
# are covered in test_table_names.py.
#
# The default database list leaves out the postgres database by name.  That
# is recorded here because it is what the tool does today; whether it should
# is an open question (issue #41).

from ffharness import FREEZE_ALL

T = ("public", "t")
T_KEEP = ("public", "t_keep")


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def test_databases_option_restricts_the_run(make_db, run_ff):
    wanted, other = make_db("wanted"), make_db("other")
    wanted.create_table("t")
    other.create_table("t")
    before_wanted, before_other = wanted.frozenxids(), other.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", wanted.name)

    assert result.returncode == 0
    assert wanted.frozen_since(before_wanted) == {T}
    assert other.frozen_since(before_other) == set()
    assert other.script_statements() == []


def test_databases_option_takes_a_comma_separated_list(make_db, run_ff):
    first, second, third = make_db("first"), make_db("second"), make_db("third")
    for database in (first, second, third):
        database.create_table("t")
    before = [database.frozenxids() for database in (first, second, third)]

    result = run_ff(*FREEZE_ALL, "-v", "-d", f"{second.name},{first.name}")

    assert result.returncode == 0
    assert (
        f"Processing 2 databases (list of databases is {second.name}, {first.name})"
        in result.stdout.splitlines()
    )
    assert first.frozen_since(before[0]) == {T}
    assert second.frozen_since(before[1]) == {T}
    assert third.frozen_since(before[2]) == set()


def test_default_database_list_skips_postgres_and_templates(cluster, db, run_ff):
    assert cluster.user_databases() == [db.name]
    db.create_table("t")
    postgres = cluster.database("postgres")
    postgres.create_table("ff_in_postgres")
    try:
        before, before_postgres = db.frozenxids(), postgres.frozenxids()
        mark = cluster.log_offset()

        result = run_ff(*FREEZE_ALL, "-v")

        assert result.returncode == 0
        assert (
            f"Processing 1 database (list of databases is {db.name})"
            in result.stdout.splitlines()
        )
        assert db.frozen_since(before) == {T}
        assert postgres.frozen_since(before_postgres) == set()
        # The script visits postgres only to read the database list, and
        # sends nothing to a template database.
        assert postgres.script_vacuums(since=mark) == []
        visited = {record.dbname for record in cluster.statements(since=mark)}
        assert visited == {"postgres", db.name}
    finally:
        postgres.execute("DROP TABLE ff_in_postgres")


def test_no_user_databases_aborts(cluster, run_ff):
    assert cluster.user_databases() == []

    result = run_ff(*FREEZE_ALL)

    assert result.returncode == 1
    assert "no databases to vacuum, aborting" in result.stdout + result.stderr
    assert "All tables vacuumed." not in result.stdout


def test_exclude_table_by_bare_name(db, run_ff):
    db.create_table("t_keep")
    db.create_table("t_skip")
    db.create_table("t_skip_too")
    before = db.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "-v", "-T", "t_skip", "-T", "t_skip_too", "-d", db.name
    )

    assert result.returncode == 0
    lines = result.stdout.splitlines()
    assert "skipping table public.t_skip per --exclude-table argument" in lines
    assert "skipping table public.t_skip_too per --exclude-table argument" in lines
    assert db.frozen_since(before) == {T_KEEP}
    assert targets(db.script_vacuums()) == [T_KEEP]


def test_exclude_table_applies_in_every_database(make_db, run_ff):
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        database.create_table("t")
        database.create_table("t_keep")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(*FREEZE_ALL, "-T", "t", "-d", f"{one.name},{two.name}")

    assert result.returncode == 0
    assert one.frozen_since(before_one) == {T_KEEP}
    assert two.frozen_since(before_two) == {T_KEEP}


def test_exclude_table_in_database_applies_to_that_database_only(make_db, run_ff):
    one, two = make_db("one"), make_db("two")
    for database in (one, two):
        database.create_table("t")
        database.create_table("t_keep")
    before_one, before_two = one.frozenxids(), two.frozenxids()

    result = run_ff(
        *FREEZE_ALL, "--debug",
        "--exclude-table-in-database", f"{one.name}.t",
        "-d", f"{one.name},{two.name}",
    )

    assert result.returncode == 0
    assert (
        f"skipping table public.t in database {one.name} "
        "per --exclude-table-in-database argument"
    ) in result.stderr
    assert one.frozen_since(before_one) == {T_KEEP}
    assert two.frozen_since(before_two) == {T, T_KEEP}


def test_exclude_table_in_database_rejects_a_bare_table_name(cluster, run_ff):
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "--exclude-table-in-database", "nodatabase")

    assert result.returncode == 2
    assert result.stdout == ""
    assert (
        "invalid argument 'nodatabase' to flag --exclude-table-in-database: "
        "argument must be of the form DATABASE.TABLE"
    ) in result.stderr
    # Rejected while parsing arguments, before any connection is made.
    assert cluster.statements(since=mark) == []


def test_table_option_restricts_the_run_to_one_table(db, run_ff):
    one = db.create_table("t_one")
    db.create_table("t_two")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--table", "t_one", "-d", db.name)

    assert result.returncode == 0
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {one}
    assert targets(db.script_vacuums()) == [one]


def test_minsizemb_skips_smaller_tables(db, run_ff):
    db.create_table("t_small")
    big = db.create_table("t_big", rows=12000, pad=100)
    small_bytes, big_bytes = db.execute(
        "SELECT pg_relation_size('t_small'), pg_relation_size('t_big')"
    )[0]
    assert small_bytes < 2**20 <= big_bytes
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--minsizemb", "1", "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {big}
    assert targets(db.script_vacuums()) == [big]
