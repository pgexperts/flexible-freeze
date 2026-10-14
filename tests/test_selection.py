# Tests for which databases and tables a run covers: the default database
# list and its order, -d, -T, --exclude-table-in-database, --table and
# --minsizemb, and what the two selection queries pick (issue #41):
# materialized views, MultiXact age, no cap on the list, size including
# TOAST, and no temporary tables of other sessions.
#
# Most tests prove the selection from relfrozenxid, which moves for exactly
# the tables that were frozen; where it cannot show the point (a table
# frozen a moment earlier, a --no-freeze run) the logged VACUUM statements
# are used instead.  Table names are bare lowercase names in schema public;
# other schemas, qualified names and names that need quoting are covered in
# test_table_names.py.

import re

import pytest
from ffharness import FREEZE_ALL, wait_for

T = ("public", "t")
T_KEEP = ("public", "t_keep")

PROCESSING = re.compile(r"Processing \d+ databases? \(list of databases is (.*)\)")


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


def databases_processed(result):
    """The database list a -v run announced, as a set.

    A set because the default list is ordered by age (the greater of
    age(datfrozenxid) and mxid_age(datminmxid)), and databases made from the
    same template are the same age.  The tests of the order build databases
    that differ in age (databases_behind_on_different_horizons).
    """
    announced = [
        match.group(1)
        for match in map(PROCESSING.fullmatch, result.stdout.splitlines())
        if match
    ]
    assert len(announced) == 1, result.stdout
    return set(announced[0].split(", "))


def consume_multixacts(database, count):
    """Advance the cluster's MultiXact counter by `count`.

    Two sessions share-lock the same row, which takes a MultiXact ID to
    record; the counter is cluster-wide, so it does not matter which
    database this is done in.  Each round also uses two transaction IDs.
    """
    database.execute("CREATE TABLE ff_multixact_source AS SELECT 1 AS id")
    sessions = [database.connect(autocommit=False) for _ in range(2)]
    try:
        for _ in range(count):
            for session in sessions:
                with session.cursor() as cur:
                    cur.execute("SELECT id FROM ff_multixact_source FOR SHARE")
            for session in sessions:
                session.rollback()
    finally:
        for session in sessions:
            session.close()


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


def test_default_database_list_includes_postgres_and_skips_templates(
    cluster, db, run_ff
):
    # postgres is a database like any other: its datfrozenxid holds back the
    # cluster's, so its tables are frozen too.
    assert cluster.user_databases() == [db.name]
    db.create_table("t")
    postgres = cluster.database("postgres")
    postgres.create_table("ff_in_postgres")
    try:
        before, before_postgres = db.frozenxids(), postgres.frozenxids()
        mark = cluster.log_offset()

        result = run_ff(*FREEZE_ALL, "-v")

        assert result.returncode == 0
        assert databases_processed(result) == {"postgres", db.name}
        assert db.frozen_since(before) == {T}
        assert postgres.frozen_since(before_postgres) == {("public", "ff_in_postgres")}
        assert targets(postgres.script_vacuums(since=mark)) == [
            ("public", "ff_in_postgres")
        ]
        # Nothing is sent to a template database.
        visited = {record.dbname for record in cluster.statements(since=mark)}
        assert visited == {"postgres", db.name}
    finally:
        postgres.execute("DROP TABLE ff_in_postgres")


def test_postgres_alone_is_a_database_to_vacuum(cluster, run_ff):
    assert cluster.user_databases() == []

    result = run_ff(*FREEZE_ALL, "-v")

    assert result.returncode == 0
    assert databases_processed(result) == {"postgres"}
    assert "All tables vacuumed." in result.stdout.splitlines()


def test_no_database_to_vacuum_aborts(cluster, run_ff):
    # The list is empty only when every database is a template or refuses
    # connections.  postgres has to accept the connection that reads the
    # list, so it is made a template for the length of the run.
    assert cluster.user_databases() == []
    cluster.execute("postgres", "ALTER DATABASE postgres IS_TEMPLATE true")
    try:
        result = run_ff(*FREEZE_ALL)
    finally:
        cluster.execute("postgres", "ALTER DATABASE postgres IS_TEMPLATE false")

    assert result.returncode == 1
    assert "no databases to vacuum, aborting" in result.stderr
    assert "All tables vacuumed." not in result.stdout


@pytest.mark.parametrize(
    "flag, restore",
    [
        # Listed by name before, and reported as a skipped database (exit 1)
        # on every run.
        pytest.param(
            "ALLOW_CONNECTIONS false", "ALLOW_CONNECTIONS true",
            id="refuses-connections",
        ),
        # The two built-in templates were left out by name; any other
        # template was worked on.
        pytest.param("IS_TEMPLATE true", "IS_TEMPLATE false", id="template"),
    ],
)
def test_default_database_list_goes_by_the_catalog_flags(
    cluster, make_db, run_ff, flag, restore
):
    plain, flagged = make_db("plain"), make_db("flagged")
    for database in (plain, flagged):
        database.create_table("t")
    before = plain.frozenxids()
    cluster.execute("postgres", f"ALTER DATABASE {flagged.name} {flag}")
    try:
        mark = cluster.log_offset()

        result = run_ff(*FREEZE_ALL, "-v")

        assert result.returncode == 0, result.stdout + result.stderr
        assert databases_processed(result) == {"postgres", plain.name}
        assert "skipping database" not in result.stdout + result.stderr
        assert cluster.statements(dbname=flagged.name, since=mark) == []
        assert plain.frozen_since(before) == {T}
    finally:
        # A template cannot be dropped, and the fixture drops this database.
        cluster.execute("postgres", f"ALTER DATABASE {flagged.name} {restore}")


# -- the order of the databases ---------------------------------------------------


def databases_in_order_of_work(cluster, since, *databases):
    """Those of `databases` the script sent statements to since the log
    offset `since`, in the order in which it first did."""
    names = [database.name for database in databases]
    visited = []
    for record in cluster.statements(since=since):
        if record.dbname in names and record.dbname not in visited:
            visited.append(record.dbname)
    return visited


def databases_behind_on_different_horizons(make_db):
    """Two databases: one behind on MultiXact IDs and up to date on
    transaction IDs, and one a little behind on transaction IDs and up to
    date on MultiXact IDs.  Returns (the first, the second).

    The first is the one closer to wraparound: its MultiXact age is greater
    than either age of the second.  Ordered by transaction ID age alone the
    second comes first.  Both facts are asserted here, so that a test built
    on this cannot pass on a state that does not tell the two orders apart.

    How the state is built.  A database's two horizons are the oldest of
    its tables', and a freeze moves a table's as far as the transactions
    still open allow, which is not the same for the two:

    * a transaction that holds a row lock keeps relminmxid, in every
      database, from passing the MultiXact counter as it stood when the
      lock was taken.  With one open in a third database and MultiXact IDs
      consumed after it, a freeze of the first database leaves its MultiXact
      age at the number consumed;
    * a transaction that has a transaction ID keeps relfrozenxid from
      passing that ID, but only in its own database and in the shared
      catalogs.  One in the second database, taken before the first is
      frozen, makes the second the older of the two by transaction ID,
      though it is frozen later (with the row lock gone, so that its
      MultiXact age comes down).

    The shared catalogs have a pg_class row, with horizons of its own, in
    each database, and both transactions held those back in the first.  They
    are frozen there again at the end, when nothing is open.
    """
    multixact_old, xid_old = make_db("multixact_old"), make_db("xid_old")
    elsewhere = make_db("elsewhere")
    consumed = 40

    elsewhere.execute("CREATE TABLE ff_held AS SELECT 1 AS id")
    row_lock = elsewhere.connect(autocommit=False)
    xid_pin = xid_old.connect(autocommit=False)
    try:
        with row_lock.cursor() as cur:
            cur.execute("SELECT id FROM ff_held FOR SHARE")
        consume_multixacts(elsewhere, consumed)
        with xid_pin.cursor() as cur:
            cur.execute("SELECT pg_current_xact_id()")
        # A transaction ID that is later than the pinned one and is no
        # longer in use: the freeze that follows can go no further than the
        # last one completed.
        elsewhere.execute("SELECT pg_current_xact_id()")
        multixact_old.execute("VACUUM (FREEZE)")
        # Ended, not merely closed: a backend takes a moment to go away
        # after its client has, and the next freeze must not find it.
        row_lock.rollback()
        xid_old.execute("VACUUM (FREEZE)")
        xid_pin.rollback()
    finally:
        row_lock.close()
        xid_pin.close()
    shared = multixact_old.execute(
        "SELECT oid::regclass::text FROM pg_class WHERE relisshared AND relkind = 'r'"
    )
    multixact_old.execute("VACUUM (FREEZE) " + ", ".join(name for (name,) in shared))

    ages = {
        name: (xid_age, multixact_age)
        for name, xid_age, multixact_age in elsewhere.execute(
            "SELECT datname, age(datfrozenxid), mxid_age(datminmxid) FROM pg_database"
        )
    }
    behind_on_multixacts, behind_on_xids = ages[multixact_old.name], ages[xid_old.name]
    # By transaction ID age alone the second database is the older one...
    assert behind_on_xids[0] > behind_on_multixacts[0], ages
    # ... and it is the first database's MultiXact age that is the greatest
    # of the four, by a margin that the few transaction IDs a run of the
    # script might use cannot close.
    assert behind_on_multixacts[1] >= consumed, ages
    assert max(behind_on_xids) < consumed // 2, ages
    return multixact_old, xid_old


def test_default_database_order_goes_by_the_greater_of_the_two_ages(
    cluster, make_db, run_ff
):
    # Issue #66: ordered by age(datfrozenxid) alone, the database that is
    # closest to wraparound through its MultiXact IDs was worked on after
    # one with less to worry about, and in a run with a time limit might
    # not be reached.
    multixact_old, xid_old = databases_behind_on_different_horizons(make_db)
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "-v")

    assert result.returncode == 0, result.stdout + result.stderr
    assert databases_processed(result) >= {multixact_old.name, xid_old.name}
    assert databases_in_order_of_work(cluster, mark, multixact_old, xid_old) == [
        multixact_old.name, xid_old.name,
    ]


def test_databases_option_is_worked_through_in_the_order_given(
    cluster, make_db, run_ff
):
    # With -d the order is the operator's, whatever the ages say: neither
    # order of two databases that differ in both ages is rearranged.
    multixact_old, xid_old = databases_behind_on_different_horizons(make_db)

    for first, second in ((xid_old, multixact_old), (multixact_old, xid_old)):
        mark = cluster.log_offset()

        result = run_ff(*FREEZE_ALL, "-d", f"{first.name},{second.name}")

        assert result.returncode == 0, result.stdout + result.stderr
        assert databases_in_order_of_work(cluster, mark, multixact_old, xid_old) == [
            first.name, second.name,
        ]


# -- which tables are left out ----------------------------------------------------


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
        "SELECT pg_table_size('t_small'), pg_table_size('t_big')"
    )[0]
    assert small_bytes < 2**20 <= big_bytes
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--minsizemb", "1", "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {big}
    assert targets(db.script_vacuums()) == [big]


def toasted_table(database, name):
    """A table whose heap is a few pages and whose TOAST table is over 1 MB.

    EXTERNAL storage keeps the long values out of line and uncompressed;
    with the default storage a repeated character compresses to nothing.
    1000 rows, so that make_dead_tuples() can be used on it.
    """
    table = database.create_table(name, rows=0)
    database.execute(f"ALTER TABLE {name} ALTER COLUMN pad SET STORAGE EXTERNAL")
    database.execute(
        f"INSERT INTO {name} SELECT g, repeat('x', 3000) "
        "FROM generate_series(1, 1000) AS g"
    )
    heap_bytes, table_bytes = database.execute(
        f"SELECT pg_relation_size('{name}'), pg_table_size('{name}')"
    )[0]
    assert heap_bytes < 2**20 <= table_bytes
    return table


def test_minsizemb_counts_the_toast_table(db, run_ff):
    # The freeze age counts the TOAST table, so the size has to as well.
    db.create_table("t_small")
    toasted = toasted_table(db, "t_toasted")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--minsizemb", "1", "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {toasted}
    assert targets(db.script_vacuums()) == [toasted]


def test_no_freeze_minsizemb_counts_the_toast_table(db, run_ff):
    # --minsizemb measures the same thing in both modes.
    toasted = toasted_table(db, "t_toasted")
    db.make_dead_tuples("t_toasted")

    result = run_ff("--no-freeze", "--minsizemb", "1", "-d", db.name)

    assert result.returncode == 0
    assert targets(db.script_vacuums()) == [toasted]


# -- what the freeze query selects ----------------------------------------------


def test_materialized_view_is_frozen(db, run_ff):
    base = db.create_table("t_base")
    db.execute("CREATE MATERIALIZED VIEW mv_base AS SELECT * FROM t_base")
    view = ("public", "mv_base")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    assert db.frozen_since(before) == {base, view}
    assert set(targets(db.script_vacuums())) == {base, view}


def test_table_is_selected_on_multixact_age_alone(make_db, run_ff):
    # A real MultiXact age with a transaction ID age of next to nothing.  The
    # two horizons are held back differently, and that is what makes it
    # possible: a transaction that has locked a row keeps every table's
    # relminmxid from passing the MultiXact counter as it stood then, in
    # every database, while it holds back relfrozenxid only in its own.  So
    # with such a transaction open in another database, a freeze here brings
    # the table's transaction ID age down and leaves its MultiXact age alone.
    db, elsewhere = make_db("target"), make_db("elsewhere")
    aged = db.create_table("t_multixact_old")
    young = db.create_table("t_young")
    threshold, consumed = 20, 40

    elsewhere.execute("CREATE TABLE ff_held AS SELECT 1 AS id")
    holder = elsewhere.connect(autocommit=False)
    try:
        with holder.cursor() as cur:
            cur.execute("SELECT id FROM ff_held FOR SHARE")
        consume_multixacts(elsewhere, consumed)
        db.execute("VACUUM (FREEZE) t_multixact_old")
    finally:
        holder.close()
    # With the holder gone, both of this table's ages come down.
    db.execute("VACUUM (FREEZE) t_young")

    def ages(table):
        return db.execute(
            "SELECT age(relfrozenxid), mxid_age(relminmxid) FROM pg_class "
            "WHERE oid = %s::regclass",
            (table[1],),
        )[0]

    xid_age, multixact_age = ages(aged)
    assert xid_age <= threshold < consumed <= multixact_age
    assert max(ages(young)) <= threshold

    result = run_ff("--freezeage", str(threshold), "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert targets(db.script_vacuums()) == [aged]
    # The freeze dealt with what it was selected for.
    assert ages(aged)[1] <= threshold


def test_freeze_list_is_not_capped(db, run_ff):
    # One more table than the cap the list used to have.  Tables without
    # columns keep this cheap: no TOAST table, and nothing to scan.
    count = 1001
    names = [f"ff_many_{n:04d}" for n in range(count)]
    # In batches: every CREATE in one transaction would need more lock
    # table entries than the test cluster has.
    for start in range(0, count, 200):
        db.execute(
            "; ".join(f"CREATE TABLE {name} ()" for name in names[start:start + 200])
        )
    wanted = {("public", name) for name in names}
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "--no-analyze", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == "All tables vacuumed.\n"
    vacuumed = targets(db.script_vacuums())
    assert len(vacuumed) == count
    assert set(vacuumed) == wanted
    assert db.frozen_since(before) == wanted


# -- what the --no-freeze query selects ------------------------------------------


def test_no_freeze_leaves_other_sessions_temp_tables_alone(db, run_ff):
    # The statistics view lists every session's temporary tables.  The
    # server will not vacuum another session's, and does not say so.
    db.create_table("t_dead", rows=1000)
    db.make_dead_tuples("t_dead")
    owner = db.connect()
    try:
        with owner.cursor() as cur:
            cur.execute("CREATE TEMP TABLE ff_temp (id integer)")
            cur.execute("INSERT INTO ff_temp SELECT generate_series(1, 1000)")
            cur.execute("DELETE FROM ff_temp WHERE id % 2 = 0")

        def selectable():
            # A backend reports its counts when a statement ends, and not
            # more often than about once a second; this session has to stay
            # open, so it is given statements to end.
            with owner.cursor() as cur:
                cur.execute("SELECT 1")
            return db.execute(
                "SELECT count(*) FROM pg_stat_user_tables "
                "WHERE relname = 'ff_temp' AND schemaname LIKE 'pg_temp%' "
                "AND n_dead_tup = 500 AND n_live_tup = 500"
            )[0][0]

        wait_for(selectable, what="dead-tuple statistics for the temporary table")

        result = run_ff("--no-freeze", "-d", db.name)
    finally:
        owner.close()

    assert result.returncode == 0, result.stdout + result.stderr
    assert targets(db.script_vacuums()) == [("public", "t_dead")]
