# Tests for how the script makes its connections: that connection values
# (database names, passwords) are passed to libpq as parameters rather than
# pasted into a key=value string, and that every session runs with an empty
# search_path so catalog names resolve through pg_catalog alone.
#
# Outcomes are proven from the catalog (relfrozenxid moved) and the server
# statement log (which session ran what, as which role), not from the
# script's own output: the point of each test is what reached the server.
#
# Several tests give a database a name that, pasted unquoted into a libpq
# connection string, would parse as extra connection keywords.  The name is
# created through the fixtures (never passed as a script argument), so the
# harness's argument guards are not involved and need no weakening.
#
# Passwords are the exception to trust authentication in the test cluster:
# the role ffharness.SCRAM_ROLE must present one, so a run as that role shows
# whether the -w value arrived intact.

import pytest
from ffharness import FREEZE_ALL, SCRAM_ROLE

TARGET = ("public", "t_target")

# A role that is never created, so a connection that honoured an injected
# "user=" keyword would fail to authenticate as it.
GHOST_ROLE = "ff_ghost_role"


def targets(vacuums):
    return [vacuum.target for vacuum in vacuums]


SET_SEARCH_PATH = "SELECT pg_catalog.set_config('search_path', '', false)"


# -- connection values are parameters, not connection-string text ------------


def test_database_name_with_a_space_is_vacuumed(make_db, run_ff):
    # The name reaches the script through the default database list (no -d),
    # i.e. straight from pg_database.  Pasted into a key=value string a space
    # makes the string unparseable and the database is skipped; passed as a
    # parameter it connects.
    database = make_db(name="ff space db")
    database.create_table("t_target")
    before = database.frozenxids()

    result = run_ff(*FREEZE_ALL)

    assert result.returncode == 0, result.stdout + result.stderr
    assert database.frozen_since(before) == {TARGET}
    assert targets(database.script_vacuums()) == [TARGET]


@pytest.mark.parametrize(
    "name",
    [
        pytest.param(f"ff_inj_user user={GHOST_ROLE}", id="user-keyword"),
        pytest.param("ff_inj_opts options=-csearch_path=evil", id="options-keyword"),
    ],
)
def test_connection_keyword_embedded_in_a_database_name_has_no_effect(
    make_db, cluster, run_ff, name
):
    # A database whose name embeds a libpq keyword that the harness's forced
    # -H/-p do not override.  Pasted into a connection string the keyword
    # would take effect (connect as GHOST_ROLE, or set options); passed as a
    # parameter the whole string is the database name and the keyword is
    # inert.  The name comes from the default database list, so no script
    # argument carries it.
    database = make_db(name=name)
    database.create_table("t_target")
    before = database.frozenxids()

    result = run_ff(*FREEZE_ALL)

    assert result.returncode == 0, result.stdout + result.stderr
    # Connected to the database under its real, whole name and froze it.
    assert database.frozen_since(before) == {TARGET}
    statements = cluster.statements(dbname=name)
    assert statements, "no session connected to the database under its real name"
    # As the real connecting role, never the injected one, and with the
    # search_path emptied rather than whatever an injected options= asked for.
    assert {record.user for record in statements} == {cluster.superuser}
    assert any(record.sql == SET_SEARCH_PATH for record in statements)


def test_backslash_in_a_database_name_is_kept(make_db, run_ff):
    # In a key=value connection string a backslash is an escape character and
    # is dropped, so "ff\back" would connect to "ffback".  A twin database
    # with exactly that name exists here, so a dropped backslash would freeze
    # the wrong database and still report success.
    named = make_db(name="ff\\back")
    twin = make_db(name="ffback")
    named.create_table("t_target")
    twin.create_table("t_target")
    named_before = named.frozenxids()
    twin_before = twin.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", named.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert named.frozen_since(named_before) == {TARGET}
    assert twin.frozen_since(twin_before) == set()


@pytest.mark.parametrize(
    "password",
    [
        # In libpq's key=value syntax an unquoted value ends at a space, a
        # single quote at its start opens a quoted value, and a backslash
        # escapes the character after it (at the end of the value, the
        # separator before the next keyword).  Quotes elsewhere in a value
        # are ordinary characters there; they are here for the shell-like
        # forms a password is most often mangled in.  Passed as a parameter,
        # every one of these must arrive as written.
        pytest.param("pass word", id="space"),
        pytest.param("'quoted", id="leading-single-quote"),
        pytest.param("it's", id="single-quote"),
        pytest.param('say "when"', id="double-quote"),
        pytest.param("back\\slash", id="backslash"),
        pytest.param("end\\", id="trailing-backslash"),
        pytest.param(" a 'b' \\'c\\\\ \"d\" ", id="all-of-them"),
    ],
)
def test_password_reaches_the_server_unaltered(
    cluster, db, make_role, run_ff, password
):
    # SCRAM_ROLE is the one role the server asks for a password, so the run
    # can only connect if the value given with -w is, byte for byte, the
    # password the role was created with.
    role = make_role(name=SCRAM_ROLE, superuser=True, password=password)
    db.create_table("t_target")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-U", role, "-w", password, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {TARGET}
    assert targets(db.script_vacuums()) == [TARGET]
    assert {record.user for record in cluster.statements(dbname=db.name)} == {role}


@pytest.mark.parametrize(
    "password, given",
    [
        pytest.param("right-one", "wrong-one", id="different"),
        # Near misses of the passwords above.  The server has to tell these
        # apart from the real one for the test above to mean anything.
        pytest.param("pass word", "pass", id="cut-at-the-space"),
        pytest.param("back\\slash", "backslash", id="backslash-dropped"),
        pytest.param("right-one", "********", id="the-masked-form"),
    ],
)
def test_wrong_password_is_rejected(cluster, db, make_role, run_ff, password, given):
    # No -d: the first connection is the one that lists the databases, and
    # its failure ends the run.  (A -d run would report the database as
    # skipped instead; what follows a skipped last database is issue #40.)
    role = make_role(name=SCRAM_ROLE, superuser=True, password=password)
    db.create_table("t_target")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL, "-U", role, "-w", given)

    assert result.returncode == 1
    assert result.stdout == ""
    assert (
        "Could not list databases: connection to database postgres failed: "
        in result.stderr
    )
    assert "password authentication failed" in result.stderr
    assert "Traceback" not in result.stderr
    assert db.frozen_since(before) == set()
    assert cluster.statements(since=mark) == []


# -- empty search_path keeps name resolution in pg_catalog -------------------


def test_set_config_runs_on_every_connection(cluster, db, run_ff):
    # Default run (no -d): the search_path is emptied on the database-list
    # connection to postgres and on the connection to the user database,
    # before any other statement there.
    db.create_table("t_target")
    before = db.frozenxids()
    mark = cluster.log_offset()

    result = run_ff(*FREEZE_ALL)

    assert result.returncode == 0, result.stdout + result.stderr

    # Both lists hold the script's statements only.  What the driver sends
    # for itself when a session starts is filtered out of the log by content
    # (ffharness._DRIVER_SETUP), so index 0 is the script's first statement
    # however the server is configured.
    postgres_statements = [
        record.sql for record in cluster.statements(dbname="postgres", since=mark)
    ]
    assert postgres_statements and postgres_statements[0] == SET_SEARCH_PATH

    db_statements = db.script_statements(since=mark)
    assert db_statements and db_statements[0] == SET_SEARCH_PATH
    vacuum_index = next(
        i for i, sql in enumerate(db_statements) if sql.upper().startswith("VACUUM")
    )
    assert db_statements.index(SET_SEARCH_PATH) < vacuum_index
    assert db.frozen_since(before) == {TARGET}


def test_function_shadowing_a_catalog_function_in_public_is_not_called(
    db, make_role, run_ff
):
    # A less-privileged role with CREATE on public defines an exact-signature
    # match, public.pg_table_size(oid), where pg_catalog offers only a
    # coercion match (its function takes regclass).  Under a non-empty path
    # the planted function is resolved and runs as the connecting role; its
    # side effect (a row in ff_tripwire) is the proof.  An empty search_path
    # keeps resolution in pg_catalog, so it is never called.
    role = make_role("lowpriv", superuser=False)
    db.execute(f"GRANT CREATE ON SCHEMA public TO {role}")
    db.execute("CREATE TABLE public.ff_tripwire (n serial PRIMARY KEY)")
    db.execute(
        "CREATE FUNCTION public.pg_table_size(oid) RETURNS bigint "
        "LANGUAGE plpgsql AS "
        "$$ BEGIN INSERT INTO public.ff_tripwire DEFAULT VALUES; RETURN 0; END; $$",
        user=role,
    )
    db.create_table("t_target")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    fired = db.execute("SELECT count(*) FROM public.ff_tripwire")[0][0]
    assert fired == 0, "planted public.pg_table_size(oid) was called"
    # The real table is still selected and frozen under the empty path.
    assert TARGET in db.frozen_since(before)
    assert TARGET in set(targets(db.script_vacuums()))


def test_database_level_search_path_does_not_divert_name_resolution(db, run_ff):
    # A database-level search_path that names a writable schema before
    # pg_catalog exposes even exact catalog matches such as age(xid).  The
    # setting applies to every session that connects, including the script's;
    # an empty search_path set by the script overrides it, so the planted
    # evil.age(xid) is not called.
    db.create_schema("evil")
    db.execute("CREATE TABLE public.ff_tripwire (n serial PRIMARY KEY)")
    db.execute(
        "CREATE FUNCTION evil.age(xid) RETURNS bigint LANGUAGE plpgsql AS "
        "$$ BEGIN INSERT INTO public.ff_tripwire DEFAULT VALUES; "
        "RETURN 2000000000; END; $$"
    )
    db.create_table("t_target")
    db.execute(f'ALTER DATABASE "{db.name}" SET search_path = evil, pg_catalog')
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    fired = db.execute("SELECT count(*) FROM public.ff_tripwire")[0][0]
    assert fired == 0, "planted evil.age(xid) was called"
    assert SET_SEARCH_PATH in db.script_statements()
    assert TARGET in db.frozen_since(before)


def test_no_freeze_still_selects_and_analyzes_under_the_empty_path(db, run_ff):
    # The --no-freeze selection query (the statistics views) must keep working
    # with an empty search_path: the table is vacuumed and analyzed.
    db.create_table("t_dead", rows=1000)
    db.make_dead_tuples("t_dead")

    result = run_ff("--no-freeze", "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    vacuums = db.script_vacuums()
    assert targets(vacuums) == [("public", "t_dead")]
    assert vacuums[0].options == {"ANALYZE", "SKIP_LOCKED"}
    assert SET_SEARCH_PATH in db.script_statements()
    db.wait_for_vacuum_count("t_dead", 1)
    assert db.is_analyzed("t_dead")
