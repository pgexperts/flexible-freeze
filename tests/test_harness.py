# Tests of the harness itself: the guards that keep a script run, and the
# cluster teardown, confined to the throwaway cluster, and the VACUUM
# statement parser the other test files rely on.  Most of them build
# commands without launching anything, so they run even where no PostgreSQL
# binaries are installed.

import re
from pathlib import Path

import ffharness
import pytest
from ffharness import FREEZE_ALL, HarnessError, UnsafeInvocation, parse_vacuum

PORT = ffharness.TEST_PORT


@pytest.fixture
def sockdir(tmp_path):
    """A directory carrying the harness marker, standing in for a cluster's."""
    (tmp_path / ffharness.MARKER).write_text("")
    return str(tmp_path)


# -- the launcher's target ---------------------------------------------------


def test_command_pins_host_and_port_last(sockdir):
    argv = ffharness.ff_command(sockdir, PORT, ["-v", "-d", "somedb"])
    assert argv[1] == str(ffharness.SCRIPT)
    assert argv[2:] == [
        "--pause", "0", "-v", "-d", "somedb", "-H", sockdir, "-p", str(PORT),
    ]


def test_command_refuses_a_host_that_is_not_a_harness_directory(sockdir, tmp_path):
    unmarked = tmp_path / "unmarked"
    unmarked.mkdir()
    for host in (None, "", "localhost", "/tmp", str(unmarked), f"{sockdir},localhost"):
        with pytest.raises(UnsafeInvocation):
            ffharness.ff_command(host, PORT, [])


@pytest.mark.parametrize(
    "port",
    [None, 0, -1, "not-a-port", ffharness.FORBIDDEN_PORT, str(ffharness.FORBIDDEN_PORT)],
)
def test_command_refuses_a_missing_or_default_port(sockdir, port):
    with pytest.raises(UnsafeInvocation):
        ffharness.ff_command(sockdir, port, [])


@pytest.mark.parametrize(
    "args",
    [
        ["-H", "elsewhere"],
        ["-Helsewhere"],
        ["--host", "elsewhere"],
        ["--host=elsewhere"],
        ["--ho", "elsewhere"],
        ["-vH", "elsewhere"],
        ["-p", "1"],
        ["-p1"],
        ["--port", "1"],
        ["--port=1"],
        ["--po", "1"],
        ["-tvp1"],
        # Values the script would paste into its connection string.
        ["-U", "someone host=elsewhere"],
        ["-w", "secret hostaddr=192.0.2.1"],
        ["-d", "somedb port=1"],
        ["-d", "somedb service = other"],
    ],
)
def test_command_refuses_arguments_that_redirect_the_script(sockdir, args):
    with pytest.raises(UnsafeInvocation):
        ffharness.ff_command(sockdir, PORT, args)


def test_command_allows_the_other_options(sockdir):
    args = [
        "--pause", "3", "--print-timestamps", "-t", "-v", "-T", "people",
        "-d", "ghost,report", "-st", "t1", "-l", "/tmp/ff.log",
    ]
    argv = ffharness.ff_command(sockdir, PORT, args)
    assert argv[4:-4] == args
    assert argv[-4:] == ["-H", sockdir, "-p", str(PORT)]


def test_child_environment_has_no_libpq_variables(monkeypatch):
    monkeypatch.setenv("PGHOST", "elsewhere")
    monkeypatch.setenv("PGSERVICE", "elsewhere")
    monkeypatch.setenv("FF_HARNESS_MARKER", "kept")
    env = ffharness.child_env({"TZ": "UTC"})
    assert not [key for key in env if key.startswith("PG")]
    assert env["FF_HARNESS_MARKER"] == "kept"
    assert env["TZ"] == "UTC"


def test_child_environment_refuses_libpq_variables():
    with pytest.raises(UnsafeInvocation):
        ffharness.child_env({"PGDATABASE": "elsewhere"})


# -- the cluster's directory -------------------------------------------------


def test_cluster_cannot_be_built_on_a_directory_the_harness_did_not_make(tmp_path):
    with pytest.raises(UnsafeInvocation):
        ffharness.Cluster("/nonexistent/bin", tmp_path, PORT, "nobody")


def test_stop_refuses_to_delete_an_unmarked_directory(sockdir):
    cluster = ffharness.Cluster("/nonexistent/bin", sockdir, PORT, "nobody")
    keep = Path(sockdir) / "keep.txt"
    keep.write_text("not the harness's to delete")
    (Path(sockdir) / ffharness.MARKER).unlink()

    with pytest.raises(HarnessError):
        cluster.stop()

    assert keep.exists()


def test_stop_removes_a_marked_directory_and_is_idempotent(sockdir):
    cluster = ffharness.Cluster("/nonexistent/bin", sockdir, PORT, "nobody")

    cluster.stop()
    cluster.stop()

    assert not Path(sockdir).exists()


def test_test_files_do_not_bypass_the_harness():
    # Launching the script or opening a connection anywhere but in
    # ffharness.py would sidestep the guards above.
    bypass = re.compile(r"^\s*(import|from)\s+(subprocess|psycopg2)\b", re.MULTILINE)
    default_port = re.compile(rf"(?<!\d){ffharness.FORBIDDEN_PORT}(?!\d)")
    tests = sorted(Path(__file__).parent.glob("test_*.py"))
    assert tests
    for path in tests:
        source = path.read_text()
        assert not bypass.search(source), path.name
        assert not default_port.search(source), path.name


# -- the VACUUM parser -------------------------------------------------------


@pytest.mark.parametrize(
    "statement, options, target",
    [
        ('VACUUM FREEZE ANALYZE "t1"', {"FREEZE", "ANALYZE"}, ("public", "t1")),
        ('VACUUM "t1"', set(), ("public", "t1")),
        ("VACUUM ANALYZE t1", {"ANALYZE"}, ("public", "t1")),
        ("VACUUM ANALYZE Sales.Orders", {"ANALYZE"}, ("sales", "orders")),
        (
            'VACUUM (FREEZE, ANALYZE, SKIP_LOCKED) "sales"."Order ""Items"""',
            {"FREEZE", "ANALYZE", "SKIP_LOCKED"},
            ("sales", 'Order "Items"'),
        ),
        ('vacuum (freeze) "a.b";', {"FREEZE"}, ("public", "a.b")),
    ],
)
def test_parse_vacuum(statement, options, target):
    parsed = parse_vacuum(statement)
    assert parsed.sql == statement
    assert parsed.options == options
    assert parsed.target == target


def test_parse_vacuum_rejects_what_it_does_not_understand():
    with pytest.raises(HarnessError):
        parse_vacuum("VACUUM")
    with pytest.raises(HarnessError):
        parse_vacuum('VACUUM FREEZE "t1", "t2"')


# -- against the running cluster ---------------------------------------------


def test_cluster_is_private_and_off_the_default_port(cluster):
    assert cluster.port != ffharness.FORBIDDEN_PORT
    assert cluster.sockdir == str(cluster.basedir)
    assert (cluster.basedir / ffharness.MARKER).is_file()
    settings = dict(
        cluster.execute(
            "postgres",
            "SELECT name, setting FROM pg_settings WHERE name IN "
            "('listen_addresses', 'unix_socket_directories', 'port', 'autovacuum')",
        )
    )
    assert settings == {
        "listen_addresses": "",
        "unix_socket_directories": cluster.sockdir,
        "port": str(cluster.port),
        "autovacuum": "off",
    }


def test_run_ignores_libpq_variables_in_the_callers_environment(
    db, run_ff, monkeypatch
):
    # Had these reached the script it would fail: explicit -H/-p would still
    # win over PGHOST/PGPORT, but the role would not exist.
    monkeypatch.setenv("PGHOST", "/nonexistent/ff-elsewhere")
    monkeypatch.setenv("PGPORT", "1")
    monkeypatch.setenv("PGUSER", "ff_no_such_role")
    table = db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    assert db.frozen_since(before) == {table}


def test_statement_log_separates_script_from_harness(db, run_ff):
    table = db.create_table("t1")
    run_ff(*FREEZE_ALL, "-d", db.name)

    script = db.cluster.statements(dbname=db.name)
    assert script and {record.app for record in script} == {ffharness.SCRIPT_APP}
    assert {record.user for record in script} == {db.cluster.superuser}
    assert [vacuum.target for vacuum in db.script_vacuums()] == [table]

    harness = db.cluster.statements(dbname=db.name, app=ffharness.HARNESS_APP)
    assert any(record.sql.startswith("CREATE TABLE") for record in harness)
    assert not any(record.sql.startswith("VACUUM") for record in harness)
