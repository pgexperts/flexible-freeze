# Tests of the harness itself: the guards that keep a script run, the cluster
# teardown and the sweep for abandoned clusters confined to the throwaway
# cluster, the coverage environment for script subprocesses, and the VACUUM
# statement parser the other test files rely on.  Most of them build
# commands without launching anything, so they run even where no PostgreSQL
# binaries are installed.

import os
import re
import sys
import tempfile
import time
import types
from pathlib import Path

import ffharness
import pytest
from ffharness import FREEZE_ALL, HarnessError, UnsafeInvocation, parse_vacuum

PORT = ffharness.TEST_PORT

# Above the largest process ID either supported platform can assign (the
# Linux ceiling is 2**22), so no process has it.
NO_SUCH_PID = 2**22 + 1


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


def test_child_environment_admits_only_the_connect_timeout(monkeypatch):
    # The one libpq variable a test may set for the script: it cannot change
    # which server is reached.  It is still not inherited from the caller.
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "99")
    assert "PGCONNECT_TIMEOUT" not in ffharness.child_env()
    assert ffharness.child_env({"PGCONNECT_TIMEOUT": "2"})["PGCONNECT_TIMEOUT"] == "2"
    for redirecting in ("PGHOST", "PGPORT", "PGSERVICE", "PGHOSTADDR"):
        with pytest.raises(UnsafeInvocation):
            ffharness.child_env({redirecting: "elsewhere"})


# -- coverage of the script --------------------------------------------------


def test_coverage_environment_is_empty_unless_asked_for(monkeypatch):
    monkeypatch.delenv("COVERAGE_PROCESS_START", raising=False)
    monkeypatch.setenv("COVERAGE_FILE", "/somewhere/.coverage")
    assert ffharness.coverage_env() == {}


def test_coverage_environment_uses_absolute_paths(monkeypatch, tmp_path):
    # Relative values are resolved here, in the test process: the child may
    # be given a different working directory.
    monkeypatch.chdir(tmp_path)
    (tmp_path / "some.coveragerc").write_text("")
    monkeypatch.setenv("COVERAGE_PROCESS_START", "some.coveragerc")
    monkeypatch.setenv("COVERAGE_FILE", "data/.coverage")

    assert ffharness.coverage_env() == {
        "COVERAGE_PROCESS_START": os.path.join(os.getcwd(), "some.coveragerc"),
        "COVERAGE_FILE": os.path.join(os.getcwd(), "data", ".coverage"),
    }


def test_coverage_data_defaults_to_the_repository_root(monkeypatch):
    monkeypatch.setenv(
        "COVERAGE_PROCESS_START", str(ffharness.REPO_ROOT / ".coveragerc")
    )
    monkeypatch.delenv("COVERAGE_FILE", raising=False)

    env = ffharness.coverage_env()

    assert env["COVERAGE_FILE"] == str(ffharness.REPO_ROOT / ".coverage")


def test_coverage_configuration_that_is_not_a_file_is_an_error(
    sockdir, tmp_path, monkeypatch
):
    monkeypatch.setenv("COVERAGE_PROCESS_START", str(tmp_path / "missing.coveragerc"))

    with pytest.raises(HarnessError):
        ffharness.coverage_env()
    # And the launcher stops there instead of starting the script.
    with pytest.raises(HarnessError):
        ffharness.start_ff(sockdir, PORT, "--help")


# The two tests below launch the script with --help, which makes argparse
# print the usage and exit before the script connects to anything; no server
# is needed.


def test_script_run_records_coverage_in_a_coverage_run(sockdir, tmp_path, monkeypatch):
    # Checked only when the session itself was started as a coverage run.
    # An ordinary run, which is what decides CI, then never depends on how
    # coverage.py behaves.
    if not os.environ.get("COVERAGE_PROCESS_START"):
        pytest.skip("only checked in a coverage run (see .coveragerc)")
    pytest.importorskip("coverage")
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("COVERAGE_FILE", str(data / ".coverage"))

    # A working directory other than the one the paths were given in.
    result = ffharness.run_ff(sockdir, PORT, "--help", cwd=str(tmp_path))

    assert result.returncode == 0
    assert result.stderr == ""
    # parallel = true: one suffixed data file per process, never the bare name.
    recorded = [path.name for path in data.iterdir()]
    assert len(recorded) == 1 and recorded[0].startswith(".coverage."), recorded


def test_script_run_records_nothing_by_default(sockdir, tmp_path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.delenv("COVERAGE_PROCESS_START", raising=False)
    monkeypatch.setenv("COVERAGE_FILE", str(data / ".coverage"))

    result = ffharness.run_ff(sockdir, PORT, "--help", cwd=str(data))

    assert result.returncode == 0
    assert result.stderr == ""
    assert list(data.iterdir()) == []


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


# -- the sweep for abandoned clusters ----------------------------------------


@pytest.mark.parametrize(
    "content, expected",
    [
        pytest.param(None, None, id="no-pid-file"),
        pytest.param("", None, id="empty"),
        pytest.param("not-a-pid\n", None, id="not-a-number"),
        pytest.param(f"{NO_SUCH_PID}\n", None, id="no-such-process"),
        pytest.param(f"{2**40}\n", None, id="too-large-for-a-pid"),
        # Signal 0 to pid 0, or to a negative pid, tests a process group and
        # succeeds for the test run's own group: neither names a postmaster.
        pytest.param("0\n", None, id="zero"),
        pytest.param(f"-{os.getpgrp()}\n", None, id="negative"),
        pytest.param(f"{os.getpid()}\n/some/data\n", os.getpid(), id="live-process"),
    ],
)
def test_postmaster_pid_is_reported_only_for_a_live_process(sockdir, content, expected):
    cluster = ffharness.Cluster("/nonexistent/bin", sockdir, PORT, "nobody")
    cluster.datadir.mkdir()
    if content is not None:
        (cluster.datadir / "postmaster.pid").write_text(content)

    assert cluster._postmaster_pid() == expected


def _leftover(parent, name, *, marked=True, age=2 * ffharness.STALE_AFTER, pid=None):
    """Build what a cluster's base directory looks like from outside: the
    marker, last touched `age` seconds ago, and a postmaster.pid naming
    `pid`.  No server runs in it.  The directories are closed to group and
    others, as mkdtemp and initdb make the real ones, whatever the umask."""
    base = parent / f"{ffharness.BASE_PREFIX}{name}"
    (base / "data").mkdir(parents=True)
    base.chmod(0o700)
    (base / "data").chmod(0o700)
    if pid is not None:
        (base / "data" / "postmaster.pid").write_text(f"{pid}\n{base / 'data'}\n")
    if marked:
        marker = base / ffharness.MARKER
        marker.write_text("")
        touched = time.time() - age
        os.utime(marker, (touched, touched))
    return base


@pytest.fixture
def stopped(monkeypatch):
    """Cluster.stop() replaced by a recorder: the list of directories the
    sweep decided to stop and remove.  Nothing is signalled or deleted."""
    calls = []
    monkeypatch.setattr(
        ffharness.Cluster, "stop", lambda self: calls.append(self.basedir)
    )
    return calls


@pytest.fixture
def serving(monkeypatch):
    """Every candidate "answers on its socket".  Without this a directory
    with no server in it is passed over for that reason alone, and a test
    of any other check could not fail."""
    monkeypatch.setattr(ffharness.Cluster, "_is_serving", lambda self: True)


def _disown(monkeypatch, path):
    """Make os.lstat report `path` as belonging to another user, which a
    test cannot arrange for real without privileges."""
    real = os.lstat

    def lstat(target, *args, **kwargs):
        info = real(target, *args, **kwargs)
        if os.fspath(target) != str(path):
            return info
        return types.SimpleNamespace(
            st_mode=info.st_mode, st_uid=info.st_uid + 1, st_mtime=info.st_mtime
        )

    monkeypatch.setattr(os, "lstat", lstat)


def test_sweep_takes_a_leftover_that_passes_every_check(tmp_path, stopped, serving):
    # The control for the tests below: this is the directory each of them
    # spoils in one respect.
    base = _leftover(tmp_path, "left", pid=os.getpid())

    swept = ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path])

    assert swept == [base]
    assert stopped == [base]


@pytest.mark.parametrize(
    "leftover",
    [
        # The marker gate: without it the directory is not the harness's,
        # whatever else it contains.
        pytest.param({"marked": False, "pid": os.getpid()}, id="no-marker"),
        pytest.param({"age": 0, "pid": os.getpid()}, id="marker-not-old-enough"),
        pytest.param({}, id="no-pid-file"),
        # A pid file the sweep cannot make sense of must not stop the session.
        pytest.param({"pid": 2**40}, id="pid-out-of-range"),
    ],
)
def test_sweep_leaves_alone_what_is_not_an_abandoned_cluster(
    tmp_path, stopped, serving, leftover
):
    base = _leftover(tmp_path, "left", **leftover)

    swept = ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path])

    assert swept == []
    assert stopped == []
    assert (base / "data").is_dir()


def test_sweep_needs_a_server_answering_for_the_directory(tmp_path, stopped):
    # The pid file survived a reboot and its number now belongs to an
    # unrelated process, here the test run itself, which `pg_ctl stop` would
    # signal.  Nothing answers on the directory's socket, and that alone
    # keeps the sweep off it.
    _leftover(tmp_path, "left", pid=os.getpid())

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path]) == []
    assert stopped == []


# The marker is checked in three places (Cluster's constructor, _is_abandoned
# and stop()), so that no single mistake opens the gate.  The no-marker case
# above only shows that at least one of them held; the next two tests, and
# test_stop_refuses_to_delete_an_unmarked_directory, pin each on its own.


def test_sweep_does_not_consider_an_unmarked_directory_at_all(
    tmp_path, stopped, monkeypatch
):
    # Everything after the constructor is made to say "abandoned".
    monkeypatch.setattr(ffharness.Cluster, "_is_abandoned", lambda self, age: True)
    _leftover(tmp_path, "left", marked=False, pid=os.getpid())

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path]) == []
    assert stopped == []


def test_cluster_whose_marker_is_gone_is_not_abandoned(tmp_path, serving):
    base = _leftover(tmp_path, "left", pid=os.getpid())
    cluster = ffharness.Cluster("/nonexistent/bin", base, PORT, "nobody")
    assert cluster._is_abandoned(ffharness.STALE_AFTER)

    (base / ffharness.MARKER).unlink()

    assert not cluster._is_abandoned(ffharness.STALE_AFTER)


# /tmp is one of the directories swept, and any local user can create an
# ffpg* directory there.  The marker inside it proves nothing: it can be a
# hard link to a file of the test user's.  So the directory itself, and the
# entries the pid is read from, have to be the test user's own.


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param(0o720, id="group-writable"),
        pytest.param(0o702, id="other-writable"),
        pytest.param(0o777, id="open-to-all"),
    ],
)
def test_sweep_leaves_alone_a_directory_others_can_write_to(
    tmp_path, stopped, serving, mode
):
    base = _leftover(tmp_path, "left", pid=os.getpid())
    base.chmod(mode)

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path]) == []
    assert stopped == []


@pytest.mark.parametrize(
    "entry",
    [
        pytest.param((), id="the-directory"),
        pytest.param((ffharness.MARKER,), id="the-marker"),
        pytest.param(("data",), id="the-data-directory"),
        pytest.param(("data", "postmaster.pid"), id="the-pid-file"),
    ],
)
def test_sweep_leaves_alone_what_belongs_to_another_user(
    tmp_path, stopped, serving, monkeypatch, entry
):
    base = _leftover(tmp_path, "left", pid=os.getpid())
    _disown(monkeypatch, base.joinpath(*entry))

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[tmp_path]) == []
    assert stopped == []


@pytest.mark.parametrize(
    "entry, is_directory",
    [
        pytest.param((ffharness.MARKER,), False, id="the-marker"),
        pytest.param(("data",), True, id="the-data-directory"),
        pytest.param(("data", "postmaster.pid"), False, id="the-pid-file"),
    ],
)
def test_sweep_leaves_alone_a_directory_with_a_symbolic_link_inside(
    tmp_path, stopped, serving, entry, is_directory
):
    # The entry is moved aside and a link to it left in its place, so the
    # link leads to something that would itself pass.  The link is given an
    # old timestamp of its own, so that in the marker's case its age cannot
    # be what counts against it.
    parent = tmp_path / "parent"
    base = _leftover(parent, "left", pid=os.getpid())
    real = tmp_path / "moved-aside"
    link = base.joinpath(*entry)
    link.rename(real)
    link.symlink_to(real, target_is_directory=is_directory)
    touched = time.time() - 2 * ffharness.STALE_AFTER
    os.utime(link, (touched, touched), follow_symlinks=False)

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[parent]) == []
    assert stopped == []


def test_sweep_does_not_follow_a_symbolic_link(tmp_path, stopped, serving):
    # With "is it serving" out of the picture, only the kind of the
    # candidate itself stands between the sweep and the directory.
    elsewhere = tmp_path / "elsewhere"
    parent = tmp_path / "parent"
    parent.mkdir()
    target = _leftover(elsewhere, "target", pid=os.getpid())
    (parent / f"{ffharness.BASE_PREFIX}link").symlink_to(target)

    assert ffharness.sweep_stale_clusters("/nonexistent/bin", parents=[parent]) == []
    assert stopped == []


def test_sweep_stops_and_removes_an_abandoned_cluster(pg_bindir, monkeypatch):
    # A second server, in a parent directory of its own.  The usual parent
    # is shared with every other session on the machine: there, another
    # session's sweep could take this cluster first, and this test's sweep
    # would be let loose on theirs.  /tmp keeps the socket path short.
    parent = Path(tempfile.mkdtemp(prefix="ffsweep", dir="/tmp"))
    monkeypatch.setattr(ffharness, "_base_parents", lambda: (str(parent),))
    try:
        try:
            abandoned = ffharness.Cluster.start(pg_bindir)
        except HarnessError as exc:
            # macOS as shipped allows too few System V semaphores for two
            # servers.  Anywhere else a second server must start.
            if sys.platform != "darwin":
                raise
            pytest.skip(f"could not start a second cluster: {exc}")
        try:
            assert abandoned.basedir.parent == parent
            pid = abandoned._postmaster_pid()
            assert pid is not None

            # With a fresh marker it is a session at work, and is left alone.
            assert ffharness.sweep_stale_clusters(pg_bindir) == []
            assert abandoned.execute("postgres", "SELECT 1") == [(1,)]

            touched = time.time() - 2 * ffharness.STALE_AFTER
            os.utime(abandoned.basedir / ffharness.MARKER, (touched, touched))

            assert ffharness.sweep_stale_clusters(pg_bindir) == [abandoned.basedir]
            assert not abandoned.basedir.exists()

            def postmaster_gone():
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return True
                return False

            ffharness.wait_for(
                postmaster_gone, what="the abandoned postmaster to exit"
            )
        finally:
            abandoned.stop()
    finally:
        parent.rmdir()


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
        # An option with a value: the name in upper case, the value in lower.
        (
            "VACUUM (FREEZE, ANALYZE, TRUNCATE false, SKIP_LOCKED) public.t1",
            {"FREEZE", "ANALYZE", "TRUNCATE false", "SKIP_LOCKED"},
            ("public", "t1"),
        ),
        (
            "vacuum (truncate  FALSE,parallel 2) t1",
            {"TRUNCATE false", "PARALLEL 2"},
            ("public", "t1"),
        ),
        # Not the same entry as TRUNCATE false, nor as TRUNCATE true.
        ("VACUUM (TRUNCATE) t1", {"TRUNCATE"}, ("public", "t1")),
        ("VACUUM (TRUNCATE true) t1", {"TRUNCATE true"}, ("public", "t1")),
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


@pytest.mark.parametrize(
    "option",
    ["TRUNCATE false false", "TRUNCATE 'false'", "TRUNCATE = false", "2"],
    ids=["two-values", "quoted-value", "equals-sign", "no-name"],
)
def test_parse_vacuum_rejects_an_option_outside_its_scope(option):
    with pytest.raises(HarnessError):
        parse_vacuum(f"VACUUM (FREEZE, {option}) t1")


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


def test_statement_log_leaves_out_the_drivers_session_setup(db, run_ff):
    # A non-ISO DateStyle makes psycopg2 send a SET of its own ahead of the
    # script's first statement, under the script's application_name.  The
    # default view of the log must still begin with the script's statement.
    db.create_table("t1")
    db.execute(f'ALTER DATABASE "{db.name}" SET datestyle = \'SQL, DMY\'')

    result = run_ff(*FREEZE_ALL, "-d", db.name)

    assert result.returncode == 0
    logged = [
        record.sql
        for record in db.cluster.statements(dbname=db.name, driver_setup=True)
    ]
    # The premise: the driver's statement is in the log, and comes first.
    assert logged[0] == "SET datestyle TO 'ISO'"
    assert db.script_statements() == logged[1:]
    assert db.script_statements()[0] == (
        "SELECT pg_catalog.set_config('search_path', '', false)"
    )


def test_only_the_scram_role_is_asked_for_a_password(cluster):
    rules = cluster.execute(
        "postgres",
        "SELECT type, database, user_name, auth_method, error "
        "FROM pg_hba_file_rules ORDER BY line_number",
    )
    assert rules[0] == (
        "local", ["all"], [ffharness.SCRAM_ROLE], "scram-sha-256", None,
    )
    assert len(rules) > 1
    assert {(method, error) for *_, method, error in rules[1:]} == {("trust", None)}
