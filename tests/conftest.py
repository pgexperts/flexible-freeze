# Fixtures for the flexible_freeze.py test suite.
#
# What the harness is
# -------------------
# scripts/flexible_freeze.py does its whole run at import time, so it is
# tested from the outside: the suite starts a throwaway PostgreSQL cluster,
# runs the script against it as a subprocess, and then checks the server's
# catalogs, statistics and statement log for what actually happened.  The
# machinery is in ffharness.py; this file only turns it into fixtures.
#
# The safety rule
# ---------------
# Run without -H and -p the script connects to the machine's default server
# and VACUUM FREEZEs every database in it.  So:
#
#   * never launch the script except through the run_ff / start_ff fixtures
#     (Cluster.run_ff / Cluster.start_ff).  They always pass -H <socket dir>
#     -p <port> for the throwaway cluster, remove every PG* environment
#     variable, and raise ffharness.UnsafeInvocation instead of running if
#     an argument or environment variable tries to point somewhere else;
#   * never open a database connection except through Cluster.connect /
#     Cluster.execute / Database.execute;
#   * never write port 5432 or a host name into a test.
#
# test_harness.py enforces the first two mechanically (test files may not
# import subprocess or psycopg2).  The cluster listens only on a unix socket
# inside its own temporary directory, never on TCP, and that directory
# carries a marker file: the launcher refuses any other directory, and
# teardown deletes nothing that lacks the marker.
#
# The server is stopped and its directory removed when the session ends,
# including on Ctrl-C, SIGTERM and SIGHUP.  Only a SIGKILL of pytest leaves
# it behind.  The next session to start stops and removes such a server once
# its marker file is an hour old (ffharness.sweep_stale_clusters).  To be
# rid of it sooner, look for a postgres process whose data directory is
# under a temporary directory named ffpg*, and stop it with
# `pg_ctl -D <that directory>/data -m immediate stop`.
#
# Every connection is trusted except those of one role,
# ffharness.SCRAM_ROLE, which pg_hba.conf asks for a password.  A test that
# needs the server to check the script's -w value creates that role with
# make_role(name=ffharness.SCRAM_ROLE, password=...) and runs the script as
# it; the harness itself never connects as that role.
#
# Running it
# ----------
#   python -m pip install -r requirements-dev.txt
#   python -m pytest
#
# or, without installing anything:
#
#   uv run -q --with pytest --with psycopg2-binary pytest -q
#
# Line coverage of the script is measured only on request, because the
# script runs in subprocesses that each have to record their own:
#
#   COVERAGE_PROCESS_START=$PWD/.coveragerc python -m pytest
#   python -m coverage combine && python -m coverage report
#
# initdb and pg_ctl are looked up in $PG_BINDIR, then PATH, then
# `pg_config --bindir`, then /usr/local/pgsql/bin.  If none has them the
# tests that need a server are skipped; a PG_BINDIR that is set but wrong is
# an error.  The suite supports PostgreSQL 14 and later.
#
# Extending it
# ------------
# Each test gets fresh databases from `db` / `make_db`, dropped afterwards,
# so nothing leaks between tests and the cluster holds only the current
# test's databases (the script's default database list depends on that).
# Pass ffharness.FREEZE_ALL to make freshly created tables eligible, and
# prove outcomes from the server (Database.frozen_since, is_analyzed,
# wait_for_vacuum_count, script_vacuums, script_statements) rather than from
# the script's own output alone.  script_vacuums() returns parsed statements
# (.target, .options, .sql); assert on those rather than on literal VACUUM
# text, so a test keeps its meaning when the statement's spelling changes.

import contextlib
import itertools
import os
import signal

import ffharness
import pytest


def pytest_report_header(config):
    try:
        bindir = ffharness.find_pg_bindir()
    except ffharness.HarnessError as exc:
        return f"PostgreSQL binaries: {exc}"
    return f"PostgreSQL binaries: {bindir or 'not found (server tests skip)'}"


@pytest.fixture(scope="session", autouse=True)
def _no_libpq_environment():
    """Remove libpq's PG* variables from the test process itself.

    Harness connections name host, port, user and database explicitly, but
    libpq would still apply PGOPTIONS, PGSERVICE, PGSSLMODE and the like.
    PG_BINDIR is the harness's own setting and stays.
    """
    patch = pytest.MonkeyPatch()
    for key in list(os.environ):
        if key.startswith("PG") and key != "PG_BINDIR":
            patch.delenv(key)
    yield
    patch.undo()


@pytest.fixture(scope="session")
def pg_bindir():
    """Directory holding initdb and pg_ctl; skips the test if there is none."""
    bindir = ffharness.find_pg_bindir()
    if bindir is None:
        pytest.skip(
            "no PostgreSQL server binaries (initdb, pg_ctl) found; set "
            "PG_BINDIR to the directory that holds them"
        )
    return bindir


def _interrupt(signum, frame):
    raise KeyboardInterrupt(f"terminated by signal {signum}")


@contextlib.contextmanager
def _termination_as_interrupt():
    """Make SIGTERM and SIGHUP unwind like Ctrl-C while a cluster is running.

    Their default action kills the process without running fixture teardown,
    which would leave the detached postmaster behind.  KeyboardInterrupt is
    the one exception pytest lets through to end the session and finalise
    fixtures.  Handlers someone else installed are left alone, and so is
    everything when not on the main thread, where signal.signal() refuses.
    """
    replaced = {}
    for sig in (signal.SIGTERM, signal.SIGHUP):
        with contextlib.suppress(ValueError):
            if signal.getsignal(sig) is signal.SIG_DFL:
                replaced[sig] = signal.signal(sig, _interrupt)
    try:
        yield
    finally:
        for sig, previous in replaced.items():
            signal.signal(sig, previous)


@pytest.fixture(scope="session")
def cluster(pg_bindir):
    """The session's throwaway server (ffharness.Cluster)."""
    # Before this session's own cluster exists, so the sweep can only ever
    # meet clusters left by other sessions.
    ffharness.sweep_stale_clusters(pg_bindir)
    with _termination_as_interrupt():
        server = ffharness.Cluster.start(pg_bindir)
        try:
            yield server
        finally:
            server.stop()


def _drop_all(drop, names):
    """Drop every name even if one fails, then report the first failure.

    A database left behind would be picked up by later tests that rely on
    the script's default database list, so one failed drop must not skip
    the rest.
    """
    errors = []
    for name in reversed(names):
        try:
            drop(name)
        except ffharness.DatabaseError as exc:
            errors.append(exc)
    if errors:
        raise errors[0]


# Shared by every factory so generated names are unique within the session;
# the server log is filtered by database name, and it is never truncated.
_serial = itertools.count(1)


@pytest.fixture
def make_role(cluster):
    """Factory: make_role(label="role", *, name=None, login=True,
    superuser=False, password=None) -> role name.  Roles are dropped after
    the test.  A password is checked by the server only for the role named
    ffharness.SCRAM_ROLE."""
    created = []

    def factory(label="role", *, name=None, login=True, superuser=False, password=None):
        name = name or f"ffr{next(_serial)}_{label}"
        cluster.create_role(
            name, login=login, superuser=superuser, password=password
        )
        created.append(name)
        return name

    yield factory
    _drop_all(cluster.drop_role, created)


# make_role is requested here only for teardown order: pytest finalises
# fixtures in reverse, so databases go before the roles that may own tables
# in them, and DROP ROLE does not fail on dependent objects.
@pytest.fixture
def make_db(cluster, make_role):
    """Factory: make_db(label="db", *, name=None) -> ffharness.Database.

    The generated name is unique, lowercase and needs no quoting; pass
    `name` to test a specific one.  Databases are dropped after the test.
    """
    created = []

    def factory(label="db", *, name=None):
        name = name or f"ffd{next(_serial)}_{label}"
        database = cluster.create_database(name)
        created.append(name)
        return database

    yield factory
    _drop_all(cluster.drop_database, created)


@pytest.fixture
def db(make_db):
    """One empty database for the test (ffharness.Database)."""
    return make_db()


@pytest.fixture
def run_ff(cluster):
    """run_ff(*args, env=None, cwd=None, timeout=60) -> CompletedProcess.

    Runs the script against the throwaway cluster with text stdout/stderr.
    """
    return cluster.run_ff


@pytest.fixture
def script_clock(tmp_path):
    """A ffharness.ScriptClock.  Start the script with `env=script_clock.env`
    and call `script_clock.advance(seconds)` to put its deadline in the past
    without waiting for it."""
    return ffharness.ScriptClock(tmp_path / "clock")


@pytest.fixture
def start_ff(cluster):
    """start_ff(*args, env=None, cwd=None) -> Popen, for tests that must act
    while the script is running.  Anything still alive is killed afterwards."""
    started = []

    def launcher(*args, env=None, cwd=None):
        proc = cluster.start_ff(*args, env=env, cwd=cwd)
        started.append(proc)
        return proc

    yield launcher
    for proc in started:
        if proc.poll() is None:
            proc.kill()
        proc.communicate()
