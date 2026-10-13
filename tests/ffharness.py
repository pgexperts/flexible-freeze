# Test-harness library: the throwaway PostgreSQL cluster, the one launcher for
# scripts/flexible_freeze.py, and catalog/log probes for asserting what a run
# really did.  conftest.py wraps these in fixtures; tests import the plain
# helpers (FREEZE_ALL, wait_for, assert_lines_in_order) from here.
#
# Design constraints:
#
# * flexible_freeze.py is a top-to-bottom script that parses arguments and
#   does its whole run at import, rebinds sys.stdout under -l and installs a
#   SIGINT handler.  It therefore cannot be imported into the test process and
#   is always run as a subprocess.
#
# * Run without -H/-p the script connects to whatever libpq finds by default
#   and VACUUM FREEZEs every database there.  Every launch goes through
#   ff_command()/child_env(), which pin host and port to the throwaway cluster
#   and strip every PG* variable, and which raise UnsafeInvocation rather than
#   build a command that could reach another server.
#
# * The cluster listens on a unix socket only, in a private temporary
#   directory, so its port number never competes with anything else on the
#   machine and no other server can be reached through that directory.

from __future__ import annotations

import atexit
import os
import pwd
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import NamedTuple

import psycopg2
from psycopg2 import sql

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "flexible_freeze.py"

# The port a developer's real server is most likely to use.  The harness
# refuses to start a cluster on it or to aim the script at it.
FORBIDDEN_PORT = 5432

# Only names the socket file inside the private directory; nothing listens on
# TCP, so a fixed value cannot collide with another process.
TEST_PORT = 54329

# sockaddr_un.sun_path is 104 bytes on macOS and 108 on Linux; PostgreSQL
# refuses to start if "<dir>/.s.PGSQL.<port>" does not fit.
MAX_SOCKET_PATH = 103

# application_name values, used to tell the script's statements from the
# harness's own in the server log.  SCRIPT_APP is what the script sets.
SCRIPT_APP = "flexible_freeze"
HARNESS_APP = "ff_test_harness"

# Written into the cluster's base directory when the harness creates it.
# Its presence is what identifies a directory as the throwaway cluster's:
# the launcher refuses any socket directory without it, and teardown refuses
# to delete any directory without it.
MARKER = ".ff-test-cluster"

# Freshly created tables are a handful of transactions old, far below the
# script's default --freezeage of ten million.  Passing this makes every
# table whose relfrozenxid is behind the next XID eligible, which is every
# table built by Database.create_table.  A table that was just frozen has age
# zero and is not selected again until another transaction ID is consumed.
FREEZE_ALL = ("--freezeage", "0")

RUN_TIMEOUT = 60.0
FALLBACK_BINDIR = Path("/usr/local/pgsql/bin")

# Marker at the start of every server log line, so that records can be
# recognised without guessing at timestamps.  Fields: pid, application_name,
# database, user.
_LOG_PREFIX = "ffl|%p|%a|%d|%u|"
_LOG_STATEMENT = re.compile(
    r"^ffl\|(?P<pid>\d+)\|(?P<app>[^|]*)\|(?P<db>[^|]*)\|(?P<user>[^|]*)\|"
    r"LOG:  statement: (?P<sql>.*)$"
)

# Spellings argparse accepts for the script's host and port options:
# "-H x", "-Hx", "--host=x", unambiguous long prefixes, and either letter
# clustered behind the script's value-less short flags ("-vH x", "-tp1").
_TARGET_OPTION = re.compile(r"^(-[vt]*[Hp]|--ho|--po)")

# The script pastes -d/-U/-w values into a libpq connection string without
# quoting, so a value such as "x host=elsewhere" would add a keyword that
# overrides the harness's host.  No argument may carry one of these.
_DSN_REDIRECT = re.compile(r"\b(host|hostaddr|port|service|servicefile)\s*=", re.IGNORECASE)

# Directory names that can be pasted into that connection string, and into
# postgresql.conf, without quoting.
_PLAIN_PATH = re.compile(r"[A-Za-z0-9_./+-]+")

_IDENT = r'(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)'
_VACUUM = re.compile(
    r"^VACUUM\s+"
    r"(?:\((?P<paren>[^)]*)\)\s*|(?P<words>(?:(?:FULL|FREEZE|VERBOSE|ANALYZE)\s+)*))"
    rf"(?:(?P<schema>{_IDENT})\s*\.\s*)?(?P<table>{_IDENT})\s*;?\s*$",
    re.IGNORECASE,
)


# What a failed statement or connection raises, for callers that must not
# import the driver themselves.
DatabaseError = psycopg2.Error


class HarnessError(RuntimeError):
    """The harness could not be set up or was used incorrectly."""


class UnsafeInvocation(HarnessError):
    """A command or environment that could reach a server other than the
    throwaway cluster.  Raised before anything is launched."""


class LoggedStatement(NamedTuple):
    pid: int
    app: str
    dbname: str
    user: str
    sql: str


class VacuumStatement(NamedTuple):
    """A logged VACUUM, taken apart so that tests can assert on what was
    vacuumed and with which options without pinning the statement's spelling
    (keyword or parenthesised options, qualified or bare table name)."""

    sql: str
    options: frozenset  # upper-case, e.g. {"FREEZE", "ANALYZE"}
    schema: str
    table: str

    @property
    def target(self):
        """(schema, table), the same key the catalog probes use."""
        return (self.schema, self.table)


def _unquote_identifier(text):
    if text.startswith('"'):
        return text[1:-1].replace('""', '"')
    return text.lower()


def parse_vacuum(statement):
    """Parse one single-table VACUUM statement into a VacuumStatement.

    An unqualified table name is reported in schema "public": sessions in
    the test cluster run with the default search_path.  Note that this reads
    the statement the way the server does, so a qualified name wrongly sent
    inside one pair of quotes parses as a single table name containing a dot.
    """
    match = _VACUUM.match(statement.strip())
    if not match:
        raise HarnessError(f"cannot parse VACUUM statement: {statement!r}")
    if match.group("paren") is not None:
        options = [part.strip() for part in match.group("paren").split(",")]
    else:
        options = match.group("words").split()
    schema = match.group("schema")
    return VacuumStatement(
        statement,
        frozenset(option.upper() for option in options if option),
        _unquote_identifier(schema) if schema else "public",
        _unquote_identifier(match.group("table")),
    )


def wait_for(probe, *, timeout=30.0, interval=0.05, what="condition"):
    """Poll `probe` until it returns something truthy and return that value.

    Needed for the cumulative statistics views, which are updated
    asynchronously: before PostgreSQL 15 a backend's counts travel through the
    statistics collector, and on every version they are flushed when the
    backend exits, slightly after its client has disconnected.
    """
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"timed out after {timeout}s waiting for {what}"
            )
        time.sleep(interval)


def assert_lines_in_order(text, expected):
    """Assert that each expected line occurs in `text`, in the given order.

    Other lines may come between them, so a test pins the lines it is about
    without having to restate the script's whole output.
    """
    lines = text.splitlines()
    position = 0
    for want in expected:
        try:
            position = lines.index(want, position) + 1
        except ValueError:
            raise AssertionError(
                f"line {want!r} is missing or out of order in:\n{text}"
            ) from None


# -- launching the script ---------------------------------------------------


def check_target(host, port):
    """Validate a (socket directory, port) pair and return it normalised.

    The host must be a directory the harness created for its own cluster,
    recognised by the marker file.  That rules out a host name, an empty
    value (libpq would fall back to its default), a comma-separated host
    list, and the socket directory of any other server.
    """
    host = str(host) if host else ""
    if (
        not os.path.isabs(host)
        or not _PLAIN_PATH.fullmatch(host)
        or not os.path.isfile(os.path.join(host, MARKER))
    ):
        raise UnsafeInvocation(
            f"refusing to target host {host!r}: it is not the throwaway "
            "cluster's socket directory"
        )
    try:
        port = int(port)
    except (TypeError, ValueError):
        raise UnsafeInvocation(f"refusing to target port {port!r}") from None
    if port <= 0 or port == FORBIDDEN_PORT:
        raise UnsafeInvocation(f"refusing to target port {port!r}")
    return str(host), port


def child_env(extra=None):
    """Environment for a child process: the caller's, minus every PG* variable.

    libpq reads PGHOST, PGPORT, PGSERVICE, PGOPTIONS and friends, and the
    server programs read PGDATA and PGPORT; none of them may steer a child.
    `extra` is merged on top and may not reintroduce such a variable.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    for key in extra or {}:
        if key.startswith("PG"):
            raise UnsafeInvocation(
                f"refusing to pass {key} to a child process"
            )
    env.update(extra or {})
    return env


def ff_command(host, port, args):
    """Build the argv for one run of the script against (host, port).

    --pause 0 goes first so that a test can override it.  -H and -p go last:
    argparse keeps the last occurrence of an option, so the harness's values
    win over anything earlier.  Arguments that name a host or port are also
    rejected outright, so a test that tries to redirect the script fails
    loudly instead of being silently overridden; so are values that would
    smuggle a host or port keyword into the script's connection string,
    which the trailing -H cannot override.
    """
    host, port = check_target(host, port)
    for arg in args:
        if not isinstance(arg, str):
            raise TypeError(f"script arguments must be strings, got {arg!r}")
        if _TARGET_OPTION.match(arg) or _DSN_REDIRECT.search(arg):
            raise UnsafeInvocation(
                f"argument {arg!r} would set the script's host or port; the "
                "harness always supplies them"
            )
    return [
        sys.executable, str(SCRIPT), "--pause", "0", *args,
        "-H", host, "-p", str(port),
    ]


def start_ff(host, port, *args, env=None, cwd=None):
    """Start the script and return the Popen object.

    This is the only place in the test suite that launches the script.
    Output is captured as text on proc.stdout / proc.stderr.
    """
    return subprocess.Popen(
        ff_command(host, port, args),
        env=child_env(env),
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def run_ff(host, port, *args, env=None, cwd=None, timeout=RUN_TIMEOUT):
    """Run the script to completion and return a CompletedProcess."""
    proc = start_ff(host, port, *args, env=env, cwd=cwd)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
        raise AssertionError(
            f"flexible_freeze.py did not finish within {timeout}s\nstdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        ) from None
    return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)


# -- locating PostgreSQL ----------------------------------------------------


def _has_server_binaries(bindir):
    return all(
        os.access(str(Path(bindir) / name), os.X_OK)
        for name in ("initdb", "pg_ctl")
    )


def find_pg_bindir():
    """Return the directory holding initdb and pg_ctl, or None.

    Order: $PG_BINDIR, PATH, `pg_config --bindir`, /usr/local/pgsql/bin.
    An explicit PG_BINDIR that does not hold the binaries is an error rather
    than a reason to look elsewhere: CI sets it per matrix entry, and falling
    back (or skipping) there would report success without testing anything.
    """
    explicit = os.environ.get("PG_BINDIR")
    if explicit:
        if not _has_server_binaries(explicit):
            raise HarnessError(
                f"PG_BINDIR={explicit} does not contain initdb and pg_ctl"
            )
        return Path(explicit)

    on_path = shutil.which("initdb")
    if on_path and _has_server_binaries(Path(on_path).parent):
        return Path(on_path).parent

    pg_config = shutil.which("pg_config")
    if pg_config:
        try:
            reported = subprocess.run(
                [pg_config, "--bindir"], env=child_env(), check=True,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            reported = ""
        if reported and _has_server_binaries(reported):
            return Path(reported)

    if _has_server_binaries(FALLBACK_BINDIR):
        return FALLBACK_BINDIR
    return None


def _make_base_dir():
    """Create the cluster's private base directory and mark it as ours.

    The default temporary directory is tried first, then /tmp: the path has
    to be short enough to hold the server's socket, and plain enough to go
    unquoted into the script's connection string.
    """
    for parent in (None, "/tmp"):
        try:
            base = Path(tempfile.mkdtemp(prefix="ffpg", dir=parent))
        except OSError:
            continue
        socket_path = str(base / f".s.PGSQL.{TEST_PORT}")
        if (
            len(os.fsencode(socket_path)) <= MAX_SOCKET_PATH
            and _PLAIN_PATH.fullmatch(str(base))
        ):
            (base / MARKER).write_text("flexible_freeze test cluster\n")
            return base
        # Safe to remove unmarked: mkdtemp created it a moment ago.
        shutil.rmtree(str(base), ignore_errors=True)
    raise HarnessError(
        "no temporary directory with a path short and plain enough for the "
        "test cluster's unix socket"
    )


# -- the throwaway cluster --------------------------------------------------


class Cluster:
    """A PostgreSQL server owned by the test session.

    Always created with Cluster.start(), which makes the base directory
    itself, and destroyed by stop().  All state lives under `basedir`: the
    socket, server log and marker file directly in it, the data directory in
    `basedir/data`.  The constructor accepts only a directory carrying the
    harness's marker, so an instance can never be pointed at, and stop() can
    never delete, a directory the harness did not create.
    """

    def __init__(self, bindir, basedir, port, superuser):
        self.bindir = Path(bindir)
        self.basedir = Path(basedir)
        self.datadir = self.basedir / "data"
        self.sockdir, self.port = check_target(str(self.basedir), port)
        self.logfile = self.basedir / "server.log"
        self.superuser = superuser
        self.server_version = None  # server_version_num, e.g. 180006

    @classmethod
    def start(cls, bindir):
        # The superuser is the OS user as libpq determines it (the password
        # database entry for the effective uid, not $USER), because that is
        # the role the script connects as when it is given no -U.
        superuser = pwd.getpwuid(os.geteuid()).pw_name
        cluster = cls(bindir, _make_base_dir(), TEST_PORT, superuser)
        # Backstop for exits that bypass the fixture's teardown.  pg_ctl
        # detaches the postmaster, so nothing else would stop it; stop() is
        # idempotent, so the normal teardown and this can both run.
        atexit.register(cluster.stop)
        try:
            cluster._initdb()
            cluster._configure()
            cluster._pg_ctl("start", "-w", "-t", "60", "-l", str(cluster.logfile))
            cluster.server_version = int(
                cluster.execute("postgres", "SHOW server_version_num")[0][0]
            )
        except BaseException:
            cluster.stop()
            raise
        return cluster

    def stop(self):
        """Stop the server and delete everything it wrote.

        Immediate mode: nothing in the cluster is worth a checkpoint, and a
        fast shutdown could wait on a session a failed test left behind.
        Does nothing once the base directory is gone.
        """
        if not self.basedir.exists():
            return
        if not (self.basedir / MARKER).is_file():
            raise HarnessError(
                f"refusing to remove {self.basedir}: it does not carry the "
                "test cluster marker"
            )
        if (self.datadir / "postmaster.pid").exists():
            self._pg_ctl("stop", "-m", "immediate", "-w", "-t", "60", check=False)
        shutil.rmtree(str(self.basedir), ignore_errors=True)

    def _initdb(self):
        result = subprocess.run(
            [
                str(self.bindir / "initdb"), "-D", str(self.datadir),
                "-E", "UTF8", "--locale=C", "-A", "trust",
                "-U", self.superuser, "-N",
            ],
            env=child_env(), stdin=subprocess.DEVNULL, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        if result.returncode != 0:
            raise HarnessError(f"initdb failed:\n{result.stdout}")

    def _configure(self):
        settings = {
            # Unix socket only, in the private directory: no TCP listener.
            "listen_addresses": "",
            "unix_socket_directories": self.sockdir,
            "port": self.port,
            # Durability is irrelevant for a cluster deleted at teardown.
            "fsync": "off",
            "synchronous_commit": "off",
            "full_page_writes": "off",
            "jit": "off",
            # Only the script may vacuum or analyze, otherwise the catalog
            # and statistics assertions would race with autovacuum.
            "autovacuum": "off",
            # Small footprint beside whatever else runs on the machine.
            "max_connections": 20,
            "shared_buffers": "32MB",
            # Record every statement so tests can assert on the exact SQL
            # the script sent (see Cluster.statements).
            "log_statement": "all",
            "log_line_prefix": _LOG_PREFIX,
        }
        with open(str(self.datadir / "postgresql.conf"), "a") as conf:
            conf.write("\n# test harness settings\n")
            conf.writelines(f"{name} = '{value}'\n" for name, value in settings.items())

    def _pg_ctl(self, *args, check=True):
        # Output is discarded rather than piped: a postmaster that inherited
        # a pipe would keep it open and block the caller.  Failures are
        # explained from the server log instead.
        result = subprocess.run(
            [str(self.bindir / "pg_ctl"), "-D", str(self.datadir), *args],
            env=child_env(), stdin=subprocess.DEVNULL, check=False,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if check and result.returncode != 0:
            raise HarnessError(
                f"pg_ctl {args[0]} failed (exit {result.returncode}); "
                f"server log:\n{self._log_tail()}"
            )

    def _log_tail(self, lines=40):
        try:
            text = self.logfile.read_text(errors="replace")
        except OSError:
            return "(no server log)"
        return "\n".join(text.splitlines()[-lines:])

    # -- SQL access ---------------------------------------------------------

    def connect(self, dbname="postgres", *, user=None, autocommit=True):
        """Open a connection to the throwaway cluster.

        For tests that need to hold a session open (a lock, an open
        transaction); the caller closes it.  Use execute() otherwise.
        """
        conn = psycopg2.connect(
            host=self.sockdir, port=self.port, dbname=dbname,
            user=user or self.superuser, application_name=HARNESS_APP,
            connect_timeout=30,
        )
        conn.autocommit = autocommit
        return conn

    def execute(self, dbname, query, params=None, *, user=None):
        """Run one statement in its own session; return its rows, if any.

        A session per statement is deliberate: each statement gets its own
        transaction ID, no idle session is left holding back the xmin
        horizon or blocking DROP DATABASE, and the backend's exit flushes
        its table statistics.
        """
        conn = self.connect(dbname, user=user)
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall() if cur.description else None
        finally:
            conn.close()

    def database(self, name):
        """A handle on an existing database (create_database makes one)."""
        return Database(self, name)

    def create_database(self, name):
        self.execute(
            "postgres", sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))
        )
        return Database(self, name)

    def drop_database(self, name):
        # FORCE: a script run that just exited may not have had its backend
        # reaped yet, and a failed test may have left a session behind.
        self.execute(
            "postgres",
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name)
            ),
        )

    def user_databases(self):
        """Names of the databases the script's default list would consider."""
        rows = self.execute(
            "postgres",
            "SELECT datname FROM pg_database WHERE datname NOT IN "
            "('postgres', 'template0', 'template1') ORDER BY datname",
        )
        return [row[0] for row in rows]

    def create_role(self, name, *, login=True, superuser=False):
        self.execute(
            "postgres",
            sql.SQL("CREATE ROLE {} {} {}").format(
                sql.Identifier(name),
                sql.SQL("LOGIN" if login else "NOLOGIN"),
                sql.SQL("SUPERUSER" if superuser else "NOSUPERUSER"),
            ),
        )
        return name

    def drop_role(self, name):
        self.execute(
            "postgres", sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name))
        )

    # -- the server log -----------------------------------------------------

    def log_offset(self):
        """Current size of the server log, for statements(since=...)."""
        return self.logfile.stat().st_size

    def statements(self, *, dbname=None, app=SCRIPT_APP, user=None, since=0):
        """Statements the server logged, in order, filtered by session.

        By default only the script's (application_name flexible_freeze).
        A backend writes its log line before executing the statement, so
        everything a finished script run sent is already here: unlike the
        statistics views, this needs no waiting.  `since` is a log_offset()
        taken earlier, for databases whose name is not unique to one test.
        """
        with open(str(self.logfile), "rb") as log:
            log.seek(since)
            text = log.read().decode("utf-8", errors="replace")

        records = []
        current = None
        for line in text.splitlines():
            match = _LOG_STATEMENT.match(line)
            if match:
                current = match.groupdict()
                records.append(current)
            elif line.startswith("\t") and current is not None:
                # The server indents the later lines of a multi-line message.
                current["sql"] += "\n" + line[1:]
            else:
                current = None

        return [
            LoggedStatement(
                int(r["pid"]), r["app"], r["db"], r["user"], r["sql"]
            )
            for r in records
            if (dbname is None or r["db"] == dbname)
            and (app is None or r["app"] == app)
            and (user is None or r["user"] == user)
        ]

    # -- the script ---------------------------------------------------------

    def start_ff(self, *args, env=None, cwd=None):
        """Start the script against this cluster; returns a Popen."""
        return start_ff(self.sockdir, self.port, *args, env=env, cwd=cwd)

    def run_ff(self, *args, env=None, cwd=None, timeout=RUN_TIMEOUT):
        """Run the script against this cluster; returns a CompletedProcess."""
        return run_ff(
            self.sockdir, self.port, *args, env=env, cwd=cwd, timeout=timeout
        )


class Database:
    """One database in the cluster, with object builders and catalog probes.

    Tables are addressed as (name, schema) and looked up through pg_class and
    pg_namespace by exact name, never through a regclass cast, so the probes
    work for schema-qualified and mixed-case names without quoting rules.
    Sets and dicts of tables are keyed by (schema, name) tuples.
    """

    def __init__(self, cluster, name):
        self.cluster = cluster
        self.name = name

    def __repr__(self):
        return f"Database({self.name!r})"

    def execute(self, query, params=None, *, user=None):
        return self.cluster.execute(self.name, query, params, user=user)

    def connect(self, *, user=None, autocommit=True):
        return self.cluster.connect(self.name, user=user, autocommit=autocommit)

    # -- building objects ---------------------------------------------------

    def create_schema(self, name):
        self.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        return name

    def create_table(self, name, *, schema="public", rows=100, pad=0, owner=None):
        """Create a table (id integer, pad text) and return its (schema, name).

        `pad` is the length of the filler text per row, for tables that need
        a particular size: 12,000 rows with pad=100 is about 1.7 MB.  The
        rows are inserted in a later transaction than the CREATE, so the
        table's relfrozenxid is strictly older than the next XID and a
        freezing vacuum is certain to move it.
        """
        ident = sql.Identifier(schema, name)
        self.execute(
            sql.SQL("CREATE TABLE {} (id integer, pad text)").format(ident)
        )
        if rows:
            self.execute(
                sql.SQL(
                    "INSERT INTO {} SELECT g, repeat('x', %s) "
                    "FROM generate_series(1, %s) AS g"
                ).format(ident),
                (pad, rows),
            )
        if owner:
            self.execute(
                sql.SQL("ALTER TABLE {} OWNER TO {}").format(
                    ident, sql.Identifier(owner)
                )
            )
        return (schema, name)

    def make_dead_tuples(self, name, *, schema="public"):
        """Delete every other row and wait until the statistics show it.

        --no-freeze mode selects tables from pg_stat_user_tables (more than
        100 dead tuples and more than 5% dead), so the table needs well over
        200 rows, and the counts must have reached the statistics system
        before the script runs.  Both counters are awaited: the INSERT and
        the DELETE were reported by different backends, and a dead count
        seen without the matching live count would give the wrong ratio.
        """
        ident = sql.Identifier(schema, name)
        deleted = self.execute(
            sql.SQL(
                "WITH gone AS (DELETE FROM {} WHERE id % 2 = 0 RETURNING 1) "
                "SELECT count(*) FROM gone"
            ).format(ident)
        )[0][0]
        live = self.execute(sql.SQL("SELECT count(*) FROM {}").format(ident))[0][0]

        def reported():
            stats = self.table_stats(name, schema=schema)
            return stats["n_dead_tup"] >= deleted and stats["n_live_tup"] == live

        wait_for(reported, what=f"dead-tuple statistics for {schema}.{name}")
        return deleted

    # -- probes: transactional catalog (immediate) ---------------------------

    def frozenxids(self):
        """Map (schema, table) -> relfrozenxid for every user table.

        Take this before a run and pass it to frozen_since() afterwards.
        TOAST tables are left out: they follow their parent and would only
        add noise to set comparisons.
        """
        rows = self.execute(
            "SELECT n.nspname, c.relname, c.relfrozenxid::text::bigint "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r', 'm') "
            "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
            "AND n.nspname NOT LIKE 'pg\\_toast%'"
        )
        return {(schema, table): xid for schema, table, xid in rows}

    def frozen_since(self, before):
        """The set of (schema, table) whose relfrozenxid advanced.

        This is the proof that VACUUM FREEZE ran on a table, and that it did
        not run on the others: relfrozenxid is ordinary catalog state,
        visible as soon as the vacuum returns.  A plain VACUUM may or may
        not move it depending on the server version, so use the statistics
        probes for --no-freeze runs.
        """
        after = self.frozenxids()
        return {key for key, xid in after.items() if xid > before.get(key, xid)}

    def relfrozenxid(self, name, *, schema="public"):
        return self.frozenxids()[(schema, name)]

    def is_analyzed(self, name, *, schema="public"):
        """Whether ANALYZE has stored column statistics for the table.

        pg_statistic is written only by ANALYZE and autovacuum is off, so
        this separates VACUUM ANALYZE from VACUUM without any waiting.  It
        needs a table with rows: ANALYZE stores nothing for an empty one.
        """
        return self.execute(
            "SELECT count(*) FROM pg_stats "
            "WHERE schemaname = %s AND tablename = %s",
            (schema, name),
        )[0][0] > 0

    # -- probes: cumulative statistics (asynchronous) ------------------------

    def table_stats(self, name, *, schema="public"):
        """The table's row in pg_stat_all_tables as a dict.

        These counters lag the event that caused them; assert on them
        through wait_for() / wait_for_vacuum_count(), never directly after
        a run.
        """
        columns = (
            "n_live_tup", "n_dead_tup", "vacuum_count", "analyze_count",
            "last_vacuum", "last_analyze", "autovacuum_count",
        )
        rows = self.execute(
            f"SELECT {', '.join(columns)} FROM pg_stat_all_tables "
            "WHERE schemaname = %s AND relname = %s",
            (schema, name),
        )
        if not rows:
            raise HarnessError(f"no such table: {schema}.{name}")
        return dict(zip(columns, rows[0]))

    def vacuum_count(self, name, *, schema="public"):
        return self.table_stats(name, schema=schema)["vacuum_count"]

    def wait_for_vacuum_count(self, name, count=1, *, schema="public", timeout=30.0):
        """Wait until the table has been manually vacuumed `count` times."""
        return wait_for(
            lambda: self.vacuum_count(name, schema=schema) >= count,
            timeout=timeout,
            what=f"vacuum_count >= {count} for {schema}.{name}",
        )

    # -- probes: server log (immediate) --------------------------------------

    def script_statements(self, *, user=None, since=0):
        """Every statement the script sent to this database, in order."""
        return [
            record.sql
            for record in self.cluster.statements(
                dbname=self.name, user=user, since=since
            )
        ]

    def script_vacuums(self, *, since=0):
        """The VACUUMs the script sent to this database, in order, parsed.

        Returns VacuumStatement tuples: compare `.target` with the keys from
        frozen_since(), `.options` with a set such as {"FREEZE", "ANALYZE"},
        and use `.sql` where the literal text matters.
        """
        return [
            parse_vacuum(statement)
            for statement in self.script_statements(since=since)
            if statement.upper().startswith("VACUUM")
        ]
