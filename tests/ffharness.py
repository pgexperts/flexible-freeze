# Test-harness library: the throwaway PostgreSQL cluster, the one launcher for
# scripts/flexible_freeze.py, and catalog/log probes for asserting what a run
# really did.  conftest.py wraps these in fixtures; tests import the plain
# helpers (FREEZE_ALL, wait_for, assert_lines_in_order) from here.
#
# Design constraints:
#
# * flexible_freeze.py is a top-to-bottom script that parses arguments and
#   does its whole run at import, rebinds sys.stdout under -l and installs
#   signal handlers and a process-wide driver wait callback.  It therefore
#   cannot be imported into the test process and is always run as a
#   subprocess.
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
#
# * Because the script only ever runs in a child process, its line coverage
#   is recorded by each child for itself, on request: see coverage_env().

from __future__ import annotations

import atexit
import os
import pwd
import re
import shutil
import socket
import stat
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

# Name prefix of every base directory the harness creates, which is where
# sweep_stale_clusters() looks for clusters a killed session left running.
BASE_PREFIX = "ffpg"

# A running cluster whose marker is older than this is taken to be abandoned.
# The whole suite runs in well under a minute, so an hour leaves any session
# that is merely slow, or running beside this one, alone.  The cost is that a
# session kept open for longer (stopped in a debugger, say) looks abandoned
# to the next session that starts.
STALE_AFTER = 60 * 60

# The one role that pg_hba.conf asks a password of; every other connection is
# trusted.  Tests that need the server to check a -w value create this role
# with the password under test and run the script as it.  The name goes into
# pg_hba.conf unquoted.
SCRAM_ROLE = "ff_scram_role"

# Freshly created tables are a handful of transactions old, far below the
# script's default --freezeage of ten million.  Passing this makes every
# table whose relfrozenxid is behind the next XID eligible, which is every
# table built by Database.create_table.  A table that was just frozen has age
# zero and is not selected again until another transaction ID is consumed.
FREEZE_ALL = ("--freezeage", "0")

RUN_TIMEOUT = 60.0

# libpq variables a test may hand to the script.  None of them can change
# which server is reached; PGCONNECT_TIMEOUT has no command-line equivalent.
_HARMLESS_LIBPQ_VARIABLES = frozenset({"PGCONNECT_TIMEOUT"})
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

# Defence in depth.  The script hands its -d/-U/-w values to the driver as
# separate connection parameters, so a value such as "x host=elsewhere" is
# inert today.  Should the script ever go back to formatting a connection
# string, such a value would add a keyword that overrides the harness's host,
# and nothing else here would notice.  No argument may carry one of these;
# tests that need such text in a database name create the database through
# the fixtures instead of passing the name as an argument.
_DSN_REDIRECT = re.compile(r"\b(host|hostaddr|port|service|servicefile)\s*=", re.IGNORECASE)

# Directory names that can be written into postgresql.conf without escaping
# and that libpq can only read as one socket directory (a comma would make
# the -H value a list of hosts).
_PLAIN_PATH = re.compile(r"[A-Za-z0-9_./+-]+")

# What psycopg2 itself sends when a session starts, before the caller's first
# statement: it switches DateStyle to ISO on a server (or database, or role)
# configured with any other style.  The statement carries the session's
# application_name like any other, so only its text tells it apart.  The list
# is deliberately closed: an unrecognised statement stays in the log and
# fails a test rather than being dropped as "probably the driver's".
_DRIVER_SETUP = re.compile(r"SET datestyle TO 'ISO'", re.IGNORECASE)

_IDENT = r'(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*)'
# One entry of a parenthesised option list: a name, alone or followed by one
# unquoted word as its value.
_VACUUM_OPTION = re.compile(r"(?P<name>[A-Za-z_]+)(?:\s+(?P<value>[A-Za-z0-9_]+))?")
_VACUUM = re.compile(
    r"^VACUUM\s+"
    r"(?:\((?P<paren>[^)]*)\)\s*|(?P<words>(?:(?:FULL|FREEZE|VERBOSE|ANALYZE)\s+)*))"
    rf"(?:(?P<schema>{_IDENT})\s*\.\s*)?(?P<table>{_IDENT})\s*"
    r"(?:/\*(?:(?!\*/|/\*).)*\*/\s*)?;?\s*$",
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
    # Names in upper case; an option given a value is "NAME value", with the
    # value in lower case: {"FREEZE", "ANALYZE", "TRUNCATE false"}.
    options: frozenset
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


def _vacuum_option(text):
    match = _VACUUM_OPTION.fullmatch(text)
    if not match:
        raise HarnessError(f"cannot parse VACUUM option: {text!r}")
    name = match.group("name").upper()
    value = match.group("value")
    return name if value is None else f"{name} {value.lower()}"


def parse_vacuum(statement):
    """Parse one single-table VACUUM statement into a VacuumStatement.

    This is an approximate reading of the server's grammar, and covers no
    more than this: one table, with its options either as keywords before
    the name (FULL, FREEZE, VERBOSE, ANALYZE) or as a parenthesised list in
    which each entry is an option name, alone or followed by one unquoted
    word as its value (`TRUNCATE false`, `PARALLEL 2`).  An empty entry, or
    an empty list, is passed over, although the server refuses both.  After
    the table there may be one /* ... */ comment on the same line, which is
    what the script's --comment adds; it is accepted and not reported, and
    `.sql` still has it.  Anything else is an error: a quoted value, a value
    of several words, a column list, a second table, a comment anywhere
    else, a second one, or one with a /* inside it (the server nests
    comments; this does not).

    An entry with a value is reported as "NAME value", the value in lower
    case as the server reads a boolean word.  The meaning of a value is not
    worked out: `TRUNCATE`, `TRUNCATE true` and `TRUNCATE on` ask the server
    for the same thing and are three different entries here, so a test
    compares with the spelling the script sends.  Nor is a name checked
    against the options the server has.

    The script's own statements always name the schema: its sessions run
    with an empty search_path, so the regclass text it sends verbatim as the
    target is schema-qualified and already quoted by the server.  An
    unqualified name, as in a statement written by hand, is reported in
    schema "public".  Identifiers are read the way the server reads them, so
    a qualified name inside a single pair of quotes is one table name
    containing a dot, not a schema and a table.
    """
    match = _VACUUM.match(statement.strip())
    if not match:
        raise HarnessError(f"cannot parse VACUUM statement: {statement!r}")
    if match.group("paren") is not None:
        parts = [part.strip() for part in match.group("paren").split(",")]
        options = [_vacuum_option(part) for part in parts if part]
    else:
        options = [word.upper() for word in match.group("words").split()]
    schema = match.group("schema")
    return VacuumStatement(
        statement,
        frozenset(options),
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
    `extra` is merged on top and may not reintroduce such a variable, apart
    from the few that cannot redirect a connection
    (_HARMLESS_LIBPQ_VARIABLES).
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    for key in extra or {}:
        if key.startswith("PG") and key not in _HARMLESS_LIBPQ_VARIABLES:
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
    loudly instead of being silently overridden.  So are values that read as
    a host or port connection keyword: the script passes each value to the
    driver as a parameter of its own, which makes them harmless, but the
    trailing -H could not override one if the script ever built a connection
    string out of them (see _DSN_REDIRECT).
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


def coverage_env():
    """Environment that lets a script run record its own line coverage.

    Coverage measured in the test process says nothing about the script,
    which runs in a child.  coverage.py covers that case itself: from
    release 7.13 it installs a .pth file that starts measurement in every
    Python process that finds COVERAGE_PROCESS_START set, and with
    parallel = true in the configuration that variable names, each process
    writes a data file of its own, to be merged afterwards by
    `coverage combine`.  So the script is still launched unmodified and by
    the same argv; no hook of the harness's own is involved.

    What is left to do here is to make the two paths independent of the
    child's working directory, which a test may choose: a relative
    configuration path would not be found from there, and the data files
    would be scattered over temporary directories.  The data file defaults
    to the repository root, where `coverage combine` looks for it.

    Returns {} unless the caller set COVERAGE_PROCESS_START, so an ordinary
    run starts the script exactly as before, whether or not coverage is
    installed.  A value that does not name a file is an error here, once:
    otherwise every script run would carry coverage's complaint on its
    stderr and the tests would fail one by one on that.
    """
    config = os.environ.get("COVERAGE_PROCESS_START")
    if not config:
        return {}
    if not os.path.isfile(config):
        raise HarnessError(f"COVERAGE_PROCESS_START={config} is not a file")
    return {
        "COVERAGE_PROCESS_START": os.path.abspath(config),
        "COVERAGE_FILE": os.path.abspath(
            os.environ.get("COVERAGE_FILE") or REPO_ROOT / ".coverage"
        ),
    }


def start_ff(host, port, *args, env=None, cwd=None, merge_streams=False):
    """Start the script and return the Popen object.

    This is the only place in the test suite that launches the script.
    Output is captured as text on proc.stdout / proc.stderr.

    The script writes its failure reports to stderr and everything else to
    stdout, and two pipes cannot say in what order lines on different
    streams were written.  `merge_streams` gives the script one pipe for
    both, as `2>&1` does, for the tests of that order; proc.stderr is then
    None and nothing says which stream a line came from.
    """
    return subprocess.Popen(
        ff_command(host, port, args),
        env=child_env({**coverage_env(), **(env or {})}),
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT if merge_streams else subprocess.PIPE,
        text=True,
    )


def run_ff(host, port, *args, env=None, cwd=None, timeout=RUN_TIMEOUT,
           merge_streams=False):
    """Run the script to completion and return a CompletedProcess."""
    proc = start_ff(host, port, *args, env=env, cwd=cwd, merge_streams=merge_streams)
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


# -- a clock the test can move ----------------------------------------------

# The name is what makes it run: Python imports a module called sitecustomize
# from its path at startup, before the first line of the script.
_CLOCK_MODULE = "sitecustomize.py"
_CLOCK_OFFSET_VARIABLE = "FF_TEST_CLOCK_OFFSET_FILE"
_CLOCK_SOURCE = f'''\
# Written by the flexible_freeze test harness (ffharness.ScriptClock).
# Adds the number of seconds held in a file to time.time(), so that a test
# can move the clock of the script it is running, and waits in time.time()
# for as long as a second file exists, so that a test can stop the script
# at the point where it next reads the clock.
import os
import time

_real_time = time.time
_real_sleep = time.sleep
_offset_file = os.environ["{_CLOCK_OFFSET_VARIABLE}"]
_hold_file = _offset_file + ".hold"
_held_file = _offset_file + ".held"


def _shifted_time():
    while os.path.exists(_hold_file):
        open(_held_file, "w").close()
        _real_sleep(0.01)
    try:
        with open(_offset_file) as source:
            return _real_time() + float(source.read())
    except (OSError, ValueError):
        return _real_time()


time.time = _shifted_time
'''


class ScriptClock:
    """Moves the clock of a running script forward, on the test's command.

    The script counts its time window in whole minutes and --enforce-time
    adds a 30-second grace period, so nothing that is meant to happen "once
    the deadline has passed" can be reached in real time by a test that is
    to finish in seconds.  Started with `env=clock.env`, the script reads
    time.time() through a shim that adds an offset kept in a file;
    advance() rewrites that file, and the script sees the jump on its next
    reading of the clock.  The server's clock is not affected, so a
    statement_timeout still runs in real time.

    The script itself stays unmodified and is launched by the same argv; the
    shim is found through PYTHONPATH, which is why it shadows any
    sitecustomize module the interpreter would otherwise load.  Only
    time.time() moves: sleeps and timestamps are real.

    hold() stops the script inside its next reading of the clock until
    release().  The script reads the clock before each table, after the
    table list has been fetched and before anything is sent for the table,
    which makes that the one place where a test can keep it idle between two
    statements; wait_until_held() returns once it is there.  Its signal
    handlers still run while it is held.
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / _CLOCK_MODULE).write_text(_CLOCK_SOURCE)
        self._offset_file = self.directory / "offset"
        # Names fixed by the shim: the offset file's, with a suffix added.
        self._hold_file = self.directory / "offset.hold"
        self._held_file = self.directory / "offset.held"
        self.advance(0)

    @property
    def env(self):
        """Environment for start_ff / run_ff that puts the shim in place."""
        inherited = os.environ.get("PYTHONPATH")
        path = [str(self.directory)] + ([inherited] if inherited else [])
        return {
            "PYTHONPATH": os.pathsep.join(path),
            _CLOCK_OFFSET_VARIABLE: str(self._offset_file),
        }

    def advance(self, seconds):
        """Make the script's clock read `seconds` ahead of the real one."""
        # Renamed into place so the script never reads a half-written file,
        # which it would take for an offset of zero: a jump backwards.
        scratch = self._offset_file.with_suffix(".new")
        scratch.write_text(repr(float(seconds)))
        os.replace(str(scratch), str(self._offset_file))

    def hold(self):
        """Stop the script at its next reading of the clock."""
        self._hold_file.write_text("")

    def wait_until_held(self, timeout=30.0):
        wait_for(
            self._held_file.exists, timeout=timeout,
            what="the script to stop at its next reading of the clock",
        )

    def release(self):
        """Let a held script go on."""
        self._hold_file.unlink()


# -- which files the script left open ------------------------------------------

_OPEN_FILES_VARIABLE = "FF_TEST_OPEN_FILES_REPORT"
_OPEN_FILES_SOURCE = f'''\
# Written by the flexible_freeze test harness (ffharness.OpenFilesProbe).
# As the last thing the process does, lists the text files it still has open.
import atexit
import gc
import io
import os


def _report_open_files():
    names = sorted(
        str(stream.name) for stream in gc.get_objects()
        if isinstance(stream, io.TextIOWrapper) and not stream.closed
    )
    with open(os.environ["{_OPEN_FILES_VARIABLE}"], "w") as report:
        report.write("\\n".join(names))


atexit.register(_report_open_files)
'''


class OpenFilesProbe:
    """Tells a test which text files the script had not closed when it ended.

    Whether a process closed a file before exiting cannot be seen from
    outside it: the operating system closes whatever is left, and the file's
    content is the same either way, because the script flushes every line.
    So the question is asked inside the process, by a shim like ScriptClock's
    (a sitecustomize module found through PYTHONPATH; the script stays
    unmodified).  The shim registers an exit handler before the script's
    first line has run.  Exit handlers run in the reverse of the order they
    were registered in, so the shim's is the last, after anything the script
    does on its way out, on whichever of its exits it leaves by.

    A script that is killed runs no exit handler, and files_left_open() then
    fails for want of a report.  Not to be combined with ScriptClock: only
    one sitecustomize module is loaded.
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        (self.directory / _CLOCK_MODULE).write_text(_OPEN_FILES_SOURCE)
        self._report = self.directory / "open-files"

    @property
    def env(self):
        """Environment for start_ff / run_ff that puts the shim in place."""
        inherited = os.environ.get("PYTHONPATH")
        path = [str(self.directory)] + ([inherited] if inherited else [])
        return {
            "PYTHONPATH": os.pathsep.join(path),
            _OPEN_FILES_VARIABLE: str(self._report),
        }

    def files_left_open(self):
        """Names of the text files still open at exit, as given to open().

        The standard streams are among them, under names like <stdout>.
        """
        if not self._report.exists():
            raise AssertionError("the script ended without running its exit handlers")
        return self._report.read_text().splitlines()


# -- a server that never answers ---------------------------------------------


class SilentServer:
    """A listening socket that accepts connections and never says a word.

    To the script this is a server that can be reached and does not respond:
    its connection attempt gets as far as sending the startup packet and then
    waits, for as long as the test likes.  The real cluster cannot be made to
    do that.

    The socket is put in the throwaway cluster's own socket directory, under
    a port number of its own, so the launcher accepts it as a target exactly
    as it accepts the cluster (check_target) and it still cannot lead to any
    other server.  Start the script against it with start_ff(..., port=...).
    """

    def __init__(self, sockdir, port):
        self.port = port
        self._path = Path(sockdir) / f".s.PGSQL.{port}"
        self._clients = []
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(str(self._path))
        self._listener.listen(5)

    def wait_for_client(self, timeout=30.0):
        """Return once a client has connected; it is then left waiting."""
        self._listener.settimeout(timeout)
        try:
            client, _ = self._listener.accept()
        # socket.timeout before Python 3.10, where it is not yet the same class
        # and would simply propagate.
        except TimeoutError:
            raise AssertionError(
                f"timed out after {timeout}s waiting for a connection attempt"
            ) from None
        # Kept open: closing it would end the client's wait with an error.
        self._clients.append(client)

    def close(self):
        for sock in [*self._clients, self._listener]:
            sock.close()
        self._path.unlink()


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


def _base_parents():
    """The directories a cluster's base directory may be created in."""
    return (tempfile.gettempdir(), "/tmp")


def _make_base_dir():
    """Create the cluster's private base directory and mark it as ours.

    The default temporary directory is tried first, then /tmp: the path has
    to be short enough to hold the server's socket, and plain enough for
    check_target() to accept it as the script's -H value (see _PLAIN_PATH).
    """
    for parent in _base_parents():
        try:
            base = Path(tempfile.mkdtemp(prefix=BASE_PREFIX, dir=parent))
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
            # The pg_hba.conf rule below can only verify a password stored
            # as a SCRAM verifier.
            "password_encryption": "scram-sha-256",
        }
        with open(str(self.datadir / "postgresql.conf"), "a") as conf:
            conf.write("\n# test harness settings\n")
            conf.writelines(f"{name} = '{value}'\n" for name, value in settings.items())

        # One role must present a password, so that a test can tell whether
        # the script's -w value reached the server intact; initdb's trust
        # rules stay in force for everyone else, the harness included.  The
        # first matching rule wins, so this one has to come before them.
        hba = self.datadir / "pg_hba.conf"
        hba.write_text(
            f"local all {SCRAM_ROLE} scram-sha-256\n" + hba.read_text()
        )

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
        """Names of the databases tests have made: all but postgres and the
        two built-in templates.  (The script's default list also takes in
        postgres, and goes by datallowconn and datistemplate.)"""
        rows = self.execute(
            "postgres",
            "SELECT datname FROM pg_database WHERE datname NOT IN "
            "('postgres', 'template0', 'template1') ORDER BY datname",
        )
        return [row[0] for row in rows]

    def create_role(self, name, *, login=True, superuser=False, password=None):
        """Create a role.  A password is only ever asked of SCRAM_ROLE (see
        _configure); for any other role it is stored and never checked."""
        self.execute(
            "postgres",
            sql.SQL("CREATE ROLE {} {} {} PASSWORD {}").format(
                sql.Identifier(name),
                sql.SQL("LOGIN" if login else "NOLOGIN"),
                sql.SQL("SUPERUSER" if superuser else "NOSUPERUSER"),
                # Literal(None) renders as NULL, which is "no password".
                sql.Literal(password),
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

    def statements(
        self, *, dbname=None, app=SCRIPT_APP, user=None, since=0, driver_setup=False
    ):
        """Statements the server logged, in order, filtered by session.

        By default only the script's (application_name flexible_freeze).
        A backend writes its log line before executing the statement, so
        everything a finished script run sent is already here: unlike the
        statistics views, this needs no waiting.  `since` is a log_offset()
        taken earlier, for databases whose name is not unique to one test.

        Statements the driver sends on its own account when a session starts
        (see _DRIVER_SETUP) are left out unless `driver_setup` is true.
        Whether there are any depends on how the server is configured, not
        on the script, so with them left in "the first statement of the
        session" would not reliably be the script's first.
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
            and (driver_setup or not _DRIVER_SETUP.fullmatch(r["sql"]))
        ]

    # -- the script ---------------------------------------------------------

    def start_ff(self, *args, env=None, cwd=None, port=None):
        """Start the script against this cluster; returns a Popen.

        `port` is for a SilentServer in this cluster's socket directory.
        """
        return start_ff(self.sockdir, port or self.port, *args, env=env, cwd=cwd)

    def run_ff(self, *args, env=None, cwd=None, timeout=RUN_TIMEOUT, port=None,
               merge_streams=False):
        """Run the script against this cluster; returns a CompletedProcess.

        `merge_streams`: see the module-level start_ff.
        """
        return run_ff(
            self.sockdir, port or self.port, *args, env=env, cwd=cwd, timeout=timeout,
            merge_streams=merge_streams,
        )

    def silent_server(self):
        """A SilentServer beside this cluster's socket; the caller closes it."""
        return SilentServer(self.sockdir, self.port + 1)

    # -- recognising an abandoned cluster ------------------------------------

    def _postmaster_pid(self):
        """The pid recorded in postmaster.pid if that process exists, else None.

        A process existing is not proof that it is this cluster's postmaster:
        the file outlives a reboot or a crash, and the number may since have
        been given to something unrelated.  _is_serving() settles that.
        """
        try:
            pid = int((self.datadir / "postmaster.pid").read_text().split()[0])
        except (OSError, ValueError, IndexError):
            return None
        # Zero and negative numbers would address process groups, and a
        # negative one is how a standalone backend marks the file.
        if pid <= 0:
            return None
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, OverflowError):
            # No such process, or a number too large to be a process ID.
            return None
        except PermissionError:
            # Exists, but belongs to another user: not ours to stop.
            return None
        return pid

    def _is_serving(self):
        """Whether a server answers on this directory's socket and reports
        this data directory.

        That identifies the cluster's own postmaster only where nobody else
        can put a socket into the directory, which is what the sweep's
        ownership check on the directory establishes before it gets here.
        """
        try:
            reported = self.execute("postgres", "SHOW data_directory")[0][0]
        except psycopg2.Error:
            return False
        return os.path.realpath(reported) == os.path.realpath(str(self.datadir))

    def _is_abandoned(self, max_age):
        """Whether this is a cluster left running by a session long gone.

        The data directory and the pid file must be the current user's own
        entries before the pid is read: that number is what `pg_ctl stop`
        will signal.  See sweep_stale_clusters() for the whole list.
        """
        marker = _own_entry(self.basedir / MARKER, stat.S_ISREG)
        return (
            marker is not None
            and time.time() - marker.st_mtime > max_age
            and _own_entry(self.datadir, stat.S_ISDIR) is not None
            and _own_entry(self.datadir / "postmaster.pid", stat.S_ISREG) is not None
            and self._postmaster_pid() is not None
            and self._is_serving()
        )


def _own_entry(path, is_kind):
    """lstat() of `path` if the entry is of the wanted kind and belongs to
    the effective user, else None.

    lstat, so that a symbolic link is judged as a link (and fails the kind
    test) instead of passing on the strength of whatever it points to.
    """
    try:
        info = os.lstat(path)
    except OSError:
        return None
    if not is_kind(info.st_mode) or info.st_uid != os.geteuid():
        return None
    return info


def sweep_stale_clusters(bindir, *, parents=None, max_age=STALE_AFTER):
    """Stop and remove clusters that an earlier, killed session left running.

    A SIGKILL of pytest runs neither the fixture teardown nor the atexit
    backstop, and pg_ctl detached the postmaster, so the server keeps
    running and its directory stays.  Called once at session start, before
    this session's own cluster exists.  Returns the directories removed.

    A directory is touched only if all of these hold:

    * it is named BASE_PREFIX* directly under one of the directories
      _make_base_dir() uses, and is itself a real directory (not a symbolic
      link) that belongs to the current user and that neither group nor
      others can write to.  One of those parents is /tmp, where any local
      user can create a directory of that name and fill it as they like.
      The marker cannot settle whose directory it is: another user can
      hard-link one of the current user's files under the marker's name,
      and that file is then, truthfully, the current user's.  A directory
      cannot be hard-linked or given away, so one that is the current
      user's and closed to everyone else can only hold what that user put
      there, and stays that way while the sweep works on it;
    * it carries the harness's marker file, a regular file of the current
      user's.  The candidate is wrapped in a Cluster, whose constructor and
      stop() both refuse a directory without the marker, so the sweep cannot
      stop or delete anything those guards would not;
    * the marker is older than `max_age` (see STALE_AFTER), which is what
      keeps the sweep away from sessions running at the same time;
    * the data directory and postmaster.pid are likewise the current user's
      own directory and regular file, checked before the pid is read;
    * the pid in postmaster.pid is alive, and a server answering on the
      directory's own socket reports that data directory.  The second half
      matters because the stop is `pg_ctl stop`, which signals whatever
      process has the recorded pid: after a reboot that can be an unrelated
      process of the same user.  Given the first point, a socket in the
      directory was put there by a process of the current user, and one
      that reports this data directory is taken to be its postmaster.

    Whatever fails a check is left exactly as it is.  Stale directories
    with no running server are left for the system's temporary-file
    cleanup: they hold nothing that needs stopping.  The sweep is best
    effort; a candidate that cannot be examined or stopped is skipped rather
    than allowed to fail the session.
    """
    superuser = pwd.getpwuid(os.geteuid()).pw_name
    swept = []
    for parent in dict.fromkeys(parents or _base_parents()):
        try:
            candidates = sorted(Path(parent).glob(f"{BASE_PREFIX}*"))
        except OSError:
            continue
        for base in candidates:
            try:
                directory = _own_entry(base, stat.S_ISDIR)
                if directory is None or directory.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                    continue
                stale = Cluster(bindir, base, TEST_PORT, superuser)
                if stale._is_abandoned(max_age):
                    stale.stop()
                    swept.append(base)
            except (OSError, HarnessError):
                continue
    return swept


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
        """Every statement the script sent to this database, in order.

        The script's own only: what the driver adds at session start is
        filtered out by content (see Cluster.statements), so position 0 is
        the first statement the script issued on a connection.
        """
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
