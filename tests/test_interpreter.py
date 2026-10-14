# Tests for what the script asks of the interpreter it runs under: the oldest
# Python it accepts, and that it does not depend on argparse.__version__,
# which is undocumented and on its way out of the standard library.
#
# Neither can be reached with the interpreter the suite itself runs on, so
# the script is started with a sitecustomize module in front of it (found
# through PYTHONPATH, as ffharness.ScriptClock does) that changes what the
# script will see before its first line runs.  The script stays unmodified.

import os
import sys

import pytest

# A database that does not exist: the cheapest way to get a run past its
# startup without a statement being sent (see test_arguments.py).
NO_SUCH_DB = "ff_no_such_database"

# What the script's check and its message have to agree on.
MINIMUM = (3, 11)


def hexversion(major, minor, micro):
    # sys.hexversion of a final release.
    return major << 24 | minor << 16 | micro << 8 | 0xF0


def shim_env(directory, source):
    (directory / "sitecustomize.py").write_text(source)
    inherited = os.environ.get("PYTHONPATH")
    path = [str(directory)] + ([inherited] if inherited else [])
    return {"PYTHONPATH": os.pathsep.join(path)}


def test_python_older_than_the_minimum_is_refused(cluster, run_ff, tmp_path):
    # The last release line before the minimum, at a micro version higher
    # than any it had.
    pretended = hexversion(MINIMUM[0], MINIMUM[1] - 1, 99)
    mark = cluster.log_offset()

    result = run_ff(env=shim_env(tmp_path, f"import sys\nsys.hexversion = {pretended}\n"))

    assert result.returncode == 1
    assert result.stdout == ""
    # sys.version is left alone by the shim, so it is this interpreter's.
    assert result.stderr == (
        f"At least Python {MINIMUM[0]}.{MINIMUM[1]} required; you have {sys.version}\n"
    )
    assert cluster.statements(since=mark) == []


def test_the_minimum_python_itself_is_accepted(run_ff, tmp_path):
    pretended = hexversion(MINIMUM[0], MINIMUM[1], 0)

    result = run_ff(
        "-d", NO_SUCH_DB,
        env=shim_env(tmp_path, f"import sys\nsys.hexversion = {pretended}\n"),
    )

    assert "required; you have" not in result.stderr
    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr


@pytest.mark.parametrize("debug", [True, False], ids=["debug", "no-debug"])
def test_run_does_not_need_argparse_to_have_a_version(run_ff, tmp_path, debug):
    # The attribute is read to build the --debug line whether or not --debug
    # was given, so both runs depend on it.  The module-level __getattr__ is
    # removed as well: that is how an interpreter that has deprecated the
    # attribute still serves it.
    source = (
        "import argparse\n"
        "vars(argparse).pop('__version__', None)\n"
        "vars(argparse).pop('__getattr__', None)\n"
    )

    result = run_ff(
        *(["--debug"] if debug else []), "-d", NO_SUCH_DB,
        env=shim_env(tmp_path, source),
    )

    assert "Traceback" not in result.stderr
    assert f"skipping database {NO_SUCH_DB} (couldn't connect: " in result.stderr
    version_lines = [
        line for line in result.stderr.splitlines() if "argparse version: " in line
    ]
    assert [line.split("argparse version: ")[1] for line in version_lines] == (
        ["n/a"] if debug else []
    )


def test_debug_output_names_the_argparse_version_when_there_is_one(run_ff, tmp_path):
    result = run_ff(
        "--debug", "-d", NO_SUCH_DB,
        env=shim_env(tmp_path, "import argparse\nargparse.__version__ = '9.9-test'\n"),
    )

    assert "argparse version: 9.9-test" in result.stderr
