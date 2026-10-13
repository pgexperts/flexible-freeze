# A password given with -w must not be echoed into the debug output or the
# logfile.  Both places print the parsed argument namespace; the password is
# replaced there by a fixed placeholder, and left as None when none was given.

from ffharness import FREEZE_ALL

# Distinctive enough that a substring search cannot hit anything else.
SECRET = "s3cr3t-Zq7XvK-pw"
MASKED = "dbpass='********'"


def test_debug_output_does_not_contain_the_password(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "-w", SECRET, "--debug", "-d", db.name)

    assert result.returncode == 0
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr
    parameters = [
        line for line in result.stderr.splitlines() if "parameters: " in line
    ]
    assert len(parameters) == 1
    assert MASKED in parameters[0]


def test_logfile_does_not_contain_the_password(db, run_ff, tmp_path):
    db.create_table("t1")
    logfile = tmp_path / "flexible_freeze.log"

    result = run_ff(*FREEZE_ALL, "-w", SECRET, "-v", "-l", str(logfile), "-d", db.name)

    assert result.returncode == 0
    log = logfile.read_text()
    assert SECRET not in log
    assert SECRET not in result.stdout
    assert SECRET not in result.stderr
    arguments = [line for line in log.splitlines() if line.startswith("arguments: ")]
    assert len(arguments) == 1
    assert MASKED in arguments[0]


def test_debug_and_logfile_together_do_not_contain_the_password(
    db, run_ff, tmp_path
):
    db.create_table("t1")
    logfile = tmp_path / "flexible_freeze.log"

    result = run_ff(
        *FREEZE_ALL, "-w", SECRET, "--debug", "-v", "-l", str(logfile), "-d", db.name
    )

    assert result.returncode == 0
    for text in (logfile.read_text(), result.stdout, result.stderr):
        assert SECRET not in text


def test_without_a_password_the_debug_line_shows_none(db, run_ff):
    db.create_table("t1")

    result = run_ff(*FREEZE_ALL, "--debug", "-d", db.name)

    assert result.returncode == 0
    parameters = [
        line for line in result.stderr.splitlines() if "parameters: " in line
    ]
    assert len(parameters) == 1
    assert "dbpass=None" in parameters[0]
    assert "********" not in parameters[0]


def test_without_a_password_the_logfile_line_shows_none(db, run_ff, tmp_path):
    db.create_table("t1")
    logfile = tmp_path / "flexible_freeze.log"

    result = run_ff(*FREEZE_ALL, "-v", "-l", str(logfile), "-d", db.name)

    assert result.returncode == 0
    arguments = [
        line
        for line in logfile.read_text().splitlines()
        if line.startswith("arguments: ")
    ]
    assert len(arguments) == 1
    assert "dbpass=None" in arguments[0]
    assert "********" not in arguments[0]


def test_guard_run_with_a_password_still_connects_and_vacuums(db, run_ff):
    # Guard, expected to pass both before and after the change: masking the
    # printed copy must not alter what reaches the connection.  The cluster
    # uses trust auth, so the password is not itself checked.
    db.create_table("t1")
    before = db.frozenxids()

    result = run_ff(*FREEZE_ALL, "-w", SECRET, "-d", db.name)

    assert result.returncode == 0, result.stdout + result.stderr
    assert db.frozen_since(before) == {("public", "t1")}
