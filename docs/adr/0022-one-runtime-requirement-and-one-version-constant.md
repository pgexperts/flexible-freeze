---
id: 0022
title: Declare the runtime dependency once, and keep the version in one constant
date: 2026-10-13
status: Accepted
summary: scripts/requirements.txt is the only runtime declaration (psycopg2, built from source); requirements-dev.txt names psycopg2-binary and does not include the runtime file; there is no pyproject.toml; the version (0.7) is __version__ in the script, printed by --version, with CHANGELOG.md; tags are left to the owner.
---

# 0022. Declare the runtime dependency once, and keep the version in one constant

## Context

After the earlier packaging fixes, issue #43 still had three open items. The
runtime dependency was declared twice, in an unreferenced root
`requirements.txt` and in `scripts/requirements.txt`, with a third copy of the
line in `requirements-dev.txt`. Both named `psycopg2-binary`, which the
psycopg2 project recommends for development only. And the script's docstring
said "Version 0.6", with no changelog or tag tying that to anything.

## Decision

- `scripts/requirements.txt` is the one runtime declaration, naming
  `psycopg2>=2.9,<3`: the source-built driver, which psycopg2's documentation
  advises for production. The root `requirements.txt` is removed. The README's
  "Installing" section names `psycopg2-binary` as a convenience where building
  is impractical, and says to install one of the two, not both.
- `requirements-dev.txt` names `psycopg2-binary` with the same range, so CI and
  contributors need neither `pg_config` nor a compiler. It deliberately does
  not include `-r scripts/requirements.txt`: `psycopg2` and `psycopg2-binary`
  installed together put the same module in place twice, a known source of
  breakage. The two ranges are kept in step by hand.
- There is no `pyproject.toml`, because the script is not importable (record
  0008) and so is not a package.
- The version is `__version__ = "0.7"` in the script, the one place it is
  written; the docstring points to it rather than repeating it. `--version`
  prints `flexible_freeze 0.7`. `CHANGELOG.md` has a section per release; 0.7
  is listed as unreleased and relative to 0.6, the baseline.
- Git tags are left to the owner.

## Alternatives considered

- **A `pyproject.toml`** — the script is not importable (record 0008), so
  there is no package for it to describe.
- **`-r scripts/requirements.txt` in `requirements-dev.txt`** — would install
  `psycopg2` and `psycopg2-binary` together.
- **`psycopg2-binary` as the runtime requirement** — psycopg2's documentation
  advises the source distribution for production.
- **The version in the docstring** — a second place to keep in step.

## Consequences

Installing the runtime requirement needs `pg_config` and a C compiler. CI and
the test suite run on `psycopg2-binary`, not the source build. The two version
ranges have to be changed together. A release is a change to `__version__` and
a new `CHANGELOG.md` section; no tag exists for 0.7 or 0.6 until the owner
makes one.
