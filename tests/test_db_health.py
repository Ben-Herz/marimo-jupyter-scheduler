"""Tests for the scheduler database writability check."""

import os

import pytest

from marimo_jupyter_scheduler.db_health import (
    check_db_writable,
    db_health,
    reset_cache,
    sqlite_path,
)

# chmod is meaningless on Windows, and root bypasses permission bits entirely,
# so the read-only cases can only be exercised as an unprivileged POSIX user.
requires_posix_user = pytest.mark.skipif(
    os.name != "posix" or (hasattr(os, "getuid") and os.getuid() == 0),
    reason="requires a non-root POSIX user",
)


@pytest.fixture(autouse=True)
def _clear_cache():
    reset_cache()
    yield
    reset_cache()


def test_sqlite_path_handles_absolute_urls():
    assert sqlite_path("sqlite:////home/jovyan/scheduler.sqlite") == (
        "/home/jovyan/scheduler.sqlite"
    )


def test_sqlite_path_is_none_for_other_backends():
    assert sqlite_path("postgresql+psycopg2://user:pw@host:5432/scheduler") is None
    assert sqlite_path("sqlite://") is None


def test_writable_database_reports_no_problem(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'scheduler.sqlite'}"
    assert check_db_writable(db_url) is None


def test_probe_leaves_no_table_behind(tmp_path):
    """The probe writes inside a transaction it always rolls back."""
    import sqlite3

    db_path = tmp_path / "scheduler.sqlite"
    assert check_db_writable(f"sqlite:///{db_path}") is None

    conn = sqlite3.connect(db_path)
    try:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master")]
    finally:
        conn.close()
    assert tables == []


@requires_posix_user
def test_readonly_file_is_detected(tmp_path):
    db_path = tmp_path / "scheduler.sqlite"
    db_path.touch()
    db_path.chmod(0o444)

    problem = check_db_writable(f"sqlite:///{db_path}")

    assert problem is not None
    assert "not writable" in problem
    assert str(db_path) in problem


@requires_posix_user
def test_readonly_directory_is_detected(tmp_path):
    """A writable DB file in a read-only directory still cannot be written.

    This is the case a permissions check on the file alone would miss: SQLite
    needs to create its journal in the containing directory.
    """
    db_dir = tmp_path / "scheduler_db"
    db_dir.mkdir()
    db_path = db_dir / "scheduler.sqlite"

    # Create the database while the directory is still writable, so the file
    # itself exists and is mode 644.
    assert check_db_writable(f"sqlite:///{db_path}") is None
    db_dir.chmod(0o555)
    reset_cache()

    try:
        problem = check_db_writable(f"sqlite:///{db_path}")
    finally:
        db_dir.chmod(0o755)

    assert problem is not None
    assert "journal" in problem
    assert str(db_dir) in problem


def test_db_health_caches_within_ttl(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'scheduler.sqlite'}"
    calls = []

    def fake_probe(url):
        calls.append(url)
        return "boom"

    monkeypatch.setattr("marimo_jupyter_scheduler.db_health._probe_write", fake_probe)

    assert db_health(db_url) is not None
    assert db_health(db_url) is not None
    assert len(calls) == 1


def test_db_health_never_raises(tmp_path, monkeypatch):
    """A broken check must not take the dashboard down with it."""

    def exploding_probe(url):
        raise RuntimeError("probe exploded")

    monkeypatch.setattr(
        "marimo_jupyter_scheduler.db_health._probe_write", exploding_probe
    )

    assert db_health("sqlite:///whatever.sqlite") is None
