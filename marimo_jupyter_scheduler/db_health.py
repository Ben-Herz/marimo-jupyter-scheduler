"""
Writability checks for the scheduler database.

jupyter-scheduler notices a read-only database only when a job actually fires:
``TaskRunner.process_queue`` calls ``create_job()``, SQLAlchemy raises
``OperationalError("attempt to write a readonly database")``, and the traceback
lands in the server log — possibly hours after the deployment that broke it.
Reads keep working, so the dashboard looks healthy and a schedule that has
silently stopped running is indistinguishable from one that has not fired yet.

These helpers detect the condition at extension load and on every dashboard
poll, and explain it in terms of the actual uid and file modes involved.
"""

from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)

_PROBE_TABLE = "_marimo_write_probe"

# Probing takes a write lock, so cache the verdict rather than repeating it on
# every dashboard poll (the UI polls every few seconds).
_CACHE: dict[str, tuple[float, "str | None"]] = {}
_CACHE_TTL = 60.0


def _url(db_url: str):
    from sqlalchemy.engine import make_url

    return make_url(db_url)


def _is_sqlite(db_url: str) -> bool:
    try:
        return _url(db_url).drivername.startswith("sqlite")
    except Exception:
        return db_url.startswith("sqlite")


def sqlite_path(db_url: str) -> str | None:
    """Filesystem path behind a SQLite URL, or None for other backends.

    Uses SQLAlchemy's parser rather than urlparse: the number of slashes in a
    SQLite URL is significant (``sqlite:///rel`` vs ``sqlite:////abs``) and
    urlparse gets it wrong.
    """
    try:
        url = _url(db_url)
    except Exception:
        return None
    if not url.drivername.startswith("sqlite"):
        return None
    if not url.database or url.database == ":memory:":
        return None
    return os.path.abspath(url.database)


def _probe_write(db_url: str) -> str | None:
    """Attempt a real write, always rolled back. Returns the error, or None.

    Creating a table (rather than checking file permissions) is what makes this
    trustworthy: it forces SQLite to create its rollback journal next to the
    database, so a writable file inside a read-only directory fails here — the
    exact case an ``os.access`` check on the file alone reports as healthy.
    """
    path = sqlite_path(db_url)
    if path is not None:
        return _probe_write_sqlite(path)
    if _is_sqlite(db_url):
        return None  # in-memory: nothing to check
    return _probe_write_sqlalchemy(db_url)


def _probe_write_sqlite(path: str) -> str | None:
    """Probe a SQLite file through the stdlib driver.

    Going around SQLAlchemy here is deliberate. pysqlite issues an implicit
    COMMIT before DDL unless it is in autocommit mode, which would leave the
    probe table behind in a database we are only supposed to be inspecting.
    ``isolation_level=None`` suppresses that so the explicit ROLLBACK holds.
    """
    import sqlite3

    # Without a short busy timeout this probe would block the dashboard request
    # behind any other writer on a shared database.
    conn = sqlite3.connect(path, timeout=5, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(f"CREATE TABLE {_PROBE_TABLE} (x INTEGER)")
        finally:
            conn.execute("ROLLBACK")
    except Exception as exc:
        return str(exc).strip()
    finally:
        conn.close()
    return None


def _probe_write_sqlalchemy(db_url: str) -> str | None:
    """Probe a non-SQLite backend, where DDL is transactional."""
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text(f"CREATE TABLE {_PROBE_TABLE} (x INTEGER)"))
            finally:
                trans.rollback()
    except Exception as exc:
        return str(exc).strip()
    finally:
        engine.dispose()
    return None


def _diagnose_sqlite(path: str) -> list[str]:
    """Explain a SQLite write failure in terms of uid and mode."""
    notes: list[str] = []

    getuid = getattr(os, "getuid", None)
    if getuid is not None:
        notes.append(f"server process runs as uid {getuid()}:{os.getgid()}")

    def describe(target: str) -> str:
        st = os.stat(target)
        return f"{target} is mode {oct(st.st_mode & 0o777)} owned by {st.st_uid}:{st.st_gid}"

    directory = os.path.dirname(path) or "."

    try:
        if not os.path.exists(directory):
            notes.append(f"{directory} does not exist")
        elif not os.access(directory, os.W_OK):
            notes.append(
                f"{describe(directory)} — not writable, and SQLite must create "
                f"its journal there, not just in the database file"
            )
    except OSError as exc:
        notes.append(f"could not stat {directory}: {exc}")

    try:
        if os.path.exists(path) and not os.access(path, os.W_OK):
            notes.append(f"{describe(path)} — not writable")
    except OSError as exc:
        notes.append(f"could not stat {path}: {exc}")

    return notes


def check_db_writable(db_url: str) -> str | None:
    """Return None if the database accepts writes, else a diagnostic message.

    The message is multi-line and safe to show verbatim in a log or the UI; it
    contains paths, modes and uids but no credentials (the URL itself is never
    included, since a PostgreSQL URL would carry a password).
    """
    error = _probe_write(db_url)
    if error is None:
        return None

    lines = [f"scheduler database is not writable: {error}"]
    path = sqlite_path(db_url)
    if path:
        lines.extend(_diagnose_sqlite(path))
    return "\n".join(lines)


def db_health(db_url: str, ttl: float = _CACHE_TTL) -> str | None:
    """check_db_writable() with a short TTL cache, for hot paths."""
    now = time.monotonic()
    cached = _CACHE.get(db_url)
    if cached is not None and now - cached[0] < ttl:
        return cached[1]

    try:
        message = check_db_writable(db_url)
    except Exception as exc:  # a broken check must not break the dashboard
        logger.warning("marimo-jupyter-scheduler: db health check failed to run: %s", exc)
        message = None

    _CACHE[db_url] = (now, message)
    return message


def reset_cache() -> None:
    """Drop cached verdicts. Used by tests and after a permissions fix."""
    _CACHE.clear()
