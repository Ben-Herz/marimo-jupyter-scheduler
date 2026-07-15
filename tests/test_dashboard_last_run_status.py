"""DashboardHandler._last_run_status resolves each definition's lamp server-side.

The bug: the dashboard computed the per-definition status lamp from a single
globally-limited job list (the 100 newest jobs). A definition that ran less
often than its neighbours had all of its runs buried past that limit once busy
definitions piled up newer jobs, so its lamp went grey even though it had a
perfectly good last run. These tests exercise the SQL that replaces that scan.
"""

import time

import pytest
from jupyter_scheduler.orm import Job, create_session, create_tables

from marimo_jupyter_scheduler.handlers import DashboardHandler


@pytest.fixture
def session(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'scheduler.sqlite'}"
    create_tables(db_url)
    factory = create_session(db_url)
    with factory() as s:
        yield s


def _job(job_id, def_id, status, start_time, end_time=None):
    return Job(
        job_id=job_id,
        job_definition_id=def_id,
        runtime_environment_name="",
        input_filename="n.py",
        status=status,
        start_time=start_time,
        end_time=end_time,
    )


def test_returns_latest_terminal_status_per_definition(session):
    now = int(time.time() * 1000)
    session.add_all([
        _job("a1", "A", "FAILED", now - 1000),
        _job("a2", "A", "COMPLETED", now - 500, now - 400),
        _job("b1", "B", "COMPLETED", now - 900, now - 800),
        _job("b2", "B", "FAILED", now - 100),
    ])
    session.commit()

    assert DashboardHandler._last_run_status(session, Job) == {
        "A": "COMPLETED",
        "B": "FAILED",
    }


def test_newer_non_terminal_run_does_not_hide_last_result(session):
    """A queued/running job after a COMPLETED one must not blank the lamp."""
    now = int(time.time() * 1000)
    session.add_all([
        _job("a1", "A", "COMPLETED", now - 500, now - 400),
        _job("a2", "A", "IN_PROGRESS", now),
    ])
    session.commit()

    assert DashboardHandler._last_run_status(session, Job) == {"A": "COMPLETED"}


def test_definition_with_only_non_terminal_runs_is_absent(session):
    now = int(time.time() * 1000)
    session.add(_job("c1", "C", "QUEUED", now))
    session.commit()

    assert DashboardHandler._last_run_status(session, Job) == {}


def test_low_frequency_definition_survives_a_flood_of_other_jobs(session):
    """The original bug: a busy definition must not bury a quiet one's lamp."""
    now = int(time.time() * 1000)
    # Quiet definition: one COMPLETED run, a while ago.
    session.add(_job("quiet", "QUIET", "COMPLETED", now - 10_000_000, now - 10_000_000))
    # Busy definition: hundreds of much newer runs.
    session.add_all([
        _job(f"busy{i}", "BUSY", "COMPLETED", now - i, now - i)
        for i in range(500)
    ])
    session.commit()

    result = DashboardHandler._last_run_status(session, Job)
    assert result["QUIET"] == "COMPLETED"
    assert result["BUSY"] == "COMPLETED"
