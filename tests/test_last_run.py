"""Tests for the _last_run parameter feature in MarimoExecutionManager."""

import subprocess
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from marimo_jupyter_scheduler.executor import MarimoExecutionManager


def _make_manager(parameters=None, job_definition_id="def-1", job_id="job-1"):
    manager = MarimoExecutionManager.__new__(MarimoExecutionManager)
    manager.job_id = job_id
    manager.root_dir = "/tmp"
    manager.db_url = "sqlite://"
    manager.staging_paths = {}
    manager._db_session = None

    mock_model = MagicMock()
    mock_model.input_filename = "test.py"
    mock_model.output_formats = ["html"]
    mock_model.parameters = dict(parameters or {})
    mock_model.job_definition_id = job_definition_id
    mock_model.job_id = job_id

    type(manager).model = PropertyMock(return_value=mock_model)
    return manager


def test_last_run_injected_when_prior_run_exists():
    manager = _make_manager(parameters={"_last_run": "LAST_RUN_AT"})

    dt = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)

    with patch.object(manager, "_get_last_run_time", return_value=dt.isoformat()):
        env = manager._build_env({"_last_run": "LAST_RUN_AT"})

    assert "MARIMO_PARAM_LAST_RUN_AT" in env
    assert env["MARIMO_PARAM_LAST_RUN_AT"] == dt.isoformat()


def test_last_run_not_injected_when_no_prior_run():
    manager = _make_manager(parameters={"_last_run": "LAST_RUN_AT"})

    with patch.object(manager, "_get_last_run_time", return_value=None):
        env = manager._build_env({"_last_run": "LAST_RUN_AT"})

    assert "MARIMO_PARAM_LAST_RUN_AT" not in env


def test_last_run_key_not_passed_as_param():
    """_last_run should be consumed and not appear as MARIMO_PARAM__LAST_RUN."""
    manager = _make_manager()

    with patch.object(manager, "_get_last_run_time", return_value=None):
        env = manager._build_env({"_last_run": "LAST_RUN_AT", "date": "2024-01-01"})

    assert "MARIMO_PARAM__LAST_RUN" not in env
    assert "MARIMO_PARAM_DATE" in env


def test_get_last_run_time_returns_none_without_definition_id():
    manager = _make_manager(job_definition_id=None)
    # model has no job_definition_id
    mock_model = MagicMock()
    mock_model.job_definition_id = None
    mock_model.job_id = "job-1"
    type(manager).model = PropertyMock(return_value=mock_model)

    result = manager._get_last_run_time()
    assert result is None


def test_get_last_run_time_formats_int_timestamp():
    manager = _make_manager()

    mock_job = MagicMock()
    # 1000ms = 1 second epoch → 1970-01-01T00:00:01+00:00
    mock_job.end_time = 1000

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_job

    # db_session is a read-only property backed by _db_session; set it directly.
    manager._db_session = MagicMock(return_value=mock_session)
    result = manager._get_last_run_time()

    assert result == "1970-01-01T00:00:01+00:00"


def test_get_last_run_time_formats_datetime():
    manager = _make_manager()

    dt = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    mock_job = MagicMock()
    mock_job.end_time = dt

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_job

    manager._db_session = MagicMock(return_value=mock_session)
    result = manager._get_last_run_time()

    assert result == dt.isoformat()


def test_get_last_run_time_returns_none_on_db_error():
    manager = _make_manager()

    mock_session = MagicMock()
    mock_session.__enter__ = MagicMock(side_effect=Exception("DB gone"))
    mock_session.__exit__ = MagicMock(return_value=False)

    manager._db_session = MagicMock(return_value=mock_session)
    result = manager._get_last_run_time()

    assert result is None
