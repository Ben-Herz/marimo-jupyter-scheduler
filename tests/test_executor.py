"""Tests for MarimoExecutionManager."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from marimo_jupyter_scheduler.executor import MarimoExecutionManager


def _make_manager(notebook_path: str, output_formats=None, parameters=None, staging_paths=None):
    """
    Construct a MarimoExecutionManager bypassing the DB.

    The real constructor signature is:
        __init__(self, job_id, root_dir, db_url, staging_paths)

    Job data is accessed via self.model (a lazy DB property).
    We patch that property to return a MagicMock with the fields we need.
    """
    manager = MarimoExecutionManager.__new__(MarimoExecutionManager)
    manager.job_id = "test-job-id"
    manager.root_dir = "/tmp"
    manager.db_url = "sqlite://"
    manager.staging_paths = staging_paths or {}
    manager._db_session = None

    # Patch self.model to avoid hitting the DB
    mock_model = MagicMock()
    mock_model.input_filename = notebook_path
    mock_model.output_formats = output_formats or ["html"]
    mock_model.parameters = dict(parameters or {})

    type(manager).model = PropertyMock(return_value=mock_model)
    return manager


def test_build_env_prefixes_params():
    manager = _make_manager("test.py")
    env = manager._build_env({"date": "2024-01-01", "region": "EU"})
    assert env["MARIMO_PARAM_DATE"] == "2024-01-01"
    assert env["MARIMO_PARAM_REGION"] == "EU"


def test_build_env_applies_env_overrides():
    manager = _make_manager("test.py")
    env = manager._build_env({"_env": {"MY_SECRET": "s3cr3t"}, "param": "val"})
    assert env["MY_SECRET"] == "s3cr3t"
    assert env["MARIMO_PARAM_PARAM"] == "val"
    assert "MARIMO_PARAM__ENV" not in env


def test_output_path_from_staging(tmp_path):
    out = tmp_path / "output.html"
    manager = _make_manager("test.py", staging_paths={"output-html": str(out)})
    result = manager._output_path(Path("/tmp/test.py"), "html")
    assert result == out


def test_output_path_fallback():
    manager = _make_manager("test.py", staging_paths={})
    result = manager._output_path(Path("/tmp/test.py"), "html")
    assert result == Path("/tmp/test.html")


def test_timeout_default():
    manager = _make_manager("test.py")
    mock_job = MagicMock()
    mock_job.parameters = {}
    assert manager._timeout(mock_job) == 3600


def test_timeout_from_params():
    manager = _make_manager("test.py")
    mock_job = MagicMock()
    mock_job.parameters = {"_timeout": "120"}
    assert manager._timeout(mock_job) == 120


def test_supported_features_returns_dict():
    from jupyter_scheduler.models import JobFeature
    features = MarimoExecutionManager.supported_features()
    assert isinstance(features, dict)
    assert features[JobFeature.job_name] is True
    assert features[JobFeature.parameters] is True
    assert features[JobFeature.output_formats] is True


@patch("marimo_jupyter_scheduler.executor.subprocess.run")
def test_execute_html_success(mock_run, tmp_path):
    notebook = tmp_path / "test.py"
    notebook.write_text("# marimo notebook")

    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=0, stdout="", stderr=""
    )

    manager = _make_manager(str(notebook), output_formats=["html"])
    manager.root_dir = str(tmp_path)
    manager.execute()

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert "marimo" in cmd[0] or cmd[0].endswith("marimo")
    assert "export" in cmd
    assert "html" in cmd


@patch("marimo_jupyter_scheduler.executor.subprocess.run")
def test_execute_raises_on_nonzero(mock_run, tmp_path):
    notebook = tmp_path / "test.py"
    notebook.write_text("# marimo notebook")

    mock_run.return_value = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="Something went wrong"
    )

    manager = _make_manager(str(notebook), output_formats=["html"])
    manager.root_dir = str(tmp_path)

    with pytest.raises(RuntimeError, match="Something went wrong"):
        manager.execute()


def test_execute_missing_file_raises():
    manager = _make_manager("/nonexistent/path/notebook.py")
    with pytest.raises(FileNotFoundError):
        manager.execute()
