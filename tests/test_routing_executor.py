"""Tests for RoutingExecutionManager."""

from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from marimo_jupyter_scheduler.executor import RoutingExecutionManager


def _make_routing_manager(notebook_path: str, output_formats=None, parameters=None):
    manager = RoutingExecutionManager.__new__(RoutingExecutionManager)
    manager.job_id = "test-job-id"
    manager.root_dir = "/tmp"
    manager.db_url = "sqlite://"
    manager.staging_paths = {}
    manager._db_session = None

    mock_model = MagicMock()
    mock_model.input_filename = notebook_path
    mock_model.output_formats = output_formats or ["html"]
    mock_model.parameters = dict(parameters or {})
    mock_model.job_definition_id = None

    type(manager).model = PropertyMock(return_value=mock_model)
    return manager


@patch("marimo_jupyter_scheduler.executor.MarimoExecutionManager.execute")
def test_routes_py_to_marimo(mock_execute, tmp_path):
    notebook = tmp_path / "report.py"
    notebook.write_text("# marimo")

    manager = _make_routing_manager(str(notebook), output_formats=["html"])
    manager.root_dir = str(tmp_path)
    manager.execute()

    mock_execute.assert_called_once()


@patch("marimo_jupyter_scheduler.executor.DefaultExecutionManager.execute")
def test_routes_ipynb_to_default(mock_execute, tmp_path):
    notebook = tmp_path / "report.ipynb"
    notebook.write_text("{}")

    manager = _make_routing_manager(str(notebook))
    manager.root_dir = str(tmp_path)
    manager.execute()

    mock_execute.assert_called_once()


def test_supported_features_includes_both():
    from jupyter_scheduler.models import JobFeature
    features = RoutingExecutionManager.supported_features()
    # From MarimoExecutionManager
    assert features[JobFeature.parameters] is True
    assert features[JobFeature.job_definition] is True
    # From DefaultExecutionManager
    assert features[JobFeature.job_name] is True
    assert features[JobFeature.output_formats] is True


def test_validate_always_true():
    assert RoutingExecutionManager.validate() is True
    assert RoutingExecutionManager.validate("anything", "extra") is True
