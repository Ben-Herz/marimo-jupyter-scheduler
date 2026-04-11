"""Tests for MarimoScheduler.copy_input_file and update_job_definition."""

import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

from jupyter_scheduler.scheduler import Scheduler


# ─── copy_input_file ──────────────────────────────────────────────────────────

def _make_scheduler(root_dir: str, staging_path: str):
    """Construct a MarimoScheduler without hitting the DB or Traitlets."""
    from marimo_jupyter_scheduler.scheduler import MarimoScheduler
    scheduler = MarimoScheduler.__new__(MarimoScheduler)
    scheduler.root_dir = root_dir
    scheduler.staging_path = staging_path
    return scheduler


def test_copy_creates_target_directory(tmp_path):
    src = tmp_path / "notebook.py"
    src.write_text("# marimo")

    staging = tmp_path / "staging"
    target_dir = staging / "job-123"
    target_file = target_dir / "notebook.py"

    scheduler = _make_scheduler(str(tmp_path), str(staging))
    scheduler.copy_input_file(str(src), str(target_file))

    assert target_file.exists()
    assert target_file.read_text() == "# marimo"


def test_copy_relative_input_uri(tmp_path):
    """Relative input_uri is resolved against root_dir."""
    (tmp_path / "notebooks").mkdir()
    src = tmp_path / "notebooks" / "report.py"
    src.write_text("# marimo")

    staging = tmp_path / "staging"
    target_dir = staging / "job-456"
    target_file = target_dir / "notebooks" / "report.py"

    scheduler = _make_scheduler(str(tmp_path), str(staging))
    scheduler.copy_input_file("notebooks/report.py", str(target_file))

    assert target_file.exists()


def test_copy_falls_back_to_root_dir_when_abs_staging_missing(tmp_path):
    """If absolute input_uri doesn't exist, fall back via copy_to_path structure."""
    (tmp_path / "notebooks").mkdir()
    real_notebook = tmp_path / "notebooks" / "report.py"
    real_notebook.write_text("# real notebook")

    staging = tmp_path / "staging"
    target_dir = staging / "job-789"
    target_file = target_dir / "notebooks" / "report.py"

    # input_uri is an absolute staging path that doesn't exist yet
    fake_staging_path = str(staging / "defs" / "notebooks" / "report.py")

    scheduler = _make_scheduler(str(tmp_path), str(staging))
    scheduler.copy_input_file(fake_staging_path, str(target_file))

    assert target_file.exists()


def test_copy_raises_when_source_not_found(tmp_path):
    staging = tmp_path / "staging"
    target_file = staging / "job-000" / "missing.py"

    scheduler = _make_scheduler(str(tmp_path), str(staging))

    with pytest.raises(FileNotFoundError, match="Cannot find notebook"):
        scheduler.copy_input_file("/nonexistent/path/missing.py", str(target_file))


# ─── update_job_definition ────────────────────────────────────────────────────

def test_update_job_definition_applies_extra_fields(tmp_path):
    """Extra fields (output_formats, parameters, tags, name) are written to DB."""
    from marimo_jupyter_scheduler.scheduler import MarimoScheduler

    scheduler = MarimoScheduler.__new__(MarimoScheduler)
    scheduler.root_dir = str(tmp_path)
    scheduler.staging_path = str(tmp_path / "staging")

    # Mock the DB session — db_session is a read-only property backed by _db_session.
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query

    scheduler._db_session = MagicMock(return_value=mock_session)

    model = MagicMock()
    model.output_formats = ["html"]
    model.parameters = {"date": "2024-01-01"}
    model.tags = ["daily"]
    model.name = "my-job"

    with patch.object(Scheduler, "update_job_definition"):
        scheduler.update_job_definition("def-123", model)

    mock_query.update.assert_called_once_with({
        "output_formats": ["html"],
        "parameters": {"date": "2024-01-01"},
        "tags": ["daily"],
        "name": "my-job",
    })
    mock_session.commit.assert_called_once()


def test_update_job_definition_skips_none_fields(tmp_path):
    """Fields that are None on the model are not included in the DB update."""
    from marimo_jupyter_scheduler.scheduler import MarimoScheduler

    scheduler = MarimoScheduler.__new__(MarimoScheduler)
    scheduler.root_dir = str(tmp_path)
    scheduler.staging_path = str(tmp_path / "staging")

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_session.__enter__ = MagicMock(return_value=mock_session)
    mock_session.__exit__ = MagicMock(return_value=False)
    mock_session.query.return_value = mock_query
    mock_query.filter.return_value = mock_query

    scheduler._db_session = MagicMock(return_value=mock_session)

    model = MagicMock()
    model.output_formats = None
    model.parameters = None
    model.tags = None
    model.name = "renamed"

    with patch.object(Scheduler, "update_job_definition"):
        scheduler.update_job_definition("def-456", model)

    mock_query.update.assert_called_once_with({"name": "renamed"})
