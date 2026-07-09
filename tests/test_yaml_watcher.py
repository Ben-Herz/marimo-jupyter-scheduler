"""Tests for YamlScheduleWatcher input staging.

jupyter-scheduler snapshots a notebook into
{staging_path}/{job_definition_id}/{input_filename} when a definition is created
through its own API, and re-opens that snapshot on every cron tick
(jupyter_scheduler/executors.py, ExecutionManager.validate). The YAML watcher
writes definitions straight to the database, so it has to stage the file itself.
"""

import os
from types import SimpleNamespace

import pytest

from marimo_jupyter_scheduler.yaml_watcher import YamlScheduleWatcher

NOTEBOOK = "collabs/HDR_Tasks/Test/Marimo_Scheduler_test_mo.py"
DEF_ID = "92afd52c-fb35-4890-8472-e2a02da2164e"


def _make_watcher(tmp_path, db_url="sqlite://"):
    root = tmp_path / "root"
    staging = tmp_path / "staging"
    notebook = root / NOTEBOOK
    notebook.parent.mkdir(parents=True)
    notebook.write_text("import marimo\n")

    scheduler = SimpleNamespace(staging_path=str(staging), task_runner=None)
    watcher = YamlScheduleWatcher(root_dir=str(root), db_url=db_url, scheduler=scheduler)
    return watcher, staging / DEF_ID / NOTEBOOK


def test_ensure_staged_copies_notebook(tmp_path):
    watcher, staged = _make_watcher(tmp_path)

    watcher._ensure_staged(DEF_ID, NOTEBOOK)

    assert staged.exists()
    assert staged.read_text() == "import marimo\n"


def test_ensure_staged_opens_where_validate_would(tmp_path):
    """executors.py:194 does open(input_path); that call must now succeed."""
    watcher, staged = _make_watcher(tmp_path)

    watcher._ensure_staged(DEF_ID, NOTEBOOK)

    with open(staged, encoding="utf-8") as f:
        assert f.read() == "import marimo\n"


def test_ensure_staged_does_not_overwrite_existing_snapshot(tmp_path):
    watcher, staged = _make_watcher(tmp_path)
    staged.parent.mkdir(parents=True)
    staged.write_text("# snapshot taken when the definition was created\n")

    watcher._ensure_staged(DEF_ID, NOTEBOOK)

    assert staged.read_text() == "# snapshot taken when the definition was created\n"


def test_ensure_staged_restores_wiped_staging_area(tmp_path):
    """A container rebuild can drop the staging area while the DB row survives."""
    watcher, staged = _make_watcher(tmp_path)
    watcher._ensure_staged(DEF_ID, NOTEBOOK)
    staged.unlink()

    watcher._ensure_staged(DEF_ID, NOTEBOOK)

    assert staged.exists()


def test_ensure_staged_missing_notebook_warns_without_raising(tmp_path, caplog):
    """The watcher runs on a daemon thread; a bad definition must not kill it."""
    watcher, _ = _make_watcher(tmp_path)

    watcher._ensure_staged(DEF_ID, "nope/missing.py")

    assert "cannot stage" in caplog.text


def test_ensure_staged_ignores_empty_input_filename(tmp_path):
    watcher, _ = _make_watcher(tmp_path)

    watcher._ensure_staged(DEF_ID, None)  # must not raise


def test_ensure_staged_falls_back_to_jupyter_data_dir(tmp_path, monkeypatch):
    """Without a scheduler we still resolve the same path jupyter-scheduler uses."""
    watcher, _ = _make_watcher(tmp_path)
    watcher.scheduler = None
    monkeypatch.setattr(
        "jupyter_core.paths.jupyter_data_dir", lambda: str(tmp_path / "jdd")
    )

    watcher._ensure_staged(DEF_ID, NOTEBOOK)

    assert (tmp_path / "jdd" / "scheduler_staging_area" / DEF_ID / NOTEBOOK).exists()


def test_upsert_job_definition_stages_notebook(tmp_path):
    """End to end: a YAML-defined job definition leaves a staged input behind."""
    orm = pytest.importorskip("jupyter_scheduler.orm")

    db_path = tmp_path / "scheduler.sqlite"
    db_url = f"sqlite:///{db_path}"
    orm.create_tables(db_url)

    watcher, _ = _make_watcher(tmp_path, db_url=db_url)
    watcher._upsert_job_definition(
        {
            "name": "Marimo_Scheduler_test",
            "input_filename": NOTEBOOK,
            "schedule": "* * * * *",
            "timezone": "UTC",
            "output_formats": ["html"],
            "active": True,
        }
    )

    with orm.create_session(db_url)() as session:
        record = (
            session.query(orm.JobDefinition)
            .filter_by(name="Marimo_Scheduler_test")
            .one()
        )
        job_definition_id = record.job_definition_id

    staged = tmp_path / "staging" / job_definition_id / NOTEBOOK
    assert staged.exists(), "cron tick would raise FileNotFoundError in validate()"
