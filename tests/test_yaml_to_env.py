"""End-to-end: YAML file → JobDefinition row → DescribeJobDefinition → executor env.

The _last_run and _env unit tests call _build_env() directly with a hand-built
parameters dict, so they stayed green while both persistence paths silently
dropped those keys on the way into the database. These tests drive the whole
chain: parse the YAML, upsert it exactly as the watcher and the import handler
do, read the row back through jupyter-scheduler's pydantic model (which types
parameters as Dict[str, str]), and only then build the subprocess environment.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from jupyter_scheduler.models import DescribeJobDefinition
from jupyter_scheduler.orm import JobDefinition, create_session, create_tables

from marimo_jupyter_scheduler.executor import MarimoExecutionManager
from marimo_jupyter_scheduler.handlers import YamlImportHandler
from marimo_jupyter_scheduler.yaml_watcher import YamlScheduleWatcher

NOTEBOOK = "collabs/HDR_Tasks/W738_InverseReverse/w738_mail_data_task_mo.py"
NAME = "W738 Inverse Reverse"

SCHEDULE_YAML = f"""\
version: "1"

schedules:
  - name: {NAME}
    notebook: {NOTEBOOK}
    cron: "*/5 * * * *"
    timezone: "Europe/Berlin"
    output_formats:
      - html
    parameters:
      _last_run: "LAST_RUN_AT"
      region: "EMEA"
    env:
      DATABASE_URL: "postgres://example/db"
    enabled: true
"""

LAST_RUN = datetime(2024, 3, 15, 10, 30, tzinfo=timezone.utc)


@pytest.fixture
def workspace(tmp_path):
    """A root dir holding the notebook and a schedule file, plus an empty DB."""
    root = tmp_path / "root"
    notebook = root / NOTEBOOK
    notebook.parent.mkdir(parents=True)
    notebook.write_text("import marimo\n")
    (root / "w738.marimo-schedule.yml").write_text(SCHEDULE_YAML)

    db_url = f"sqlite:///{tmp_path / 'scheduler.sqlite'}"
    create_tables(db_url)

    return SimpleNamespace(
        root=root,
        db_url=db_url,
        staging=tmp_path / "staging",
    )


def _sync_via_watcher(workspace) -> None:
    scheduler = SimpleNamespace(staging_path=str(workspace.staging), task_runner=None)
    watcher = YamlScheduleWatcher(
        root_dir=str(workspace.root), db_url=workspace.db_url, scheduler=scheduler
    )
    watcher._sync_all()


def _sync_via_import_handler(workspace) -> None:
    # RequestHandler.settings is a read-only proxy for application.settings
    handler = YamlImportHandler.__new__(YamlImportHandler)
    handler.application = SimpleNamespace(
        settings={"scheduler": SimpleNamespace(db_url=workspace.db_url, task_runner=None)}
    )

    from marimo_jupyter_scheduler.yaml_jobs import parse_schedule_file

    for job_def in parse_schedule_file(workspace.root / "w738.marimo-schedule.yml"):
        handler._upsert_job_definition(job_def)


def _read_back_parameters(workspace) -> dict:
    """Load the stored definition through the same pydantic model the scheduler uses."""
    session_factory = create_session(workspace.db_url)
    with session_factory() as session:
        record = session.query(JobDefinition).filter_by(name=NAME).one()
        described = DescribeJobDefinition.from_orm(record)
    return dict(described.parameters or {})


def _build_env(parameters: dict) -> dict:
    manager = MarimoExecutionManager.__new__(MarimoExecutionManager)
    manager.job_id = "job-1"
    manager.root_dir = "/tmp"
    manager.db_url = "sqlite://"
    manager.staging_paths = {}
    manager._db_session = None

    model = MagicMock()
    model.parameters = dict(parameters)
    type(manager).model = PropertyMock(return_value=model)

    with patch.object(manager, "_get_last_run_time", return_value=LAST_RUN.isoformat()):
        return manager._build_env(dict(parameters))


@pytest.mark.parametrize(
    "sync", [_sync_via_watcher, _sync_via_import_handler], ids=["watcher", "import_handler"]
)
def test_special_parameters_survive_persistence(workspace, sync):
    sync(workspace)

    parameters = _read_back_parameters(workspace)

    assert parameters["_last_run"] == "LAST_RUN_AT"
    assert json.loads(parameters["_env"]) == {"DATABASE_URL": "postgres://example/db"}
    assert parameters["region"] == "EMEA"


@pytest.mark.parametrize(
    "sync", [_sync_via_watcher, _sync_via_import_handler], ids=["watcher", "import_handler"]
)
def test_stored_parameters_reach_the_notebook_environment(workspace, sync):
    sync(workspace)

    env = _build_env(_read_back_parameters(workspace))

    assert env["MARIMO_PARAM_LAST_RUN_AT"] == LAST_RUN.isoformat()
    assert env["DATABASE_URL"] == "postgres://example/db"
    assert env["MARIMO_PARAM_REGION"] == "EMEA"


@pytest.mark.parametrize(
    "sync", [_sync_via_watcher, _sync_via_import_handler], ids=["watcher", "import_handler"]
)
def test_internal_keys_are_not_leaked_as_notebook_parameters(workspace, sync):
    sync(workspace)

    env = _build_env(_read_back_parameters(workspace))

    assert "MARIMO_PARAM__LAST_RUN" not in env
    assert "MARIMO_PARAM__ENV" not in env
