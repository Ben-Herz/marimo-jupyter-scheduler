# marimo-jupyter-scheduler — Agent Instructions

## Project overview

A JupyterLab extension that schedules Marimo notebooks (.py files) from within JupyterLab.
Built on top of `jupyter-scheduler` (provides SQLAlchemy job store, APScheduler cron engine, REST API).
Our layer adds Marimo execution, YAML config-as-code, and an enhanced dashboard UI.

## Key architecture

- **`jupyter-scheduler`** handles: job CRUD, SQLite/PostgreSQL storage, APScheduler cron, base REST API.
- **We override** execution, environment, task runner, and scheduler classes via `~/.jupyter/jupyter_server_config.py`.

```python
c.SchedulerApp.scheduler_class        = "marimo_jupyter_scheduler.scheduler.MarimoScheduler"
c.Scheduler.execution_manager_class   = "marimo_jupyter_scheduler.executor.MarimoExecutionManager"
c.Scheduler.task_runner_class         = "marimo_jupyter_scheduler.task_runner.FixedTaskRunner"
c.SchedulerApp.environment_manager_class = "marimo_jupyter_scheduler.environment.MarimoEnvironmentManager"
```

## Python package — `marimo_jupyter_scheduler/`

| File | Purpose |
|------|---------|
| `executor.py` | `MarimoExecutionManager` — runs `marimo export html/md` or `python` subprocess; injects params as `MARIMO_PARAM_*` env vars; supports `_last_run`, `_env`, `_python`, `_timeout` special params |
| `scheduler.py` | `MarimoScheduler` — fixes `copy_input_file` (creates staging dir, fallback path) and `update_job_definition` (applies output_formats/parameters/tags/name which jupyter-scheduler silently ignores) |
| `task_runner.py` | `FixedTaskRunner` / `FixedCache` — fixes jupyter-scheduler in-memory SQLite threading bug using SQLAlchemy `StaticPool` |
| `environment.py` | `MarimoEnvironmentManager` — registers `html`, `script`, `md` as valid output formats |
| `yaml_jobs.py` | YAML parser with `${VAR}` / `${TODAY}` substitution |
| `yaml_watcher.py` | watchdog file watcher — auto-imports `*.marimo-schedule.yml`; startup sync uses `create_only=True` so GUI edits survive restarts |
| `handlers.py` | Extra Tornado endpoints: `/marimo-scheduler/api/v1/dashboard`, `/yaml-import`, `/yaml-files` |

## Frontend — `src/`

| File | Purpose |
|------|---------|
| `index.ts` | Plugin entry point, registers launcher icon (`LabIcon` with inline SVG) |
| `dashboard.tsx` | Main React dashboard: definitions table, job runs table, YAML editor tab |
| `api.ts` | TypeScript API client |
| `components/DefinitionEditor.tsx` | Inline YAML editor for job definitions; minimal line-by-line YAML parser |
| `components/JobsTable.tsx` | Job runs table with expandable error rows |
| `components/StatusBadge.tsx` | Status badge with JP CSS variable colours |
| `style/index.css` | All styles using `--jp-*` CSS variables (no hardcoded hex) |

## YAML schedule format

```yaml
version: "1"
schedules:
  - name: my-job
    notebook: notebooks/report.py
    cron: "*/10 * * * *"
    timezone: "Europe/Berlin"
    output_formats:
      - html        # marimo export html
      # - script    # python <notebook.py> (no output file)
      # - md        # marimo export md
    parameters:
      date: "${TODAY}"          # substituted at import time
      _last_run: "LAST_RUN_AT"  # injects last COMPLETED run's end_time as MARIMO_PARAM_LAST_RUN_AT
      _python: "/path/to/venv/bin/python"  # optional interpreter override
      _timeout: "3600"          # subprocess timeout in seconds
    tags:
      - daily
    enabled: true
```

Auto-detected on startup: any `*.marimo-schedule.yml` file in the workspace. Startup sync is `create_only=True` — existing DB definitions are not overwritten (GUI edits survive restarts).

## Special executor parameters

| Parameter | Behaviour |
|-----------|-----------|
| `_last_run: "<NAME>"` | Injects last COMPLETED run's `end_time` as `MARIMO_PARAM_<NAME>` (ISO-8601 UTC). Not set on first run. |
| `_env: {KEY: value}` | Injected as plain env vars (no prefix) |
| `_python: "/path"` | Overrides Python interpreter / marimo binary |
| `_timeout: 3600` | subprocess timeout in seconds |

## Build & run

```bash
source .venv/bin/activate
jlpm run build:lib && jlpm run build:labextension:dev   # frontend
pip install -e ".[dev]"                                  # Python package
jupyter lab                                              # start server
```

## Known quirks / bugs worked around

- **jupyter-scheduler `update_job_definition` bug**: silently ignores `output_formats`, `parameters`, `tags` if `schedule`/`timezone`/`active` are unchanged → fixed in `MarimoScheduler.update_job_definition`.
- **In-memory SQLite threading bug in `Cache`**: fixed with `StaticPool` in `FixedCache`.
- **`validate()` called as unbound method** with 3 args in jupyter-scheduler 2.12 → fixed with `*args`.
- **`copy_input_file` staging directory**: fsspec doesn't create parent dirs → fixed in `MarimoScheduler.copy_input_file`.
- **Task runner cache populated before YAML watcher inserts**: fixed by calling `task_runner.add_job_definition()` after each upsert, with exponential retry for "no such table" errors.
