# marimo-jupyter-scheduler

A JupyterLab extension that schedules [Marimo](https://marimo.io) notebooks
as recurring jobs inside JupyterLab and JupyterHub environments.

Built on top of the official
[jupyter-scheduler](https://github.com/jupyter-server/jupyter-scheduler) package,
so it inherits a battle-tested REST API, APScheduler cron engine, and
SQLAlchemy-backed job store (SQLite by default, PostgreSQL optional).

---

## Features

| Feature | Description |
|---------|-------------|
| **Marimo-native executor** | Runs `.py` Marimo notebooks via `marimo export html` or as plain Python scripts |
| **YAML schedules** | Define jobs in `*.marimo-schedule.yml` files (GitHub Actions syntax) — auto-detected on startup |
| **GUI dashboard** | JupyterLab panel showing running / failed / completed jobs with live refresh |
| **SQLite / PostgreSQL** | Switch backends via a single env var (`SCHEDULER_DB_URL`) |
| **Parameter injection** | Pass parameters to notebooks as `MARIMO_PARAM_*` env vars + `${TODAY}` / `${VAR}` substitution |
| **Last-run injection** | Optionally pass the datetime of the last successful run as a parameter (`_last_run`) |

---

## Installation (from source)

Prerequisites: Node ≥ 18, Python ≥ 3.9, JupyterLab 4.

```bash
pip install jupyterlab hatch
npm install -g jlpm

# Clone
git clone https://github.com/your-org/marimo-jupyter-scheduler
cd marimo-jupyter-scheduler

# Install Python deps + build the frontend
pip install -e ".[dev]"
jlpm install
jlpm run build

# Enable the extension
jupyter labextension develop --overwrite .
jupyter server extension enable marimo_jupyter_scheduler
```

### Configure the Marimo executor

Add to `~/.jupyter/jupyter_server_config.py`:

```python
c.SchedulerApp.scheduler_class = (
    "marimo_jupyter_scheduler.scheduler.MarimoScheduler"
)
c.Scheduler.execution_manager_class = (
    "marimo_jupyter_scheduler.executor.MarimoExecutionManager"
)
c.Scheduler.task_runner_class = (
    "marimo_jupyter_scheduler.task_runner.FixedTaskRunner"
)
c.SchedulerApp.environment_manager_class = (
    "marimo_jupyter_scheduler.environment.MarimoEnvironmentManager"
)
# Optional: use PostgreSQL
# c.Scheduler.db_url = "postgresql+psycopg2://user:pass@host:5432/scheduler"
```

---

## Defining schedules

### Option A — YAML file (config-as-code)

Create a `*.marimo-schedule.yml` file anywhere in your workspace:

```yaml
version: "1"

schedules:
  - name: daily-sales-report
    notebook: notebooks/sales_report.py
    cron: "0 9 * * 1-5"       # weekdays at 09:00
    timezone: "Europe/Berlin"
    output_formats:
      - html
    parameters:
      date: "${TODAY}"         # substituted at runtime
      _last_run: "LAST_RUN_AT" # injects last successful run's datetime as MARIMO_PARAM_LAST_RUN_AT
    tags:
      - daily
    enabled: true
```

The extension auto-detects and imports this file on startup and on every save.

### Option B — GUI

1. Open the **Marimo Scheduler** panel from the command palette
   (`Ctrl+Shift+P` → "Open Marimo Scheduler Dashboard")
2. Switch to the **YAML Schedules** tab to paste/edit YAML
3. Or use the built-in jupyter-scheduler UI to create jobs directly

---

## Architecture

```
marimo-jupyter-scheduler/
├── marimo_jupyter_scheduler/       # Python backend
│   ├── executor.py                 # MarimoExecutionManager (core)
│   ├── scheduler.py                # MarimoScheduler (fixes copy_input_file + update_job_definition)
│   ├── task_runner.py              # FixedTaskRunner (fixes SQLite threading bug)
│   ├── environment.py              # MarimoEnvironmentManager (registers output formats)
│   ├── yaml_jobs.py                # YAML parser + ${VAR} substitution
│   ├── yaml_watcher.py             # File watcher (watchdog)
│   └── handlers.py                 # Extra REST endpoints (/marimo-scheduler/api/v1/)
├── src/                            # TypeScript / React frontend
│   ├── index.ts                    # JupyterLab plugin entry point
│   ├── dashboard.tsx               # Dashboard React component
│   ├── api.ts                      # API client
│   └── components/
│       ├── JobsTable.tsx
│       ├── DefinitionEditor.tsx
│       └── StatusBadge.tsx
└── examples/
    ├── sample.marimo-schedule.yml
    └── sales_report.py             # Parameterised Marimo notebook demo
```

### How the executor works

`MarimoExecutionManager` subclasses `jupyter_scheduler.executors.ExecutionManager`.
When a scheduled job fires, it:

1. Resolves the notebook path relative to `root_dir`
2. Injects parameters as `MARIMO_PARAM_<NAME>=<value>` environment variables
3. Runs `marimo export html <notebook.py> -o <output.html>` (or Python in script mode)
4. Writes a `.log` sidecar with stdout/stderr
5. Raises `RuntimeError` on non-zero exit → jupyter-scheduler marks the job FAILED

### Special parameters

| Parameter | Behaviour |
|-----------|-----------|
| `_last_run: "<NAME>"` | Injects last COMPLETED run's `end_time` as `MARIMO_PARAM_<NAME>` (ISO-8601 UTC) |
| `_env: {KEY: value}` | Injected as plain env vars (no `MARIMO_PARAM_` prefix) |
| `_python: "/path"` | Overrides the Python interpreter / marimo binary for this job |
| `_timeout: 3600` | Subprocess timeout in seconds (default: 3600) |

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_DB_URL` | `sqlite:///~/.jupyter-scheduler.db` | SQLAlchemy database URL |
| `SCHEDULER_MAX_CONCURRENT` | `5` | Max parallel jobs |
| `LOG_LEVEL` | `INFO` | Python logging level |

---

## Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## JupyterHub deployment

In your JupyterHub `singleuser` image, add to `/etc/jupyter/jupyter_server_config.py`:

```python
import os
c.SchedulerApp.scheduler_class = (
    "marimo_jupyter_scheduler.scheduler.MarimoScheduler"
)
c.Scheduler.execution_manager_class = (
    "marimo_jupyter_scheduler.executor.MarimoExecutionManager"
)
c.Scheduler.task_runner_class = (
    "marimo_jupyter_scheduler.task_runner.FixedTaskRunner"
)
c.SchedulerApp.environment_manager_class = (
    "marimo_jupyter_scheduler.environment.MarimoEnvironmentManager"
)
c.Scheduler.db_url = os.environ.get(
    "SCHEDULER_DB_URL",
    "sqlite:////home/jovyan/.jupyter-scheduler.db"
)
```

For a shared PostgreSQL backend (so all Hub users share one job store),
set `SCHEDULER_DB_URL` to a PostgreSQL connection string in the JupyterHub
`singleuser` environment config.

---

## Publishing to PyPI

**1. Bump the version** in `package.json`:

```json
"version": "0.1.1"
```

**2. Build the production frontend:**

```bash
source .venv/bin/activate
jlpm run build:prod
```

**3. Build the distribution packages:**

```bash
pip install build
python -m build
```

This produces `dist/marimo_jupyter_scheduler-<version>.tar.gz` and `.whl`.

**4. Verify the wheel contains the labextension:**

```bash
pip install check-wheel-contents
check-wheel-contents dist/*.whl
unzip -l dist/*.whl | grep labextension
```

You should see `share/jupyter/labextensions/marimo-jupyter-scheduler/...` entries.

**5. (Optional) Test on TestPyPI first:**

```bash
pip install twine
twine upload --repository testpypi dist/*
pip install --index-url https://test.pypi.org/simple/ marimo-jupyter-scheduler
```

**6. Upload to PyPI:**

```bash
twine upload dist/*
```

---

## License

BSD 3-Clause. See [LICENSE](LICENSE).
