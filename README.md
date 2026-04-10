# marimo-jupyter-scheduler

A JupyterLab extension that schedules [Marimo](https://marimo.io) notebooks
as recurring jobs inside JupyterHub Docker environments.

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
| **Docker-ready** | Multi-profile `docker-compose.yml` for SQLite and PostgreSQL stacks |

---

## Quick start (Docker)

```bash
# Clone and build
git clone https://github.com/your-org/marimo-jupyter-scheduler
cd marimo-jupyter-scheduler

# Start with SQLite (default)
docker compose -f docker/docker-compose.yml up

# Or with PostgreSQL
docker compose -f docker/docker-compose.yml --profile pg up
```

Open http://localhost:8888/?token=mydevtoken

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

Add to `~/.jupyter/jupyter_server_config.py` (or use the Docker image
which does this automatically):

```python
c.Scheduler.execution_manager_class = (
    "marimo_jupyter_scheduler.executor.MarimoExecutionManager"
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
    env:
      DATABASE_URL: "${DATABASE_URL}"
    tags:
      - daily
    enabled: true
```

The extension auto-detects and imports this file on startup (and on every
save if `watchdog` is installed).

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
│   ├── yaml_jobs.py                # YAML parser + ${VAR} substitution
│   ├── yaml_watcher.py             # File watcher (watchdog / polling)
│   └── handlers.py                 # Extra REST endpoints (/marimo-scheduler/api/v1/)
├── src/                            # TypeScript / React frontend
│   ├── index.ts                    # JupyterLab plugin entry point
│   ├── dashboard.tsx               # Dashboard React component
│   ├── api.ts                      # API client
│   └── components/
│       ├── JobsTable.tsx
│       └── StatusBadge.tsx
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml          # SQLite + PostgreSQL profiles
│   └── jupyter_server_config.py
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
c.Scheduler.execution_manager_class = (
    "marimo_jupyter_scheduler.executor.MarimoExecutionManager"
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

## License

BSD 3-Clause. See [LICENSE](LICENSE).
