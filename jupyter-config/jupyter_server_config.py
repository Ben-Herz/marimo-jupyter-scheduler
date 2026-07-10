"""Default jupyter-scheduler class overrides for marimo-jupyter-scheduler.

Installed by pip into {sys.prefix}/etc/jupyter/jupyter_server_config.py.

These cannot live in jupyter-config/server-config/*.json: jupyter_server reads
the jupyter_server_config.d directory with ExtensionConfigManager, which only
picks up `jpserver_extensions` and ignores every other trait.

Environment-level config is loaded before the user's ~/.jupyter and /etc/jupyter,
so anything set there still wins over these defaults.
"""

# `c` is injected by traitlets when this file is executed as config.
c = get_config()  # noqa: F821

# Routes .py notebooks to marimo and .ipynb to nbconvert. Without this, the
# stock validate() tries to parse a marimo .py file as JSON and the job never
# runs (NotJSONError, swallowed by TaskRunner.process_queue).
c.Scheduler.execution_manager_class = (
    "marimo_jupyter_scheduler.executor.RoutingExecutionManager"
)

# Creates the staging directory before copying, and falls back to the notebook
# under root_dir when a definition's staged snapshot is missing.
c.SchedulerApp.scheduler_class = "marimo_jupyter_scheduler.scheduler.MarimoScheduler"

# Works around jupyter-scheduler's in-memory SQLite cache being unusable from
# the event loop thread (sqlite3.OperationalError: no such table).
c.Scheduler.task_runner_class = "marimo_jupyter_scheduler.task_runner.FixedTaskRunner"

# Advertises marimo's output formats (html, script).
c.SchedulerApp.environment_manager_class = (
    "marimo_jupyter_scheduler.environment.MarimoEnvironmentManager"
)
