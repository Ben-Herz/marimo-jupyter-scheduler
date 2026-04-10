"""
marimo-jupyter-scheduler: Schedule Marimo notebooks from JupyterLab/JupyterHub.

This extension wraps jupyter-scheduler and provides:
  - MarimoExecutionManager: runs .py Marimo notebooks via subprocess
  - YAML-based job definitions (GitHub Actions style)
  - File watcher that auto-imports schedule YAML files into the job database
  - Enhanced REST endpoints for the dashboard
"""

from ._version import __version__


def _jupyter_labextension_paths():
    """Called by Jupyter to discover the labextension."""
    return [{"src": "labextension", "dest": "marimo-jupyter-scheduler"}]


def _jupyter_server_extension_points():
    """Called by Jupyter to discover the server extension."""
    return [{"module": "marimo_jupyter_scheduler"}]




def _load_jupyter_server_extension(server_app):
    """
    Called when the extension is loaded.

    Registers additional Tornado handlers and starts the YAML file watcher.
    The core scheduler functionality comes from jupyter_scheduler itself;
    we only add Marimo-specific endpoints here.
    """
    from .handlers import setup_handlers
    from .yaml_watcher import YamlScheduleWatcher

    web_app = server_app.web_app
    setup_handlers(web_app)

    # Start watching for *.marimo-schedule.yml files in the root dir
    root_dir = getattr(server_app, "root_dir", ".")
    try:
        _scheduler = server_app.web_app.settings.get("scheduler")
        _db_url = _scheduler.db_url if _scheduler else None
    except Exception:
        _db_url = None

    if not _db_url:
        from jupyter_core.paths import jupyter_data_dir
        _db_url = f"sqlite:///{jupyter_data_dir()}/scheduler.sqlite"
    db_url = _db_url
    try:
        scheduler = server_app.web_app.settings.get("scheduler")
        watcher = YamlScheduleWatcher(root_dir=root_dir, db_url=db_url, scheduler=scheduler)
        watcher.start()
        server_app.io_loop.add_callback(lambda: None)  # ensure event loop is running
    except Exception as e:
        server_app.log.warning(f"marimo-jupyter-scheduler: YAML watcher failed to start: {e}")

    # Log task runner status so we can confirm the fix is active
    try:
        _sched = server_app.web_app.settings.get("scheduler")
        _tr = getattr(_sched, "task_runner", None)
        server_app.log.info(
            "marimo-jupyter-scheduler: task_runner is %s",
            type(_tr).__name__ if _tr else "None",
        )
    except Exception:
        pass

    server_app.log.info(
        "marimo-jupyter-scheduler extension loaded. "
        "Configure jupyter-scheduler with:\n"
        "  c.Scheduler.execution_manager_class = "
        "'marimo_jupyter_scheduler.executor.MarimoExecutionManager'"
    )
