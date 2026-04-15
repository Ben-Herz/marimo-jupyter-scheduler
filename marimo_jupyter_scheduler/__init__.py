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

    Everything that could block (DB access, filesystem access, watchdog setup)
    is deferred to a background thread so this function returns immediately.
    """
    import threading
    from .handlers import setup_handlers

    web_app = server_app.web_app
    setup_handlers(web_app)

    # Capture values we need in the background thread while still on the main thread.
    root_dir = getattr(server_app, "root_dir", ".")
    settings = web_app.settings

    def _background_init():
        import time
        from .yaml_watcher import YamlScheduleWatcher
        from jupyter_core.paths import jupyter_data_dir

        # Give jupyter-scheduler a moment to finish its own initialization
        # and populate web_app.settings["scheduler"].
        time.sleep(2)

        try:
            _scheduler = settings.get("scheduler")
            _db_url = getattr(_scheduler, "db_url", None) if _scheduler else None
        except Exception:
            _db_url = None

        if not _db_url:
            _db_url = f"sqlite:///{jupyter_data_dir()}/scheduler.sqlite"

        try:
            scheduler = settings.get("scheduler")
            watcher = YamlScheduleWatcher(root_dir=root_dir, db_url=_db_url, scheduler=scheduler)
            watcher.start()
        except Exception as e:
            server_app.log.warning(f"marimo-jupyter-scheduler: YAML watcher failed to start: {e}")

        try:
            _sched = settings.get("scheduler")
            _tr = getattr(_sched, "task_runner", None)
            server_app.log.info(
                "marimo-jupyter-scheduler: task_runner is %s",
                type(_tr).__name__ if _tr else "None",
            )
        except Exception:
            pass

    t = threading.Thread(target=_background_init, daemon=True, name="marimo-extension-init")
    t.start()

    server_app.log.info("marimo-jupyter-scheduler extension loaded.")
