"""
File system watcher that auto-imports *.marimo-schedule.yml files
into the jupyter-scheduler job definition database.

When a YAML file is created or modified, the watcher:
  1. Parses the file into one or more JobDefinition dicts
  2. Compares against existing definitions (by name)
  3. Creates new definitions or updates existing ones via the SQLAlchemy ORM

This allows a "config-as-code" workflow: commit YAML files to git and
JupyterHub will automatically pick up the schedule changes on next startup
or whenever the files change.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Watchdog is optional; if not available we fall back to periodic polling
try:
    from watchdog.events import FileSystemEvent, PatternMatchingEventHandler
    from watchdog.observers import Observer

    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False
    logger.warning(
        "watchdog not installed; YAML schedule watcher will use polling (every 60s). "
        "Install with: pip install watchdog"
    )


class YamlScheduleWatcher:
    """
    Watches for *.marimo-schedule.yml changes and syncs them to the scheduler DB.

    Usage::

        watcher = YamlScheduleWatcher(root_dir="/home/jovyan", db_url="sqlite:///...")
        watcher.start()
        # ... later ...
        watcher.stop()
    """

    def __init__(self, root_dir: str, db_url: str, poll_interval: int = 60, scheduler: Any = None):
        self.root_dir = root_dir
        self.db_url = db_url
        self.poll_interval = poll_interval
        self.scheduler = scheduler
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._observer: Any = None

    def start(self) -> None:
        """Start the watcher in a background thread.

        The watchdog observer setup is intentionally deferred to a background
        thread. On large or slow filesystems (NFS, CIFS) watchdog's InotifyObserver
        walks the entire directory tree synchronously to register inotify watches,
        which can block for minutes and prevent the server from starting.
        """
        # Defer the initial scan AND watcher setup so neither blocks server startup.
        def _deferred_start() -> None:
            # Give jupyter-scheduler time to create its DB schema.
            self._stop_event.wait(timeout=5)
            if self._stop_event.is_set():
                return
            # Start the file watcher.
            if _WATCHDOG_AVAILABLE:
                self._start_watchdog()
            else:
                self._start_polling()
            # Run the initial sync.
            self._sync_all(create_only=True)

        t = threading.Thread(target=_deferred_start, daemon=True, name="marimo-yaml-initial-sync")
        t.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._observer:
            self._observer.stop()
            self._observer.join()
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Watchdog-based watching (preferred)
    # ------------------------------------------------------------------

    def _start_watchdog(self) -> None:
        handler = _YamlEventHandler(watcher=self)
        self._observer = Observer()
        self._observer.schedule(handler, self.root_dir, recursive=True)
        self._observer.start()
        logger.info(
            "marimo-jupyter-scheduler: watching %s for *.marimo-schedule.yml (watchdog)",
            self.root_dir,
        )

    # ------------------------------------------------------------------
    # Polling fallback
    # ------------------------------------------------------------------

    def _start_polling(self) -> None:
        def _loop() -> None:
            while not self._stop_event.wait(timeout=self.poll_interval):
                self._sync_all()

        self._thread = threading.Thread(target=_loop, daemon=True, name="marimo-yaml-watcher")
        self._thread.start()
        logger.info(
            "marimo-jupyter-scheduler: polling %s every %ds for *.marimo-schedule.yml",
            self.root_dir,
            self.poll_interval,
        )

    # ------------------------------------------------------------------
    # Core sync logic
    # ------------------------------------------------------------------

    def _sync_all(self, create_only: bool = False) -> None:
        """Scan all YAML files under root_dir and sync to the DB.

        Args:
            create_only: If True, only create missing definitions — do not
                overwrite existing ones. Used on startup so that GUI edits
                (e.g. schedule changes) survive a server restart.
        """
        from .yaml_jobs import find_schedule_files, parse_schedule_file, YamlParseError

        files = find_schedule_files(self.root_dir)
        for path in files:
            try:
                jobs = parse_schedule_file(path)
                for job_def in jobs:
                    self._upsert_job_definition(job_def, create_only=create_only)
            except YamlParseError as exc:
                logger.warning("marimo-jupyter-scheduler: YAML parse error: %s", exc)
            except Exception as exc:
                logger.warning(
                    "marimo-jupyter-scheduler: unexpected error processing %s: %s",
                    path,
                    exc,
                    exc_info=True,
                )

    def _upsert_job_definition(self, job_def: dict[str, Any], create_only: bool = False) -> None:
        """
        Create or update a JobDefinition in the scheduler database.

        Args:
            create_only: If True and the definition already exists, skip the
                update so GUI edits are not overwritten on restart.
        """
        try:
            from jupyter_scheduler.orm import JobDefinition, create_session

            session_factory = create_session(self.db_url)
            with session_factory() as session:
                existing = (
                    session.query(JobDefinition)
                    .filter_by(name=job_def["name"])
                    .first()
                )

                # Only pass fields that exist as columns on the ORM model;
                # flatten parameters so all values are strings (pydantic requires it)
                valid_columns = {c.key for c in JobDefinition.__table__.columns}
                job_def_clean = {
                    k: v for k, v in job_def.items()
                    if not k.startswith("_") and k in valid_columns
                }
                if "parameters" in job_def_clean and isinstance(job_def_clean["parameters"], dict):
                    job_def_clean["parameters"] = {
                        k: str(v) for k, v in job_def_clean["parameters"].items()
                        if not isinstance(v, dict)
                    }
                job_def_clean.setdefault("runtime_environment_name", "")

                if existing is None:
                    record = JobDefinition(**job_def_clean)
                    session.add(record)
                    session.flush()
                    job_definition_id = record.job_definition_id
                    is_new = True
                    logger.info(
                        "marimo-jupyter-scheduler: created job definition '%s'",
                        job_def["name"],
                    )
                elif create_only:
                    # Don't overwrite — the user may have edited this via the GUI
                    logger.debug(
                        "marimo-jupyter-scheduler: skipping update for existing '%s' (create_only)",
                        job_def["name"],
                    )
                    job_definition_id = existing.job_definition_id
                    is_new = False
                    # Still register with task_runner in case it missed it on startup
                    session.commit()
                    self._notify_task_runner(job_definition_id, is_new, job_def_clean)
                    return
                else:
                    for key, value in job_def_clean.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    job_definition_id = existing.job_definition_id
                    is_new = False
                    logger.debug(
                        "marimo-jupyter-scheduler: updated job definition '%s'",
                        job_def["name"],
                    )

                session.commit()

            self._notify_task_runner(job_definition_id, is_new, job_def_clean)

        except ImportError:
            logger.debug(
                "jupyter-scheduler ORM not available; skipping DB upsert for '%s'",
                job_def.get("name"),
            )
        except Exception as exc:
            # OperationalError("no such table") means jupyter-scheduler hasn't
            # created its schema yet — log at debug level, the watcher will retry.
            msg = str(exc).lower()
            level = logger.debug if "no such table" in msg else logger.warning
            level(
                "marimo-jupyter-scheduler: failed to upsert job definition '%s': %s",
                job_def.get("name"),
                exc,
            )

    def _notify_task_runner(
        self, job_definition_id: str, is_new: bool, job_def: dict, attempt: int = 0
    ) -> None:
        """Tell the task runner about a new or updated job definition.

        If the cache table doesn't exist yet (jupyter-scheduler still starting up),
        retries up to 5 times with a 3-second backoff.
        """
        try:
            task_runner = getattr(self.scheduler, "task_runner", None)
            if task_runner is None:
                return
            if is_new:
                task_runner.add_job_definition(job_definition_id)
            else:
                from jupyter_scheduler.models import UpdateJobDefinition
                patch = UpdateJobDefinition(
                    schedule=job_def.get("schedule"),
                    timezone=job_def.get("timezone"),
                    active=job_def.get("active"),
                )
                task_runner.update_job_definition(job_definition_id, patch)
            logger.debug(
                "marimo-jupyter-scheduler: notified task_runner for '%s' (new=%s)",
                job_definition_id,
                is_new,
            )
        except Exception as exc:
            if "no such table" in str(exc).lower() and attempt < 5:
                delay = 3 * (attempt + 1)
                logger.debug(
                    "marimo-jupyter-scheduler: task_runner cache not ready, retrying in %ds (attempt %d)",
                    delay, attempt + 1,
                )
                t = threading.Timer(
                    delay,
                    self._notify_task_runner,
                    args=(job_definition_id, is_new, job_def, attempt + 1),
                )
                t.daemon = True
                t.start()
            else:
                logger.warning(
                    "marimo-jupyter-scheduler: could not notify task_runner: %s", exc
                )


if _WATCHDOG_AVAILABLE:

    class _YamlEventHandler(PatternMatchingEventHandler):
        def __init__(self, watcher: YamlScheduleWatcher):
            super().__init__(
                patterns=["*.marimo-schedule.yml"],
                ignore_patterns=None,
                ignore_directories=True,
                case_sensitive=False,
            )
            self._watcher = watcher

        def on_created(self, event: FileSystemEvent) -> None:
            logger.info("marimo-jupyter-scheduler: new schedule file: %s", event.src_path)
            self._watcher._sync_all()

        def on_modified(self, event: FileSystemEvent) -> None:
            logger.info("marimo-jupyter-scheduler: schedule file changed: %s", event.src_path)
            self._watcher._sync_all()

        def on_deleted(self, event: FileSystemEvent) -> None:
            logger.info(
                "marimo-jupyter-scheduler: schedule file removed: %s — "
                "existing job definitions are NOT auto-deleted for safety.",
                event.src_path,
            )
