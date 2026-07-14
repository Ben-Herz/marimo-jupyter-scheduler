"""
Additional Tornado REST handlers for the marimo-jupyter-scheduler extension.

These supplement the handlers already provided by jupyter-scheduler.
Mounted at /marimo-scheduler/api/v1/.

Endpoints:
  GET  /marimo-scheduler/api/v1/dashboard   — aggregated job stats for the UI
  POST /marimo-scheduler/api/v1/yaml-import — validate & import a YAML schedule
  GET  /marimo-scheduler/api/v1/yaml-files  — list detected *.marimo-schedule.yml files
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import tornado.web
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)

# One Engine per database, reused across requests. create_session() builds a new
# Engine and connection pool on every call, which is wasteful when the database
# lives on NFS and each connect pays a round trip.
_SESSION_FACTORIES: dict = {}


def _get_session_factory(db_url: str):
    factory = _SESSION_FACTORIES.get(db_url)
    if factory is None:
        from jupyter_scheduler.orm import create_session

        factory = create_session(db_url)
        _SESSION_FACTORIES[db_url] = factory
    return factory


class DashboardHandler(APIHandler):
    """
    Returns aggregated job statistics for the dashboard panel.

    Response schema:
    {
      "total": 42,
      "by_status": {
        "COMPLETED": 30,
        "FAILED": 5,
        "IN_PROGRESS": 2,
        "STOPPED": 3,
        "QUEUED": 2
      },
      "recent_failures": [
        {
          "job_id": "...",
          "name": "...",
          "input_filename": "...",
          "start_time": "...",
          "end_time": "...",
          "status_message": "..."
        }
      ],
      "in_progress": [...]
    }
    """

    @tornado.web.authenticated
    async def get(self) -> None:
        try:
            # _gather_stats() hits SQLite, which on an NFS home directory can take
            # seconds. Running it inline would block the event loop for the whole
            # server, including JupyterHub's health probes.
            stats = await IOLoop.current().run_in_executor(None, self._gather_stats)
            self.finish(json.dumps(stats))
        except Exception as exc:
            logger.exception("DashboardHandler error")
            self.set_status(500)
            self.finish(json.dumps({"error": str(exc)}))

    def _gather_stats(self) -> dict:
        try:
            from jupyter_scheduler.orm import Job
        except ImportError:
            return {
                "total": 0,
                "by_status": {},
                "recent_failures": [],
                "in_progress": [],
                "warning": "jupyter-scheduler ORM not available",
            }

        from sqlalchemy import func

        session_factory = _get_session_factory(self._get_db_url())
        with session_factory() as session:
            # Aggregate in SQL rather than loading every job row into memory.
            by_status = {
                str(status): count
                for status, count in session.query(Job.status, func.count(Job.job_id)).group_by(
                    Job.status
                )
            }

            def _recent(status: str, limit: int) -> list[dict]:
                rows = (
                    session.query(Job)
                    .filter(Job.status == status)
                    .order_by(Job.start_time.desc())
                    .limit(limit)
                )
                return [self._job_summary(job) for job in rows]

            return {
                "total": sum(by_status.values()),
                "by_status": by_status,
                "recent_failures": _recent("FAILED", 20),
                "in_progress": _recent("IN_PROGRESS", 100),
            }

    def _job_summary(self, job) -> dict:
        return {
            "job_id": str(job.job_id),
            "name": str(job.name or ""),
            "input_filename": str(job.input_filename or ""),
            "status": str(job.status),
            "status_message": str(job.status_message or ""),
            "start_time": self._fmt_time(job.start_time),
            "end_time": self._fmt_time(job.end_time),
        }

    @staticmethod
    def _fmt_time(value) -> str | None:
        if value is None:
            return None
        if isinstance(value, int):
            from datetime import datetime, timezone
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
        return value.isoformat()

    def _get_db_url(self) -> str:
        try:
            return self.settings["scheduler"].db_url
        except (KeyError, AttributeError):
            from jupyter_core.paths import jupyter_data_dir
            return f"sqlite:///{jupyter_data_dir()}/scheduler.sqlite"


class YamlImportHandler(APIHandler):
    """
    Validate and import a YAML schedule definition.

    POST body: { "content": "<yaml string>" }
             or { "path": "<relative path to .yml file>" }

    Response: { "imported": 3, "jobs": [...] }
             or { "error": "..." } on parse failure
    """

    @tornado.web.authenticated
    def post(self) -> None:
        try:
            body = json.loads(self.request.body.decode("utf-8"))
            job_defs = self._parse_body(body)
            imported = 0
            for job_def in job_defs:
                self._upsert_job_definition(job_def)
                imported += 1
            self.finish(json.dumps({"imported": imported, "jobs": job_defs}))
        except ValueError as exc:
            self.set_status(400)
            self.finish(json.dumps({"error": str(exc)}))
        except Exception as exc:
            logger.exception("YamlImportHandler error")
            self.set_status(500)
            self.finish(json.dumps({"error": str(exc)}))

    def _parse_body(self, body: dict) -> list[dict]:
        from .yaml_jobs import parse_schedule_file, YamlParseError
        import tempfile, os

        if "content" in body:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".marimo-schedule.yml", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(body["content"])
                tmp_path = tmp.name
            try:
                jobs = parse_schedule_file(Path(tmp_path))
            except YamlParseError as exc:
                raise ValueError(str(exc)) from exc
            finally:
                os.unlink(tmp_path)
        elif "path" in body:
            root_dir = self.settings.get("server_root_dir", ".")
            file_path = Path(root_dir) / body["path"]
            if not file_path.exists():
                raise ValueError(f"File not found: {body['path']}")
            try:
                jobs = parse_schedule_file(file_path)
            except YamlParseError as exc:
                raise ValueError(str(exc)) from exc
        else:
            raise ValueError("Request must contain 'content' or 'path'")

        return jobs

    def _upsert_job_definition(self, job_def: dict) -> None:
        from jupyter_scheduler.orm import JobDefinition, create_session

        from .yaml_jobs import serialize_parameters

        db_url = self._get_db_url()
        session_factory = create_session(db_url)

        # Only pass fields that exist as columns on the ORM model
        valid_columns = {c.key for c in JobDefinition.__table__.columns}
        record_data = {
            k: v for k, v in job_def.items()
            if not k.startswith("_") and k in valid_columns
        }
        if "parameters" in record_data and isinstance(record_data["parameters"], dict):
            record_data["parameters"] = serialize_parameters(record_data["parameters"])
        record_data.setdefault("runtime_environment_name", "")

        with session_factory() as session:
            existing = (
                session.query(JobDefinition)
                .filter_by(name=record_data["name"])
                .first()
            )
            if existing is None:
                record = JobDefinition(**record_data)
                session.add(record)
                session.flush()
                job_definition_id = record.job_definition_id
                is_new = True
                logger.info("YamlImportHandler: created job definition '%s'", record_data["name"])
            else:
                for key, value in record_data.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                job_definition_id = existing.job_definition_id
                is_new = False
                logger.info("YamlImportHandler: updated job definition '%s'", record_data["name"])
            session.commit()

        self._notify_task_runner(job_definition_id, is_new, record_data)

    def _notify_task_runner(
        self, job_definition_id: str, is_new: bool, record_data: dict, attempt: int = 0
    ) -> None:
        import threading
        try:
            scheduler = self.settings.get("scheduler")
            task_runner = getattr(scheduler, "task_runner", None)
            if task_runner is None:
                return
            if is_new:
                task_runner.add_job_definition(job_definition_id)
            else:
                from jupyter_scheduler.models import UpdateJobDefinition
                patch = UpdateJobDefinition(
                    schedule=record_data.get("schedule"),
                    timezone=record_data.get("timezone"),
                    active=record_data.get("active"),
                )
                task_runner.update_job_definition(job_definition_id, patch)
            logger.debug("YamlImportHandler: notified task_runner for '%s'", job_definition_id)
        except Exception as exc:
            if "no such table" in str(exc).lower() and attempt < 5:
                delay = 3 * (attempt + 1)
                settings = self.settings
                def _retry():
                    self._notify_task_runner(job_definition_id, is_new, record_data, attempt + 1)
                t = threading.Timer(delay, _retry)
                t.daemon = True
                t.start()
            else:
                logger.warning("YamlImportHandler: could not notify task_runner: %s", exc)

    def _get_db_url(self) -> str:
        try:
            return self.settings["scheduler"].db_url
        except (KeyError, AttributeError):
            from jupyter_core.paths import jupyter_data_dir
            return f"sqlite:///{jupyter_data_dir()}/scheduler.sqlite"


class YamlFilesHandler(APIHandler):
    """Return all detected *.marimo-schedule.yml files in the workspace."""

    @tornado.web.authenticated
    def get(self) -> None:
        from .yaml_jobs import find_schedule_files

        root_dir = self.settings.get("server_root_dir", ".")
        files = find_schedule_files(root_dir)
        self.finish(
            json.dumps({
                "files": [str(p.relative_to(root_dir)) for p in files]
            })
        )


def setup_handlers(web_app) -> None:
    """Register handlers with the Tornado web application."""
    base_url = web_app.settings.get("base_url", "/")

    def url(path: str) -> str:
        return url_path_join(base_url, "marimo-scheduler", "api", "v1", path)

    handlers = [
        (url("dashboard"), DashboardHandler),
        (url("yaml-import"), YamlImportHandler),
        (url("yaml-files"), YamlFilesHandler),
    ]
    web_app.add_handlers(".*$", handlers)
    logger.info(
        "marimo-jupyter-scheduler: registered handlers at %s",
        url_path_join(base_url, "marimo-scheduler/api/v1/"),
    )
