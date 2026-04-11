"""
Execution managers for marimo-jupyter-scheduler.

RoutingExecutionManager (recommended):
    Routes jobs by file extension:
      - .py  → MarimoExecutionManager  (marimo export / python subprocess)
      - .ipynb → DefaultExecutionManager (nbconvert, original jupyter-scheduler behaviour)

    Configure in jupyter_server_config.py:
        c.Scheduler.execution_manager_class = (
            "marimo_jupyter_scheduler.executor.RoutingExecutionManager"
        )

MarimoExecutionManager:
    Handles only Marimo .py notebooks. Use RoutingExecutionManager instead
    unless you are certain no .ipynb jobs will be created.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict

from jupyter_scheduler.executors import DefaultExecutionManager, ExecutionManager
from jupyter_scheduler.models import JobFeature

logger = logging.getLogger(__name__)


class MarimoExecutionManager(ExecutionManager):
    """
    Execute Marimo notebooks as scheduled jobs.

    Marimo notebooks are plain Python files (.py). This executor supports
    two execution modes controlled by the job's output_formats:

    - "html"   → `marimo export html <notebook.py> -o <output.html>`
    - "script" → `python <notebook.py>` (no output file, just stdout/stderr)
    - default  → html

    Parameters passed to the job are injected as environment variables
    (prefixed MARIMO_PARAM_) so notebooks can read them via os.environ or
    mo.cli_args().
    """

    @classmethod
    def validate(cls, *args) -> bool:
        """Accept any input path — existence is checked at execution time.
        Accepts extra positional args to work around a jupyter-scheduler 2.x
        bug where validate() is called as an unbound method with the class
        passed as an explicit first argument."""
        return True

    @classmethod
    def supported_features(cls) -> Dict[JobFeature, bool]:
        return {
            JobFeature.job_name: True,
            JobFeature.parameters: True,
            JobFeature.output_formats: True,
            JobFeature.job_definition: True,
            JobFeature.tags: True,
            JobFeature.stop_job: True,
            JobFeature.delete_job: True,
            JobFeature.idempotency_token: False,
            JobFeature.email_notifications: False,
            JobFeature.timeout_seconds: False,
            JobFeature.retry_on_timeout: False,
            JobFeature.max_retries: False,
            JobFeature.min_retry_interval_millis: False,
            JobFeature.output_filename_template: False,
        }

    def execute(self) -> None:
        """
        Main execution entrypoint called by jupyter-scheduler in a thread.

        Job data is accessed via self.model (lazy DB query).
        self.staging_paths contains {"input": ..., "output-<fmt>": ...}.
        """
        job = self.model

        input_path = self._resolve_input(job)
        if not input_path.exists():
            raise FileNotFoundError(
                f"Marimo notebook not found: {input_path}. "
                "Check that the path is relative to the JupyterHub root directory."
            )

        output_formats: list[str] = list(job.output_formats or ["html"])
        parameters: dict = dict(job.parameters or {})
        env = self._build_env(parameters)

        for fmt in output_formats:
            if fmt == "html":
                self._run_marimo_export(input_path, fmt="html", env=env, job=job)
            elif fmt in ("md", "markdown"):
                self._run_marimo_export(input_path, fmt="md", env=env, job=job)
            elif fmt == "script":
                self._run_as_script(input_path, env=env, job=job)
            else:
                logger.warning("Unsupported output format '%s', falling back to html", fmt)
                self._run_marimo_export(input_path, fmt="html", env=env, job=job)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_input(self, job) -> Path:
        p = Path(job.input_filename)
        if p.is_absolute():
            return p
        return Path(self.root_dir) / p

    def _build_env(self, parameters: dict) -> dict:
        """
        Inject parameters as MARIMO_PARAM_<NAME>=<value> environment variables.

        Notebooks can read them with:
            import os; date = os.environ.get("MARIMO_PARAM_DATE", "")

        A special _env key (set by the YAML loader) is applied as plain env vars.
        A special _last_run key causes the last successful run's end_time to be
        injected as MARIMO_PARAM_<value> (e.g. _last_run: "LAST_RUN_AT").
        """
        env = os.environ.copy()

        # Apply _env overrides first (from YAML `env:` block)
        for key, value in (parameters.pop("_env", None) or {}).items():
            env[key] = str(value)

        # Handle _last_run: inject last successful run datetime
        last_run_param = parameters.pop("_last_run", None)
        if last_run_param:
            last_run_time = self._get_last_run_time()
            if last_run_time is not None:
                env[f"MARIMO_PARAM_{str(last_run_param).upper()}"] = last_run_time
                logger.debug(
                    "MarimoExecutionManager: injected %s=%s",
                    f"MARIMO_PARAM_{str(last_run_param).upper()}",
                    last_run_time,
                )
            else:
                logger.debug(
                    "MarimoExecutionManager: _last_run set but no prior COMPLETED run found"
                )

        # Inject remaining parameters with prefix
        for key, value in parameters.items():
            if not key.startswith("_"):
                env[f"MARIMO_PARAM_{key.upper()}"] = str(value)

        return env

    def _get_last_run_time(self) -> str | None:
        """
        Query the DB for the most recent COMPLETED job for this job's definition
        and return its end_time as an ISO-8601 string, or None if not found.
        """
        from jupyter_scheduler.orm import Job
        from jupyter_scheduler.models import Status

        job = self.model
        definition_id = getattr(job, "job_definition_id", None)
        current_job_id = getattr(job, "job_id", None)
        if not definition_id:
            return None

        try:
            with self.db_session() as session:
                last = (
                    session.query(Job)
                    .filter(
                        Job.job_definition_id == definition_id,
                        Job.status == Status.COMPLETED,
                        Job.job_id != current_job_id,
                    )
                    .order_by(Job.end_time.desc())
                    .first()
                )
                if last is None or last.end_time is None:
                    return None
                end_time = last.end_time
        except Exception as exc:
            logger.debug("MarimoExecutionManager: could not query last run: %s", exc)
            return None

        # end_time may be an int (ms epoch) or a datetime
        if isinstance(end_time, int):
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
            return dt.isoformat()
        return end_time.isoformat()

    def _output_path(self, input_path: Path, fmt: str) -> Path:
        staging_key = f"output-{fmt}"
        if self.staging_paths and staging_key in self.staging_paths:
            return Path(self.staging_paths[staging_key])
        suffix = ".html" if fmt == "html" else f".{fmt}"
        return input_path.with_suffix(suffix)

    def _python_executable(self, job) -> str:
        """
        Resolve the Python interpreter to use for this job.

        Priority:
          1. Job parameter  _python  (e.g. "/usr/bin/python3.11" or "/opt/envs/ds/bin/python")
          2. runtime_environment_name field — if it looks like a path to a python binary or venv dir
          3. The current interpreter (sys.executable)
        """
        import sys

        params = dict(job.parameters or {})
        if "_python" in params:
            return str(params["_python"])

        env_name = getattr(job, "runtime_environment_name", "") or ""
        if env_name:
            # Accept either a direct python path or a venv root directory
            candidate = Path(env_name)
            if candidate.is_file() and os.access(str(candidate), os.X_OK):
                return str(candidate)
            # venv directory — try bin/python or Scripts/python.exe (Windows)
            for rel in ("bin/python", "bin/python3", "Scripts/python.exe"):
                p = candidate / rel
                if p.exists():
                    return str(p)

        return sys.executable

    def _marimo_bin(self, python_exe: str) -> str:
        """Find the marimo binary co-located with the given python executable."""
        python_path = Path(python_exe)
        # Look in the same bin/ directory as the python executable
        candidate = python_path.parent / "marimo"
        if candidate.exists():
            return str(candidate)
        # Fall back to PATH lookup
        return shutil.which("marimo") or "marimo"

    def _run_marimo_export(self, input_path: Path, fmt: str, env: dict, job) -> None:
        output_path = self._output_path(input_path, fmt)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        python_exe = self._python_executable(job)
        marimo_bin = self._marimo_bin(python_exe)
        cmd = [marimo_bin, "export", fmt, str(input_path), "--output", str(output_path), "--no-sandbox"]

        logger.info("MarimoExecutionManager: running %s", " ".join(cmd))
        start = time.monotonic()

        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=self._timeout(job))

        elapsed = time.monotonic() - start
        logger.info("MarimoExecutionManager: finished in %.1fs, returncode=%d", elapsed, result.returncode)

        if result.stdout:
            logger.debug("stdout: %s", result.stdout[-4000:])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[-4000:])

        if result.returncode != 0:
            raise RuntimeError(
                f"marimo export {fmt} failed (exit {result.returncode}).\n"
                f"stderr: {result.stderr[-2000:]}"
            )

        self._write_log(output_path, result)

    def _run_as_script(self, input_path: Path, env: dict, job) -> None:
        python_exe = self._python_executable(job)
        cmd = [python_exe, str(input_path)]

        logger.info("MarimoExecutionManager (script): running %s", " ".join(cmd))
        start = time.monotonic()

        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=self._timeout(job))

        elapsed = time.monotonic() - start
        logger.info("MarimoExecutionManager (script): finished in %.1fs, returncode=%d", elapsed, result.returncode)

        if result.returncode != 0:
            raise RuntimeError(
                f"Python script execution failed (exit {result.returncode}).\n"
                f"stderr: {result.stderr[-2000:]}"
            )

        staging_key = "output-script"
        if self.staging_paths and staging_key in self.staging_paths:
            out_path = Path(self.staging_paths[staging_key])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(result.stdout or "(no output)")

    def _write_log(self, output_path: Path, result: subprocess.CompletedProcess) -> None:
        try:
            output_path.with_suffix(".log").write_text(
                f"=== stdout ===\n{result.stdout}\n\n=== stderr ===\n{result.stderr}"
            )
        except Exception as exc:
            logger.debug("Could not write log file: %s", exc)

    def _timeout(self, job) -> int:
        try:
            return int((job.parameters or {}).get("_timeout", 3600))
        except (TypeError, ValueError):
            return 3600


class RoutingExecutionManager(ExecutionManager):
    """
    Routes jobs to the appropriate executor based on the input file extension:
      - .py   → MarimoExecutionManager
      - .ipynb → DefaultExecutionManager (nbconvert)

    This allows the original jupyter-scheduler UI to continue scheduling
    .ipynb notebooks while Marimo .py notebooks are handled natively.
    """

    @classmethod
    def validate(cls, *args) -> bool:
        return True

    @classmethod
    def supported_features(cls) -> Dict[JobFeature, bool]:
        # Union of both executors' features.
        # DefaultExecutionManager.supported_features is not decorated as a
        # classmethod in some jupyter-scheduler versions, so we call it directly.
        marimo = MarimoExecutionManager.supported_features()
        try:
            default = DefaultExecutionManager.supported_features()
        except TypeError:
            default = DefaultExecutionManager.__dict__["supported_features"](
                DefaultExecutionManager
            )
        return {feature: marimo.get(feature, False) or default.get(feature, False)
                for feature in JobFeature}

    def execute(self) -> None:
        input_filename = self.model.input_filename or ""
        if Path(input_filename).suffix.lower() == ".ipynb":
            logger.info("RoutingExecutionManager: routing '%s' to DefaultExecutionManager", input_filename)
            DefaultExecutionManager(
                job_id=self.job_id,
                root_dir=self.root_dir,
                db_url=self.db_url,
                staging_paths=self.staging_paths,
            ).execute()
        else:
            logger.info("RoutingExecutionManager: routing '%s' to MarimoExecutionManager", input_filename)
            MarimoExecutionManager(
                job_id=self.job_id,
                root_dir=self.root_dir,
                db_url=self.db_url,
                staging_paths=self.staging_paths,
            ).execute()
