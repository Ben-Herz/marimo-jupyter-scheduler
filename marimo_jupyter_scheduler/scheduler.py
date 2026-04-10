"""
MarimoScheduler — subclass of jupyter_scheduler.Scheduler with two fixes:

1. Creates the per-job staging directory before copying the input file.
   The default copy_input_file() uses fsspec which does not create parent
   directories, causing the spawned executor to die silently (job stays STOPPED).

2. When triggered from a job definition (via task runner), the input_uri passed
   to copy_input_file() is the definition's staging path, which does not exist.
   We fall back to the actual notebook file under root_dir.

Configure in jupyter_server_config.py:
    c.SchedulerApp.scheduler_class = (
        "marimo_jupyter_scheduler.scheduler.MarimoScheduler"
    )
"""

from __future__ import annotations

import logging
import os

from jupyter_scheduler.scheduler import Scheduler

logger = logging.getLogger(__name__)


class MarimoScheduler(Scheduler):

    def update_job_definition(self, job_definition_id: str, model) -> None:
        """
        Override to fix a jupyter-scheduler bug where output_formats, parameters
        and tags are silently ignored when schedule/timezone/active are unchanged.

        We apply those fields directly, then call the parent for everything else.
        """
        from jupyter_scheduler.orm import JobDefinition

        extra = {}
        if model.output_formats is not None:
            extra['output_formats'] = model.output_formats
        if model.parameters is not None:
            extra['parameters'] = model.parameters
        if model.tags is not None:
            extra['tags'] = model.tags
        if model.name is not None:
            extra['name'] = model.name

        if extra:
            with self.db_session() as session:
                session.query(JobDefinition).filter(
                    JobDefinition.job_definition_id == job_definition_id
                ).update(extra)
                session.commit()
            logger.debug("MarimoScheduler: applied extra fields %s to %s", list(extra), job_definition_id)

        # Let the parent handle schedule/timezone/active/task_runner
        super().update_job_definition(job_definition_id, model)

    def copy_input_file(self, input_uri: str, copy_to_path: str) -> None:
        # Ensure target directory exists
        os.makedirs(os.path.dirname(copy_to_path), exist_ok=True)

        # Resolve the actual source path
        # When triggered from a definition, input_uri is an absolute staging
        # path that doesn't exist yet. Detect this and fall back to the real
        # notebook by extracting input_filename from copy_to_path.
        if os.path.isabs(input_uri):
            source = input_uri
        else:
            source = os.path.join(self.root_dir, input_uri)

        if not os.path.exists(source):
            # Extract the relative input_filename from copy_to_path by
            # stripping the staging_path + job_id prefix:
            # copy_to_path = {staging_path}/{job_id}/{input_filename}
            rel = os.path.relpath(copy_to_path, self.staging_path)
            # rel = {job_id}/{input_filename} — strip the first component
            parts = rel.split(os.sep, 1)
            if len(parts) == 2:
                fallback = os.path.join(self.root_dir, parts[1])
                if os.path.exists(fallback):
                    logger.debug(
                        "MarimoScheduler: source '%s' not found, using '%s'",
                        source, fallback,
                    )
                    source = fallback

        if not os.path.exists(source):
            raise FileNotFoundError(
                f"Cannot find notebook to copy: tried '{source}'. "
                f"Check that the notebook path is relative to the JupyterLab root directory."
            )

        import shutil
        shutil.copy2(source, copy_to_path)
