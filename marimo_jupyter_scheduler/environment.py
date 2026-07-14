"""
MarimoEnvironmentManager — tells jupyter-scheduler about Marimo output formats.

Configure in jupyter_server_config.py:
    c.SchedulerApp.environment_manager_class = (
        "marimo_jupyter_scheduler.environment.MarimoEnvironmentManager"
    )
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List

from jupyter_scheduler.environments import EnvironmentManager
from jupyter_scheduler.models import RuntimeEnvironment


class MarimoEnvironmentManager(EnvironmentManager):
    """
    Environment manager that registers Marimo-specific output formats
    (html, script, md) so jupyter-scheduler can list jobs that use them
    without throwing a KeyError.
    """

    # Formats a Marimo notebook can be rendered to. These are what the UI offers
    # when scheduling a new job.
    MARIMO_OUTPUT_FORMATS: Dict[str, str] = {
        "html": "HTML",
        "script": "Script",
        "md": "Markdown",
    }

    # Every format that may appear on a *stored* job, including stock
    # jupyter-scheduler's `ipynb`. This manager replaces the stock one for all
    # jobs in the database, and add_job_files() does a bare
    # mapping[output_format] lookup, so a missing key 500s the whole list_jobs
    # endpoint for anyone whose DB predates this package. RoutingExecutionManager
    # runs .ipynb jobs via nbconvert regardless, so they must stay listable.
    OUTPUT_FORMATS: Dict[str, str] = {
        **MARIMO_OUTPUT_FORMATS,
        "ipynb": "Notebook",
    }

    def list_environments(self) -> List[RuntimeEnvironment]:
        python_path = sys.executable
        env_name = os.path.basename(os.path.dirname(os.path.dirname(python_path)))
        return [
            RuntimeEnvironment(
                name=env_name,
                label=env_name,
                description=f"Python environment: {python_path}",
                file_extensions=["py"],
                output_formats=list(self.MARIMO_OUTPUT_FORMATS.keys()),
            )
        ]

    def manage_environments_command(self) -> str:
        return ""

    def output_formats_mapping(self) -> Dict[str, str]:
        return self.OUTPUT_FORMATS
