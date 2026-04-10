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

    OUTPUT_FORMATS: Dict[str, str] = {
        "html": "HTML",
        "script": "Script",
        "md": "Markdown",
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
                output_formats=list(self.OUTPUT_FORMATS.keys()),
            )
        ]

    def manage_environments_command(self) -> str:
        return ""

    def output_formats_mapping(self) -> Dict[str, str]:
        return self.OUTPUT_FORMATS
