"""
YAML-based schedule definitions (GitHub Actions style).

Users can place `*.marimo-schedule.yml` anywhere in their workspace and
this module will parse them into JobDefinition-compatible dicts.

Example YAML:

    version: "1"
    schedules:
      - name: daily-report
        description: "Regenerate sales report every weekday morning"
        notebook: notebooks/sales_report.py
        cron: "0 9 * * 1-5"
        timezone: "Europe/Berlin"
        output_formats:
          - html
        parameters:
          date: "${TODAY}"
          region: "EMEA"
        env:
          DATABASE_URL: "${DATABASE_URL}"
        tags:
          - daily
          - finance
        enabled: true
        max_kept_outputs: 10
"""

from __future__ import annotations

import copy
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Regex for ${VAR} substitution inside YAML values
_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


class YamlParseError(ValueError):
    pass


def _substitute_env(value: Any) -> Any:
    """Recursively replace ${VAR} placeholders with environment variables."""
    if isinstance(value, str):
        def _replace(m: re.Match) -> str:
            key = m.group(1)
            if key == "TODAY":
                return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
            return os.environ.get(key, m.group(0))  # keep original if not set

        return _ENV_VAR_RE.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env(v) for v in value]
    return value


def parse_schedule_file(path: Path) -> list[dict[str, Any]]:
    """
    Parse a *.marimo-schedule.yml file and return a list of job definition dicts
    ready to be posted to the jupyter-scheduler REST API.

    Raises YamlParseError on invalid structure.
    """
    try:
        raw = path.read_text(encoding="utf-8")
        doc = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise YamlParseError(f"Invalid YAML in {path}: {exc}") from exc

    if not isinstance(doc, dict):
        raise YamlParseError(f"{path}: top-level document must be a mapping")

    version = str(doc.get("version", "1"))
    if version not in ("1",):
        raise YamlParseError(f"{path}: unsupported version '{version}' (expected '1')")

    schedules_raw = doc.get("schedules", [])
    if not isinstance(schedules_raw, list):
        raise YamlParseError(f"{path}: 'schedules' must be a list")

    jobs: list[dict[str, Any]] = []
    for idx, entry in enumerate(schedules_raw):
        try:
            job = _parse_entry(entry, source_file=str(path), idx=idx)
            if job is not None:
                jobs.append(job)
        except YamlParseError:
            raise
        except Exception as exc:
            raise YamlParseError(
                f"{path}: error in schedule[{idx}]: {exc}"
            ) from exc

    return jobs


def _parse_entry(
    entry: dict[str, Any], source_file: str, idx: int
) -> dict[str, Any] | None:
    """Convert a single schedule entry to a jupyter-scheduler JobDefinition dict."""
    if not isinstance(entry, dict):
        raise YamlParseError(f"schedule[{idx}] must be a mapping, got {type(entry)}")

    enabled = entry.get("enabled", True)
    if not enabled:
        logger.debug("Skipping disabled schedule: %s", entry.get("name", idx))
        return None

    name = str(entry.get("name") or f"unnamed-{idx}")
    notebook = entry.get("notebook")
    if not notebook:
        raise YamlParseError(f"schedule '{name}': 'notebook' is required")

    cron = entry.get("cron") or entry.get("schedule")
    if not cron:
        raise YamlParseError(f"schedule '{name}': 'cron' (or 'schedule') is required")

    # Merge parameters and env into one parameters dict for jupyter-scheduler.
    # Env vars are injected separately by the executor.
    parameters: dict[str, Any] = dict(entry.get("parameters") or {})
    parameters = _substitute_env(parameters)

    # Stash env overrides as a special _env key so the executor can pick them up
    env: dict[str, str] = {
        k: str(v) for k, v in (entry.get("env") or {}).items()
    }
    env = _substitute_env(env)
    if env:
        parameters["_env"] = env

    timezone_str = entry.get("timezone", "UTC")
    output_formats = entry.get("output_formats") or entry.get("output_format") or ["html"]
    if isinstance(output_formats, str):
        output_formats = [output_formats]

    max_kept_outputs = int(entry.get("max_kept_outputs", 10))

    job_def: dict[str, Any] = {
        "name": name,
        "input_filename": str(notebook),
        "output_formats": output_formats,
        "schedule": cron,
        "timezone": timezone_str,
        "parameters": parameters,
        "max_kept_outputs": max_kept_outputs,
        # Store provenance so we can detect duplicates / stale entries
        "_source": {
            "file": source_file,
            "entry_index": idx,
        },
    }

    if "description" in entry:
        job_def["description"] = str(entry["description"])

    if "tags" in entry and isinstance(entry["tags"], list):
        job_def["tags"] = [str(t) for t in entry["tags"]]

    return job_def


def find_schedule_files(root_dir: str | Path) -> list[Path]:
    """
    Recursively find all *.marimo-schedule.yml files under root_dir,
    excluding hidden directories and __pycache__.
    """
    root = Path(root_dir)
    results: list[Path] = []
    for p in root.rglob("*.marimo-schedule.yml"):
        # Skip hidden dirs and common noise
        parts = p.parts
        if any(part.startswith(".") or part == "__pycache__" for part in parts):
            continue
        results.append(p)
    return sorted(results)
