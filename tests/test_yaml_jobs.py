"""Tests for YAML schedule parsing."""

import textwrap
from pathlib import Path

import pytest

from marimo_jupyter_scheduler.yaml_jobs import (
    YamlParseError,
    parse_schedule_file,
    find_schedule_files,
)


@pytest.fixture
def tmp_yaml(tmp_path):
    """Write YAML content to a temp file and return its Path."""
    def _write(content: str) -> Path:
        p = tmp_path / "test.marimo-schedule.yml"
        p.write_text(textwrap.dedent(content))
        return p
    return _write


def test_basic_parse(tmp_yaml):
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: my-job
            notebook: notebooks/test.py
            cron: "0 9 * * *"
            output_formats:
              - html
    """)
    jobs = parse_schedule_file(p)
    assert len(jobs) == 1
    job = jobs[0]
    assert job["name"] == "my-job"
    assert job["input_filename"] == "notebooks/test.py"
    assert job["schedule"] == "0 9 * * *"
    assert job["output_formats"] == ["html"]


def test_disabled_schedule_skipped(tmp_yaml):
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: disabled-job
            notebook: notebooks/test.py
            cron: "0 * * * *"
            enabled: false
    """)
    jobs = parse_schedule_file(p)
    assert jobs == []


def test_missing_notebook_raises(tmp_yaml):
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: bad-job
            cron: "0 * * * *"
    """)
    with pytest.raises(YamlParseError, match="notebook"):
        parse_schedule_file(p)


def test_missing_cron_raises(tmp_yaml):
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: bad-job
            notebook: notebooks/test.py
    """)
    with pytest.raises(YamlParseError, match="cron"):
        parse_schedule_file(p)


def test_env_substitution(tmp_yaml, monkeypatch):
    monkeypatch.setenv("MY_SECRET", "supersecret")
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: env-job
            notebook: notebooks/test.py
            cron: "0 * * * *"
            env:
              SECRET: "${MY_SECRET}"
    """)
    jobs = parse_schedule_file(p)
    assert jobs[0]["parameters"]["_env"]["SECRET"] == "supersecret"


def test_today_substitution(tmp_yaml):
    from datetime import datetime, timezone
    today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    p = tmp_yaml("""\
        version: "1"
        schedules:
          - name: today-job
            notebook: notebooks/test.py
            cron: "0 * * * *"
            parameters:
              date: "${TODAY}"
    """)
    jobs = parse_schedule_file(p)
    assert jobs[0]["parameters"]["date"] == today


def test_find_schedule_files(tmp_path):
    (tmp_path / "a.marimo-schedule.yml").write_text("version: '1'\nschedules: []")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.marimo-schedule.yml").write_text("version: '1'\nschedules: []")
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "c.marimo-schedule.yml").write_text("version: '1'\nschedules: []")

    found = find_schedule_files(tmp_path)
    names = {f.name for f in found}
    assert "a.marimo-schedule.yml" in names
    assert "b.marimo-schedule.yml" in names
    assert "c.marimo-schedule.yml" not in names  # hidden dir excluded


def test_invalid_yaml_raises(tmp_yaml):
    p = tmp_yaml("""\
        version: "1"
        schedules: [
          - broken yaml: {{
    """)
    with pytest.raises(YamlParseError):
        parse_schedule_file(p)
