"""Tests for MarimoEnvironmentManager output formats."""

from marimo_jupyter_scheduler.environment import MarimoEnvironmentManager


def test_ipynb_is_mapped():
    """Jobs created under stock jupyter-scheduler carry `ipynb`.

    add_job_files() looks up mapping[output_format] with no default, so a
    missing key raises KeyError and 500s GET /scheduler/jobs for the whole
    job list -- not just the offending job.
    """
    mapping = MarimoEnvironmentManager().output_formats_mapping()
    assert mapping["ipynb"] == "Notebook"


def test_stock_formats_are_mapped():
    """Everything stock jupyter-scheduler can persist must stay renderable."""
    mapping = MarimoEnvironmentManager().output_formats_mapping()
    for output_format in ("ipynb", "html"):
        assert output_format in mapping


def test_list_environments_does_not_offer_ipynb():
    """`ipynb` is listable but not selectable.

    Marimo notebooks are .py; nbconvert cannot render one to a notebook, so
    the format must not be offered when scheduling a new job.
    """
    environments = MarimoEnvironmentManager().list_environments()

    assert environments
    for environment in environments:
        assert "ipynb" not in environment.output_formats
        assert "html" in environment.output_formats


def test_add_job_files_survives_ipynb_job():
    """End-to-end reproduction of the KeyError via the real jupyter-scheduler code."""
    from unittest.mock import MagicMock

    from jupyter_scheduler.scheduler import Scheduler

    scheduler = Scheduler.__new__(Scheduler)
    scheduler.environments_manager = MarimoEnvironmentManager()

    model = MagicMock()
    model.output_formats = ["ipynb", "html"]
    model.job_files = []
    model.output_filenames = {}
    model.job_id = "job-1"
    model.status = "COMPLETED"

    # Raised KeyError: 'ipynb' before the fix.
    scheduler.add_job_files(model=model)

    display_names = {job_file.display_name for job_file in model.job_files}
    assert "Notebook" in display_names
