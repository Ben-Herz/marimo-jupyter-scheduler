"""Tests for FixedCache and FixedTaskRunner."""

import pytest
import threading

from marimo_jupyter_scheduler.task_runner import FixedCache, FixedTaskRunner


def test_fixed_cache_creates_tables():
    """FixedCache should create the job_definitions_cache table on init."""
    cache = FixedCache()
    with cache.session() as session:
        # If the table doesn't exist this will raise
        from jupyter_scheduler.task_runner import JobDefinitionCache
        result = session.query(JobDefinitionCache).all()
        assert result == []


def test_fixed_cache_shared_across_threads():
    """The same in-memory DB should be accessible from multiple threads."""
    cache = FixedCache()
    errors = []

    def query_from_thread():
        try:
            from jupyter_scheduler.task_runner import JobDefinitionCache
            with cache.session() as session:
                session.query(JobDefinitionCache).all()
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=query_from_thread) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"Thread errors: {errors}"


def test_fixed_cache_write_and_read():
    """Data written from one thread should be visible from another."""
    from jupyter_scheduler.task_runner import JobDefinitionCache

    cache = FixedCache()

    with cache.session() as session:
        entry = JobDefinitionCache(
            job_definition_id="test-def-1",
            next_run_time=12345678,
        )
        session.add(entry)
        session.commit()

    result_holder = []

    def read_from_thread():
        with cache.session() as session:
            rows = session.query(JobDefinitionCache).filter_by(
                job_definition_id="test-def-1"
            ).all()
            result_holder.extend(rows)

    t = threading.Thread(target=read_from_thread)
    t.start()
    t.join()

    assert len(result_holder) == 1
    assert result_holder[0].job_definition_id == "test-def-1"


def test_fixed_task_runner_uses_fixed_cache():
    """FixedTaskRunner should attach a FixedCache instance."""
    mock_scheduler = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
    runner = FixedTaskRunner(mock_scheduler)
    assert isinstance(runner.cache, FixedCache)
