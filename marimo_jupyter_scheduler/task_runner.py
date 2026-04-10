"""
Fixed TaskRunner for jupyter-scheduler 2.x.

jupyter_scheduler.task_runner.Cache creates an in-memory SQLite DB without
StaticPool or check_same_thread=False. When TaskRunner.start() runs in the
asyncio event loop thread it gets a fresh empty connection (different from the
one used to create the tables), causing:
    sqlite3.OperationalError: no such table: job_definitions_cache

We fix this by subclassing Cache to use StaticPool, then subclassing TaskRunner
to use the fixed Cache. Configure in jupyter_server_config.py:
    c.Scheduler.task_runner_class = (
        "marimo_jupyter_scheduler.task_runner.FixedTaskRunner"
    )
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from jupyter_scheduler.task_runner import Base, Cache, TaskRunner


class FixedCache(Cache):
    """Cache backed by a single shared in-memory SQLite connection."""

    def __init__(self) -> None:
        self.cache_url = "sqlite://"
        engine = create_engine(
            self.cache_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)


class FixedTaskRunner(TaskRunner):
    """TaskRunner that uses FixedCache instead of the broken default Cache."""

    def __init__(self, scheduler, config=None) -> None:
        # Call grandparent (BaseTaskRunner) init to avoid Cache() construction
        super(TaskRunner, self).__init__(config=config)
        self.scheduler = scheduler
        self.db_session = scheduler.db_session
        self.cache = FixedCache()
        from jupyter_scheduler.task_runner import PriorityQueue
        self.queue = PriorityQueue()
