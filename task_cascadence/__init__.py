"""Cascadence package root.

This package provides task orchestration utilities described in the PRD.
"""

from .scheduler import (
    set_default_scheduler,
    CronScheduler,
    TemporalScheduler,
    create_scheduler,
)
from . import plugins  # noqa: F401
from . import ume  # noqa: F401
from . import metrics  # noqa: F401
from . import temporal  # noqa: F401
from . import research  # noqa: F401
from .config import load_config
from apscheduler.triggers.cron import CronTrigger
from .task_store import TaskStore




def initialize() -> None:
    """Load built-in tasks and any external plugins."""

    cfg = load_config()
    store = TaskStore()
    loaded = store.load_tasks()
    backend = cfg.get("backend", cfg.get("scheduler", "cron"))
    timezone = cfg.get("timezone", "UTC")
    sched = create_scheduler(backend, tasks=loaded, timezone=timezone)
    set_default_scheduler(sched)
    for task in loaded.values():
        if task.name not in sched._tasks:
            sched.register_task(task.name, task)

    plugins.initialize()
    plugins.load_cronyx_tasks()

    if cfg.get("cronyx_refresh", True) and isinstance(sched, CronScheduler):
        trigger = CronTrigger.from_crontab(
            "*/10 * * * *", timezone=sched.scheduler.timezone
        )
        sched.scheduler.add_job(
            plugins.load_cronyx_tasks,
            trigger=trigger,
            id="cronyx_refresh",
            replace_existing=True,
        )

    if hasattr(sched, "start"):
        sched.start()


from . import cli  # noqa: F401,E402
from . import api  # noqa: F401,E402


__all__ = [
    "scheduler",
    "plugins",
    "ume",
    "cli",
    "api",
    "metrics",
    "temporal",
    "research",
    "initialize",
    "create_scheduler",
    "TemporalScheduler",
]


