"""Cascadence package root.

This package provides task orchestration utilities described in the PRD.
"""

from .scheduler import (
    set_default_scheduler,
    CronScheduler,
    BaseScheduler,
)
from . import plugins  # noqa: F401
from . import ume  # noqa: F401
from . import metrics  # noqa: F401
from . import temporal  # noqa: F401
from .config import load_config
from apscheduler.triggers.cron import CronTrigger




def initialize() -> None:
    """Load built-in tasks and any external plugins."""

    cfg = load_config()
    backend = cfg.get("scheduler", "cron")
    if backend == "cron":
        sched = CronScheduler()
    elif backend == "base":
        sched = BaseScheduler()
    else:
        raise ValueError(f"Unknown scheduler backend: {backend}")
    set_default_scheduler(sched)

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


from . import cli  # noqa: F401,E402


__all__ = ["scheduler", "plugins", "ume", "cli", "metrics", "temporal", "initialize"]


