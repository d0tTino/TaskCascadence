"""Wrappers for APScheduler/Cronyx.

See PRD section 'Scheduling' for design details.
"""

from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import yaml


class BaseScheduler:
    """Placeholder for scheduler integrations with APScheduler or Cronyx."""

    def schedule_task(self, *args, **kwargs):
        """Stub method for scheduling tasks."""
        pass


class CronScheduler(BaseScheduler):
    """APScheduler-based scheduler using cron triggers.

    Provides timezone-aware scheduling of tasks.
    """

    def __init__(self, timezone="UTC", storage_path="schedules.yml"):
        self._CronTrigger = CronTrigger
        self._yaml = yaml
        tz = pytz.timezone(timezone) if isinstance(timezone, str) else timezone
        self.scheduler = BackgroundScheduler(timezone=tz)
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.schedules = self._load_schedules()

    def _load_schedules(self):
        if self.storage_path.exists():
            with open(self.storage_path, "r") as fh:
                data = self._yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    return data
        return {}

    def _save_schedules(self):
        with open(self.storage_path, "w") as fh:
            self._yaml.safe_dump(self.schedules, fh)

    def _wrap_task(self, task):
        def runner():
            from ..ume import emit_task_run

            try:
                result = task.run()
                emit_task_run(
                    {"task": task.__class__.__name__, "result": result}
                )
            except Exception as exc:  # pragma: no cover - passthrough
                emit_task_run(
                    {"task": task.__class__.__name__, "error": str(exc)}
                )
                raise

        return runner

    def register_task(self, task, cron_expression):
        job_id = task.__class__.__name__
        self.schedules[job_id] = cron_expression
        self._save_schedules()

        trigger = self._CronTrigger.from_crontab(
            cron_expression, timezone=self.scheduler.timezone
        )
        self.scheduler.add_job(
            self._wrap_task(task), trigger=trigger, id=job_id
        )

    def start(self):
        self.scheduler.start()

    def shutdown(self, wait=True):
        self.scheduler.shutdown(wait=wait)

    def list_jobs(self):
        return self.scheduler.get_jobs()
