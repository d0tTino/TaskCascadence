"""Wrappers for APScheduler/Cronyx.

See PRD section 'Scheduling' for design details.
"""


from ..metrics import track_task


class BaseScheduler:
    """Simplistic scheduler that runs tasks immediately and records metrics."""

    def schedule_task(self, task_func, *args, **kwargs):
        """Execute ``task_func`` and record execution metrics."""
        wrapped = track_task(task_func)
        return wrapped(*args, **kwargs)
