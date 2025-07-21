import importlib

import task_cascadence


def test_scheduler_running_after_initialize():
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    sched = task_cascadence.scheduler.get_default_scheduler()
    assert isinstance(sched, task_cascadence.scheduler.CronScheduler)
    assert sched.scheduler.running
