from task_cascadence.scheduler import CronScheduler, get_default_scheduler
from task_cascadence import initialize



def test_sanity():
    assert 1 + 1 == 2


def test_default_scheduler_available():
    initialize()
    assert isinstance(get_default_scheduler(), CronScheduler)


def test_example_task_registered():
    initialize()

    tasks = [name for name, _ in get_default_scheduler().list_tasks()]
    assert "example" in tasks
