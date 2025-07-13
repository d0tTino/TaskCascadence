from task_cascadence.scheduler import CronScheduler, default_scheduler


def test_sanity():
    assert 1 + 1 == 2


def test_default_scheduler_available():
    assert isinstance(default_scheduler, CronScheduler)


def test_example_task_registered():
    from task_cascadence import plugins  # noqa: F401 - trigger side effects

    tasks = [name for name, _ in default_scheduler.list_tasks()]
    assert "example" in tasks
