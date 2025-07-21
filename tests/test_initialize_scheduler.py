import importlib
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import CronTask


def test_jobs_run_on_initialize(monkeypatch):
    executed = []

    class DummyTask(CronTask):
        def run(self):
            executed.append(1)

    def fake_plugins_initialize():
        from task_cascadence.scheduler import get_default_scheduler
        sched = get_default_scheduler()
        sched.register_task(name_or_task=DummyTask(), task_or_expr="* * * * *")

    monkeypatch.setattr("task_cascadence.plugins.initialize", fake_plugins_initialize)
    monkeypatch.setattr("task_cascadence.plugins.load_entrypoint_plugins", lambda: None)
    monkeypatch.setattr("task_cascadence.plugins.load_cronyx_plugins", lambda url: None)
    monkeypatch.setattr("task_cascadence.plugins.load_cronyx_tasks", lambda: None)

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    start_called = {"val": False}

    def fake_start(self):
        start_called["val"] = True
        for job in self.scheduler.get_jobs():
            job.func()

    monkeypatch.setattr(CronScheduler, "start", fake_start)

    import task_cascadence
    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    assert start_called["val"]
    assert executed
