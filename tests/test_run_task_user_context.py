from task_cascadence.scheduler import BaseScheduler


class MinimalTask:
    def run(self):  # pragma: no cover - behaviour tested via scheduler
        pass


def test_run_task_sets_user_and_group(monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)
    task = MinimalTask()
    scheduler = BaseScheduler()
    scheduler.register_task("minimal", task)
    scheduler.run_task("minimal", user_id="u1", group_id="g1")
    assert task.user_id == "u1"
    assert task.group_id == "g1"
