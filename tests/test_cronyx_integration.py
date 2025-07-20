import importlib
from textwrap import dedent

def test_tasks_loaded_from_cronyx(monkeypatch, tmp_path):
    # create a plugin to be loaded dynamically
    plugin_file = tmp_path / "remote_mod.py"
    plugin_file.write_text(
        dedent(
            """
            from task_cascadence.plugins import CronTask

            class RemoteTask(CronTask):
                name = "remote"

                def run(self):
                    return "ok"
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    # fake loader that returns our plugin path
    class DummyLoader:
        def __init__(self, base_url: str) -> None:
            self.base_url = base_url

        def list_tasks(self):
            return [{"id": "remote"}]

        def load_task(self, task_id: str):
            assert task_id == "remote"
            return {"id": "remote", "path": "remote_mod:RemoteTask"}

    monkeypatch.setenv("CRONYX_BASE_URL", "http://server")
    monkeypatch.setattr(
        "task_cascadence.plugins.cronyx_server.CronyxServerLoader",
        DummyLoader,
    )

    import task_cascadence

    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    tasks = [
        name for name, _ in task_cascadence.scheduler.get_default_scheduler().list_tasks()
    ]
    assert "remote" in tasks


def test_cronyx_refresh_job(monkeypatch, tmp_path):
    mod1 = tmp_path / "mod1.py"
    mod1.write_text(
        dedent(
            """
            from task_cascadence.plugins import CronTask

            class T1(CronTask):
                name = "t1"

                def run(self):
                    return "t1"
            """
        )
    )
    mod2 = tmp_path / "mod2.py"
    mod2.write_text(
        dedent(
            """
            from task_cascadence.plugins import CronTask

            class T2(CronTask):
                name = "t2"

                def run(self):
                    return "t2"
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    class DummyLoader:
        calls = 0

        def __init__(self, base_url: str) -> None:
            pass

        def list_tasks(self):
            type(self).calls += 1
            if type(self).calls == 1:
                return [{"id": "t1"}]
            return [{"id": "t1"}, {"id": "t2"}]

        def load_task(self, task_id: str):
            if task_id == "t1":
                return {"id": "t1", "path": "mod1:T1"}
            if task_id == "t2":
                return {"id": "t2", "path": "mod2:T2"}

    monkeypatch.setenv("CRONYX_BASE_URL", "http://server")
    monkeypatch.setenv("CASCADENCE_CRONYX_REFRESH", "1")
    monkeypatch.setattr(
        "task_cascadence.plugins.load_cronyx_plugins",
        lambda url: None,
    )
    monkeypatch.setattr(
        "task_cascadence.plugins.cronyx_server.CronyxServerLoader",
        DummyLoader,
    )

    import task_cascadence

    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    sched = task_cascadence.scheduler.get_default_scheduler()
    job = sched.scheduler.get_job("cronyx_refresh")
    assert job is not None

    names = [name for name, _ in sched.list_tasks()]
    assert "t1" in names
    assert "t2" not in names

    job.func()

    names = [name for name, _ in sched.list_tasks()]
    assert "t1" in names
    assert "t2" in names
