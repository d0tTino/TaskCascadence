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

    tasks = [name for name, _ in task_cascadence.scheduler.default_scheduler.list_tasks()]
    assert "remote" in tasks
