import sys
from task_cascadence.task_store import TaskStore
from task_cascadence import plugins


def test_add_path_deduplicates_and_loads(tmp_path, monkeypatch):
    tasks_file = tmp_path / 'tasks.yml'
    store = TaskStore(path=tasks_file)

    module = tmp_path / 'myplugin.py'
    module.write_text(
        "from task_cascadence.plugins import BaseTask\n"
        "class MyTask(BaseTask):\n"
        "    name = 'my'\n"
        "    def run(self):\n"
        "        return 'ok'\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    path = 'myplugin:MyTask'
    store.add_path(path)
    store.add_path(path)

    assert store.get_paths() == [path]
    if 'myplugin' in sys.modules:
        del sys.modules['myplugin']

    tasks = store.load_tasks()
    assert list(tasks) == ['MyTask']
    assert isinstance(tasks['MyTask'], plugins.BaseTask)
