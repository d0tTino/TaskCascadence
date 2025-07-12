import importlib

from task_cascadence.plugins import CronTask


def load_plugin(path: str):
    module_path, class_name = path.split(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


class DummyCronTask(CronTask):
    def __init__(self):
        self.executed = False

    def run(self):
        self.executed = True
        return "ran"


def test_plugin_loading_and_execution(tmp_path, monkeypatch):
    module_file = tmp_path / "myplugin.py"
    module_file.write_text(
        "from task_cascadence.plugins import CronTask\n"
        "class Plugin(CronTask):\n"
        "    def __init__(self):\n"
        "        self.executed = False\n"
        "    def run(self):\n"
        "        self.executed = True\n"
        "        return 'ok'\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    plugin = load_plugin("myplugin:Plugin")
    result = plugin.run()

    assert result == "ok"
    assert plugin.executed
