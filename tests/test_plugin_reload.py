import importlib
import sys
import time
from importlib import metadata

from typer.testing import CliRunner

from task_cascadence.cli import app
from task_cascadence import plugins as pl
from task_cascadence.plugins.watcher import PluginWatcher


PLUGIN_V1 = (
    "from task_cascadence.plugins import CronTask\n"
    "class Plugin(CronTask):\n"
    "    name = 'ep'\n"
    "    def run(self):\n"
    "        return 'v1'\n"
)

PLUGIN_V2 = (
    "from task_cascadence.plugins import CronTask\n"
    "class Plugin(CronTask):\n"
    "    name = 'ep'\n"
    "    def run(self):\n"
    "        return 'v2'\n"
)


def setup_plugin(tmp_path, monkeypatch, content):
    module = tmp_path / "plug.py"
    module.write_text(content)
    monkeypatch.syspath_prepend(str(tmp_path))
    ep = metadata.EntryPoint(name="ep", value="plug:Plugin", group="task_cascadence.plugins")
    monkeypatch.setattr(metadata, "entry_points", lambda: metadata.EntryPoints([ep]))
    if "plug" in sys.modules:
        del sys.modules["plug"]
    importlib.reload(pl)
    import task_cascadence
    task_cascadence.initialize()

    pl.initialize()
    return module


def test_cli_reload_plugins(tmp_path, monkeypatch):
    module = setup_plugin(tmp_path, monkeypatch, PLUGIN_V1)

    assert pl.registered_tasks["ep"].run() == "v1"

    runner = CliRunner()
    time.sleep(1)
    module.write_text(PLUGIN_V2)
    from task_cascadence import scheduler

    def _reload():
        from importlib import reload, invalidate_caches
        invalidate_caches()
        reload(sys.modules["plug"])  # reload plugin module
        scheduler.set_default_scheduler(scheduler.CronScheduler())
        pl.initialize()

    monkeypatch.setattr(pl, "reload_plugins", _reload)
    monkeypatch.setattr("task_cascadence.plugins.watcher.reload_plugins", _reload)

    from task_cascadence import initialize
    initialize()

    result = runner.invoke(app, ["reload-plugins"])
    assert result.exit_code == 0

    pl_mod = importlib.reload(pl)
    pl_mod.initialize()
    assert pl_mod.registered_tasks["ep"].run() == "v2"


def test_plugin_watcher_auto_reload(tmp_path, monkeypatch):
    module = setup_plugin(tmp_path, monkeypatch, PLUGIN_V1)
    assert pl.registered_tasks["ep"].run() == "v1"

    from task_cascadence import scheduler

    def _reload():
        from importlib import reload, invalidate_caches
        invalidate_caches()
        reload(sys.modules["plug"])  # reload plugin module
        scheduler.set_default_scheduler(scheduler.CronScheduler())
        pl.initialize()

    monkeypatch.setattr(pl, "reload_plugins", _reload)

    watcher = PluginWatcher(tmp_path)
    watcher.start()
    try:
        time.sleep(1)
        module.write_text(PLUGIN_V2)
        time.sleep(2)
    finally:
        watcher.stop()
    _reload()
    pl_mod = importlib.reload(pl)
    pl_mod.initialize()
    assert pl_mod.registered_tasks["ep"].run() == "v2"


def test_plugin_watcher_triggers_reload_once(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONDONTWRITEBYTECODE", "1")
    module = setup_plugin(tmp_path, monkeypatch, PLUGIN_V1)

    call_count = 0

    def _reload():
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(pl, "reload_plugins", _reload)
    monkeypatch.setattr("task_cascadence.plugins.watcher.reload_plugins", _reload)

    watcher = PluginWatcher(tmp_path)
    watcher.start()
    try:
        time.sleep(1)
        module.write_text(PLUGIN_V2)
        time.sleep(2)
    finally:
        watcher.stop()

    assert call_count == 1


def test_plugin_watcher_stop_stops_thread(tmp_path):
    watcher = PluginWatcher(tmp_path)
    watcher.start()
    try:
        assert watcher._observer.is_alive()
    finally:
        watcher.stop()
    assert not watcher._observer.is_alive()
