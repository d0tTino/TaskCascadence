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


def test_scheduler_type_preserved_on_reload(tmp_path, monkeypatch):
    setup_plugin(tmp_path, monkeypatch, PLUGIN_V1)

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

    before_type = type(scheduler.get_default_scheduler())

    runner = CliRunner()
    result = runner.invoke(app, ["reload-plugins"])

    assert result.exit_code == 0
    assert type(scheduler.get_default_scheduler()) is before_type


def test_reload_preserves_temporal_scheduler(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "temporal")
    monkeypatch.setenv("TEMPORAL_SERVER", "local:7233")

    import importlib
    import task_cascadence

    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    from task_cascadence import scheduler

    before_type = type(scheduler.get_default_scheduler())

    runner = CliRunner()
    result = runner.invoke(app, ["reload-plugins"])

    assert result.exit_code == 0
    assert type(scheduler.get_default_scheduler()) is before_type


def test_reload_preserves_cronyx_scheduler(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "cronyx")
    monkeypatch.setenv("CRONYX_BASE_URL", "http://server")
    monkeypatch.setattr(pl, "load_cronyx_plugins", lambda url: None)

    class DummyResp:
        def __init__(self, data: dict):
            self.data = data

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return self.data

    import requests
    monkeypatch.setattr(requests, "request", lambda *a, **k: DummyResp({}))

    import importlib
    import task_cascadence

    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    from task_cascadence import scheduler

    before_type = type(scheduler.get_default_scheduler())

    runner = CliRunner()
    result = runner.invoke(app, ["reload-plugins"])

    assert result.exit_code == 0
    assert type(scheduler.get_default_scheduler()) is before_type


def test_watch_plugins_invokes_watcher(tmp_path, monkeypatch):
    """Running 'task watch-plugins' should start and stop the watcher."""

    calls = {"start": 0, "stop": 0}

    def fake_start(self):
        calls["start"] += 1

    def fake_stop(self):
        calls["stop"] += 1

    monkeypatch.setattr(PluginWatcher, "start", fake_start)
    monkeypatch.setattr(PluginWatcher, "stop", fake_stop)
    monkeypatch.setattr(time, "sleep", lambda s: (_ for _ in ()).throw(KeyboardInterrupt()))

    runner = CliRunner()
    result = runner.invoke(app, ["watch-plugins", str(tmp_path)])

    assert result.exit_code == 0
    assert calls["start"] == 1
    assert calls["stop"] == 1
