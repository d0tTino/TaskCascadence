import time
from typer.testing import CliRunner

from task_cascadence.cli import app


def test_cli_watch_plugins(monkeypatch, tmp_path):
    events = []

    class DummyWatcher:
        def __init__(self, path):
            events.append(("init", path))

        def start(self):
            events.append("start")

        def stop(self):
            events.append("stop")

    monkeypatch.setattr(
        "task_cascadence.plugins.watcher.PluginWatcher", DummyWatcher
    )

    def raise_interrupt(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(time, "sleep", raise_interrupt)

    runner = CliRunner()
    result = runner.invoke(app, ["watch-plugins", str(tmp_path)])
    assert result.exit_code == 0
    assert events == [("init", str(tmp_path)), "start", "stop"]

