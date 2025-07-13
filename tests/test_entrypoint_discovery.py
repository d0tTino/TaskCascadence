import importlib
import sys
from importlib import metadata
from types import ModuleType

from task_cascadence.plugins import CronTask


def test_entrypoint_loading(monkeypatch):
    mod = ModuleType("ep_mod")

    class PluginTask(CronTask):
        name = "ep"

        def run(self):
            return "ok"

    mod.PluginTask = PluginTask
    sys.modules["ep_mod"] = mod

    ep = metadata.EntryPoint(name="ep", value="ep_mod:PluginTask", group="task_cascadence.plugins")
    monkeypatch.setattr(metadata, "entry_points", lambda: metadata.EntryPoints([ep]))

    import task_cascadence.plugins as pl
    importlib.reload(pl)
    pl.initialize()
    import importlib as _importlib
    import task_cascadence.webhook as wh
    _importlib.reload(wh)

    assert "ep" in pl.registered_tasks
    assert isinstance(pl.registered_tasks["ep"], PluginTask)
