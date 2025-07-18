import importlib
import os

import task_cascadence
from task_cascadence.scheduler import get_default_scheduler, BaseScheduler, CronScheduler


def test_env_selects_base_scheduler(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "base")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), BaseScheduler)


def test_yaml_config(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("scheduler: base")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    if "CASCADENCE_SCHEDULER" in os.environ:
        monkeypatch.delenv("CASCADENCE_SCHEDULER", raising=False)
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), BaseScheduler)


def test_env_overrides_yaml(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("scheduler: base")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "cron")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), CronScheduler)

