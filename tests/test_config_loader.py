import importlib
import os

import task_cascadence
from task_cascadence.scheduler import get_default_scheduler, BaseScheduler, CronScheduler
from task_cascadence.scheduler import TemporalScheduler
from task_cascadence.config import load_config


def test_env_selects_base_scheduler(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "base")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), BaseScheduler)


def test_yaml_config(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("backend: base")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    if "CASCADENCE_SCHEDULER" in os.environ:
        monkeypatch.delenv("CASCADENCE_SCHEDULER", raising=False)
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), BaseScheduler)


def test_env_overrides_yaml(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("backend: base")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "cron")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), CronScheduler)


def test_disable_cronyx_refresh(monkeypatch):
    monkeypatch.setenv("CASCADENCE_CRONYX_REFRESH", "0")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    sched = get_default_scheduler()
    assert sched.scheduler.get_job("cronyx_refresh") is None


def test_env_selects_temporal_scheduler(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "temporal")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), TemporalScheduler)


def test_yaml_temporal_scheduler(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("backend: temporal")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    if "CASCADENCE_SCHEDULER" in os.environ:
        monkeypatch.delenv("CASCADENCE_SCHEDULER", raising=False)
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    assert isinstance(get_default_scheduler(), TemporalScheduler)


def test_env_parsed(monkeypatch):
    monkeypatch.setenv("CRONYX_BASE_URL", "http://server")
    monkeypatch.setenv("CRONYX_TIMEOUT", "7.5")
    monkeypatch.setenv("TEMPORAL_SERVER", "remote:7233")
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_STUB", "pkg:stub")
    monkeypatch.setenv("UME_GRPC_METHOD", "Foo")
    monkeypatch.setenv("UME_NATS_CONN", "pkg:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "demo")
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    cfg = load_config()
    assert cfg["cronyx_base_url"] == "http://server"
    assert cfg["cronyx_timeout"] == 7.5
    assert cfg["temporal_server"] == "remote:7233"
    assert cfg["ume_transport"] == "grpc"
    assert cfg["ume_grpc_stub"] == "pkg:stub"
    assert cfg["ume_grpc_method"] == "Foo"
    assert cfg["ume_nats_conn"] == "pkg:conn"
    assert cfg["ume_nats_subject"] == "demo"
    assert cfg["hash_secret"] == "s"

