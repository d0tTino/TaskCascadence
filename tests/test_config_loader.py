import importlib
import os
import pytest

import task_cascadence
from task_cascadence.scheduler import get_default_scheduler, BaseScheduler, CronScheduler
from task_cascadence.scheduler import TemporalScheduler
from task_cascadence.config import load_config


def test_yaml_timezone(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("timezone: Europe/Paris")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.delenv("CASCADENCE_TIMEZONE", raising=False)
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    from zoneinfo import ZoneInfo

    sched = get_default_scheduler()
    assert str(sched.scheduler.timezone) == "Europe/Paris"
    assert isinstance(sched.scheduler.timezone, ZoneInfo)


def test_env_overrides_yaml_timezone(monkeypatch, tmp_path):
    cfg = tmp_path / "cfg.yml"
    cfg.write_text("timezone: Europe/Paris")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg))
    monkeypatch.setenv("CASCADENCE_TIMEZONE", "Asia/Tokyo")
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    from zoneinfo import ZoneInfo

    sched = get_default_scheduler()
    assert str(sched.scheduler.timezone) == "Asia/Tokyo"
    assert isinstance(sched.scheduler.timezone, ZoneInfo)


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
    from typing import cast
    from task_cascadence.scheduler import CronScheduler

    sched = cast(CronScheduler, get_default_scheduler())
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


def test_create_scheduler_env_timezone(monkeypatch):
    monkeypatch.setenv("CASCADENCE_TIMEZONE", "Asia/Kolkata")
    cfg = load_config()
    from task_cascadence.scheduler import create_scheduler

    sched = create_scheduler(cfg["backend"], timezone=cfg["timezone"])
    from zoneinfo import ZoneInfo

    assert str(sched.scheduler.timezone) == "Asia/Kolkata"
    assert isinstance(sched.scheduler.timezone, ZoneInfo)


def test_create_scheduler_yaml_timezone(monkeypatch, tmp_path):
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text("timezone: Australia/Sydney")
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg_file))
    monkeypatch.delenv("CASCADENCE_TIMEZONE", raising=False)
    cfg = load_config()
    from task_cascadence.scheduler import create_scheduler

    sched = create_scheduler(cfg["backend"], timezone=cfg["timezone"])
    from zoneinfo import ZoneInfo

    assert str(sched.scheduler.timezone) == "Australia/Sydney"
    assert isinstance(sched.scheduler.timezone, ZoneInfo)


def test_initialize_unknown_scheduler(monkeypatch):
    """Providing an invalid scheduler should raise a ValueError."""

    monkeypatch.setenv("CASCADENCE_SCHEDULER", "bogus")
    import importlib
    import task_cascadence

    with pytest.raises(ValueError):
        importlib.reload(task_cascadence)
        task_cascadence.initialize()


def test_suggestions_defaults(monkeypatch):
    monkeypatch.delenv("CASCADENCE_SUGGESTIONS_ENABLED", raising=False)
    monkeypatch.delenv("CASCADENCE_SUGGESTIONS_CATEGORIES", raising=False)
    monkeypatch.delenv("CASCADENCE_CONFIG", raising=False)
    cfg = load_config()
    assert cfg["suggestions"]["enabled"] is True
    assert cfg["suggestions"]["categories"] == []


def test_suggestions_yaml(monkeypatch, tmp_path):
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text(
        "suggestions:\n  enabled: false\n  categories:\n    - personal\n    - finance\n"
    )
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg_file))
    monkeypatch.delenv("CASCADENCE_SUGGESTIONS_ENABLED", raising=False)
    monkeypatch.delenv("CASCADENCE_SUGGESTIONS_CATEGORIES", raising=False)
    cfg = load_config()
    assert cfg["suggestions"]["enabled"] is False
    assert cfg["suggestions"]["categories"] == ["personal", "finance"]


def test_suggestions_env_overrides(monkeypatch, tmp_path):
    cfg_file = tmp_path / "cfg.yml"
    cfg_file.write_text(
        "suggestions:\n  enabled: true\n  categories:\n    - personal\n"
    )
    monkeypatch.setenv("CASCADENCE_CONFIG", str(cfg_file))
    monkeypatch.setenv("CASCADENCE_SUGGESTIONS_ENABLED", "0")
    monkeypatch.setenv("CASCADENCE_SUGGESTIONS_CATEGORIES", "finance,work")
    cfg = load_config()
    assert cfg["suggestions"]["enabled"] is False
    assert cfg["suggestions"]["categories"] == ["finance", "work"]

