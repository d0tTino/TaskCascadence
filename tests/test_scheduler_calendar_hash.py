from task_cascadence.scheduler import CronScheduler
import task_cascadence.scheduler as scheduler


class DummyResp:
    def json(self) -> dict:
        return {}


def test_fetch_calendar_event_hashes_user_id(monkeypatch, tmp_path):
    captured: dict[str, dict | None] = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["params"] = kwargs.get("params")
        return DummyResp()

    monkeypatch.setattr(scheduler, "request_with_retry", fake_request)
    monkeypatch.setenv("UME_BASE_URL", "http://ume")
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    sched._fetch_calendar_event("foo", user_id="alice")
    from task_cascadence.ume import _hash_user_id

    assert captured["params"]["user_id"] == _hash_user_id("alice")
