"""Scheduler communicating with a CronyxServer instance."""

from __future__ import annotations

from typing import Any

from ..http_utils import request_with_retry
from . import BaseScheduler


class CronyxScheduler(BaseScheduler):
    """Run and schedule tasks via a remote CronyxServer."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        timeout: float | None = None,
        retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        super().__init__()
        from ..config import load_config

        cfg = load_config()
        self.base_url = (base_url or cfg.get("cronyx_base_url", "")).rstrip("/")
        if not self.base_url:
            raise ValueError("CronyxScheduler requires cronyx_base_url")
        if timeout is None:
            cfg_timeout = cfg.get("cronyx_timeout")
            timeout = float(cfg_timeout if cfg_timeout is not None else 5.0)
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor

    def _request(self, method: str, path: str, json: Any | None = None) -> Any:
        url = f"{self.base_url}{path}"
        response = request_with_retry(
            method,
            url,
            timeout=self.timeout,
            retries=self.retries,
            backoff_factor=self.backoff_factor,
            json=json,
        )
        return response.json()

    def run_task(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
    ) -> Any:
        if name in self._tasks and not self._tasks[name]["disabled"]:
            return super().run_task(name, use_temporal=use_temporal, user_id=user_id)
        payload = {"task": name}
        if user_id is not None:
            payload["user_id"] = user_id
        data = self._request("POST", "/jobs", json=payload)
        return data.get("result")

    def schedule_task(
        self, name: str, cron_expression: str, *, user_id: str | None = None
    ) -> Any:
        payload = {"task": name, "cron": cron_expression}
        if user_id is not None:
            payload["user_id"] = user_id
        return self._request("POST", "/jobs", json=payload)
