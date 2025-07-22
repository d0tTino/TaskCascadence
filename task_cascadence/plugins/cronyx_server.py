from typing import Any

import requests

from ..http_utils import request_with_retry


class CronyxServerLoader:
    """Loader for tasks via CronyxServer's HTTP API."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float | None = None,
        retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        if timeout is None:
            from ..config import load_config

            cfg_timeout = load_config().get("cronyx_timeout")
            timeout = float(cfg_timeout if cfg_timeout is not None else 5.0)
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        try:
            response = request_with_retry(
                "GET",
                url,
                timeout=self.timeout,
                retries=self.retries,
                backoff_factor=self.backoff_factor,
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            print(f"Failed to reach CronyxServer at {url}: {exc}")
            return None
        return response.json()

    def list_tasks(self):
        """Return a list of available tasks."""
        return self._get('/tasks') or []

    def load_task(self, task_id: str):
        """Return a single task definition by ID."""
        return self._get(f'/tasks/{task_id}') or {}
