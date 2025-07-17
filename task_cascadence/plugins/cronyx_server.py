import os
from typing import Any

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
            timeout = float(os.getenv("CRONYX_TIMEOUT", "5.0"))
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        response = request_with_retry(
            "GET",
            url,
            timeout=self.timeout,
            retries=self.retries,
            backoff_factor=self.backoff_factor,
        )
        return response.json()

    def list_tasks(self):
        """Return a list of available tasks."""
        return self._get('/tasks')

    def load_task(self, task_id: str):
        """Return a single task definition by ID."""
        return self._get(f'/tasks/{task_id}')
