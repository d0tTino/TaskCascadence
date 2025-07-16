import time
from typing import Any

import requests


class CronyxServerLoader:
    """Loader for tasks via CronyxServer's HTTP API."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 5.0,
        retries: int = 3,
        backoff_factor: float = 0.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.backoff_factor = backoff_factor

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException:
                if attempt == self.retries:
                    raise
                time.sleep(self.backoff_factor * 2 ** (attempt - 1))

    def list_tasks(self):
        """Return a list of available tasks."""
        return self._get('/tasks')

    def load_task(self, task_id: str):
        """Return a single task definition by ID."""
        return self._get(f'/tasks/{task_id}')
