import requests


class CronyxServerLoader:
    """Loader for tasks via CronyxServer's HTTP API."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def _get(self, path: str):
        url = f"{self.base_url}{path}"
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return response.json()

    def list_tasks(self):
        """Return a list of available tasks."""
        return self._get('/tasks')

    def load_task(self, task_id: str):
        """Return a single task definition by ID."""
        return self._get(f'/tasks/{task_id}')
