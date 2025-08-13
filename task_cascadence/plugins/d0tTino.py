from __future__ import annotations

import subprocess
import logging

import requests

from . import CronTask

logger = logging.getLogger(__name__)


class D0tTinoTask(CronTask):
    """Cron task calling out to d0tTino's AI helpers."""

    name = "d0tTino"

    def __init__(self, prompt: str = "ping", *, use_api: bool = False, base_url: str = "http://localhost:8080") -> None:
        self.prompt = prompt
        self.use_api = use_api
        self.base_url = base_url.rstrip("/")

    def _call(self, command: str) -> str:
        if self.use_api:
            url = f"{self.base_url}/{command}"
            try:
                response = requests.post(url, json={"prompt": self.prompt}, timeout=30)
                response.raise_for_status()
            except requests.RequestException as exc:
                logger.error("d0tTino API request failed: %s", exc)
                raise
            return response.text.strip()
        result = subprocess.run(
            ["d0tTino", command, self.prompt],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def plan(self) -> str:
        """Invoke the ``ai-plan`` helper."""
        return self._call("ai-plan")

    def run(self) -> str:
        """Invoke the ``ai`` helper."""
        return self._call("ai")
