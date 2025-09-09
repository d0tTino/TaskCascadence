import asyncio
import time
from typing import Any, Optional

import requests
import httpx


def request_with_retry(
    method: str,
    url: str,
    *,
    timeout: float,
    retries: int = 3,
    backoff_factor: float = 0.5,
    session: Optional[requests.Session] = None,
    **kwargs: Any,
) -> requests.Response:
    """Perform an HTTP request with simple retry logic."""
    sess = session or requests
    for attempt in range(1, retries + 1):
        try:
            response = sess.request(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(backoff_factor * 2 ** (attempt - 1))
    raise RuntimeError("unreachable")


async def request_with_retry_async(
    method: str,
    url: str,
    *,
    timeout: float,
    retries: int = 3,
    backoff_factor: float = 0.5,
    client: Optional[httpx.AsyncClient] = None,
    **kwargs: Any,
) -> httpx.Response:
    """Asynchronously perform an HTTP request with simple retry logic."""
    for attempt in range(1, retries + 1):
        try:
            if client is None:
                async with httpx.AsyncClient() as c:
                    response = await c.request(method, url, timeout=timeout, **kwargs)
            else:
                response = await client.request(method, url, timeout=timeout, **kwargs)
            response.raise_for_status()
            return response
        except httpx.HTTPError:
            if attempt == retries:
                raise
            await asyncio.sleep(backoff_factor * 2 ** (attempt - 1))
    raise RuntimeError("unreachable")
