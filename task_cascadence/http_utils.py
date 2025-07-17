import time
from typing import Any, Optional

import requests


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
