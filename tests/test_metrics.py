import socket
import time

import httpx
from task_cascadence.metrics import start_metrics_server


def _get_free_port():
    sock = socket.socket()
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_metrics_endpoint_exposes_counters():
    port = _get_free_port()
    start_metrics_server(port)
    for _ in range(10):
        try:
            resp = httpx.get(f"http://localhost:{port}/metrics")
            if resp.status_code == 200:
                break
        except Exception:
            time.sleep(0.05)
    else:
        raise AssertionError("metrics endpoint not reachable")

    body = resp.text
    assert "task_latency_seconds" in body
    assert "task_success_total" in body
    assert "task_failure_total" in body
