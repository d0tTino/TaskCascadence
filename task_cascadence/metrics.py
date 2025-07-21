"""Prometheus metrics for Cascadence tasks."""

from prometheus_client import Counter, Histogram, start_http_server
import functools
import time

# Public exports
__all__ = [
    "TASK_LATENCY",
    "TASK_SUCCESS",
    "TASK_FAILURE",
    "start_metrics_server",
    "track_task",
]
# Histogram tracking how long each task takes to run.
TASK_LATENCY = Histogram(
    "task_latency_seconds",
    "Time spent executing tasks",
    ["task_name"],
)

# Counters for successes and failures.
TASK_SUCCESS = Counter(
    "task_success_total",
    "Total number of tasks completed successfully",
    ["task_name"],
)

TASK_FAILURE = Counter(
    "task_failure_total",
    "Total number of tasks that raised an exception",
    ["task_name"],
)

def start_metrics_server(port: int = 8000) -> None:
    """Start an HTTP server to expose Prometheus metrics."""
    start_http_server(port)


def track_task(func=None, *, name: str | None = None):
    """Decorator to record metrics for a task function.

    Can be used without parentheses as ``@track_task`` or with a custom task
    name as ``@track_task(name="CustomTask")``.
    """

    def decorator(func):
        task_name = name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.monotonic()
            try:
                result = func(*args, **kwargs)
            except Exception:
                TASK_FAILURE.labels(task_name).inc()
                raise
            else:
                TASK_SUCCESS.labels(task_name).inc()
                return result
            finally:
                duration = time.monotonic() - start_time
                TASK_LATENCY.labels(task_name).observe(duration)

        return wrapper

    if func is None:
        return decorator

    return decorator(func)
