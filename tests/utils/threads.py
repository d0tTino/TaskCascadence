from __future__ import annotations

from threading import Thread
from typing import Callable


def run_workers(func: Callable[[int], None], workers: int = 2, iterations: int = 1) -> None:
    """Run *func* concurrently across multiple worker threads."""

    def _run(worker_id: int) -> None:
        for _ in range(iterations):
            func(worker_id)

    threads = [Thread(target=_run, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
