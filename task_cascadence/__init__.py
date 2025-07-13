"""Cascadence package root.

This package provides task orchestration utilities described in the PRD.
"""

from . import scheduler  # noqa: F401
from . import plugins  # noqa: F401
from . import ume  # noqa: F401
from . import cli  # noqa: F401
from . import metrics  # noqa: F401
from . import temporal  # noqa: F401


def _init_plugins() -> None:
    """Initialise and load plugins."""

    plugins.initialize()
    plugins.load_cronyx_tasks()


_init_plugins()




__all__ = ["scheduler", "plugins", "ume", "cli", "metrics", "temporal"]


