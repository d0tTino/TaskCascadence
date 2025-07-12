"""Plugin base classes for tasks.

See PRD section 'Plugin Architecture' for details.
"""

from .cronyx_server import CronyxServerLoader


class CronTask:
    """Base class for tasks triggered by cron schedules."""
    pass


class WebhookTask:
    """Base class for tasks triggered via webhooks."""
    pass


class ManualTrigger:
    """Base class for tasks triggered manually."""
    pass


__all__ = [
    "CronTask",
    "WebhookTask",
    "ManualTrigger",
    "CronyxServerLoader",
]
