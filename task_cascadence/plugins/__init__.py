"""Plugin base classes for tasks.

See PRD section 'Plugin Architecture' for details.
"""

from .cronyx_server import CronyxServerLoader


class CronTask:
    """Base class for tasks triggered by cron schedules."""
    pass


webhook_task_registry = []


class WebhookTask:
    """Base class for tasks triggered via webhooks.

    Subclasses are automatically registered so the webhook server can
    invoke them when events arrive.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        webhook_task_registry.append(cls)

    def handle_event(self, source: str, event_type: str, payload: dict) -> None:
        """Handle an incoming webhook event.

        Parameters
        ----------
        source:
            The webhook source, e.g. ``"github"`` or ``"calcom"``.
        event_type:
            The event type string from the provider.
        payload:
            The JSON payload sent by the provider.
        """
        raise NotImplementedError()


class ManualTrigger:
    """Base class for tasks triggered manually."""
    pass


__all__ = [
    "CronTask",
    "WebhookTask",
    "ManualTrigger",
    "CronyxServerLoader",
]
