"""Plugin base classes for tasks.

See PRD section 'Plugin Architecture' for details.
"""


class CronTask:
    """Base class for tasks triggered by cron schedules."""
    def run(self):
        """Execute the task. Subclasses must override this method."""
        raise NotImplementedError


class WebhookTask:
    """Base class for tasks triggered via webhooks."""
    pass


class ManualTrigger:
    """Base class for tasks triggered manually."""
    pass
