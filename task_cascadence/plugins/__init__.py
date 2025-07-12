"""Plugin base classes and example tasks.

The project is designed to be extensible via plugins.  For demonstration
purposes we provide a tiny plugin system and a single example task.  More
complex projects could load plugins dynamically using entry points.
"""

from typing import Dict


from ..scheduler import default_scheduler


class BaseTask:
    """Base class for all tasks."""

    name: str = "base"

    def run(self):  # pragma: no cover - trivial demo function
        """Run the task."""

        print(f"running task {self.name}")


class CronTask(BaseTask):
    """Base class for tasks triggered by cron schedules."""
    pass


class WebhookTask(BaseTask):
    """Base class for tasks triggered via webhooks."""
    pass



class ManualTrigger(BaseTask):
    """Base class for tasks triggered manually."""
    pass


# ---------------------------------------------------------------------------
# Example tasks shipped with this repository.  Real deployments would load
# plugins in a more dynamic fashion.

class ExampleTask(CronTask):
    """Very small task used in the examples."""

    name = "example"

    def run(self):  # pragma: no cover - illustrative
        print("Example task executed")


# ``registered_tasks`` is consumed by the scheduler during initialisation.
registered_tasks: Dict[str, BaseTask] = {
    ExampleTask.name: ExampleTask(),
}

# Register all tasks with the default scheduler on import so the CLI can access
# them immediately.
for _name, _task in registered_tasks.items():
    default_scheduler.register_task(_name, _task)

