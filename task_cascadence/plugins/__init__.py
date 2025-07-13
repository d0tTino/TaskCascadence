"""Plugin base classes and example tasks.

The project is designed to be extensible via plugins.  For demonstration
purposes we provide a tiny plugin system and a single example task.  More
complex projects could load plugins dynamically using entry points.
"""

import importlib
import os
from importlib import metadata
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
    def run(self):
        """Execute the task. Subclasses must override this method."""
        raise NotImplementedError


class WebhookTask(BaseTask):
    """Base class for tasks triggered via webhooks."""
    pass


webhook_task_registry: list[type[WebhookTask]] = []


def register_webhook_task(cls: type[WebhookTask]) -> type[WebhookTask]:
    """Register a ``WebhookTask`` subclass for event delivery."""

    webhook_task_registry.append(cls)
    return cls



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

def load_plugin(path: str) -> BaseTask:
    """Load ``path`` of the form ``module:Class`` and return an instance."""

    module_path, class_name = path.split(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


def load_entrypoint_plugins() -> None:
    """Load tasks exposed via ``task_cascadence.plugins`` entry points."""

    for ep in metadata.entry_points().select(group="task_cascadence.plugins"):
        task = load_plugin(ep.value)
        registered_tasks[task.name] = task
        default_scheduler.register_task(task.name, task)


def load_cronyx_plugins(base_url: str) -> None:
    """Load tasks from a CronyxServer instance."""

    from .cronyx_server import CronyxServerLoader

    loader = CronyxServerLoader(base_url)
    for info in loader.list_tasks():
        data = loader.load_task(info["id"])
        task = load_plugin(data["path"])
        registered_tasks[task.name] = task
        default_scheduler.register_task(task.name, task)


def initialize() -> None:
    """Register built-in tasks and load any external plugins."""

    for _name, _task in registered_tasks.items():
        default_scheduler.register_task(_name, _task)

    load_entrypoint_plugins()

    if "CRONYX_BASE_URL" in os.environ:
        load_cronyx_plugins(os.environ["CRONYX_BASE_URL"])



