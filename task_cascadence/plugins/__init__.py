"""Plugin base classes and example tasks.

The project is designed to be extensible via plugins.  For demonstration
purposes we provide a tiny plugin system and a single example task.  More
complex projects could load plugins dynamically using entry points.
"""

from typing import Dict
import os
import importlib


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

# Register all tasks with the default scheduler on import so the CLI can access
# them immediately.
for _name, _task in registered_tasks.items():
    default_scheduler.register_task(_name, _task)


_loaded_cronyx = False


def maybe_load_cronyx_tasks() -> None:
    """Load tasks from a Cronyx server if ``CRONYX_BASE_URL`` is set."""

    global _loaded_cronyx
    if _loaded_cronyx:
        return
    base_url = os.getenv("CRONYX_BASE_URL")
    if not base_url:
        return
    try:  # pragma: no cover - optional integration
        from . import cronyx_server
        loader = cronyx_server.CronyxServerLoader(base_url)
        for info in loader.list_tasks():
            spec = loader.load_task(info["id"])
            module_path, cls_name = spec["path"].split(":")
            module = importlib.import_module(module_path)
            cls = getattr(module, cls_name)
            task = cls()
            registered_tasks[task.name] = task
            default_scheduler.register_task(task.name, task)
        _loaded_cronyx = True
    except Exception:
        pass

