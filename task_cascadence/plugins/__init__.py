"""Plugin base classes and example tasks.

The project is designed to be extensible via plugins.  For demonstration
purposes we provide a tiny plugin system and a single example task.  More
complex projects could load plugins dynamically using entry points.
"""

import importlib
import sys
from importlib import metadata
from typing import Dict

from ..ume.models import TaskPointer
from ..pointer_store import PointerStore


from ..scheduler import get_default_scheduler


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


_old_module = sys.modules.get(__name__)
webhook_task_registry: list[type[WebhookTask]]
if _old_module and hasattr(_old_module, "webhook_task_registry"):
    webhook_task_registry = _old_module.webhook_task_registry  # type: ignore[assignment]
else:
    webhook_task_registry = []


def register_webhook_task(cls: type[WebhookTask]) -> type[WebhookTask]:
    """Register a ``WebhookTask`` subclass for event delivery."""

    webhook_task_registry.append(cls)
    return cls



class ManualTrigger(BaseTask):
    """Base class for tasks triggered manually."""
    pass


class PointerTask(BaseTask):
    """Task referencing other users' task runs via pointers."""

    def __init__(self, *, store: PointerStore | None = None) -> None:
        self.store = store or PointerStore()
        self.pointers: list[TaskPointer] = [
            TaskPointer(**p) for p in self.store.get_pointers(self.name)
        ]

    def add_pointer(self, user_id: str, run_id: str) -> None:
        from ..ume import _hash_user_id

        self.pointers.append(TaskPointer(run_id=run_id, user_hash=_hash_user_id(user_id)))
        self.store.add_pointer(self.name, user_id, run_id)

    def get_pointers(self) -> list[TaskPointer]:
        return list(self.pointers)


# ---------------------------------------------------------------------------
# Example tasks shipped with this repository.  Real deployments would load
# plugins in a more dynamic fashion.

class ExampleTask(CronTask):
    """Very small task used in the examples."""

    name = "example"

    def run(self):  # pragma: no cover - illustrative
        print("Example task executed")


@register_webhook_task
class GitHubWebhookTask(WebhookTask):
    """Example task parsing GitHub webhook events."""

    events: list[str] = []

    def handle_event(self, source: str, event_type: str, payload: dict) -> None:
        """Record issue actions from GitHub events."""

        if source == "github" and event_type == "issues":
            action = payload.get("action")
            if action:
                self.__class__.events.append(action)


@register_webhook_task
class CalComWebhookTask(WebhookTask):
    """Example task parsing Cal.com webhook events."""

    events: list[str] = []

    def handle_event(self, source: str, event_type: str, payload: dict) -> None:
        """Record booking events from Cal.com."""

        if source == "calcom" and event_type == "booking":
            event = payload.get("event")
            if event:
                self.__class__.events.append(event)


# ``registered_tasks`` is consumed by the scheduler during initialisation.
registered_tasks: Dict[str, BaseTask] = {
    ExampleTask.name: ExampleTask(),
}

try:
    _tino_cls = importlib.import_module("task_cascadence.plugins.d0tTino").D0tTinoTask
    registered_tasks[_tino_cls.name] = _tino_cls()
except Exception:  # pragma: no cover - optional plugin may fail to import
    pass

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
        get_default_scheduler().register_task(task.name, task)


def load_cronyx_plugins(base_url: str) -> None:
    """Load tasks from a CronyxServer instance."""

    from .cronyx_server import CronyxServerLoader

    loader = CronyxServerLoader(base_url)
    for info in loader.list_tasks():
        data = loader.load_task(info["id"])
        task = load_plugin(data["path"])
        registered_tasks[task.name] = task
        get_default_scheduler().register_task(task.name, task)


def initialize() -> None:
    """Register built-in tasks and load any external plugins."""

    for _name, _task in registered_tasks.items():
        get_default_scheduler().register_task(_name, _task)

    load_entrypoint_plugins()

    from ..config import load_config

    cfg = load_config()
    if cfg.get("cronyx_base_url"):
        load_cronyx_plugins(cfg["cronyx_base_url"])




def load_cronyx_tasks() -> None:
    """Load tasks from a configured Cronyx server."""
    from ..config import load_config

    url = load_config().get("cronyx_base_url")
    if not url:
        return
    try:
        from .cronyx_server import CronyxServerLoader
        loader = CronyxServerLoader(url)
        for info in loader.list_tasks():
            meta = loader.load_task(info["id"])
            module, cls_name = meta["path"].split(":")
            mod = importlib.import_module(module)
            cls = getattr(mod, cls_name)
            obj = cls()
            registered_tasks[obj.name] = obj
            get_default_scheduler().register_task(obj.name, obj)
    except Exception:  # pragma: no cover - best effort loading
        pass


def reload_plugins() -> None:
    """Reload plugin modules and reset the default scheduler."""

    from importlib import reload, invalidate_caches
    import task_cascadence
    from .. import scheduler as _scheduler
    from ..config import load_config

    for task in registered_tasks.values():
        mod = task.__class__.__module__
        if mod != __name__ and mod in sys.modules:
            del sys.modules[mod]

    invalidate_caches()
    _scheduler._default_scheduler = None  # reset singleton
    cfg = load_config()
    sched = _scheduler.create_scheduler(cfg["backend"])
    _scheduler.set_default_scheduler(sched)
    reload(sys.modules[__name__])
    task_cascadence.initialize()

