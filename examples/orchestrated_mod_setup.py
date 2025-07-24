"""Example demonstrating staged mod installation with pause/resume."""

from task_cascadence.scheduler.dag import DagCronScheduler
from task_cascadence.plugins import CronTask
from task_cascadence.ume import emit_stage_update


def plan_mod_setup() -> list[str]:
    """Return the ordered installation steps."""

    # In a real deployment this would call into an external planner
    return ["download", "install", "enable"]


class ModSetup(CronTask):
    """Multi-stage mod setup task using :class:`TaskPipeline`."""

    name = "ModSetup"

    def __init__(self) -> None:
        self._plan: list[str] = []

    def plan(self) -> list[str]:
        print("Planning mod setup")
        self._plan = plan_mod_setup()
        return self._plan

    def run(self) -> None:
        for step in self._plan:
            if step == "download":
                print("Downloading mod archive")
            elif step == "install":
                print("Installing mod")
            elif step == "enable":
                print("Enabling mod")
            emit_stage_update("mod_setup", step)

    def verify(self, _result: None) -> None:
        print("Mod enabled and verified")


def main() -> None:
    sched = DagCronScheduler(timezone="UTC")
    sched.register_task(ModSetup(), "* * * * *")

    # Run the full pipeline once
    sched.run_task("ModSetup")

    # Pause the task and attempt another run
    sched.pause_task("ModSetup")
    try:
        sched.run_task("ModSetup")
    except ValueError as exc:
        print(f"Pipeline halted: {exc}")

    # Resume the task and finish the pipeline
    sched.resume_task("ModSetup")
    sched.run_task("ModSetup")


if __name__ == "__main__":
    main()
