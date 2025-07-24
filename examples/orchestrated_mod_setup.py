"""Example demonstrating staged mod installation with pause/resume."""

from task_cascadence.scheduler.dag import DagCronScheduler
from task_cascadence.plugins import CronTask
from task_cascadence.ume import emit_stage_update


class DownloadMod(CronTask):
    name = "DownloadMod"

    def run(self) -> None:
        print("Downloading mod archive")
        emit_stage_update("mod_setup", "download")


class InstallMod(CronTask):
    name = "InstallMod"

    def run(self) -> None:
        print("Installing mod")
        emit_stage_update("mod_setup", "install")


class EnableMod(CronTask):
    name = "EnableMod"

    def run(self) -> None:
        print("Enabling mod")
        emit_stage_update("mod_setup", "enable")


def main() -> None:
    sched = DagCronScheduler(timezone="UTC")
    sched.register_task(DownloadMod(), "* * * * *")
    sched.register_task(InstallMod(), "* * * * *", dependencies=["DownloadMod"]) \
        # run after download
    sched.register_task(EnableMod(), "* * * * *", dependencies=["InstallMod"])  # final step

    # Run the full pipeline once
    sched.run_task("EnableMod")

    # Pause the install stage
    sched.pause_task("InstallMod")
    try:
        sched.run_task("EnableMod")
    except ValueError as exc:
        print(f"Pipeline halted: {exc}")

    # Resume the stage and finish the pipeline
    sched.resume_task("InstallMod")
    sched.run_task("EnableMod")


if __name__ == "__main__":
    main()
