from task_cascadence.plugins import ManualTrigger, PointerTask

class DemoTask(ManualTrigger):
    name = "demo_python"

    def run(self):
        print("demo python task")


class FamilyTask(PointerTask):
    """Example task pointing to TaskRun IDs from different users."""

    name = "family_pointer"

    def run(self):
        for ref in self.get_pointers():
            print(f"pointer to {ref.run_id} for {ref.user_hash}")
