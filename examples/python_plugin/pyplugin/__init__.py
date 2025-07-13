from task_cascadence.plugins import ManualTrigger

class DemoTask(ManualTrigger):
    name = "demo_python"

    def run(self):
        print("demo python task")
