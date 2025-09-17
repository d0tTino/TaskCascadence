# Quickstart for TaskCascadence

This guide walks you through running a simple task pipeline locally using TaskCascadence.

## Prerequisites

- Python 3.9+
- Optional: Docker and a Temporal server (for advanced workflow execution)

## Steps

1. Clone the repository and set up a virtual environment:
   ```bash
   git clone https://github.com/d0tTino/TaskCascadence.git
   cd TaskCascadence
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

2. Define a demo pipeline and run it:
   ```python
   from task_cascadence.orchestrator import TaskPipeline

   class Demo:
       def research(self):
           return "search terms"

       def plan(self):
           return "plan"

       def run(self):
           return "result"

       def verify(self, result):
           return result == "result"

   pipeline = TaskPipeline(Demo())
   pipeline.run(user_id="alice")
   ```
   Running this script will execute the pipeline locally, emitting UME events for each stage.

3. To enable Temporal integration, install the `temporalio` package and start a Temporal server via Docker. Then set the `TEMPORAL_HOST` and `TEMPORAL_PORT` environment variables before running your pipeline.

4. Schedule the task to run automatically:
   1. Create a `schedules.yml` file:
      ```yaml
      Demo:
        expr: "*/5 * * * *"
        user_id: alice
        group_id: engineering
      ```
   2. Load the configuration with the CLI:
      ```bash
      task load-schedules schedules.yml
      ```
   Cron expressions follow the `minute hour day-of-month month day-of-week` pattern.
   The example above runs the `Demo` task every five minutes with the provided
   user and group identifiers. Use values such as `0 9 * * MON-FRI` for weekday
   runs or `30 2 1 * *` for a monthly schedule.

## More information

Refer to the full documentation in `README.md` for details about pipeline configuration, plugins, and advanced features.
