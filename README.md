# Cascadence

Cascadence aims to provide a flexible framework for orchestrating complex, multi-step tasks. The project is inspired by the Product Requirements Document (PRD), which outlines features such as:

- Graph-based task definitions for clear dependency management.
- Plugin architecture allowing custom processors and extensions.
- Configurable execution engine supporting synchronous and asynchronous flows.
- Built-in instrumentation and monitoring.

This repository lays the groundwork for the Python package implementation.

## Temporal Integration

The :mod:`task_cascadence.temporal` module wraps the ``temporalio`` client.
Schedulers can use this backend to execute workflows remotely and replay
workflow histories for debugging purposes.

## Task Orchestrator

Tasks can be executed through the :class:`~task_cascadence.orchestrator.TaskPipeline`.
Each pipeline step emits UME events so external systems can observe progress.
Stages are executed in the following order when present on the task object:
``intake`` → ``research`` → ``plan`` → ``run`` → ``verify``.

```python
from task_cascadence.orchestrator import TaskPipeline

class Demo:
    def research(self):
        return "search terms"

    def plan(self):
        return "plan"

    def run(self):
        return "result"

pipeline = TaskPipeline(Demo())
pipeline.run(user_id="alice")
```

The optional ``research`` step relies on the ``tino_storm`` package which may
be installed separately. The provided ``user_id`` is hashed before transport.
If the optional ``ai_plan`` package is present, tasks without a ``plan`` method
fall back to ``ai_plan.plan`` during the planning stage.

### Preflight Checks

Before executing a task, ``TaskPipeline`` looks for a ``precheck()`` method.
If implemented and it returns ``False`` or raises an error the pipeline stops
before the ``run`` stage.

## Asynchronous Tasks

Cascadence supports coroutine-based tasks. Define an ``async`` ``run`` method
or other asynchronous stage functions and the scheduler will ensure they are
executed in an event loop. If no loop is active, ``asyncio.run`` is used; when a
loop is already running the coroutine is returned so callers can ``await`` it.

```python
class AsyncExample:
    async def run(self) -> str:
        await asyncio.sleep(0)
        return "async result"

```

This task can be registered dynamically via the API:

```python
httpx.post(
    "http://localhost:8000/tasks",
    params={"path": "myproject.tasks:AsyncExample", "schedule": "*/5 * * * *"},
)
```

## Mandatory User and Group Headers

Tasks run with a user identifier and a group identifier so results and audit
logs can be scoped. When calling the HTTP API you **must** include both
``X-User-ID`` and ``X-Group-ID`` headers; requests missing either header are
rejected:

```bash
curl -X POST http://localhost:8000/tasks/example/run \
     -H "X-User-ID: alice" \
     -H "X-Group-ID: engineering"
```

The response includes the unique run identifier alongside the task output, e.g.
``{"run_id": "20240101T120000Z", "result": "ok"}``.

In code, pass the same information via ``user_id`` and ``group_id`` arguments:

```python
pipeline.run(user_id="alice", group_id="engineering")
from task_cascadence.workflows import dispatch
dispatch("calendar.event.create_request", payload, user_id="alice", group_id="engineering")
```

User and group identifiers are hashed before being persisted.

### Flow Through Tasks and Audit Logs

1. **Request handling.** The HTTP API requires ``X-User-ID`` and optionally
   ``X-Group-ID`` headers. The dependency helpers
   :func:`task_cascadence.api.get_user_id` and
   :func:`task_cascadence.api.get_group_id` validate and return these values, so
   each endpoint consistently receives them before invoking the scheduler.【F:task_cascadence/api/__init__.py†L36-L145】
2. **Task execution.** The scheduler forwards the identifiers to the
   :class:`~task_cascadence.orchestrator.TaskPipeline`, whose stage methods pass
   them into :func:`task_cascadence.ume.emit_audit_log` and
   :func:`task_cascadence.ume.emit_stage_update_event` calls. Every stage
   transition therefore records the user and group context alongside the stage
   outcome.【F:task_cascadence/orchestrator.py†L77-L151】
3. **Audit persistence and emission.** ``emit_audit_log`` hashes any supplied
   ``user_id`` with the configured secret, stores the hash together with the
   ``group_id`` in :class:`~task_cascadence.stage_store.StageStore`, and attaches
   both the raw identifier and the hash to outbound ``AuditEvent`` messages so
   listeners can correlate activity without exposing the plain ID.【F:task_cascadence/ume/__init__.py†L90-L179】【F:task_cascadence/stage_store.py†L73-L121】

## Nested Pipelines

The return value of ``plan`` may include a list of tasks or ``TaskPipeline``
objects. When present, the pipeline runs each subtask in sequence and passes the
list of results to the parent ``run`` or ``verify`` stage. Subtasks can also be
executed in parallel by returning ``{"execution": "parallel", "tasks": [...]}``
or a :class:`~task_cascadence.orchestrator.ParallelPlan` instance. Results from
parallel execution are aggregated in the same way.

```python
class Parent:
    def plan(self):
        return [Child("a"), Child("b")]

    def verify(self, results):
        assert results == ["a", "b"]
```

Parallel execution is similar:

```python
from task_cascadence.orchestrator import ParallelPlan

class Parent:
    def plan(self):
        return {"execution": "parallel", "tasks": [Child("a"), Child("b")]}

    # or equivalently
class Parent:
    def plan(self):
        return ParallelPlan([Child("a"), Child("b")])
```

This allows tasks to be decomposed into smaller reusable units without defining
separate schedules.

## Command Line Usage

After installing the package in an environment with ``typer`` available, the
``task`` command becomes available.  It exposes several sub-commands:

```bash
$ task list            # show all registered tasks
$ task run NAME --user-id USER --group-id GROUP
$ task trigger NAME --user-id USER --group-id GROUP
$ task disable NAME --group-id GROUP
$ task pause NAME [--user-id USER] --group-id GROUP
$ task resume NAME [--user-id USER] --group-id GROUP
$ task schedule NAME CRON --user-id USER --group-id GROUP
$ task unschedule NAME   # remove a cron schedule
$ task schedules       # list configured schedules
$ task ai-idea TEXT    # emit an idea seed via UME
$ task export-n8n FILE # dump tasks as n8n workflow
$ task webhook [--host HOST] [--port PORT]  # start webhook server
$ task watch-plugins DIR # reload plugins on changes in DIR
```

### Cron schedules and YAML configuration

Cron expressions use the standard five-field format interpreted by
APScheduler: ``minute hour day-of-month month day-of-week``. Wildcards
(``*``), step values (``*/5``), ranges (``1-5``), and named weekdays or
months (``MON-FRI``, ``JAN``) are all accepted. For example:

- ``*/5 * * * *`` – run every five minutes.
- ``0 9 * * MON-FRI`` – run at 09:00 Monday through Friday.
- ``30 2 1 * *`` – run at 02:30 on the first day of each month.

Schedules can be loaded in bulk from a YAML file. Each key should match the
task's class name or registered identifier. Values may be a bare cron string
or a mapping with extra metadata:

```yaml
# schedules.yml
ExampleTask: "*/5 * * * *"

NightlySummary:
  expr: "0 1 * * *"
  user_id: analytics@example.com
  group_id: insights
  recurrence:
    note: Nightly ETL refresh

CalendarDrivenTask:
  calendar_event:
    node: /calendar/nodes/team-sync
    poll: 600  # refresh every 10 minutes
  user_id: manager@example.com
  group_id: leadership
```

Use ``task load-schedules schedules.yml`` to register every entry. ``user_id``
and ``group_id`` values are hashed on disk automatically. When a
``calendar_event`` is supplied the scheduler pulls recurrence details from the
referenced UME node and keeps them current.

Global options allow metrics and UME transport configuration:

```bash
$ task --metrics-port 9000 --transport grpc \
      --grpc-stub module:Stub run example
```

### Plugin Watcher

Follow these steps to reload plugins while you develop:

1. **Start the watcher** with the directory containing your plugin files:

   ```bash
   $ task watch-plugins myplugins/
   ```

2. **Edit your plugin** files and save them. The watcher recursively monitors
   the directory and reloads any modules that change.

3. **Platform considerations** – Cascadence uses a polling observer so changes
   on network filesystems or uncommon platforms are still detected. This may
   introduce a small delay between saving a file and the reload.

4. **Stop watching** at any time by pressing ``Ctrl+C`` in the terminal. The
   command exits gracefully and closes the observer.

### Metrics Endpoint
Start the CLI with ``--metrics-port`` to serve Prometheus metrics:

```bash
$ task --metrics-port 9000 run example
```

Visiting ``http://localhost:9000/metrics`` returns standard output such as:

```text
# HELP task_latency_seconds Time spent executing tasks
# TYPE task_latency_seconds histogram
task_latency_seconds_bucket{le="0.005",task_name="example"} 1.0
task_success_total{task_name="example"} 1.0
```

Configure Prometheus to scrape this endpoint:

```yaml
scrape_configs:
  - job_name: cascadence
    static_configs:
      - targets: ['localhost:9000']
```

``task webhook`` launches a FastAPI application that dispatches incoming
events to any registered :class:`WebhookTask` implementations.

Two built-in webhook tasks show how payloads from different services can be
handled:

* ``GitHubWebhookTask`` extracts the ``action`` field from ``issues`` events.
* ``CalComWebhookTask`` records the ``event`` name from Cal.com bookings.

The repository also ships with a small ``example`` cron task to demonstrate the
mechanics.

The CLI's ``main`` function can also be called programmatically:

```python
from task_cascadence.cli import main

main([])  # run without command-line arguments
```

``main`` accepts an optional ``args`` list which defaults to ``[]`` and is
passed to the underlying Typer application.

## REST API

In addition to the CLI and webhook server, Cascadence provides a small FastAPI
application for programmatic task management. Start it with:

```bash
uvicorn task_cascadence.api:app
```

Example usage with ``httpx``:

```python
import httpx

tasks = httpx.get("http://localhost:8000/tasks").json()
run = httpx.post(
    "http://localhost:8000/tasks/example/run",
    headers={"X-User-ID": "bob", "X-Group-ID": "engineering"},
).json()
print(f"Started job {run['run_id']} with result {run['result']}")

# capture a task definition from another machine
httpx.post(
    "http://localhost:8000/tasks",
    params={"path": "myproject.tasks:Demo", "schedule": "0 * * * *"},
)
```

This allows capturing a task definition on one device and registering it on another.

Including ``X-User-ID`` and ``X-Group-ID`` headers attaches identifiers to
emitted events while hashing user values for privacy.

Pointers for :class:`PointerTask` implementations can be managed via the API as well:

```python
httpx.post(
    "http://localhost:8000/pointers/family_pointer",
    params={"user_id": "alice", "run_id": "run42"},
)
httpx.get("http://localhost:8000/pointers/family_pointer").json()
httpx.post(
    "http://localhost:8000/pointers/family_pointer/receive",
    params={"run_id": "xyz", "user_hash": "abc"},
)
```

## Dashboard

Launch the web dashboard with:

```bash
uvicorn task_cascadence.dashboard:app
```

Navigate to ``http://localhost:8000`` to see task statuses and pause or resume
individual tasks. The table also lists the number of pointers stored for each
task via ``PointerStore``.

## Audit Log Behavior

Each stage transition within a :class:`TaskPipeline` results in an entry
recorded via ``emit_audit_log``. A record is written when a stage starts,
completes, or fails. Each entry includes the stage name, status (for example
``started``, ``success``, ``error`` or ``skipped``), optional ``reason`` or
``output`` fields, and the hashed user and group identifiers supplied through
the mandatory headers. Audit records are persisted by
:class:`StageStore` and emitted on the configured transport so external systems
can observe them. Retrieve stored logs through the API:

```bash
curl http://localhost:8000/pipeline/MyTask
```

This endpoint returns the chronological audit trail for ``MyTask``.

## Recurring Schedule YAML Format

``CronScheduler`` persists recurrence rules in ``schedules.yml``.  The file is
created next to the running application unless ``storage_path`` is overridden
and maps task names to cron expressions and optional context. For privacy,
user identifiers are stored as hashes when schedules are persisted.
Schedules may be specified directly with an ``expr`` field or by referencing a
UME calendar event:

```yaml
ExampleTask:
  expr: "0 12 * * *"
  user_id: alice  # stored hashed on disk
  group_id: ops
  recurrence:
    note: "every day at noon"
```

Another form references an existing calendar event. The scheduler fetches the
event's recurrence rule and applies it, optionally polling for changes:

```yaml
ExampleTask:
  calendar_event:
    node: "evt123"
    poll: 3600  # refresh every hour
```

A calendar event can also drive domain-specific tasks. The following example
links a monthly budget review to a calendar entry whose recurrence is expressed
in cron syntax:

```python
from task_cascadence.workflows import dispatch

# create the calendar event once
dispatch(
    "calendar.event.create_request",
    {
        "title": "Monthly Budget Review",
        "start_time": "2024-01-01T09:00:00Z",
        "recurrence": {"cron": "0 9 1 * *"},
    },
    user_id="finance",
    group_id="ops",
)
```

```yaml
BudgetReview:
  calendar_event:
    node: "evt-budget-review"  # ID returned from event creation
    poll: 86400  # refresh daily for updates
```

This causes the ``BudgetReview`` task to run at 09:00 on the first day of each
month. Adjust the ``poll`` interval if changes to the calendar event should be
picked up more frequently.

The scheduler reloads this file on startup and recreates any jobs whose task
objects are available via the ``tasks`` argument. Editing the file adjusts
recurrence without changing code. Removing an entry triggers an ``unschedule``
stage event for that task.

Timezone-aware scheduling requires access to the IANA timezone database
provided by the ``tzdata`` package when creating schedulers with non-UTC
timezones.

## Task DAGs

``DagCronScheduler`` extends the cron backend with support for task
dependencies.  Tasks can declare prerequisite task names which are executed in
topological order before the task itself.

```python
from task_cascadence.scheduler.dag import DagCronScheduler
from task_cascadence.plugins import CronTask


class Extract(CronTask):
    def run(self):
        print("extract")


class Transform(CronTask):
    def run(self):
        print("transform")


class Load(CronTask):
    def run(self):
        print("load")


sched = DagCronScheduler()
sched.register_task(Extract(), "0 * * * *")
sched.register_task(Transform(), "5 * * * *", dependencies=["Extract"])
sched.register_task(Load(), "10 * * * *", dependencies=["Transform"])
```

Running ``Load`` via ``sched.run_task("Load")`` first executes ``Extract`` and
``Transform`` before ``Load``.

## Scheduler Backend

``task_cascadence.initialize`` reads configuration to decide which scheduler
backend to instantiate. By default the cron-based scheduler is used. Set the
``CASCADENCE_SCHEDULER`` environment variable to ``base``, ``temporal`` or
``cronyx`` or provide a YAML file via ``CASCADENCE_CONFIG`` containing::

    backend: temporal

This selects the Temporal-based scheduler. ``backend: base`` chooses the simple
in-memory scheduler instead. ``backend: cronyx`` configures a
``CronyxScheduler`` which forwards task execution to a running CronyxServer.

### Cronyx Backend

Selecting ``backend: cronyx`` or setting ``CASCADENCE_SCHEDULER=cronyx`` makes
Cascadence retrieve tasks from a running CronyxServer. Configure the
connection by setting the following environment variables before starting
Cascadence:

``CRONYX_BASE_URL``
    Base URL of the CronyxServer instance.
``CRONYX_TIMEOUT``
    Request timeout in seconds when contacting the server.
``CASCADENCE_CRONYX_REFRESH``
    Disable periodic refreshes when ``0`` or ``false``.

Ensure a CronyxServer is running (for instance ``CronyxServer --listen :8000``) before starting Cascadence.

A minimal ``cascadence.yml`` enabling this backend:

```yaml
backend: cronyx
cronyx_base_url: http://localhost:8000
# cronyx_timeout: 5
```

## CronyxServer Setup

Cascadence can fetch tasks from a running CronyxServer. Start the server,
register tasks, and configure the scheduler with environment variables.

1. **Launch the server**:

   ```bash
   CronyxServer --listen :8000
   ```

2. **Register a task**:

   ```bash
   curl -X POST -H 'Content-Type: application/json' \
        -d '{"id":"demo","path":"examples.python_plugin.demo:DemoTask"}' \
        http://localhost:8000/tasks
   ```

3. **Configure Cascadence**:

   ```bash
   export CRONYX_BASE_URL=http://localhost:8000
   export CRONYX_TIMEOUT=5
   export CASCADENCE_CRONYX_REFRESH=1
   export CASCADENCE_SCHEDULER=cronyx
   ```

4. **Schedule the remote task**:

   ```bash
   task list
   task schedule demo "*/5 * * * *" --user-id alice --group-id engineering
   ```


## Plugin Discovery

Additional tasks can be provided by external packages using the
``task_cascadence.plugins`` entry point group. Each entry should resolve to a
class deriving from :class:`~task_cascadence.plugins.BaseTask`. When the package
is imported these entry points are loaded automatically and registered with the
default scheduler.

An example ``pyproject.toml`` exposing a plugin looks like:

```toml
[project.entry-points."task_cascadence.plugins"]
demo = "myplugin:DemoTask"
```

Plugins implemented in Rust or Go can be built and served via a CronyxServer
instance. Set the ``CRONYX_BASE_URL`` environment variable to the server's URL
and Cascadence will fetch and register any advertised tasks on startup. Example
plugin source for Python, Rust and Go lives in the ``examples/`` directory.
See the [Go plugin README](examples/go_plugin/README.md) and
[Rust plugin README](examples/rust_plugin/README.md) for build instructions.
The ``examples/orchestrated_mod_setup.py`` script demonstrates a staged mod
installation workflow that can be paused and resumed.

To try the Python demo plugin install it in editable mode and list the
available tasks:

```bash
$ pip install -e examples/python_plugin
$ task list
```

``DemoTask`` from the plugin will appear alongside the built-in example task.

The package also ships with ``D0tTinoTask`` which talks to the
`d0tTino` helpers.  It invokes ``d0tTino ai-plan`` during the planning stage
and ``d0tTino ai`` when running.  Instantiate it with a prompt and optionally
``use_api=True`` to call a running API server:

```python
from task_cascadence.plugins import D0tTinoTask

task = D0tTinoTask("Hello world")
task.plan()
task.run()
```

## Environment Variables

``load_config`` merges values from a YAML file specified by ``CASCADENCE_CONFIG``
with the variables below. When set, they override values from the YAML file:

``CASCADENCE_CONFIG``
    Path to a YAML configuration file.

``CASCADENCE_SCHEDULER``
    Select the scheduler backend (``cron``/``base``/``temporal``/``cronyx``).
``CASCADENCE_CRONYX_REFRESH``
    Disable Cronyx task refreshing when ``0`` or ``false``.
``CRONYX_BASE_URL``
    URL of a CronyxServer exposing additional tasks.
``CRONYX_TIMEOUT``
    Timeout in seconds when contacting the CronyxServer.
``TEMPORAL_SERVER``
    Address of the Temporal service.
``UME_TRANSPORT``
    Transport type for UME event emission (``grpc`` or ``nats``).
``UME_GRPC_STUB``
    Import path to the gRPC stub class.
``UME_GRPC_METHOD``
    RPC method used by the gRPC stub.
``UME_NATS_CONN``
    Import path to a configured NATS connection.
``UME_NATS_SUBJECT``
    NATS subject for event publishing.
``UME_BROADCAST_POINTERS``
    When ``true``, ``pointer_sync`` re-emits every pointer update so other listeners
    stay synchronized.
``CASCADENCE_HASH_SECRET``
    Salt used when hashing user identifiers.
``CASCADENCE_STAGES_PATH``
    Location of the ``stages.yml`` file used by :class:`StageStore`.
``CASCADENCE_POINTERS_PATH``
    Location of the ``pointers.yml`` file used by :class:`PointerStore`.
``CASCADENCE_TASKS_PATH``
    Location of the ``tasks.yml`` file used by :class:`TaskStore`.
``CASCADENCE_IDEAS_PATH``
    Location of the ``ideas.yml`` file used by :class:`IdeaStore`.

The YAML configuration may also define ``stages_path``, ``pointers_path``, ``tasks_path`` and ``ideas_path`` to override these defaults.

Example ``cascadence.yml`` enabling gRPC transport and research support:

```yaml
backend: cron
ume_transport: grpc
ume_grpc_stub: myproject.rpc:Stub
ume_grpc_method: Send
hash_secret: supersecret
stages_path: /tmp/stages.yml
pointers_path: /tmp/pointers.yml
tasks_path: /tmp/tasks.yml
ideas_path: /tmp/ideas.yml
broadcast_pointers: true
```

### Configuring UME Transports

Use :func:`ume.configure_transport` to set the default transport in code. The
function accepts the transport name and initialization parameters.

```python
from task_cascadence import ume
from myproject.rpc import Stub

stub = Stub()
ume.configure_transport("grpc", stub=stub, method="Send")

from nats.aio.client import Client as NATS

conn = NATS()
ume.configure_transport("nats", connection=conn, subject="events")
```


Install ``tino_storm`` to allow tasks to perform research queries during the
``research`` pipeline stage.
Install ``ai_plan`` if you want automatic planning for tasks missing a ``plan``
method.

## Hashing User IDs

User identifiers passed to emission helpers are hashed before transport using
``_hash_user_id`` from ``task_cascadence/ume/__init__.py``. The function salts
each ID with ``CASCADENCE_HASH_SECRET`` and is used throughout the API, CLI and
dashboard so the same input maps to a consistent hash. Salting allows
deployments to generate unique hashes while avoiding exposure of the raw ID,
helping preserve privacy when events are stored or forwarded.

## Pointer Sync Service

When tasks emit pointer updates they can be synchronized across multiple
instances.  Run the small service in :mod:`task_cascadence.pointer_sync` to
store updates received via the configured transport:

```bash
$ python -m task_cascadence.pointer_sync
```

The service reads the same ``UME_*`` environment variables used for event
emission and persists incoming :class:`PointerUpdate` messages via
``PointerStore.apply_update``.

### Running ``task pointer-sync``

The service can also be started through the CLI which wraps the module entry
point.

1. **Configure UME transport** using environment variables. For example with
   NATS:

   ```bash
   export UME_TRANSPORT=nats
   export UME_NATS_CONN=myproject.nats:conn
   export UME_NATS_SUBJECT=events
   ```

   Or with gRPC:

   ```bash
   export UME_TRANSPORT=grpc
   export UME_GRPC_STUB=myproject.rpc:Stub
   export UME_GRPC_METHOD=Subscribe
   ```

2. **Start the service** on each instance:

   ```bash
   $ task pointer-sync
   ```

3. *(Optional)* Set ``UME_BROADCAST_POINTERS=1`` so each instance re-emits
   updates it receives. This allows other listeners on the same transport to
   apply the changes and keeps the ``PointerStore`` files in sync.

### Propagation Example

Start ``task pointer-sync`` on both instances. Adding a pointer on **instance
A** automatically appears on **instance B**:

```bash
# Instance A
$ task pointer-add demo_pointer alice run42

# Instance B
$ task pointer-list demo_pointer
run42    HASHED_USER
```

## Capability Planner

The :mod:`task_cascadence.capability_planner` module watches UME for
``TaskRun`` events with ``status='error'``. When a failure is detected a
placeholder follow-up task is scheduled via the default scheduler.

Start the service directly or via the CLI:

```bash
$ python -m task_cascadence.capability_planner
$ task capability-planner
```

## Event-Driven Workflows

Cascadence includes sample workflows that react to events published on the UME
bus.  Use :func:`task_cascadence.workflows.dispatch` to trigger them from your
code or services.

### CalendarEventCreation

This workflow listens for ``calendar.event.create_request`` and persists a
validated event to an external calendar service after optional research. A
``calendar.event.created`` event is emitted once the entry is stored. Include
recurrence metadata to schedule repeating events:

```python
from task_cascadence.workflows import dispatch

payload = {
    "title": "Team Sync",
    "start_time": "2024-04-01T09:00:00Z",
    "invitees": ["bob"],
    "recurrence": {"cron": "0 9 * * 1"},
}
dispatch(
    "calendar.event.create_request",
    payload,
    user_id="alice",
    group_id="engineering",
)
```

### FinancialDecisionSupport

``FinancialDecisionSupport`` aggregates accounts and goals, calls an external
engine, and stores the resulting analysis when ``finance.decision.request`` is
dispatched. It emits ``finance.decision.result`` and optionally
``finance.explain.request`` events. Recurrence metadata schedules periodic
analyses:

```python
from task_cascadence.workflows import dispatch

dispatch(
    "finance.decision.request",
    {"explain": True, "recurrence": {"cron": "0 8 * * *"}},
    user_id="bob",
    group_id="finance",
)
```

## n8n Export

Tasks registered with Cascadence can be exported as an n8n workflow using the
``export-n8n`` command:

```bash
$ task export-n8n workflow.json
```

The resulting ``workflow.json`` can be imported into your n8n instance by
selecting **Import from File** in the workflow menu and choosing the generated
file.

## Development Setup

Install Cascadence with its optional development dependencies in editable mode.
The package pins ``httpx`` to ``<0.28`` for compatibility, so make sure this
version constraint is respected. You can do this with:

```bash
$ pip install -e .[dev]
```

The ``dev`` extras include ``mypy`` as well as the ``types-requests`` and
``types-PyYAML`` stub packages. These are required for ``test_mypy_runs`` to
pass.

## Testing

After installing the development extras, run the linters and test suite:

```bash
ruff .
mypy .
pytest
```

The test suite requires ``protobuf>=6``.

