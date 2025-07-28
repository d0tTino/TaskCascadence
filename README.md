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

## Nested Pipelines

The return value of ``plan`` may include a list of tasks or ``TaskPipeline``
objects. When present, the pipeline runs each subtask in sequence and passes the
list of results to the parent ``run`` or ``verify`` stage.

```python
class Parent:
    def plan(self):
        return [Child("a"), Child("b")]

    def verify(self, results):
        assert results == ["a", "b"]
```

This allows tasks to be decomposed into smaller reusable units without defining
separate schedules.

## Command Line Usage

After installing the package in an environment with ``typer`` available, the
``task`` command becomes available.  It exposes several sub-commands:

```bash
$ task list            # show all registered tasks
$ task run NAME        # execute a task
$ task trigger NAME    # run a manual trigger task
$ task disable NAME    # disable a task
$ task pause NAME      # pause a task
$ task resume NAME     # resume a paused task
$ task schedule NAME CRON  # register a cron schedule
$ task schedules       # list configured schedules
$ task ai-idea TEXT    # emit an idea seed via UME
$ task export-n8n FILE # dump tasks as n8n workflow
$ task webhook [--host HOST] [--port PORT]  # start webhook server
$ task watch-plugins DIR # reload plugins on changes in DIR
```

Global options allow metrics and UME transport configuration:

```bash
$ task --metrics-port 9000 --transport grpc \
      --grpc-stub module:Stub run example
```

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
httpx.post("http://localhost:8000/tasks/example/run", headers={"X-User-ID": "bob"})

# capture a task definition from another machine
httpx.post(
    "http://localhost:8000/tasks",
    params={"path": "myproject.tasks:Demo", "schedule": "0 * * * *"},
)
```

This allows capturing a task definition on one device and registering it on another.

Including the ``X-User-ID`` header attaches a hashed identifier to emitted
events, aligning with the project's privacy goals.

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

## Schedule Persistence

``CronScheduler`` stores cron expressions in ``schedules.yml`` by default.  The
file is created next to the running application unless ``storage_path`` is
overridden.  It contains a simple YAML mapping of task class names to their
crontab schedules:

```yaml
ExampleTask: "0 12 * * *"
```

Timezone-aware scheduling requires access to the IANA timezone database
provided by the `tzdata` package. Ensure this package is installed when
creating schedulers with non-UTC timezones.

When a new ``CronScheduler`` instance starts it reads this file and re-creates
any jobs for which task objects are supplied via the ``tasks`` argument.  This
allows scheduled tasks to survive process restarts.

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
Cascadence retrieve tasks from a running CronyxServer. Use the following
environment variables to configure the integration:

``CRONYX_BASE_URL``
    Base URL of the CronyxServer instance.
``CRONYX_TIMEOUT``
    Request timeout in seconds when contacting the server.
``CASCADENCE_CRONYX_REFRESH``
    Disable periodic refreshes when ``0`` or ``false``.

Example workflow::

    CronyxServer --listen :8000 &
    curl -X POST -H 'Content-Type: application/json' \
        -d '{"id":"demo","path":"examples.python_plugin.demo:DemoTask"}' \
        http://localhost:8000/tasks
    export CRONYX_BASE_URL=http://localhost:8000
    export CASCADENCE_SCHEDULER=cronyx
    task list


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
    Re-emit pointer updates received by ``pointer_sync`` when ``true``.
``CASCADENCE_HASH_SECRET``
    Salt used when hashing user identifiers.
``CASCADENCE_STAGES_PATH``
    Location of the ``stages.yml`` file used by :class:`StageStore`.
``CASCADENCE_POINTERS_PATH``
    Location of the ``pointers.yml`` file used by :class:`PointerStore`.
``CASCADENCE_TASKS_PATH``
    Location of the ``tasks.yml`` file used by :class:`TaskStore`.

The YAML configuration may also define ``stages_path``, ``pointers_path`` and ``tasks_path`` to override these defaults.

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
broadcast_pointers: true
```

Install ``tino_storm`` to allow tasks to perform research queries during the
``research`` pipeline stage.
Install ``ai_plan`` if you want automatic planning for tasks missing a ``plan``
method.

## Hashing User IDs

User identifiers passed to emission helpers are hashed before transport.
Provide a secret via the ``CASCADENCE_HASH_SECRET`` environment variable to
salt these hashes, e.g. ``CASCADENCE_HASH_SECRET=abc123``. Omitting the
variable hashes the plain ID.

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
version constraint is respected:

```bash
$ pip install -e .[dev]
```

The ``dev`` extras also install stub packages for ``requests`` and ``PyYAML`` so
that ``mypy`` can perform complete type checking. Install these extras before
running ``ruff``, ``mypy`` or ``pytest``.

This will install tools like ``ruff``, ``pytest`` and ``mypy``. Run them from
the project root to lint, type-check and test the codebase:

```bash
$ ruff .
$ mypy .
$ pytest
```

The test suite requires ``protobuf>=6``.

