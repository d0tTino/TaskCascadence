"""Entry points for the command-line interface.

This CLI exposes a minimal ``task`` command with sub-commands to list, run and
disable tasks as described in the PRD (FR-12).
"""

from __future__ import annotations

import click  # noqa: F401 - re-exported for CLI extensions

import importlib
import typer

from ..scheduler import (
    get_default_scheduler,
    default_scheduler as _default_scheduler,
    CronScheduler,
    BaseScheduler,
)
from .. import plugins  # noqa: F401
from ..metrics import start_metrics_server  # noqa: F401
from ..pointer_store import PointerStore
import task_cascadence as tc
from ..n8n import export_workflow

from typing import Callable, Union

default_scheduler: Union[Callable[[], BaseScheduler], BaseScheduler] = _default_scheduler


app = typer.Typer(help="Interact with Cascadence tasks")


@app.callback()
def _global_options(
    metrics_port: int | None = typer.Option(
        None,
        "--metrics-port",
        help="Expose Prometheus metrics on PORT before executing the command",
    ),
    transport: str | None = typer.Option(
        None,
        "--transport",
        help="Configure UME transport [grpc|nats]",
    ),
    grpc_stub: str | None = typer.Option(
        None,
        "--grpc-stub",
        help="Dotted path to a gRPC stub instance",
    ),
    grpc_method: str = typer.Option(
        "Send",
        "--grpc-method",
        help="Method name for gRPC emission",
    ),
    nats_conn: str | None = typer.Option(
        None,
        "--nats-conn",
        help="Dotted path to a NATS connection object",
    ),
    nats_subject: str = typer.Option(
        "events",
        "--nats-subject",
        help="Subject for NATS messages",
    ),
) -> None:
    """Handle global options for the CLI."""

    if metrics_port is not None:
        start_metrics_server(metrics_port)

    if transport:
        def _load(path: str):
            module, attr = path.split(":")
            mod = importlib.import_module(module)
            return getattr(mod, attr)

        if transport == "grpc":
            if grpc_stub is None:
                raise typer.BadParameter("--grpc-stub is required for grpc transport")
            stub = _load(grpc_stub)
            tc.ume.configure_transport("grpc", stub=stub, method=grpc_method)
        elif transport == "nats":
            if nats_conn is None:
                raise typer.BadParameter("--nats-conn is required for nats transport")
            conn = _load(nats_conn)
            tc.ume.configure_transport("nats", connection=conn, subject=nats_subject)
        else:  # pragma: no cover - validation by typer
            raise typer.BadParameter(f"Unknown transport: {transport}")


@app.command("list")
def list_tasks() -> None:
    """List all registered tasks."""

    sched = get_default_scheduler()
    for name, disabled in sched.list_tasks():
        status = "disabled" if disabled else "enabled"
        typer.echo(f"{name}\t{status}")


@app.command("run")
def run_task(
    name: str,
    temporal: bool = typer.Option(False, "--temporal", help="Execute via Temporal"),
    user_id: str | None = typer.Option(None, "--user-id", help="User ID for UME events"),
) -> None:
    """Run ``NAME`` if it exists and is enabled."""

    from ..ume import emit_stage_update

    try:
        emit_stage_update(name, "start", user_id=user_id)
        get_default_scheduler().run_task(name, use_temporal=temporal, user_id=user_id)
        emit_stage_update(name, "finish", user_id=user_id)
    except Exception as exc:  # pragma: no cover - simple error propagation
        emit_stage_update(name, "error", user_id=user_id)
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("trigger")
def manual_trigger(
    name: str,
    user_id: str | None = typer.Option(None, "--user-id", help="User ID for UME events"),
) -> None:
    """Run ``NAME`` if it is a ManualTrigger task."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info or not isinstance(task_info["task"], plugins.ManualTrigger):
        typer.echo(f"error: '{name}' is not a manual task", err=True)
        raise typer.Exit(code=1)
    sched.run_task(name, user_id=user_id)



@app.command("disable")
def disable_task(name: str) -> None:
    """Disable ``NAME`` so it can no longer be executed."""

    try:
        get_default_scheduler().disable_task(name)
        typer.echo(f"{name} disabled")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("pause")
def pause_task(name: str) -> None:
    """Pause ``NAME`` so it temporarily stops running."""

    try:
        get_default_scheduler().pause_task(name)
        typer.echo(f"{name} paused")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("resume")
def resume_task(name: str) -> None:
    """Resume a paused task called ``NAME``."""

    try:
        get_default_scheduler().resume_task(name)
        typer.echo(f"{name} resumed")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("schedule")
def schedule_task(
    name: str,
    expression: str,
    user_id: str | None = typer.Option(None, "--user-id", help="User ID for UME events"),
) -> None:
    """Schedule ``NAME`` according to ``EXPRESSION``."""

    sched = get_default_scheduler()
    if not isinstance(sched, CronScheduler):
        typer.echo("error: scheduler lacks cron capabilities", err=True)
        raise typer.Exit(code=1)
    task_info = dict(sched._tasks).get(name)
    if not task_info:
        typer.echo(f"error: unknown task '{name}'", err=True)
        raise typer.Exit(code=1)

    try:
        task = task_info["task"]
        sched.register_task(
            name_or_task=task, task_or_expr=expression, user_id=user_id
        )
        typer.echo(f"{name} scheduled: {expression}")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("schedules")
def show_schedules() -> None:
    """List configured cron schedules."""

    sched = get_default_scheduler()
    schedules = getattr(sched, "schedules", {})
    for name, expr in schedules.items():
        typer.echo(f"{name}\t{expr}")


@app.command("replay-history")
def replay_history(path: str) -> None:
    """Replay a workflow history from ``PATH``."""

    try:
        sched = default_scheduler
        if callable(sched):
            sched = sched()
        sched.replay_history(path)
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("export-n8n")
def export_n8n(path: str) -> None:
    """Export registered tasks as an n8n workflow to ``PATH``."""

    try:
        export_workflow(get_default_scheduler(), path)
        typer.echo(f"workflow written to {path}")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("webhook")
def webhook(
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(8000, "--port"),
) -> None:
    """Start the webhook server."""

    from .. import webhook as wh

    wh.start_server(host=host, port=port)


@app.command("reload-plugins")
def reload_plugins_cmd() -> None:
    """Reload installed plugins and refresh the scheduler."""

    from .. import plugins as pl
    pl.reload_plugins()

    global default_scheduler
    default_scheduler = get_default_scheduler()
    typer.echo("plugins reloaded")


@app.command("pointer-add")
def pointer_add(name: str, user_id: str, run_id: str) -> None:
    """Add a pointer to ``NAME`` for ``USER_ID`` and ``RUN_ID``."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info or not isinstance(task_info["task"], plugins.PointerTask):
        typer.echo(f"error: '{name}' is not a pointer task", err=True)
        raise typer.Exit(code=1)

    task: plugins.PointerTask = task_info["task"]
    task.add_pointer(user_id, run_id)
    typer.echo("pointer added")


@app.command("pointer-list")
def pointer_list(name: str) -> None:
    """List pointers for ``NAME``."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info or not isinstance(task_info["task"], plugins.PointerTask):
        typer.echo(f"error: '{name}' is not a pointer task", err=True)
        raise typer.Exit(code=1)

    store = PointerStore()
    for entry in store.get_pointers(name):
        typer.echo(f"{entry['run_id']}\t{entry['user_hash']}")


@app.command("pointer-send")
def pointer_send(name: str, user_id: str, run_id: str) -> None:
    """Publish a pointer update via the configured transport."""

    from ..ume import emit_pointer_update, _hash_user_id
    from ..ume.models import PointerUpdate

    update = PointerUpdate(
        task_name=name, run_id=run_id, user_hash=_hash_user_id(user_id)
    )
    emit_pointer_update(update)
    typer.echo("pointer sent")


@app.command("pointer-receive")
def pointer_receive(name: str, run_id: str, user_hash: str) -> None:
    """Store a received pointer update."""

    store = PointerStore()
    from ..ume.models import PointerUpdate

    store.apply_update(PointerUpdate(task_name=name, run_id=run_id, user_hash=user_hash))
    typer.echo("pointer stored")


@app.command("pointer-sync")
def pointer_sync_cmd() -> None:
    """Start the pointer synchronization service."""

    from .. import pointer_sync

    pointer_sync.run()



def main(args: list[str] | None = None) -> None:
    """CLI entry point used by ``console_scripts`` or directly.

    Parameters
    ----------
    args:
        Optional list of CLI arguments. If ``None`` (default), an empty list is
        passed so that pytest arguments are ignored during tests.
    """

    tc.initialize()
    app(args or [], standalone_mode=False)



__all__ = [
    "app",
    "main",
    "export_n8n",
    "webhook",
    "start_metrics_server",
    "reload_plugins_cmd",
    "pointer_add",
    "pointer_list",
    "pointer_send",
    "pointer_receive",
    "pointer_sync_cmd",
]

