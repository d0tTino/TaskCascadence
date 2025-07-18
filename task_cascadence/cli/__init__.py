"""Entry points for the command-line interface.

This CLI exposes a minimal ``task`` command with sub-commands to list, run and
disable tasks as described in the PRD (FR-12).
"""

from __future__ import annotations

import click  # noqa: F401 - re-exported for CLI extensions

import typer

import importlib

from .. import ume

from ..scheduler import get_default_scheduler
from .. import plugins  # noqa: F401
from ..metrics import start_metrics_server  # noqa: F401
import task_cascadence as tc
from ..n8n import export_workflow


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
            ume.configure_transport("grpc", stub=stub, method=grpc_method)
        elif transport == "nats":
            if nats_conn is None:
                raise typer.BadParameter("--nats-conn is required for nats transport")
            conn = _load(nats_conn)
            ume.configure_transport("nats", connection=conn, subject=nats_subject)
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
) -> None:
    """Run ``NAME`` if it exists and is enabled."""

    try:
        get_default_scheduler().run_task(name, use_temporal=temporal)
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("trigger")
def manual_trigger(name: str) -> None:
    """Run ``NAME`` if it is a ManualTrigger task."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info or not isinstance(task_info["task"], plugins.ManualTrigger):
        typer.echo(f"error: '{name}' is not a manual task", err=True)
        raise typer.Exit(code=1)
    sched.run_task(name)



@app.command("disable")
def disable_task(name: str) -> None:
    """Disable ``NAME`` so it can no longer be executed."""

    try:
        get_default_scheduler().disable_task(name)
        typer.echo(f"{name} disabled")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command("schedule")
def schedule_task(name: str, expression: str) -> None:
    """Schedule ``NAME`` according to ``EXPRESSION``."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info:
        typer.echo(f"error: unknown task '{name}'", err=True)
        raise typer.Exit(code=1)

    try:
        task = task_info["task"]
        sched.register_task(task, expression)
        typer.echo(f"{name} scheduled: {expression}")
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
]

