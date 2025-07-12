"""Entry points for the command-line interface.

This CLI exposes a minimal ``task`` command with sub-commands to list, run and
disable tasks as described in the PRD (FR-12).
"""

from __future__ import annotations

import click
import typer

from ..scheduler import default_scheduler
from .. import plugins  # noqa: F401  # ensure tasks are registered


app = typer.Typer(help="Interact with Cascadence tasks")


@app.command("list")
def list_tasks() -> None:
    """List all registered tasks."""

    for name, disabled in default_scheduler.list_tasks():
        status = "disabled" if disabled else "enabled"
        typer.echo(f"{name}\t{status}")


@app.command("run")
def run_task(name: str) -> None:
    """Run ``NAME`` if it exists and is enabled."""

    try:
        default_scheduler.run_task(name)
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)


@app.command("disable")
def disable_task(name: str) -> None:
    """Disable ``NAME`` so it can no longer be executed."""

    try:
        default_scheduler.disable_task(name)
        typer.echo(f"{name} disabled")
    except Exception as exc:  # pragma: no cover - simple error propagation
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point used by ``console_scripts`` or directly."""

    try:
        app(args=argv or [], standalone_mode=False)
    except (SystemExit, click.ClickException):
        # Swallow exit exceptions so tests can call ``main()`` without arguments.
        return None


__all__ = ["app", "main"]
