from __future__ import annotations

import requests
import typer

from task_cascadence.intent import sanitize_input

app = typer.Typer(help="Interact with TaskCascadence intent API")


def _post(base_url: str, payload: dict) -> dict:
    url = f"{base_url.rstrip('/')}/intent"
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


@app.command()
def intent(text: str, base_url: str = "http://localhost:8000") -> None:
    """Submit ``TEXT`` and interactively resolve intent."""

    payload: dict = {"message": sanitize_input(text), "context": []}

    while True:
        data = _post(base_url, payload)
        clarification = data.get("clarification")
        if clarification:
            typer.echo(clarification)
            answer = sanitize_input(input(""))
            payload["clarification"] = answer
            continue
        tasks = data.get("tasks", [])
        if tasks:
            typer.echo("Derived tasks:")
            for task in tasks:
                typer.echo(f"- {task}")
        break


def main() -> None:
    app()


if __name__ == "__main__":
    main()
