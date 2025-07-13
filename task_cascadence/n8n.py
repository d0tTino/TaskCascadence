"""Utilities for exporting tasks to n8n workflows."""

from __future__ import annotations

import json
from typing import Dict, Any

from .scheduler import BaseScheduler


NODE_DISTANCE = 250


def to_workflow(scheduler: BaseScheduler) -> Dict[str, Any]:
    """Return a dict representing a minimal n8n workflow."""
    nodes = [
        {
            "id": "1",
            "name": "Start",
            "type": "n8n-nodes-base.start",
            "typeVersion": 1,
            "position": [0, 0],
            "parameters": {},
        }
    ]
    connections: Dict[str, Any] = {}
    previous = "Start"
    node_id = 2
    y = 0
    for name, _disabled in scheduler.list_tasks():
        nodes.append(
            {
                "id": str(node_id),
                "name": name,
                "type": "n8n-nodes-base.code",
                "typeVersion": 1,
                "position": [NODE_DISTANCE * (node_id - 1), y],
                "parameters": {"jsCode": f"// run {name}"},
            }
        )
        connections.setdefault(previous, {"main": [[]]})
        connections[previous]["main"][0].append(
            {"node": name, "type": "main", "index": 0}
        )
        previous = name
        node_id += 1
    return {"name": "Cascadence Export", "nodes": nodes, "connections": connections}


def export_workflow(scheduler: BaseScheduler, path: str) -> None:
    """Write workflow JSON for ``scheduler`` to ``path``."""
    wf = to_workflow(scheduler)
    with open(path, "w") as fh:
        json.dump(wf, fh, indent=2)
