import types
from unittest.mock import Mock

import pytest
from task_cascadence.orchestrator import TaskPipeline


@pytest.mark.skip(reason="requires transport client configuration")
def test_ai_plan_module_used(monkeypatch):
    mock_ai_plan = types.SimpleNamespace(plan=Mock(return_value="auto"))
    monkeypatch.setattr("task_cascadence.orchestrator.ai_plan", mock_ai_plan)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class DemoTask:
        def run(self, plan: str) -> str:
            return plan

    task = DemoTask()
    result = TaskPipeline(task).run(user_id="alice")

    assert result == "auto"
    mock_ai_plan.plan.assert_called_once_with(task)
