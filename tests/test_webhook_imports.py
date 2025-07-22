import importlib

import fastapi


def test_webhook_imports():
    module = importlib.import_module("task_cascadence.webhook")
    assert module.FastAPI is fastapi.FastAPI
    assert module.Request is fastapi.Request
