import asyncio
import pytest

import task_cascadence.research as research


def test_gather_with_search(monkeypatch):
    class Stub:
        def search(self, query):
            return f"result:{query}"

    monkeypatch.setattr(research, "tino_storm", Stub())
    assert research.gather("foo") == "result:foo"


def test_gather_with_callable(monkeypatch):
    def func(query):
        return f"call:{query}"

    monkeypatch.setattr(research, "tino_storm", func)
    assert research.gather("bar") == "call:bar"


def test_gather_invalid_interface(monkeypatch):
    monkeypatch.setattr(research, "tino_storm", object())
    with pytest.raises(RuntimeError):
        research.gather("oops")


def test_async_gather_search_returns_coroutine(monkeypatch):
    class Stub:
        async def search(self, query):
            await asyncio.sleep(0)
            return f"async:{query}"

    monkeypatch.setattr(research, "tino_storm", Stub())
    result = asyncio.run(research.async_gather("abc"))
    assert result == "async:abc"


def test_async_gather_callable_returns_coroutine(monkeypatch):
    async def func(query):
        await asyncio.sleep(0)
        return f"coro:{query}"

    monkeypatch.setattr(research, "tino_storm", func)
    result = asyncio.run(research.async_gather("xyz"))
    assert result == "coro:xyz"


def test_async_gather_invalid_interface(monkeypatch):
    monkeypatch.setattr(research, "tino_storm", object())
    with pytest.raises(RuntimeError):
        asyncio.run(research.async_gather("fail"))
