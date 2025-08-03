import asyncio
import pytest

import task_cascadence.research as research


def test_gather_with_search(monkeypatch):
    class Stub:
        def search(self, query):
            return f"result:{query}"

    monkeypatch.setattr(research, "tino_storm", Stub())
    assert research.gather("foo", user_id="u1") == "result:foo"


def test_gather_with_callable(monkeypatch):
    def func(query):
        return f"call:{query}"

    monkeypatch.setattr(research, "tino_storm", func)
    assert research.gather("bar", user_id="u1") == "call:bar"


def test_gather_invalid_interface(monkeypatch):
    monkeypatch.setattr(research, "tino_storm", object())
    with pytest.raises(RuntimeError):
        research.gather("oops", user_id="u1")


def test_async_gather_search_returns_coroutine(monkeypatch):
    class Stub:
        async def search(self, query, user_id=None, group_id=None):
            await asyncio.sleep(0)
            assert user_id == "u1"
            assert group_id == "g1"
            return f"async:{query}"

    monkeypatch.setattr(research, "tino_storm", Stub())
    result = asyncio.run(research.async_gather("abc", user_id="u1", group_id="g1"))
    assert result == "async:abc"


def test_async_gather_callable_returns_coroutine(monkeypatch):
    async def func(query, user_id=None):
        await asyncio.sleep(0)
        assert user_id == "u1"
        return f"coro:{query}"

    monkeypatch.setattr(research, "tino_storm", func)
    result = asyncio.run(research.async_gather("xyz", user_id="u1"))
    assert result == "coro:xyz"


def test_async_gather_invalid_interface(monkeypatch):
    monkeypatch.setattr(research, "tino_storm", object())
    with pytest.raises(RuntimeError):
        asyncio.run(research.async_gather("fail", user_id="u1"))
