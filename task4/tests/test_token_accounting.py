"""Token accounting when real usage differs from the reservation.

The gateway reserves an estimate before calling a model and reconciles it to
the provider's reported usage afterwards. These tests cover the cases where
the real figure is *larger*: larger than the estimate, larger than what the
tenant had left, absurdly large, or impossible to record.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, List

import httpx
import pytest

from gateway import Message, Settings, create_app, estimate_prompt_tokens
from rate_limiter import SlidingWindowTokenLimiter
from test_rate_limiter import Clock

A = {"Authorization": "Bearer tenant-a-key"}


def chat(content: str = "hello", max_tokens: int = 50) -> Dict[str, Any]:
    return {"messages": [{"role": "user", "content": content}], "max_tokens": max_tokens}


def estimate_of(body: Dict[str, Any]) -> int:
    return estimate_prompt_tokens([Message(**m) for m in body["messages"]]) + body["max_tokens"]


# ------------------------------------------------------------------ limiter
class TestLimiterOverrun:
    async def test_overrun_past_the_remaining_quota_is_charged_as_debt(self, tmp_path):
        clock = Clock()
        lim = SlidingWindowTokenLimiter(tmp_path / "rl.db", limit=50_000, window_seconds=60, clock=clock)
        try:
            await lim.acquire("t", 45_000)                     # t=1000
            clock.t += 10
            fill = await lim.acquire("t", 5_000)               # t=1010: fills the window exactly
            assert fill.allowed and fill.used == 50_000
            assert await lim.reconcile(fill.reservation_id, 8_000) == 8_000   # really cost 8k: 3k over
            assert await lim.usage("t") == 53_000

            clock.t += 1
            refused = await lim.acquire("t", 100)
            assert not refused.allowed and refused.remaining == 0
            # Paying the debt needs the 45k row (t=1000) to expire, at t=1060: 49 s from t=1011.
            assert refused.retry_after == pytest.approx(49.0)
            clock.t += 48.9
            assert not (await lim.acquire("t", 100)).allowed
            clock.t += 0.2
            assert (await lim.acquire("t", 100)).allowed       # 8k + 100 left in the window
        finally:
            lim.close()

    async def test_one_row_is_capped_at_the_limit_and_floored_at_zero(self, tmp_path):
        clock = Clock()
        lim = SlidingWindowTokenLimiter(tmp_path / "rl.db", limit=1_000, clock=clock)
        try:
            d = await lim.acquire("t", 10)
            assert await lim.reconcile(d.reservation_id, 10 ** 30) == 1_000   # no OverflowError
            assert await lim.usage("t") == 1_000
            assert await lim.reconcile(d.reservation_id, -5) == 0
            assert await lim.usage("t") == 0
        finally:
            lim.close()


# ------------------------------------------------------------------ gateway
class ReportingProvider:
    """A provider that reports whatever usage the test sets."""

    def __init__(self) -> None:
        self.total_tokens: Any = 10

    def __call__(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "id": "c1", "object": "chat.completion", "model": "m",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"total_tokens": self.total_tokens}})


@pytest.fixture()
async def stack(tmp_path):
    provider = ReportingProvider()
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(provider))
    settings = Settings(primary_url="http://p/v1/chat/completions", secondary_url="http://s/v1/chat/completions",
                        tokens_per_minute=1_000, circuit_failure_threshold=0, db_path=str(tmp_path / "rl.db"))
    app = create_app(settings, http_client=upstream)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://gw")
    yield client, app, provider
    await client.aclose()
    await upstream.aclose()
    app.state.limiter.close()


async def used(client) -> int:
    return (await client.get("/v1/rate-limit", headers=A)).json()["used"]


def events(caplog, name: str) -> List[logging.LogRecord]:
    return [r for r in caplog.records if getattr(r, "event", None) == name]


class TestGatewayOverrun:
    async def test_usage_above_the_estimate_is_charged_and_logged(self, stack, caplog):
        caplog.set_level(logging.INFO, logger="llm-gateway")
        client, _, provider = stack
        body = chat()
        provider.total_tokens = estimate_of(body) + 40
        r = await client.post("/v1/chat/completions", json=body, headers=A)
        assert r.status_code == 200
        assert await used(client) == provider.total_tokens
        assert r.headers["x-ratelimit-remaining"] == str(1_000 - provider.total_tokens)
        (over,) = events(caplog, "ratelimit.overrun")
        assert (over.levelname, over.tokens_over, over.over_limit, over.capped) == ("WARNING", 40, False, False)

    async def test_overrun_past_the_remaining_quota_blocks_the_next_request(self, stack, caplog):
        caplog.set_level(logging.INFO, logger="llm-gateway")
        client, _, provider = stack
        provider.total_tokens = 900
        assert (await client.post("/v1/chat/completions", json=chat(), headers=A)).status_code == 200
        body = chat()                                          # ~60-token estimate fits in the 100 left
        provider.total_tokens = 400                            # ...but really costs 400
        r = await client.post("/v1/chat/completions", json=body, headers=A)
        assert r.status_code == 200                            # already generated and paid for: delivered
        assert await used(client) == 1_300                     # charged in full: 300 tokens of debt
        assert r.headers["x-ratelimit-remaining"] == "0"
        assert events(caplog, "ratelimit.overrun")[-1].over_limit is True

        refused = await client.post("/v1/chat/completions", json=chat(), headers=A)
        assert refused.status_code == 429
        assert 0 < float(refused.headers["retry-after"]) <= 60

    async def test_absurd_reported_usage_is_capped_not_a_500(self, stack, caplog):
        caplog.set_level(logging.INFO, logger="llm-gateway")
        client, _, provider = stack
        provider.total_tokens = 10 ** 30                        # > SQLite's 64-bit INTEGER
        r = await client.post("/v1/chat/completions", json=chat(), headers=A)
        assert r.status_code == 200
        assert await used(client) == 1_000                     # one window's budget: blocked until it expires
        (over,) = events(caplog, "ratelimit.overrun")
        assert over.capped is True and over.tokens_reported == 10 ** 30
        assert (await client.post("/v1/chat/completions", json=chat(), headers=A)).status_code == 429

    @pytest.mark.parametrize("bad", [-1, 12.5, "100", True, None])
    async def test_unusable_reported_usage_keeps_the_estimate(self, stack, bad):
        client, _, provider = stack
        body = chat()
        provider.total_tokens = bad
        assert (await client.post("/v1/chat/completions", json=body, headers=A)).status_code == 200
        assert await used(client) == estimate_of(body)

    async def test_reconcile_failure_still_delivers_and_keeps_the_estimate(self, stack, caplog, monkeypatch):
        caplog.set_level(logging.INFO, logger="llm-gateway")
        client, app, provider = stack
        provider.total_tokens = 5

        async def broken(*args, **kwargs):
            raise RuntimeError("database is locked")

        monkeypatch.setattr(app.state.limiter, "reconcile", broken)
        body = chat()
        r = await client.post("/v1/chat/completions", json=body, headers=A)
        assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "ok"
        assert await used(client) == estimate_of(body)         # never under-charged
        (failed,) = events(caplog, "ratelimit.reconcile_failed")
        assert failed.levelname == "ERROR"


# ---------------------------------------------------------------- estimator
class TestEstimator:
    def test_ascii_is_about_four_characters_per_token(self):
        text = "The quick brown fox jumps over the lazy dog. " * 10
        assert estimate_prompt_tokens([Message(role="user", content=text)]) == 3 + 4 + math.ceil(len(text) / 4)

    @pytest.mark.parametrize("text", ["你好，世界。请帮我总结这份文件。", "Привет, как дела?", "🙂🙃😉😊"])
    def test_non_ascii_is_not_under_reserved(self, text):
        # chars/4 would reserve a quarter of these; real tokenizers use about one per character or more.
        non_ascii = sum(1 for ch in text if ord(ch) > 127)
        assert estimate_prompt_tokens([Message(role="user", content=text)]) >= 3 + 4 + non_ascii
        assert estimate_prompt_tokens([Message(role="user", content=text)]) > 3 + 4 + math.ceil(len(text) / 4)
