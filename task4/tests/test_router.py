"""Tests for failover and timeout handling (httpx.MockTransport, no sockets)."""
from __future__ import annotations

import asyncio
import json
import random
import time

import httpx
import pytest

from errors import GatewayError
from gateway import Settings
from router import PRIMARY_TIMEOUT_SECONDS, ModelRouter, Provider

LEAKY_TEXT = "Traceback (most recent call last): psycopg2 at 10.0.3.17 org-8f2c91"


def ok(request: httpx.Request) -> httpx.Response:
    model = json.loads(request.content)["model"]
    return httpx.Response(200, json={
        "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi from " + model}}],
        "usage": {"total_tokens": 12},
    })


def status(code: int):
    def handler(request):
        return httpx.Response(code, text=LEAKY_TEXT)
    return handler


class Recorder:
    """Wraps a handler; counts calls and whether a call was cancelled midway."""

    def __init__(self, handler, delay: float = 0.0):
        self.handler, self.delay = handler, delay
        self.calls = self.cancelled = self.completed = 0

    async def __call__(self, request):
        self.calls += 1
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        self.completed += 1
        return self.handler(request)


def build(primary, secondary, primary_timeout=0.2, secondary_timeout=0.5) -> ModelRouter:
    async def dispatch(request):
        handler = primary if request.url.host == "primary" else secondary
        response = handler(request)
        return await response if asyncio.iscoroutine(response) else response

    client = httpx.AsyncClient(transport=httpx.MockTransport(dispatch))
    return ModelRouter(client, Provider("primary", "http://primary/v1", "model-a"),
                       Provider("secondary", "http://secondary/v1", "model-b"), primary_timeout, secondary_timeout)


PAYLOAD = {"messages": [{"role": "user", "content": "hello"}], "max_tokens": 10}


def test_default_primary_deadline_is_3000ms():
    assert PRIMARY_TIMEOUT_SECONDS == 3.0
    assert Settings().primary_timeout_ms == 3000


async def test_primary_success_does_not_touch_secondary():
    secondary = Recorder(ok)
    result = await build(Recorder(ok), secondary).complete(PAYLOAD)
    assert result.provider == "primary" and not result.fell_back
    assert result.body["model"] == "model-a"
    assert secondary.calls == 0


@pytest.mark.parametrize("failure, reason", [
    (status(429), "rate_limited"),
    (status(500), "server_error"),
    (status(503), "server_error"),
    (lambda r: httpx.Response(200, text="<html>proxy error</html>"), "bad_response"),
    (lambda r: httpx.Response(200, json={"no": "choices"}), "bad_response"),
])
async def test_fails_over(failure, reason):
    result = await build(failure, ok).complete(PAYLOAD)
    assert result.provider == "secondary" and result.fell_back
    assert result.body["model"] == "model-b"                 # secondary's own model name
    assert [a.outcome for a in result.attempts] == [reason, "ok"]


async def test_connection_error_fails_over():
    def refuse(request):
        raise httpx.ConnectError("connection refused")

    result = await build(refuse, ok).complete(PAYLOAD)
    assert [a.outcome for a in result.attempts] == ["connection_error", "ok"]


async def test_timeout_fails_over_and_cancels_the_primary():
    primary = Recorder(ok, delay=2.0)
    start = time.perf_counter()
    result = await build(primary, ok, primary_timeout=0.2).complete(PAYLOAD)
    elapsed = time.perf_counter() - start
    assert result.provider == "secondary"
    assert [a.outcome for a in result.attempts] == ["timeout", "ok"]
    assert 0.19 < elapsed < 0.4                              # deadline, not the 2 s the primary wanted
    assert (primary.cancelled, primary.completed) == (1, 0)  # late response can never arrive


async def test_deadline_covers_a_slowly_trickled_body():
    """Headers arrive at once but the body drips: per-read timeouts would never fire."""
    async def drip():
        for _ in range(20):
            await asyncio.sleep(0.05)
            yield b" "

    def trickle(request):
        return httpx.Response(200, content=drip(), headers={"Content-Type": "application/json"})

    start = time.perf_counter()
    result = await build(trickle, ok, primary_timeout=0.2).complete(PAYLOAD)
    assert result.attempts[0].outcome == "timeout"
    assert time.perf_counter() - start < 0.4


async def test_client_error_does_not_fail_over():
    secondary = Recorder(ok)
    with pytest.raises(GatewayError) as info:
        await build(status(400), secondary).complete(PAYLOAD)
    assert (info.value.status, info.value.error_type) == (502, "upstream_error")
    assert secondary.calls == 0


@pytest.mark.parametrize("secondary, status_code, error_type", [
    (status(429), 503, "upstream_unavailable"),
    (status(500), 503, "upstream_unavailable"),
    (Recorder(ok, delay=2.0), 504, "upstream_timeout"),
])
async def test_both_fail_gives_standard_error_without_upstream_details(secondary, status_code, error_type):
    with pytest.raises(GatewayError) as info:
        await build(status(429), secondary, secondary_timeout=0.2).complete(PAYLOAD)
    err = info.value
    assert (err.status, err.error_type) == (status_code, error_type)
    for leak in ("Traceback", "psycopg2", "10.0.3.17", "org-8f2c91", "primary", "secondary"):
        assert leak not in err.message
    assert err.__cause__ is None and err.__suppress_context__          # no chained upstream exception


async def test_concurrent_timeouts_fail_over_in_parallel():
    router = build(Recorder(ok, delay=2.0), ok, primary_timeout=0.2)
    start = time.perf_counter()
    results = await asyncio.gather(*(router.complete(PAYLOAD) for _ in range(50)))
    assert all(r.provider == "secondary" for r in results)
    assert time.perf_counter() - start < 0.6                 # 50 deadlines run concurrently, not 50 x 0.2 s


async def test_response_racing_the_deadline_is_resolved_exactly_once():
    """Primary answers right around the deadline. Each request must end with
    exactly one result: primary (completed, not cancelled) or secondary
    (primary cancelled), never both and never an error."""
    rng = random.Random(42)
    timeout = 0.05

    async def one():
        primary = Recorder(ok, delay=timeout + rng.uniform(-0.01, 0.01))
        secondary = Recorder(ok)
        result = await build(primary, secondary, primary_timeout=timeout).complete(PAYLOAD)
        if result.provider == "primary":
            assert (primary.completed, primary.cancelled, secondary.calls) == (1, 0, 0)
        else:
            assert (primary.completed, primary.cancelled, secondary.calls) == (0, 1, 1)
            assert result.attempts[0].outcome == "timeout"
        return result.provider

    providers = await asyncio.gather(*(one() for _ in range(200)))
    assert set(providers) == {"primary", "secondary"}         # both sides of the race were exercised
