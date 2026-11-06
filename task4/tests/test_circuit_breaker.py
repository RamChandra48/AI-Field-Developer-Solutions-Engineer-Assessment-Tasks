"""Circuit breaker: state machine (fake clock), router integration, gateway wiring."""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from circuit_breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker
from router import ModelRouter, Provider
from test_gateway import A, chat, make_gateway, reset_mock, set_mode  # noqa: F401  (fixtures)
from test_router import PAYLOAD, Recorder, ok, status


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture()
def clock() -> Clock:
    return Clock()


def breaker(clock: Clock, threshold: int = 3, cooldown: float = 30.0) -> CircuitBreaker:
    return CircuitBreaker("primary", threshold, cooldown, clock=clock)


# ------------------------------------------------------------- state machine
def test_opens_after_consecutive_failures_only(clock):
    b = breaker(clock)
    b.record_failure(); b.record_failure(); b.record_success()   # success resets the count
    b.record_failure(); b.record_failure()
    assert b.state == CLOSED and b.allow()
    b.record_failure()
    assert b.state == OPEN and not b.allow()


def test_stays_open_for_the_cooldown_then_allows_one_probe(clock):
    b = breaker(clock, threshold=1, cooldown=30)
    b.record_failure()
    clock.now += 29.9
    assert not b.allow() and b.retry_in() == pytest.approx(0.1)
    clock.now += 0.1
    assert b.allow()                       # the probe
    assert b.state == HALF_OPEN
    assert not b.allow() and not b.allow()  # everyone else waits for the probe


def test_probe_success_closes(clock):
    b = breaker(clock, threshold=1)
    b.record_failure(); clock.now += 30
    assert b.allow()
    b.record_success()
    assert b.state == CLOSED and b.allow() and b.allow()


def test_probe_failure_reopens_for_a_full_cooldown(clock):
    b = breaker(clock, threshold=1, cooldown=30)
    b.record_failure(); clock.now += 30
    assert b.allow()
    b.record_failure()
    assert b.state == OPEN and b.retry_in() == pytest.approx(30)


def test_neutral_outcome_releases_the_probe_without_changing_state(clock):
    b = breaker(clock, threshold=1)
    b.record_failure(); clock.now += 30
    assert b.allow()
    b.record_neutral()                     # probe was cancelled / got a 4xx
    assert b.state == HALF_OPEN
    assert b.allow()                       # another request may probe


def test_rejects_bad_config(clock):
    with pytest.raises(ValueError):
        CircuitBreaker("p", failure_threshold=0)
    with pytest.raises(ValueError):
        CircuitBreaker("p", cooldown_seconds=0)


# ------------------------------------------------------------------- router
def build(primary, secondary, b: CircuitBreaker, primary_timeout=0.2) -> ModelRouter:
    async def dispatch(request):
        handler = primary if request.url.host == "primary" else secondary
        response = handler(request)
        return await response if asyncio.iscoroutine(response) else response

    client = httpx.AsyncClient(transport=httpx.MockTransport(dispatch))
    return ModelRouter(client, Provider("primary", "http://primary/v1", "model-a"),
                       Provider("secondary", "http://secondary/v1", "model-b"),
                       primary_timeout, 0.5, breaker=b)


async def test_open_circuit_skips_the_slow_primary(clock):
    primary = Recorder(ok, delay=2.0)                # hangs past the deadline
    router = build(primary, ok, breaker(clock, threshold=2))
    for _ in range(2):
        await router.complete(PAYLOAD)              # each waits for the deadline
    assert router.breaker.state == OPEN

    start = time.perf_counter()
    result = await router.complete(PAYLOAD)
    elapsed = time.perf_counter() - start
    assert result.provider == "secondary" and result.fell_back and result.primary_skipped
    assert [a.provider for a in result.attempts] == ["secondary"]
    assert primary.calls == 2                       # the primary was not called again
    assert elapsed < 0.1                            # no deadline wait


async def test_half_open_sends_exactly_one_probe_under_concurrency(clock):
    primary = Recorder(ok, delay=0.05)
    b = breaker(clock, threshold=1)
    b.record_failure()
    clock.now += 30
    results = await asyncio.gather(*(build(primary, ok, b).complete(PAYLOAD) for _ in range(10)))
    assert primary.calls == 1                       # one probe, not a stampede
    assert sorted(r.provider for r in results) == ["primary"] + ["secondary"] * 9
    assert b.state == CLOSED


async def test_client_errors_do_not_open_the_circuit(clock):
    router = build(status(400), ok, breaker(clock, threshold=1))
    for _ in range(3):
        with pytest.raises(Exception):
            await router.complete(PAYLOAD)
    assert router.breaker.state == CLOSED


async def test_cancelled_probe_does_not_wedge_the_circuit(clock):
    b = breaker(clock, threshold=1)
    b.record_failure()
    clock.now += 30
    router = build(Recorder(ok, delay=5.0), ok, b, primary_timeout=10)
    probe = asyncio.create_task(router.complete(PAYLOAD))
    await asyncio.sleep(0.05)
    probe.cancel()                                  # client disconnected mid-probe
    with pytest.raises(asyncio.CancelledError):
        await probe
    assert b.state == HALF_OPEN and b.allow()       # next request can probe


async def test_without_a_breaker_the_primary_is_always_tried():
    primary = Recorder(status(500))
    router = ModelRouter(httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: primary(r) if r.url.host == "primary" else ok(r))),
        Provider("primary", "http://primary/v1", "a"), Provider("secondary", "http://secondary/v1", "b"))
    for _ in range(5):
        await router.complete(PAYLOAD)
    assert primary.calls == 5


# ------------------------------------------------------------------ gateway
async def test_gateway_reports_circuit_and_marks_fallback(make_gateway):
    client, app = await make_gateway(circuit_failure_threshold=2)
    assert (await client.get("/healthz")).json()["primary_circuit"] == "closed"
    set_mode("primary", "500")
    for _ in range(2):
        assert (await client.post("/v1/chat/completions", json=chat(), headers=A)).status_code == 200
    assert (await client.get("/healthz")).json()["primary_circuit"] == "open"

    response = await client.post("/v1/chat/completions", json=chat(), headers=A)
    assert response.status_code == 200
    assert response.headers["x-gateway-provider"] == "secondary"
    assert response.headers["x-gateway-fallback"] == "true"


async def test_threshold_zero_disables_the_breaker(make_gateway):
    client, app = await make_gateway(circuit_failure_threshold=0)
    assert app.state.router.breaker is None
    assert (await client.get("/healthz")).json()["primary_circuit"] == "disabled"
