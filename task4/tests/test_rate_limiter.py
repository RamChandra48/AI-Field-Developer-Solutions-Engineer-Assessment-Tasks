"""Tests for the SQLite sliding-window token limiter (fake clock, real on-disk DB)."""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

from rate_limiter import SlidingWindowTokenLimiter


class Clock:
    def __init__(self, t: float = 1_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


@pytest.fixture()
def clock():
    return Clock()


@pytest.fixture()
def make(tmp_path, clock):
    created = []

    def _make(limit=50_000, path=None, **kwargs):
        limiter = SlidingWindowTokenLimiter(path or tmp_path / "rl.db", limit=limit, clock=kwargs.pop("clock", clock), **kwargs)
        created.append(limiter)
        return limiter

    yield _make
    for limiter in created:
        limiter.close()


async def test_admits_up_to_the_limit_exactly(make):
    lim = make()
    a = await lim.acquire("t", 30_000)
    b = await lim.acquire("t", 20_000)
    c = await lim.acquire("t", 1)
    assert (a.allowed, a.used, a.remaining) == (True, 30_000, 20_000)
    assert (b.allowed, b.used, b.remaining) == (True, 50_000, 0)
    assert (c.allowed, c.used, c.reservation_id) == (False, 50_000, None)
    assert c.retry_after == pytest.approx(60.0)


async def test_window_slides_and_evicts_at_the_boundary(make, clock):
    lim = make()
    assert (await lim.acquire("t", 30_000)).allowed       # t = 0 s
    clock.t += 30
    assert (await lim.acquire("t", 20_000)).allowed       # t = 30 s
    clock.t += 29.999
    denied = await lim.acquire("t", 1)                    # t = 59.999 s: both rows still count
    assert not denied.allowed and denied.retry_after == pytest.approx(0.001, abs=1e-6)
    clock.t += 0.001                                      # t = 60 s: first row leaves
    assert await lim.usage("t") == 20_000
    assert (await lim.acquire("t", 30_000)).allowed
    assert not (await lim.acquire("t", 1)).allowed


async def test_retry_after_is_when_enough_tokens_expire(make, clock):
    lim = make()
    for tokens in (10_000, 20_000, 20_000):               # at t = 0, 10, 20 s
        assert (await lim.acquire("t", tokens)).allowed
        clock.t += 10
    decision = await lim.acquire("t", 25_000)             # t = 30 s, needs 25k freed
    # 10k (t=0) is not enough; 10k + 20k (t=10) is, and that row expires at t=70
    assert not decision.allowed and decision.retry_after == pytest.approx(40.0)
    clock.t += 40
    assert (await lim.acquire("t", 25_000)).allowed


async def test_reconcile_replaces_the_estimate(make):
    lim = make()
    d = await lim.acquire("t", 40_000)
    await lim.reconcile(d.reservation_id, 1_500)          # provider reported far less
    assert await lim.usage("t") == 1_500
    assert (await lim.acquire("t", 48_500)).allowed
    d2 = await lim.acquire("u", 100)
    await lim.reconcile(d2.reservation_id, 60_000)        # or more: charged as reported
    assert not (await lim.acquire("u", 1)).allowed


async def test_release_refunds(make):
    lim = make()
    d = await lim.acquire("t", 50_000)
    await lim.release(d.reservation_id)
    assert await lim.usage("t") == 0
    assert (await lim.acquire("t", 50_000)).allowed


async def test_tenants_are_isolated(make):
    lim = make()
    assert (await lim.acquire("tenant-a", 50_000)).allowed
    assert not (await lim.acquire("tenant-a", 1)).allowed
    assert (await lim.acquire("tenant-b", 50_000)).allowed


async def test_request_bigger_than_limit_never_fits(make):
    d = await make().acquire("t", 50_001)
    assert not d.allowed and d.retry_after is None


@pytest.mark.parametrize("tokens", [0, -1, True, 1.5])
async def test_invalid_token_counts(make, tokens):
    with pytest.raises(ValueError):
        await make().acquire("t", tokens)


async def test_concurrent_acquires_never_overshoot(make):
    lim = make()
    results = await asyncio.gather(*(lim.acquire("t", 1_000) for _ in range(200)))
    assert sum(r.allowed for r in results) == 50
    assert await lim.usage("t") == 50_000


async def test_separate_connections_share_the_limit(make, tmp_path):
    """Two limiters on one file stand in for two gateway processes."""
    path = tmp_path / "shared.db"
    one, two = make(path=path), make(path=path)
    results = await asyncio.gather(*((one if i % 2 else two).acquire("t", 1_000) for i in range(200)))
    assert sum(r.allowed for r in results) == 50
    assert await one.usage("t") == await two.usage("t") == 50_000


async def test_state_survives_restart(make, tmp_path):
    path = tmp_path / "persist.db"
    first = make(path=path)
    assert (await first.acquire("t", 40_000)).allowed
    first.close()
    second = make(path=path)
    assert await second.usage("t") == 40_000
    assert not (await second.acquire("t", 20_000)).allowed


async def test_expired_rows_are_deleted_for_every_tenant(make, clock, tmp_path):
    lim = make()
    for tenant in ("a", "b", "c"):
        await lim.acquire(tenant, 100)
    clock.t += 61
    await lim.acquire("d", 100)                           # any acquire evicts globally
    with sqlite3.connect(tmp_path / "rl.db") as conn:
        assert conn.execute("SELECT key FROM token_usage").fetchall() == [("d",)]
