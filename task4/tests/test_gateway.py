"""End-to-end gateway tests: gateway app -> router -> real mock provider app,
all in-process via httpx.ASGITransport, with an on-disk SQLite limiter."""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest

import mock_providers
from gateway import Settings, create_app

A = {"Authorization": "Bearer tenant-a-key"}
B = {"Authorization": "Bearer tenant-b-key"}
LEAKS = ("Traceback", "psycopg2", "10.0.3.17", "org-8f2c91", "worker.py", "primary", "secondary")


def chat(content="hello", **extra):
    return dict({"messages": [{"role": "user", "content": content}], "max_tokens": 50}, **extra)


def set_mode(provider, mode, delay_ms=1000):
    mock_providers.app.state.config[provider] = {"mode": mode, "delay_ms": delay_ms}


@pytest.fixture(autouse=True)
def reset_mock():
    for provider in ("primary", "secondary"):
        set_mode(provider, "ok")
        mock_providers.app.state.calls[provider] = 0


@pytest.fixture()
async def make_gateway(tmp_path):
    opened = []

    async def _make(**overrides):
        settings = Settings(
            primary_url="http://mock/primary/v1/chat/completions",
            secondary_url="http://mock/secondary/v1/chat/completions",
            primary_timeout_ms=200, secondary_timeout_ms=400,
            db_path=str(tmp_path / "ratelimit.db"),
        )
        for key, value in overrides.items():
            setattr(settings, key, value)
        upstream = httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_providers.app))
        app = create_app(settings, http_client=upstream)
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://gw")
        opened.append((client, upstream, app))
        return client, app

    yield _make
    for client, upstream, app in opened:
        await client.aclose()
        await upstream.aclose()
        app.state.limiter.close()


@pytest.fixture()
async def gw(make_gateway):
    client, _ = await make_gateway()
    return client


def assert_standard_error(response, status, error_type):
    assert response.status_code == status
    body = response.json()
    assert set(body) == {"error"}
    assert body["error"]["type"] == error_type
    assert body["error"]["request_id"] == response.headers["x-request-id"]
    assert all(leak not in response.text for leak in LEAKS), response.text
    return body["error"]


async def used(client, headers=A) -> int:
    return (await client.get("/v1/rate-limit", headers=headers)).json()["used"]


class TestHappyPathAndFailover:
    async def test_primary_success(self, gw):
        r = await gw.post("/v1/chat/completions", json=chat(), headers=A)
        assert r.status_code == 200
        assert r.headers["x-gateway-provider"] == "primary" and r.headers["x-gateway-fallback"] == "false"
        assert r.json()["model"] == "primary-model"
        assert r.headers["x-ratelimit-limit"] == "50000"
        assert r.headers["x-request-id"].startswith("req_")

    async def test_usage_is_reconciled_to_reported_tokens(self, gw):
        r = await gw.post("/v1/chat/completions", json=chat(max_tokens=5000), headers=A)
        actual = r.json()["usage"]["total_tokens"]
        assert actual < 100                                   # the 5000-token estimate was released
        assert await used(gw) == actual
        assert int(r.headers["x-ratelimit-remaining"]) == 50_000 - actual

    @pytest.mark.parametrize("mode", ["429", "500", "garbage", "slow"])
    async def test_failover_to_secondary(self, gw, mode):
        set_mode("primary", mode)
        start = time.perf_counter()
        r = await gw.post("/v1/chat/completions", json=chat(), headers=A)
        assert r.status_code == 200
        assert r.headers["x-gateway-provider"] == "secondary" and r.headers["x-gateway-fallback"] == "true"
        assert r.json()["model"] == "secondary-model"
        assert time.perf_counter() - start < 0.5              # slow primary cut off at 200 ms, not 1 s

    @pytest.mark.parametrize("secondary_mode", ["429", "500", "garbage"])
    async def test_all_providers_down_is_503_and_refunded(self, gw, secondary_mode):
        set_mode("primary", "500")
        set_mode("secondary", secondary_mode)
        r = await gw.post("/v1/chat/completions", json=chat(), headers=A)
        assert_standard_error(r, 503, "upstream_unavailable")
        assert await used(gw) == 0

    async def test_secondary_timeout_is_504(self, gw):
        set_mode("primary", "429")
        set_mode("secondary", "slow")
        r = await gw.post("/v1/chat/completions", json=chat(), headers=A)
        assert_standard_error(r, 504, "upstream_timeout")


class TestRateLimiting:
    async def test_tenant_limit_429_with_retry_after(self, make_gateway):
        client, _ = await make_gateway(tokens_per_minute=1000)
        big = chat("x" * 2000, max_tokens=100)               # ~600 tokens each
        assert (await client.post("/v1/chat/completions", json=big, headers=A)).status_code == 200
        r = await client.post("/v1/chat/completions", json=big, headers=A)
        err = assert_standard_error(r, 429, "rate_limit_exceeded")
        assert 1 <= err["retry_after_seconds"] <= 60
        assert r.headers["retry-after"] == str(err["retry_after_seconds"])
        assert mock_providers.app.state.calls["primary"] == 1  # rejected before any upstream call
        # another tenant is unaffected
        assert (await client.post("/v1/chat/completions", json=big, headers=B)).status_code == 200

    async def test_in_flight_reservations_block_concurrent_overshoot(self, make_gateway):
        client, _ = await make_gateway(tokens_per_minute=2500)
        set_mode("primary", "slow", delay_ms=100)            # in flight long enough to overlap
        responses = await asyncio.gather(*(
            client.post("/v1/chat/completions", json=chat(max_tokens=1000), headers=A) for _ in range(6)))
        codes = sorted(r.status_code for r in responses)
        assert codes == [200, 200, 429, 429, 429, 429]       # 2 x ~1010 fit in 2500, the rest don't

    async def test_request_larger_than_limit_is_413(self, gw):
        r = await gw.post("/v1/chat/completions", json=chat(max_tokens=60_000), headers=A)
        assert_standard_error(r, 413, "request_too_large")


class TestErrorsAreStandardized:
    @pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}, {"Authorization": "Basic tenant-a-key"}])
    async def test_authentication(self, gw, headers):
        r = await gw.post("/v1/chat/completions", json=chat(), headers=headers)
        assert_standard_error(r, 401, "authentication_error")

    @pytest.mark.parametrize("body", [
        {"messages": []},
        {"messages": [{"role": "hacker", "content": "x"}]},
        {"messages": [{"role": "user", "content": "x"}], "max_tokens": 0},
        {"nothing": True},
    ])
    async def test_validation(self, gw, body):
        r = await gw.post("/v1/chat/completions", json=body, headers=A)
        err = assert_standard_error(r, 400, "invalid_request")
        assert "pydantic" not in r.text and "errors.pydantic.dev" not in r.text

    async def test_malformed_json(self, gw):
        r = await gw.post("/v1/chat/completions", content=b"{not json", headers=dict(A, **{"Content-Type": "application/json"}))
        assert_standard_error(r, 400, "invalid_request")

    async def test_unknown_route_and_method(self, gw):
        assert_standard_error(await gw.get("/nope", headers=A), 404, "not_found")
        assert_standard_error(await gw.get("/v1/chat/completions", headers=A), 405, "method_not_allowed")

    async def test_unexpected_exception_is_hidden_and_refunded(self, make_gateway):
        client, app = await make_gateway()

        async def explode(payload):
            raise RuntimeError("db password=hunter2 at /srv/app/internal.py")

        app.state.router.complete = explode
        r = await client.post("/v1/chat/completions", json=chat(), headers=A)
        err = assert_standard_error(r, 500, "internal_error")
        assert "hunter2" not in r.text and "internal.py" not in r.text
        assert err["message"] == "An internal error occurred."
        assert await used(client) == 0

    async def test_client_disconnect_refunds_reservation(self, make_gateway):
        client, _ = await make_gateway(primary_timeout_ms=2000, secondary_timeout_ms=2000)
        set_mode("primary", "slow", delay_ms=1500)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.post("/v1/chat/completions", json=chat(), headers=A), timeout=0.1)
        await asyncio.sleep(0.05)
        assert await used(client) == 0
