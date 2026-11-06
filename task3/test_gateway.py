"""Tests for SSE handling and the end-to-end gateway.

Unit tests drive ``redact_sse_stream`` with hand-made line iterators. End-to-end
tests run the real gateway under uvicorn in front of a small *test-only* fake
provider (defined below, OpenAI/Groq stream format). Tests must not depend on a
real API key, the network, or an LLM's non-deterministic output.
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
import time
from types import SimpleNamespace
from typing import Any, AsyncIterator, Dict, Iterable, List

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from gateway import create_app, iter_sse_data, parse_client_keys, redact_sse_stream
from redactor import REPLACEMENT as R
from structured_logging import JsonFormatter

TEXT = (
    "Sure! You can reach Jane Doe at jane.doe@example.com. Her SSN is 123-45-6789 "
    "and the card on file is 4111 1111 1111 1111. Let me know if you need anything else."
)
SECRETS = ["jane.doe@example.com", "123-45-6789", "4111 1111 1111 1111"]
REDACTED_TEXT = TEXT.replace(SECRETS[0], R).replace(SECRETS[1], R).replace(SECRETS[2], R)

CLIENT_KEY = "test-client-key-0123456789"
CLIENTS = {CLIENT_KEY: "test-app"}
AUTH = {"Authorization": "Bearer " + CLIENT_KEY}
UPSTREAM_KEY = "gsk_upstream_secret"


# ------------------------------------------------------- test-only fake provider
fake_provider = FastAPI()


seen_by_provider: List[Dict[str, str]] = []  # headers of every request the provider received


@fake_provider.post("/v1/chat/completions")
async def _fake_completions(body: Dict[str, Any], request: Request):
    """Streams ``test_text`` (default TEXT) in 4-char chunks, ``test_delay_ms`` apart."""
    seen_by_provider.append(dict(request.headers))
    text = body.get("test_text") or TEXT
    delay = body.get("test_delay_ms", 20) / 1000

    def chunk(delta, finish=None) -> bytes:
        return b"data: " + json.dumps({"id": "c1", "object": "chat.completion.chunk", "model": "fake",
                                        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}).encode() + b"\n\n"

    if body.get("stream") is not True:
        return JSONResponse({"choices": [{"index": 0, "message": {"role": "assistant", "content": text}}]})

    async def events():
        yield chunk({"role": "assistant", "content": ""})
        for i in range(0, len(text), 4):
            await asyncio.sleep(delay)
            yield chunk({"content": text[i:i + 4]})
        yield chunk({}, finish="stop")
        yield b"data: [DONE]\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


# --------------------------------------------------------------------- helpers
def chunk_line(content=None, index=0, finish=None, **delta) -> str:
    if content is not None:
        delta["content"] = content
    return "data: " + json.dumps({
        "id": "c1", "object": "chat.completion.chunk", "model": "m",
        "choices": [{"index": index, "delta": delta, "finish_reason": finish}],
    })


def provider_lines(text: str, size: int = 3, done: bool = True) -> List[str]:
    lines = [chunk_line(role="assistant"), ""]
    for i in range(0, len(text), size):
        lines += [chunk_line(text[i:i + size]), ""]
    lines += [chunk_line(finish="stop"), ""]
    if done:
        lines += ["data: [DONE]", ""]
    return lines


async def aiter(items: Iterable[str]) -> AsyncIterator[str]:
    for item in items:
        yield item


async def collect(lines, trace=None) -> List[bytes]:
    return [event async for event in redact_sse_stream(aiter(lines), trace)]


def contents(events: List[bytes]) -> str:
    text = ""
    for event in events:
        payload = event.decode()[len("data: "):].strip()
        if payload == "[DONE]":
            continue
        for choice in json.loads(payload).get("choices", []):
            text += (choice.get("delta") or {}).get("content") or ""
    return text


# --------------------------------------------------------------------- SSE unit
class TestSseParsing:
    async def test_events_comments_and_multiline_data(self):
        lines = [": keep-alive", "data: a", "", "event: x", "data:b", "data: c", "", "id: 7", "data: tail"]
        assert [d async for d in iter_sse_data(aiter(lines))] == ["a", "b\nc", "tail"]


class TestRedactSseStream:
    async def test_redacts_across_chunk_boundaries_and_keeps_framing(self):
        events = await collect(provider_lines(TEXT, size=3))
        raw = b"".join(events).decode()
        assert all(secret not in raw for secret in SECRETS)
        assert contents(events) == REDACTED_TEXT
        assert events[-1] == b"data: [DONE]\n\n"
        assert all(e.startswith(b"data: ") and e.endswith(b"\n\n") for e in events)

    async def test_output_is_incremental(self):
        """Each upstream event produces output before the next one exists."""
        queue: asyncio.Queue = asyncio.Queue()

        async def source():
            while (line := await queue.get()) is not None:
                yield line

        out = redact_sse_stream(source())
        for word in ["Hello ", "there ", "friend "]:
            await queue.put(chunk_line(word))
            await queue.put("")
            event = await asyncio.wait_for(out.__anext__(), timeout=1)
            assert json.loads(event[6:])["choices"][0]["delta"]["content"] == word
        await queue.put(None)
        await out.aclose()

    async def test_invalid_json_fails_closed(self):
        lines = [chunk_line("Hi "), "", "data: {not json jane.doe@example.com", "", chunk_line("more"), ""]
        events = await collect(lines)
        raw = b"".join(events).decode()
        assert "jane.doe" not in raw and "more" not in raw
        assert json.loads(events[-1][6:])["error"]["type"] == "gateway_error"

    async def test_held_text_is_flushed_when_upstream_ends_without_done(self):
        events = await collect(provider_lines("mail me at jane@exa", done=False)[:-2])  # no finish, no [DONE]
        assert contents(events) == "mail me at jane@exa"

    async def test_transport_error_mid_stream_flushes_and_reports(self):
        async def broken():
            yield chunk_line("call jane.doe@example.com")
            yield ""
            yield chunk_line(" now")
            yield ""
            raise httpx.ReadError("connection reset")

        events = [e async for e in redact_sse_stream(broken())]
        assert contents(events) == "call %s now" % R
        assert json.loads(events[-1][6:])["error"]["message"] == "Upstream stream interrupted"

    async def test_each_choice_has_its_own_buffer(self):
        lines = []
        for a, b in zip(["jane.d", "oe@exa", "mple.com", " x"], ["123-4", "5-67", "89", " y"]):
            lines += [chunk_line(a, index=0), "", chunk_line(b, index=1), ""]
        lines += [chunk_line(index=0, finish="stop"), "", chunk_line(index=1, finish="stop"), "", "data: [DONE]", ""]
        events = await collect(lines)
        per_choice = {0: "", 1: ""}
        for event in events[:-1]:
            for choice in json.loads(event[6:])["choices"]:
                per_choice[choice["index"]] += choice["delta"].get("content") or ""
        assert per_choice == {0: R + " x", 1: R + " y"}

    async def test_non_string_content_is_not_forwarded(self):
        line = "data: " + json.dumps({"choices": [{"index": 0, "delta": {"content": [{"text": "a@b.com"}]}}]})
        events = await collect([line, ""])
        assert "a@b.com" not in b"".join(events).decode()


class TestOtherModelText:
    """Reasoning models (Groq gpt-oss, qwen3) stream ``delta.reasoning``; tools stream
    ``tool_calls[].function.arguments``. Both reach the client, so both are redacted."""

    @staticmethod
    def deltas(events: List[bytes]) -> List[Dict[str, Any]]:
        return [c["delta"] for e in events if e != b"data: [DONE]\n\n" for c in json.loads(e[6:])["choices"]]

    async def test_reasoning_is_redacted_across_chunks(self):
        lines = []
        for piece in ["The user is jane.d", "oe@exa", "mple.com, SSN 123-4", "5-6789."]:
            lines += [chunk_line(reasoning=piece), ""]
        lines += [chunk_line("Done."), "", chunk_line(finish="stop"), "", "data: [DONE]", ""]
        events = await collect(lines)
        raw = b"".join(events).decode()
        assert "jane" not in raw and "123-4" not in raw and "6789" not in raw
        reasoning = "".join(d.get("reasoning") or "" for d in self.deltas(events))
        assert reasoning == "The user is %s, SSN %s." % (R, R)
        assert contents(events) == "Done."

    async def test_tool_call_arguments_are_redacted_across_chunks(self):
        first = {"index": 0, "id": "call_1", "type": "function",
                 "function": {"name": "send_email", "arguments": '{"to": "bob.sm'}}
        lines = [chunk_line(tool_calls=[first]), "",
                 chunk_line(tool_calls=[{"index": 0, "function": {"arguments": 'ith@corp.io", "n": 1}'}}]), "",
                 chunk_line(finish="tool_calls"), "", "data: [DONE]", ""]
        events = await collect(lines)
        assert "bob" not in b"".join(events).decode()
        args = "".join(c["function"].get("arguments") or ""
                       for d in self.deltas(events) for c in d.get("tool_calls") or [])
        assert json.loads(args) == {"to": R, "n": 1}  # still valid JSON
        names = [c["function"].get("name") for d in self.deltas(events) for c in d.get("tool_calls") or []]
        assert "send_email" in names

    async def test_held_reasoning_is_flushed_without_done(self):
        events = await collect([chunk_line(reasoning="mail a@b.co"), ""])
        assert "".join(d.get("reasoning") or "" for d in self.deltas(events)) == "mail " + R

    async def test_non_streaming_redacts_reasoning_and_tool_arguments(self):
        from gateway import _complete

        message = {"role": "assistant", "content": "ok", "reasoning": "card 4111 1111 1111 1111",
                   "tool_calls": [{"id": "c", "type": "function",
                                   "function": {"name": "f", "arguments": '{"ssn": "123-45-6789"}'}}]}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"choices": [{"index": 0, "message": message}]})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            request = SimpleNamespace(state=SimpleNamespace(request_id="req_test"))
            response = await _complete(client, "http://upstream/v1/chat/completions", {}, {}, request)
        out = json.loads(response.body)["choices"][0]["message"]
        assert out["reasoning"] == "card " + R
        assert json.loads(out["tool_calls"][0]["function"]["arguments"]) == {"ssn": R}
        assert out["content"] == "ok"


class TestTrace:
    async def test_trace_reports_every_step(self):
        trace: List[Dict[str, Any]] = []
        events = await collect(provider_lines(TEXT, size=3), trace=trace.append)
        chunks = [e for e in trace if e["event"] == "chunk"]
        redactions = [e for e in trace if e["event"] == "redact"]
        # what came in is exactly the raw text; what went out is exactly the client's output
        assert "".join(e["received"] for e in chunks) == TEXT
        assert "".join(e["emitted"] for e in trace if e["event"] in ("chunk", "flush")) == contents(events)
        assert sorted(e["kind"] for e in redactions) == ["credit_card", "email", "ssn"]
        assert {e["text"] for e in redactions} == set(SECRETS)
        assert max(len(e["held"]) for e in chunks) <= 25         # buffer stays small

    async def test_no_trace_by_default(self):
        assert contents(await collect(provider_lines(TEXT))) == REDACTED_TEXT


# -------------------------------------------------------------------- end to end
class Server:
    def __init__(self, app):
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="on"))
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [self.sock]}, daemon=True)
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started:
            assert time.time() < deadline, "server did not start"
            time.sleep(0.01)

    def stop(self):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        self.sock.close()


@pytest.fixture(scope="module")
def servers():
    provider = Server(fake_provider)
    gateway = Server(create_app(upstream_url="http://127.0.0.1:%d/v1/chat/completions" % provider.port,
                                upstream_api_key=UPSTREAM_KEY, client_keys=CLIENTS))
    yield gateway, provider
    gateway.stop()
    provider.stop()


def url(server) -> str:
    return "http://127.0.0.1:%d/v1/chat/completions" % server.port


async def timed_stream(target: str, body) -> List[tuple]:
    """Return [(seconds_since_start, content)] for each content delta."""
    out, start = [], time.perf_counter()
    async with httpx.AsyncClient(timeout=30) as client:
        async with client.stream("POST", target, json=body, headers=AUTH) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            async for line in response.aiter_lines():
                if line.startswith("data: {"):
                    for choice in json.loads(line[6:])["choices"]:
                        if choice["delta"].get("content"):
                            out.append((time.perf_counter() - start, choice["delta"]["content"]))
    return out


class TestEndToEnd:
    BODY = {"stream": True, "messages": [{"role": "user", "content": "hi"}], "test_delay_ms": 20}

    async def test_stream_is_redacted_and_incremental(self, servers):
        gateway, _ = servers
        deltas = await timed_stream(url(gateway), self.BODY)
        text = "".join(c for _, c in deltas)
        assert text == REDACTED_TEXT
        # ~43 upstream chunks at 20 ms: the first token must arrive long before the end
        assert len(deltas) > 20
        assert deltas[0][0] < 0.25, deltas[0][0]
        assert deltas[-1][0] - deltas[0][0] > 0.5

    async def test_ttft_overhead_is_small(self, servers):
        gateway, provider = servers
        body = dict(self.BODY, test_text="Hello world, this is a plain answer with nothing sensitive.")
        direct = await timed_stream(url(provider), body)
        proxied = await timed_stream(url(gateway), body)
        # The gateway holds back at most one word: one upstream chunk (20 ms) plus overhead.
        assert proxied[0][0] - direct[0][0] < 0.15, (direct[0][0], proxied[0][0])

    async def test_non_streaming_is_redacted(self, servers):
        gateway, _ = servers
        async with httpx.AsyncClient() as client:
            response = await client.post(url(gateway), json={"messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
        assert response.status_code == 200
        assert response.json()["choices"][0]["message"]["content"] == REDACTED_TEXT

    async def test_upstream_down_is_502(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            dead = s.getsockname()[1]
        gateway = Server(create_app(upstream_url="http://127.0.0.1:%d/v1/chat/completions" % dead,
                                    client_keys=CLIENTS))
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url(gateway), json=dict(self.BODY), headers=AUTH)
            assert response.status_code == 502
            assert response.json() == {"error": {"type": "upstream_unavailable", "message": "LLM provider unavailable",
                                                 "request_id": response.headers["x-request-id"]}}
        finally:
            gateway.stop()

    async def test_invalid_body_is_400(self, servers):
        gateway, _ = servers
        async with httpx.AsyncClient() as client:
            response = await client.post(url(gateway), content=b"not json", headers=dict(AUTH, **{"Content-Type": "application/json"}))
        assert response.status_code == 400 and response.json()["error"]["type"] == "invalid_request"


# ------------------------------------------------------------------ server log
class TestServerLog:
    """The gateway's own JSON Lines log: correlated by request id, and free of PII."""

    BODY = {"stream": True, "messages": [{"role": "user", "content": "hi"}], "test_delay_ms": 1}

    @staticmethod
    async def events_for(caplog, rid: str, wanted: int) -> List[Dict[str, Any]]:
        """The request's events, formatted as production writes them (the server
        thread may still be finishing its last line when the client is done)."""
        formatter = JsonFormatter()
        for _ in range(100):
            lines = [formatter.format(r) for r in caplog.records if getattr(r, "request_id", None) == rid]
            if len(lines) >= wanted:
                break
            await asyncio.sleep(0.01)
        return [json.loads(line) for line in lines]

    async def test_stream_logs_counts_and_kinds_never_the_pii(self, servers, caplog):
        caplog.set_level(logging.INFO)
        gateway, _ = servers
        async with httpx.AsyncClient(timeout=30) as client:
            async with client.stream("POST", url(gateway), json=self.BODY, headers=AUTH) as response:
                rid = response.headers["x-request-id"]
                await response.aread()
        events = await self.events_for(caplog, rid, 2)
        assert [e["event"] for e in events] == ["stream.completed", "http.access"]
        done = events[0]
        assert (done["outcome"], done["redactions"], done["redaction_total"]) == (
            "done", {"email": 1, "ssn": 1, "credit_card": 1}, 3)
        assert done["chunks"] > 20 and 0 < done["max_held_chars"] <= 25 and done["first_chunk_ms"] is not None
        assert events[1]["status"] == 200 and events[1]["path"] == "/v1/chat/completions"
        text = "\n".join(JsonFormatter().format(r) for r in caplog.records)
        assert not any(secret in text for secret in SECRETS)
        assert "jane" not in text.lower()

    async def test_non_streaming_logs_redaction_counts(self, servers, caplog):
        caplog.set_level(logging.INFO)
        gateway, _ = servers
        async with httpx.AsyncClient() as client:
            response = await client.post(url(gateway), json={"messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
        events = await self.events_for(caplog, response.headers["x-request-id"], 2)
        assert events[0]["event"] == "completion.redacted" and events[0]["redaction_total"] == 3

    async def test_errors_carry_the_request_id(self, servers, caplog):
        caplog.set_level(logging.INFO)
        gateway, _ = servers
        async with httpx.AsyncClient() as client:
            response = await client.post(url(gateway), content=b"not json", headers=dict(AUTH, **{"Content-Type": "application/json"}))
        rid = response.headers["x-request-id"]
        assert response.json()["error"]["request_id"] == rid
        events = await self.events_for(caplog, rid, 1)
        assert events[-1]["event"] == "http.access" and events[-1]["status"] == 400

    async def test_client_disconnect_is_recorded(self, servers, caplog):
        caplog.set_level(logging.INFO)
        gateway, _ = servers
        body = dict(self.BODY, test_delay_ms=50)
        async with httpx.AsyncClient(timeout=30) as client:
            async with client.stream("POST", url(gateway), json=body, headers=AUTH) as response:
                rid = response.headers["x-request-id"]
                async for _ in response.aiter_lines():
                    break  # hang up after the first line
        events = await self.events_for(caplog, rid, 1)
        completed = [e for e in events if e["event"] == "stream.completed"]
        assert completed and completed[0]["outcome"] == "client_disconnected"
        assert completed[0]["level"] == "WARNING"


# ----------------------------------------------------------------- client auth
class TestClientAuth:
    """Only clients holding a gateway key may spend the gateway's Groq key."""

    BODY = {"messages": [{"role": "user", "content": "hi"}]}

    @staticmethod
    async def post(gateway, headers=None, **kwargs):
        async with httpx.AsyncClient() as client:
            return await client.post(url(gateway), headers=headers or {}, **kwargs)

    @pytest.mark.parametrize("headers", [
        {},                                                  # no credential
        {"Authorization": "Bearer wrong-key-0123456789"},    # unknown key
        {"Authorization": "Bearer " + CLIENT_KEY[:-1]},      # prefix of a real key
        {"Authorization": "Basic " + CLIENT_KEY},            # wrong scheme
        {"Authorization": "Bearer"},                         # scheme, no key
        {"Authorization": CLIENT_KEY},                       # no scheme
    ])
    async def test_rejected_before_the_provider_is_called(self, servers, headers):
        gateway, _ = servers
        before = len(seen_by_provider)
        response = await self.post(gateway, headers, json=dict(self.BODY, stream=True))
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"
        assert response.json() == {"error": {"type": "authentication_error", "message": "Missing or invalid API key",
                                             "request_id": response.headers["x-request-id"]}}
        assert len(seen_by_provider) == before

    async def test_auth_is_checked_before_the_body(self, servers):
        gateway, _ = servers
        response = await self.post(gateway, content=b"not json", headers={"Content-Type": "application/json"})
        assert response.status_code == 401  # not 400: an anonymous caller learns nothing

    async def test_client_key_never_reaches_the_provider(self, servers):
        gateway, _ = servers
        response = await self.post(gateway, dict(AUTH), json=self.BODY)
        assert response.status_code == 200
        seen = seen_by_provider[-1]
        assert seen["authorization"] == "Bearer " + UPSTREAM_KEY
        assert not any(CLIENT_KEY in value for value in seen.values())

    async def test_log_names_the_client_and_never_the_key(self, servers, caplog):
        caplog.set_level(logging.INFO)
        gateway, _ = servers
        ok = await self.post(gateway, dict(AUTH), json=self.BODY)
        bad = await self.post(gateway, {"Authorization": "Bearer wrong-key-0123456789"}, json=self.BODY)
        ok_events = await TestServerLog.events_for(caplog, ok.headers["x-request-id"], 2)
        bad_events = await TestServerLog.events_for(caplog, bad.headers["x-request-id"], 2)
        assert ok_events[-1]["event"] == "http.access" and ok_events[-1]["client"] == "test-app"
        failed = bad_events[0]
        assert (failed["event"], failed["level"], failed["credential_present"]) == ("auth.failed", "WARNING", True)
        assert bad_events[-1]["client"] is None and bad_events[-1]["status"] == 401
        text = "\n".join(JsonFormatter().format(r) for r in caplog.records)
        assert CLIENT_KEY not in text and "wrong-key-0123456789" not in text

    async def test_no_keys_configured_refuses_everyone(self, caplog):
        caplog.set_level(logging.INFO)
        provider = Server(fake_provider)
        gateway = Server(create_app(upstream_url=url(provider), client_keys={}))
        try:
            before = len(seen_by_provider)
            response = await self.post(gateway, dict(AUTH), json=self.BODY)
            assert response.status_code == 401 and len(seen_by_provider) == before
            assert any(getattr(r, "event", None) == "config.no_client_keys" for r in caplog.records)
        finally:
            gateway.stop()
            provider.stop()

    def test_key_spec_parsing(self):
        assert parse_client_keys("") == {}
        assert parse_client_keys("aaaaaaaaaaaaaaaa:app-a, bbbbbbbbbbbbbbbb:app-b ,") == {
            "aaaaaaaaaaaaaaaa": "app-a", "bbbbbbbbbbbbbbbb": "app-b"}
        for bad in ("short:app", "aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa:", ":app"):
            with pytest.raises(ValueError):
                parse_client_keys(bad)
        with pytest.raises(ValueError):
            create_app(client_keys={"short": "app"})
