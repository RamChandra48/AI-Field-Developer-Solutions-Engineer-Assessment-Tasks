"""LLM gateway with a streaming PII-redaction guardrail.

    client --POST /v1/chat/completions--> gateway --> LLM provider (Groq, OpenAI-style SSE)
           <--- redacted SSE, chunk by chunk ---

The upstream stream is consumed line by line and each chunk is re-emitted as
soon as it has been redacted. Nothing is accumulated beyond the redactor's
small hold-back buffer (see redactor.py), so time-to-first-token stays close
to the provider's.

    export GROQ_API_KEY=gsk_...
    export GATEWAY_API_KEYS="$(openssl rand -hex 24):my-app"
    python -m uvicorn gateway:app --port 8200 --no-access-log

Clients authenticate with ``Authorization: Bearer <gateway key>``. That key is
the gateway's own, issued per client; it is checked in constant time and never
forwarded. The Groq key stays in this process. Without GATEWAY_API_KEYS every
request is refused (fail closed), because an open port here would let anyone
spend the Groq key.

Environment: GATEWAY_API_KEYS ("key:client,key2:client2"; keys of at least
16 characters), LLM_UPSTREAM_URL (default: Groq's chat completions URL),
LLM_UPSTREAM_API_KEY (default: $GROQ_API_KEY) and LOG_LEVEL.

Logs are JSON Lines on stdout (structured_logging.py): one ``http.access`` and,
for streams, one ``stream.completed`` event per request, tagged with the
request id that is also returned as ``X-Request-ID``. They record counts and
kinds of PII redacted, never the text or the PII itself.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import time
import uuid
from collections import Counter
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from redactor import StreamRedactor
from structured_logging import configure_logging

log = logging.getLogger("llm-gateway")

DEFAULT_UPSTREAM = "https://api.groq.com/openai/v1/chat/completions"
SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}  # stop proxies buffering

MIN_KEY_LENGTH = 16

# Optional hook that sees every redaction step. Off by default: in production
# you would never log the raw (pre-redaction) text.
Trace = Callable[[Dict[str, Any]], None]


# ----------------------------------------------------------------- SSE parsing
async def iter_sse_data(lines: AsyncIterator[str]) -> AsyncIterator[str]:
    """Yield the ``data`` payload of each server-sent event as it completes."""
    data: List[str] = []
    async for line in lines:
        if not line:  # blank line ends the event
            if data:
                yield "\n".join(data)
                data = []
            continue
        if line.startswith(":"):  # comment / keep-alive
            continue
        name, _, value = line.partition(":")
        if name == "data":
            data.append(value[1:] if value.startswith(" ") else value)
        # event:, id:, retry: are not used by chat-completion streams
    if data:
        yield "\n".join(data)


def sse(obj: Any) -> bytes:
    return b"data: " + json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n\n"


def error_event(message: str) -> bytes:
    return sse({"error": {"type": "gateway_error", "message": message}})


# ---------------------------------------------------------- stream redaction
# Every model-generated text field a client receives. ``reasoning`` is where
# Groq streams the thinking of reasoning models (gpt-oss, qwen3); tool-call
# arguments are model text too. Any field left out here would bypass redaction.
TEXT_FIELDS = ("content", "reasoning")
TOOL_FIELD = "tool_call:%d"  # one stream per tool call: its ``function.arguments``


def _tool_arguments(call: Any) -> Optional[Dict[str, Any]]:
    """The ``function`` object of a tool call, if it carries ``arguments``."""
    function = call.get("function") if isinstance(call, dict) else None
    return function if isinstance(function, dict) and "arguments" in function else None


def _append(delta: Dict[str, Any], field: str, text: str) -> None:
    """Add released text to a delta, under the field it came from."""
    if field in TEXT_FIELDS:
        delta[field] = (delta.get(field) or "") + text
        return
    tool_index = int(field.rsplit(":", 1)[1])
    calls = delta.setdefault("tool_calls", [])
    for call in calls:
        function = _tool_arguments(call)
        if function is not None and call.get("index", 0) == tool_index:
            function["arguments"] = (function.get("arguments") or "") + text
            return
    calls.append({"index": tool_index, "function": {"arguments": text}})


class ChunkRedactor:
    """Applies one StreamRedactor per (choice, field) to OpenAI-style chunks.

    Fields are separate streams: a reasoning chunk and a content chunk are not
    one text, and joining them could create or hide a match.
    """

    def __init__(self, trace: Optional[Trace] = None, request_id: Optional[str] = None) -> None:
        self._redactors: Dict[Tuple[int, str], StreamRedactor] = {}
        self._template: Dict[str, Any] = {}
        self._trace = trace
        self._request_id = request_id
        self.max_held = 0  # longest text held back at once, in characters

    def _redactor(self, index: int, field: str) -> StreamRedactor:
        key = (index, field)
        if key not in self._redactors:
            hook = None
            if self._trace is not None and field == "content":  # the trace follows the visible answer
                trace = self._trace
                hook = lambda kind, text: trace({"event": "redact", "choice": index, "kind": kind, "text": text})  # noqa: E731
            self._redactors[key] = StreamRedactor(on_redact=hook)
        return self._redactors[key]

    def _feed(self, index: int, field: str, value: Any) -> str:
        if not isinstance(value, str):
            # Unknown shape: it can't be inspected, so it isn't forwarded.
            log.warning("dropping a field that is not a string", extra={
                "event": "redaction.dropped_field", "request_id": self._request_id,
                "field": field, "value_type": type(value).__name__})
            return ""
        redactor = self._redactor(index, field)
        emitted = redactor.feed(value)
        self.max_held = max(self.max_held, len(redactor.pending))
        if self._trace is not None and field == "content":
            self._trace({"event": "chunk", "choice": index, "received": value,
                         "emitted": emitted, "held": redactor.pending})
        return emitted

    def _release(self, index: int, delta: Dict[str, Any]) -> None:
        """Flush every stream of one choice into ``delta``."""
        for (i, field), redactor in list(self._redactors.items()):
            if i != index:
                continue
            tail = redactor.flush()
            if self._trace is not None and field == "content":
                self._trace({"event": "flush", "choice": index, "emitted": tail})
            if tail:
                _append(delta, field, tail)

    def process(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        self._template = {k: chunk[k] for k in ("id", "object", "created", "model") if k in chunk}
        for choice in chunk.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            index = choice.get("index", 0)
            self._redactor(index, "content")  # so the finish flush is always traced
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                delta = choice["delta"] = {}
            for field in TEXT_FIELDS:
                if delta.get(field) is not None:
                    delta[field] = self._feed(index, field, delta[field])
            calls = delta.get("tool_calls")
            if calls is not None and not isinstance(calls, list):
                log.warning("dropping a field that is not a list", extra={
                    "event": "redaction.dropped_field", "request_id": self._request_id,
                    "field": "tool_calls", "value_type": type(calls).__name__})
                del delta["tool_calls"]
            for call in delta.get("tool_calls") or []:
                function = _tool_arguments(call)
                if function is not None and function["arguments"] is not None:
                    field = TOOL_FIELD % call.get("index", 0)
                    function["arguments"] = self._feed(index, field, function["arguments"])
            if choice.get("finish_reason") is not None:
                self._release(index, delta)
        return chunk

    def flush(self) -> Optional[Dict[str, Any]]:
        """Release held-back text for every choice (stream ended)."""
        choices = []
        for index in sorted({i for i, _ in self._redactors}):
            delta: Dict[str, Any] = {}
            self._release(index, delta)
            if delta:
                choices.append({"index": index, "delta": delta, "finish_reason": None})
        return dict(self._template, choices=choices) if choices else None

    @property
    def redactions(self) -> Counter:
        """PII redacted so far, by kind (email, ssn, credit_card), across all fields."""
        total: Counter = Counter()
        for redactor in self._redactors.values():
            total.update(redactor.redactions)
        return total

    @property
    def redaction_count(self) -> int:
        return sum(self.redactions.values())


async def redact_sse_stream(lines: AsyncIterator[str], trace: Optional[Trace] = None,
                            request_id: Optional[str] = None) -> AsyncIterator[bytes]:
    """Turn upstream SSE lines into redacted SSE bytes, one event at a time."""
    chunks = ChunkRedactor(trace, request_id)
    start = time.perf_counter()
    first_chunk_ms: Optional[float] = None
    events = 0
    outcome = "client_disconnected"  # unless the stream reaches one of the ends below
    try:
        async for payload in iter_sse_data(lines):
            if payload.strip() == "[DONE]":
                tail = chunks.flush()
                if tail:
                    yield sse(tail)
                outcome = "done"
                yield b"data: [DONE]\n\n"
                return
            try:
                chunk = json.loads(payload)
            except ValueError:
                chunk = None
            if not isinstance(chunk, dict):
                # Fail closed: forwarding text we cannot parse could leak PII.
                log.error("upstream sent an unparseable stream chunk; ending the stream", extra={
                    "event": "stream.invalid_chunk", "request_id": request_id, "payload_chars": len(payload)})
                outcome = "invalid_chunk"
                yield error_event("Upstream sent an invalid stream chunk")
                return
            event = sse(chunks.process(chunk))
            if first_chunk_ms is None:
                first_chunk_ms = (time.perf_counter() - start) * 1000
            events += 1
            yield event
        tail = chunks.flush()  # upstream closed without [DONE]
        outcome = "upstream_closed"
        if tail:
            yield sse(tail)
    except Exception as exc:  # transport failure mid-stream (not CancelledError)
        log.error("upstream stream failed mid-way", extra={
            "event": "stream.interrupted", "request_id": request_id, "error_class": type(exc).__name__})
        outcome = "interrupted"
        tail = chunks.flush()
        if tail:
            yield sse(tail)
        yield error_event("Upstream stream interrupted")
    finally:
        log.log(logging.INFO if outcome in ("done", "upstream_closed") else logging.WARNING, "stream completed", extra={
            "event": "stream.completed", "request_id": request_id, "outcome": outcome, "chunks": events,
            "first_chunk_ms": None if first_chunk_ms is None else round(first_chunk_ms, 1),
            "duration_ms": round((time.perf_counter() - start) * 1000, 1),
            "redactions": dict(chunks.redactions), "redaction_total": chunks.redaction_count,
            "max_held_chars": chunks.max_held})


# ----------------------------------------------------------------------- auth
def parse_client_keys(spec: str) -> Dict[str, str]:
    """"key1:app-a,key2:app-b" -> {"key1": "app-a", ...}"""
    keys: Dict[str, str] = {}
    for item in filter(None, (part.strip() for part in spec.split(","))):
        key, _, client = item.rpartition(":")
        keys[key.strip()] = client.strip()
    return check_client_keys(keys)


def check_client_keys(keys: Dict[str, str]) -> Dict[str, str]:
    for key, client in keys.items():
        if not key or not client:
            raise ValueError("GATEWAY_API_KEYS entries must look like key:client")
        if len(key) < MIN_KEY_LENGTH:
            raise ValueError("gateway API keys must be at least %d characters (client %r)" % (MIN_KEY_LENGTH, client))
    return dict(keys)


def authenticate(request: Request, keys: Dict[str, str]) -> Optional[str]:
    """The client name for a valid ``Bearer`` key, else None. Every known key is
    compared (constant time each), so timing doesn't reveal which one matched."""
    scheme, _, presented = request.headers.get("authorization", "").partition(" ")
    presented = presented.strip().encode()
    client = None
    if scheme.lower() == "bearer" and presented:
        for key, name in keys.items():
            if hmac.compare_digest(key.encode(), presented):
                client = name
    if client is None:
        # The presented value is never logged, only whether one was sent.
        log.warning("authentication failed", extra={
            "event": "auth.failed", "request_id": request.state.request_id,
            "credential_present": bool(presented), "keys_configured": bool(keys)})
    return client


# ------------------------------------------------------------------------ app
def _error(request: Request, status: int, error_type: str, message: str,
           headers: Optional[Dict[str, str]] = None) -> JSONResponse:
    body = {"error": {"type": error_type, "message": message, "request_id": request.state.request_id}}
    return JSONResponse(body, status_code=status, headers=headers)


class AccessLog:
    """ASGI middleware: a request id for every request (``X-Request-ID``) and one
    ``http.access`` event when it ends. Plain ASGI rather than ``@app.middleware``,
    which re-buffers streaming responses through a memory channel.
    For streams, ``latency_ms`` covers the whole stream."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        rid = "req_" + uuid.uuid4().hex[:16]
        scope.setdefault("state", {})["request_id"] = rid  # read back as request.state.request_id
        start, status = time.perf_counter(), 500

        async def send_with_id(message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message["headers"] = list(message.get("headers", [])) + [(b"x-request-id", rid.encode())]
            await send(message)

        try:
            await self.app(scope, receive, send_with_id)
        except asyncio.CancelledError:
            status = 499  # client closed the connection
            raise
        finally:
            log.log(logging.ERROR if status >= 500 else logging.INFO, "request completed", extra={
                "event": "http.access", "request_id": rid, "client": scope["state"].get("client"),
                "method": scope["method"], "path": scope["path"],
                "status": status, "latency_ms": round((time.perf_counter() - start) * 1000, 1)})


def create_app(
    upstream_url: Optional[str] = None,
    upstream_api_key: Optional[str] = None,
    trace: Optional[Trace] = None,
    client_keys: Optional[Dict[str, str]] = None,
) -> FastAPI:
    """``client_keys`` maps gateway API key -> client name (default: $GATEWAY_API_KEYS)."""
    upstream_url = upstream_url or os.environ.get("LLM_UPSTREAM_URL", DEFAULT_UPSTREAM)
    upstream_api_key = upstream_api_key or os.environ.get("LLM_UPSTREAM_API_KEY") or os.environ.get("GROQ_API_KEY")
    client_keys = (parse_client_keys(os.environ.get("GATEWAY_API_KEYS", "")) if client_keys is None
                   else check_client_keys(client_keys))
    if not client_keys:
        log.warning("no client API keys configured; every request will be refused",
                    extra={"event": "config.no_client_keys"})

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # One pooled client for the process: no per-request TCP/TLS handshake.
        # read=None: an LLM may pause between tokens for a long time.
        timeout = httpx.Timeout(connect=5.0, read=None, write=10.0, pool=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            app.state.client = client
            yield

    app = FastAPI(title="LLM gateway with streaming PII redaction", lifespan=lifespan)
    app.add_middleware(AccessLog)

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        client_name = authenticate(request, client_keys)
        if client_name is None:  # before the body is read or the provider is called
            return _error(request, 401, "authentication_error", "Missing or invalid API key",
                          headers={"WWW-Authenticate": "Bearer"})
        request.state.client = client_name
        try:
            body = await request.json()
        except ValueError:
            return _error(request, 400, "invalid_request", "Request body must be JSON")
        if not isinstance(body, dict):
            return _error(request, 400, "invalid_request", "Request body must be a JSON object")

        headers = {"Content-Type": "application/json"}
        if upstream_api_key:  # the provider key lives in the gateway, never with the client
            headers["Authorization"] = "Bearer " + upstream_api_key
        client: httpx.AsyncClient = request.app.state.client

        if body.get("stream") is not True:
            return await _complete(client, upstream_url, body, headers, request)

        upstream = client.build_request("POST", upstream_url, json=body, headers=headers)
        try:
            response = await client.send(upstream, stream=True)
        except httpx.HTTPError as exc:
            log.error("LLM provider unavailable", extra={
                "event": "upstream.unavailable", "request_id": request.state.request_id,
                "error_class": type(exc).__name__})
            return _error(request, 502, "upstream_unavailable", "LLM provider unavailable")
        if response.status_code != 200:
            await response.aclose()
            log.error("LLM provider returned an error", extra={
                "event": "upstream.error", "request_id": request.state.request_id,
                "upstream_status": response.status_code})
            return _error(request, 502, "upstream_error", "LLM provider returned HTTP %d" % response.status_code)
        rid = request.state.request_id

        async def body_iterator() -> AsyncIterator[bytes]:
            try:
                async for event in redact_sse_stream(response.aiter_lines(), trace, rid):
                    yield event
            finally:
                await response.aclose()  # also runs if the client disconnects

        return StreamingResponse(body_iterator(), media_type="text/event-stream", headers=SSE_HEADERS)

    return app


def _redact_all(text: str, found: Counter) -> str:
    redactor = StreamRedactor()
    out = redactor.feed(text) + redactor.flush()
    found.update(redactor.redactions)
    return out


async def _complete(client: httpx.AsyncClient, url: str, body: Dict[str, Any], headers: Dict[str, str],
                    request: Request) -> JSONResponse:
    """Non-streaming request: redact each choice's full message."""
    rid = request.state.request_id
    try:
        response = await client.post(url, json=body, headers=headers)
        data = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        log.error("LLM provider unavailable", extra={
            "event": "upstream.unavailable", "request_id": rid, "error_class": type(exc).__name__})
        return _error(request, 502, "upstream_unavailable", "LLM provider unavailable")
    if response.status_code != 200 or not isinstance(data, dict):
        log.error("LLM provider returned an error", extra={
            "event": "upstream.error", "request_id": rid, "upstream_status": response.status_code})
        return _error(request, 502, "upstream_error", "LLM provider returned HTTP %d" % response.status_code)
    found: Counter = Counter()
    for choice in data.get("choices") or []:
        message = choice.get("message") if isinstance(choice, dict) else None
        if not isinstance(message, dict):
            continue
        for field in TEXT_FIELDS:
            if message.get(field) is not None:
                message[field] = _redact_all(message[field], found) if isinstance(message[field], str) else ""
        calls = message.get("tool_calls")
        for call in calls if isinstance(calls, list) else []:
            function = _tool_arguments(call)
            if function is not None:
                args = function["arguments"]
                function["arguments"] = _redact_all(args, found) if isinstance(args, str) else ""
    log.info("completion redacted", extra={
        "event": "completion.redacted", "request_id": rid, "redactions": dict(found),
        "redaction_total": sum(found.values())})
    return JSONResponse(data)


configure_logging()
_app: Optional[FastAPI] = None


def __getattr__(name: str) -> Any:
    """``gateway.app`` for ``uvicorn gateway:app``, built from the environment on
    first access (PEP 562). Importing the module (the demo, the tests) then has
    no side effects: no app, no ``config.no_client_keys`` warning."""
    global _app
    if name == "app":
        if _app is None:
            _app = create_app()
        return _app
    raise AttributeError("module %r has no attribute %r" % (__name__, name))
