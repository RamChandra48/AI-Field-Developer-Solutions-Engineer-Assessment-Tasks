"""LLM gateway: per-tenant token rate limiting plus primary/secondary model failover.

    POST /v1/chat/completions   Authorization: Bearer <tenant api key>
    GET  /v1/rate-limit         the caller's current usage in the window
    GET  /healthz               includes the primary's circuit state

Request flow:

    1. authenticate the tenant API key
    2. validate the body; estimate tokens = prompt estimate + max_tokens
    3. reserve that estimate in the sliding window (429 if it does not fit)
    4. route: primary (3000 ms deadline) -> secondary on 429/timeout/5xx;
       after repeated primary failures the circuit opens and requests go
       straight to the secondary until a probe shows the primary recovered
    5. reconcile the reservation with the provider's reported usage;
       refund it entirely if no provider produced a completion

    uvicorn gateway:app --port 8300 --no-access-log    # logs: JSON Lines on stdout
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import httpx
from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from errors import GatewayError, error_body
from rate_limiter import SlidingWindowTokenLimiter
from circuit_breaker import CircuitBreaker
from router import ModelRouter, Provider, RouteResult
from structured_logging import configure_logging

log = logging.getLogger("llm-gateway")
HERE = Path(__file__).resolve().parent


# --------------------------------------------------------------------- config
def _parse_tenants(spec: str) -> Dict[str, str]:
    """"key1:tenant-a,key2:tenant-b" -> {"key1": "tenant-a", ...}"""
    tenants = {}
    for item in filter(None, (part.strip() for part in spec.split(","))):
        key, _, tenant = item.partition(":")
        if not key or not tenant:
            raise ValueError("TENANT_API_KEYS entries must look like key:tenant")
        tenants[key] = tenant
    return tenants


@dataclass
class Settings:
    primary_url: str = "http://127.0.0.1:8301/primary/v1/chat/completions"
    secondary_url: str = "http://127.0.0.1:8301/secondary/v1/chat/completions"
    primary_model: str = "primary-model"
    secondary_model: str = "secondary-model"
    primary_api_key: Optional[str] = None
    secondary_api_key: Optional[str] = None
    primary_timeout_ms: int = 3000
    secondary_timeout_ms: int = 10000
    circuit_failure_threshold: int = 3  # consecutive primary failures that open the circuit; 0 disables
    circuit_cooldown_seconds: float = 30.0
    tokens_per_minute: int = 50_000
    window_seconds: float = 60.0
    default_max_tokens: int = 1024
    db_path: str = str(HERE / "ratelimit.db")
    tenants: Dict[str, str] = field(default_factory=lambda: {"tenant-a-key": "tenant-a", "tenant-b-key": "tenant-b"})

    @classmethod
    def from_env(cls) -> "Settings":
        env, s = os.environ, cls()
        s.primary_url = env.get("PRIMARY_URL", s.primary_url)
        s.secondary_url = env.get("SECONDARY_URL", s.secondary_url)
        s.primary_model = env.get("PRIMARY_MODEL", s.primary_model)
        s.secondary_model = env.get("SECONDARY_MODEL", s.secondary_model)
        s.primary_api_key = env.get("PRIMARY_API_KEY") or None
        s.secondary_api_key = env.get("SECONDARY_API_KEY") or None
        s.primary_timeout_ms = int(env.get("PRIMARY_TIMEOUT_MS", s.primary_timeout_ms))
        s.secondary_timeout_ms = int(env.get("SECONDARY_TIMEOUT_MS", s.secondary_timeout_ms))
        s.circuit_failure_threshold = int(env.get("CIRCUIT_FAILURE_THRESHOLD", s.circuit_failure_threshold))
        s.circuit_cooldown_seconds = float(env.get("CIRCUIT_COOLDOWN_SECONDS", s.circuit_cooldown_seconds))
        s.tokens_per_minute = int(env.get("RATE_LIMIT_TOKENS_PER_MINUTE", s.tokens_per_minute))
        s.default_max_tokens = int(env.get("DEFAULT_MAX_TOKENS", s.default_max_tokens))
        s.db_path = env.get("RATE_LIMIT_DB", s.db_path)
        if env.get("TENANT_API_KEYS"):
            s.tenants = _parse_tenants(env["TENANT_API_KEYS"])
        return s


# ------------------------------------------------------------------ API models
class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: str = Field(max_length=400_000)


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    messages: List[Message] = Field(min_length=1, max_length=512)
    max_tokens: Optional[int] = Field(default=None, ge=1, le=100_000)
    temperature: Optional[float] = Field(default=None, ge=0, le=2)
    model: Optional[str] = None  # accepted for compatibility; the router picks the model


def estimate_prompt_tokens(messages: List[Message]) -> int:
    """~4 ASCII characters per token, one token per non-ASCII character, plus
    per-message framing. Chinese, Japanese, Cyrillic or emoji tokenize far
    denser than English, so chars/4 alone would under-reserve them by up to 4x.
    A tokenizer would be exact. Reconciliation charges the provider's real
    count either way."""
    total = 3
    for m in messages:
        ascii_chars = len(m.content.encode("ascii", "ignore"))
        total += 4 + math.ceil(ascii_chars / 4) + (len(m.content) - ascii_chars)
    return total


def reported_tokens(body: Dict[str, Any]) -> Optional[int]:
    usage = body.get("usage")
    if not isinstance(usage, dict):
        return None
    total = usage.get("total_tokens")
    if isinstance(total, int) and not isinstance(total, bool) and total >= 0:
        return total
    parts = [usage.get("prompt_tokens"), usage.get("completion_tokens")]
    if all(isinstance(p, int) and not isinstance(p, bool) and p >= 0 for p in parts):
        return sum(parts)
    return None


# ------------------------------------------------------------------------ app
def create_app(
    settings: Optional[Settings] = None,
    *,
    http_client: Optional[httpx.AsyncClient] = None,
    limiter: Optional[SlidingWindowTokenLimiter] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    owns_client = http_client is None
    # httpx timeouts are only a backstop; the router enforces the real deadlines.
    client = http_client or httpx.AsyncClient(timeout=httpx.Timeout(30.0), limits=httpx.Limits(max_connections=200))
    limiter = limiter or SlidingWindowTokenLimiter(settings.db_path, settings.tokens_per_minute, settings.window_seconds)
    router = ModelRouter(
        client,
        Provider("primary", settings.primary_url, settings.primary_model, settings.primary_api_key),
        Provider("secondary", settings.secondary_url, settings.secondary_model, settings.secondary_api_key),
        primary_timeout=settings.primary_timeout_ms / 1000,
        secondary_timeout=settings.secondary_timeout_ms / 1000,
        breaker=CircuitBreaker("primary", settings.circuit_failure_threshold, settings.circuit_cooldown_seconds)
        if settings.circuit_failure_threshold > 0 else None,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        if owns_client:
            await client.aclose()
        limiter.close()

    app = FastAPI(title="LLM gateway: rate limiting and model fallback", lifespan=lifespan)
    app.state.settings, app.state.limiter, app.state.router = settings, limiter, router

    # -- request ids and standardized errors
    def request_id(request: Request) -> str:
        rid = getattr(request.state, "request_id", None)
        if rid is None:
            rid = request.state.request_id = "req_" + uuid.uuid4().hex[:16]
        return rid

    def error_response(request: Request, status: int, error_type: str, message: str,
                       retry_after: Optional[int] = None) -> JSONResponse:
        rid = request_id(request)
        headers = {"X-Request-ID": rid}
        if retry_after is not None:
            headers["Retry-After"] = str(retry_after)
        return JSONResponse(error_body(error_type, message, rid, retry_after), status_code=status, headers=headers)

    @app.middleware("http")
    async def access_log(request: Request, call_next):
        """Adds X-Request-ID and writes one ``http.access`` event per request."""
        rid = request_id(request)
        start = time.perf_counter()
        status = 500  # if the app raises, ServerErrorMiddleware answers 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = rid
            return response
        except asyncio.CancelledError:
            status = 499  # client closed the connection before the response
            raise
        finally:
            log.log(logging.ERROR if status >= 500 else logging.INFO, "request completed", extra={
                "event": "http.access", "request_id": rid, "method": request.method, "path": request.url.path,
                "status": status, "latency_ms": round((time.perf_counter() - start) * 1000, 1),
                "tenant": getattr(request.state, "tenant", None)})

    @app.exception_handler(GatewayError)
    async def on_gateway_error(request: Request, exc: GatewayError):
        return error_response(request, exc.status, exc.error_type, exc.message, exc.retry_after)

    @app.exception_handler(RequestValidationError)
    async def on_validation_error(request: Request, exc: RequestValidationError):
        first = exc.errors()[0] if exc.errors() else {}
        location = ".".join(str(part) for part in first.get("loc", ()) if part != "body") or "body"
        return error_response(request, 400, "invalid_request", "Invalid request: %s: %s" % (location, first.get("msg", "invalid")))

    @app.exception_handler(StarletteHTTPException)
    async def on_http_error(request: Request, exc: StarletteHTTPException):
        kinds = {404: ("not_found", "Not found."), 405: ("method_not_allowed", "Method not allowed.")}
        error_type, message = kinds.get(exc.status_code, ("http_error", "Request failed."))
        return error_response(request, exc.status_code, error_type, message)

    @app.exception_handler(Exception)
    async def on_unexpected(request: Request, exc: Exception):
        # Full details go to the server log only, keyed by request id.
        log.exception("unhandled error", extra={"event": "request.unhandled_exception", "request_id": request_id(request)})
        return error_response(request, 500, "internal_error", "An internal error occurred.")

    # -- auth
    async def tenant(request: Request) -> str:
        header = request.headers.get("authorization", "")
        scheme, _, key = header.partition(" ")
        if scheme.lower() == "bearer" and key:
            for known_key, tenant_id in settings.tenants.items():
                if hmac.compare_digest(known_key.encode(), key.strip().encode()):
                    request.state.tenant = tenant_id
                    return tenant_id
        # The presented key is never logged, only whether one was sent.
        log.warning("authentication failed", extra={"event": "auth.failed", "request_id": request_id(request),
                                                    "credential_present": bool(key)})
        raise GatewayError(401, "authentication_error", "Missing or invalid API key.")

    # -- routes
    @app.get("/healthz")
    async def healthz():
        breaker = router.breaker
        return {"status": "ok", "primary_circuit": breaker.state if breaker else "disabled"}

    @app.get("/v1/rate-limit")
    async def rate_limit(tenant_id: str = Depends(tenant)):
        used = await limiter.usage(tenant_id)
        return {"tenant": tenant_id, "limit": limiter.limit, "used": used,
                "remaining": max(0, limiter.limit - used), "window_seconds": limiter.window_ms / 1000}

    async def reconcile(rid: str, tenant_id: str, decision, estimate: int, actual: Optional[int]) -> int:
        """Charge what the completion really cost and return it. Usage above the
        reservation is charged in full (debt, see rate_limiter.py) and logged.
        The completion has already been paid for, so a failure here is logged
        and the estimate stays charged; it never turns into an error for the client."""
        wanted = estimate if actual is None else actual  # no usage reported: keep the estimate
        try:
            charged = await asyncio.shield(limiter.reconcile(decision.reservation_id, wanted))
        except Exception:
            log.exception("could not reconcile usage; the estimate stays charged", extra={
                "event": "ratelimit.reconcile_failed", "request_id": rid, "tenant": tenant_id,
                "tokens_estimated": estimate, "tokens_reported": actual})
            return estimate
        if charged > estimate:
            window_used = decision.used - estimate + charged
            log.warning("usage exceeded the reservation; overrun charged", extra={
                "event": "ratelimit.overrun", "request_id": rid, "tenant": tenant_id,
                "tokens_estimated": estimate, "tokens_reported": actual, "tokens_charged": charged,
                "tokens_over": charged - estimate, "window_used": window_used,
                "over_limit": window_used > limiter.limit, "capped": charged < wanted})
        return charged

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request, body: ChatRequest, tenant_id: str = Depends(tenant)):
        rid = request_id(request)
        max_tokens = body.max_tokens or settings.default_max_tokens
        estimate = estimate_prompt_tokens(body.messages) + max_tokens
        if estimate > limiter.limit:
            log.info("request exceeds the per-minute budget", extra={
                "event": "ratelimit.too_large", "request_id": rid, "tenant": tenant_id,
                "tokens_requested": estimate, "tokens_limit": limiter.limit})
            raise GatewayError(413, "request_too_large",
                               "This request needs about %d tokens; the per-minute limit is %d." % (estimate, limiter.limit))

        decision = await limiter.acquire(tenant_id, estimate)
        if not decision.allowed:
            log.info("tenant token limit exceeded", extra={
                "event": "ratelimit.rejected", "request_id": rid, "tenant": tenant_id,
                "tokens_used": decision.used, "tokens_requested": estimate, "tokens_limit": limiter.limit,
                "retry_after_s": None if decision.retry_after is None else round(decision.retry_after, 3)})
            raise GatewayError(429, "rate_limit_exceeded", "Tenant token limit exceeded. Please retry later.",
                               retry_after=decision.retry_after)

        payload: Dict[str, Any] = {"messages": [m.model_dump() for m in body.messages], "max_tokens": max_tokens}
        if body.temperature is not None:
            payload["temperature"] = body.temperature
        try:
            result: RouteResult = await router.complete(payload, request_id=rid)
        except BaseException:
            # No completion was delivered (error, or the client disconnected):
            # refund the reservation. Shielded so a cancellation can't skip it.
            await asyncio.shield(limiter.release(decision.reservation_id))
            log.info("reservation refunded; no completion delivered", extra={
                "event": "ratelimit.refunded", "request_id": rid, "tenant": tenant_id, "tokens_refunded": estimate})
            raise

        actual = reported_tokens(result.body)
        charged = await reconcile(rid, tenant_id, decision, estimate, actual)
        log.info("completion served", extra={
            "event": "completion.served", "request_id": rid, "tenant": tenant_id, "provider": result.provider,
            "model": result.body.get("model"), "fallback": result.fell_back, "primary_skipped": result.primary_skipped,
            "attempts": [a.as_log() for a in result.attempts],
            "prompt_messages": len(body.messages), "prompt_chars": sum(len(m.content) for m in body.messages),
            "max_tokens": max_tokens, "tokens_estimated": estimate, "tokens_reported": actual, "tokens_charged": charged,
            "usage_source": "estimate" if actual is None else "provider"})

        remaining = max(0, limiter.limit - (decision.used - estimate + charged))
        return JSONResponse(result.body, headers={
            "X-Gateway-Provider": result.provider,
            "X-Gateway-Fallback": "true" if result.fell_back else "false",
            "X-RateLimit-Limit": str(limiter.limit),
            "X-RateLimit-Remaining": str(remaining),
        })

    return app


configure_logging()
app = create_app()
