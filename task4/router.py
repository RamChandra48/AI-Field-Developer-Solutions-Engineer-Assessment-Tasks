"""Primary/secondary model router with deadline-based failover.

    complete(payload, request_id=None)
      -> primary circuit open?     -> skip straight to the secondary (no 3 s wait)
      -> primary   (hard deadline: 3000 ms, covering connect + headers + body)
           ok                      -> return
           429 / timeout / 5xx /
           connection error /
           malformed body          -> fail over
           other 4xx               -> stop (the secondary would reject it too)
      -> secondary (own deadline)
           ok                      -> return
           anything else           -> standardized GatewayError

Timeout race handling: the deadline is an ``asyncio.timeout`` around the
whole upstream exchange, not httpx's per-phase timeouts (those reset on every
byte, so a provider trickling a body could run far past 3 s). When the
deadline fires, the primary request is *cancelled*: its connection is closed
and a late response cannot be read, double-counted or returned alongside the
secondary's. Only one attempt is in flight at a time.

Circuit breaking (circuit_breaker.py): consecutive primary failures open the
circuit, and while it is open requests skip the primary entirely.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import httpx

from circuit_breaker import CircuitBreaker
from errors import GatewayError, upstream_rejected, upstream_timeout, upstream_unavailable

log = logging.getLogger("llm-router")

PRIMARY_TIMEOUT_SECONDS = 3.0


@dataclass(frozen=True)
class Provider:
    name: str
    url: str
    model: str
    api_key: Optional[str] = None


class UpstreamFailure(Exception):
    """Internal only. Never shown to clients; mapped to a GatewayError."""

    def __init__(self, provider: str, reason: str, retryable: bool, status: Optional[int] = None) -> None:
        super().__init__("%s: %s" % (provider, reason))
        self.provider, self.reason, self.retryable, self.status = provider, reason, retryable, status


@dataclass
class Attempt:
    provider: str
    outcome: str  # "ok" or an UpstreamFailure reason
    elapsed_ms: float
    status: Optional[int] = None  # upstream HTTP status, when there was one

    def as_log(self) -> Dict[str, Any]:
        return {"provider": self.provider, "outcome": self.outcome, "upstream_status": self.status,
                "latency_ms": round(self.elapsed_ms, 1)}


@dataclass
class RouteResult:
    provider: str
    body: Dict[str, Any]
    attempts: List[Attempt] = field(default_factory=list)
    primary_skipped: bool = False  # circuit open: the primary was not called

    @property
    def fell_back(self) -> bool:
        return self.primary_skipped or len(self.attempts) > 1


class ModelRouter:
    def __init__(
        self,
        client: httpx.AsyncClient,
        primary: Provider,
        secondary: Provider,
        primary_timeout: float = PRIMARY_TIMEOUT_SECONDS,
        secondary_timeout: float = 10.0,
        breaker: Optional[CircuitBreaker] = None,
    ) -> None:
        self._client = client
        self.primary, self.secondary = primary, secondary
        self.primary_timeout, self.secondary_timeout = primary_timeout, secondary_timeout
        self.breaker = breaker  # None: plain failover, the primary is always tried first

    async def complete(self, payload: Dict[str, Any], request_id: Optional[str] = None) -> RouteResult:
        """``request_id`` is only used to tag log events so they can be correlated."""
        attempts: List[Attempt] = []
        breaker = self.breaker
        skipped = breaker is not None and not breaker.allow(request_id)
        if skipped:
            log.info("primary circuit open; sending straight to the secondary", extra={
                "event": "upstream.skipped", "request_id": request_id, "provider": self.primary.name,
                "reason": "circuit_open", "fallback_provider": self.secondary.name,
                "retry_primary_in_s": round(breaker.retry_in(), 1)})
        else:
            try:
                body = await self._attempt(self.primary, payload, self.primary_timeout, attempts, request_id)
            except UpstreamFailure as failure:
                if breaker is not None:
                    breaker.record_failure(request_id) if failure.retryable else breaker.record_neutral()
                self._on_primary_failure(failure, attempts[-1], request_id)
            except BaseException:  # cancelled (client went away): says nothing about health
                if breaker is not None:
                    breaker.record_neutral()
                raise
            else:
                if breaker is not None:
                    breaker.record_success(request_id)
                return RouteResult(self.primary.name, body, attempts)

        try:
            body = await self._attempt(self.secondary, payload, self.secondary_timeout, attempts, request_id)
            return RouteResult(self.secondary.name, body, attempts, primary_skipped=skipped)
        except UpstreamFailure as failure:
            log.error("all providers failed", extra={
                "event": "upstream.exhausted", "request_id": request_id, "provider": self.secondary.name,
                "reason": failure.reason, "upstream_status": failure.status, "primary_skipped": skipped,
                "attempts": [a.as_log() for a in attempts]})
            if failure.reason == "timeout":
                raise upstream_timeout() from None
            if not failure.retryable:
                raise upstream_rejected() from None
            raise upstream_unavailable() from None

    def _on_primary_failure(self, failure: UpstreamFailure, last: Attempt, request_id: Optional[str]) -> None:
        """Log the primary's failure; raise if it must not fail over."""
        if not failure.retryable:
            log.warning("primary provider rejected the request; not failing over", extra={
                "event": "upstream.rejected", "request_id": request_id, "provider": self.primary.name,
                "reason": failure.reason, "upstream_status": failure.status,
                "latency_ms": round(last.elapsed_ms, 1)})
            raise upstream_rejected() from None
        log.warning("primary provider failed; failing over", extra={
            "event": "upstream.failover", "request_id": request_id, "provider": self.primary.name,
            "reason": failure.reason, "upstream_status": failure.status,
            "fallback_provider": self.secondary.name, "latency_ms": round(last.elapsed_ms, 1)})

    async def _attempt(self, provider: Provider, payload: Dict[str, Any], deadline: float,
                       attempts: List[Attempt], request_id: Optional[str]) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            body, status = await self._call(provider, payload, deadline, request_id)
        except UpstreamFailure as failure:
            attempts.append(Attempt(provider.name, failure.reason, (time.perf_counter() - start) * 1000, failure.status))
            raise
        attempts.append(Attempt(provider.name, "ok", (time.perf_counter() - start) * 1000, status))
        return body

    async def _call(self, provider: Provider, payload: Dict[str, Any], deadline: float,
                    request_id: Optional[str] = None) -> Tuple[Dict[str, Any], int]:
        request_body = dict(payload, model=provider.model)
        headers = {"Authorization": "Bearer " + provider.api_key} if provider.api_key else {}
        try:
            async with asyncio.timeout(deadline):
                response = await self._client.post(provider.url, json=request_body, headers=headers)
        except (TimeoutError, httpx.TimeoutException):
            raise UpstreamFailure(provider.name, "timeout", retryable=True) from None
        except httpx.HTTPError as exc:
            log.warning("upstream transport error", extra={
                "event": "upstream.transport_error", "request_id": request_id, "provider": provider.name,
                "error_class": type(exc).__name__})
            raise UpstreamFailure(provider.name, "connection_error", retryable=True) from None

        status = response.status_code
        if status == 429:
            raise UpstreamFailure(provider.name, "rate_limited", retryable=True, status=status)
        if status >= 500:
            raise UpstreamFailure(provider.name, "server_error", retryable=True, status=status)
        if not 200 <= status < 300:
            raise UpstreamFailure(provider.name, "client_error", retryable=False, status=status)
        try:
            body = response.json()
        except ValueError:
            body = None
        if not isinstance(body, dict) or not isinstance(body.get("choices"), list):
            raise UpstreamFailure(provider.name, "bad_response", retryable=True, status=status)
        return body, status
