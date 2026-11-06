"""Circuit breaker for the primary provider.

Without it, every request during a primary outage pays the full failure cost
first (up to the 3000 ms deadline when the primary hangs) before failing over.
With it, after ``failure_threshold`` consecutive failures the gateway stops
calling the primary for ``cooldown_seconds`` and goes straight to the
secondary, so failover adds no wait.

    closed ──N consecutive failures──▶ open ──cooldown elapsed──▶ half_open
      ▲                                  ▲                            │
      │                                  └──────probe fails───────────┤
      └─────────────────────────probe succeeds────────────────────────┘

In ``half_open`` exactly one request (the probe) is sent to the primary; all
others keep using the secondary until the probe settles, so a recovering
primary is not hit by a stampede.

Only failures that say something about the provider's health count (429,
timeouts, 5xx, connection errors, malformed bodies). A 4xx is the caller's
fault and a cancelled request (client went away) proves nothing; both only
release the probe slot.

State is per process. With several gateway replicas each keeps its own, which
is the usual trade-off: no shared store on the hot path, at the cost of each
replica discovering an outage on its own (N failures each).

Everything runs on one event loop and no method awaits, so each call is atomic.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional

log = logging.getLogger("llm-router")

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"


class CircuitBreaker:
    def __init__(
        self,
        provider: str,
        failure_threshold: int = 3,
        cooldown_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be > 0")
        self.provider = provider
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._clock = clock
        self.state = CLOSED
        self.consecutive_failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    # ---------------------------------------------------------------- queries
    def allow(self, request_id: Optional[str] = None) -> bool:
        """May this request call the provider? In half_open, True claims the probe."""
        if self.state == CLOSED:
            return True
        if self.state == OPEN:
            if self._clock() - self._opened_at < self.cooldown_seconds:
                return False
            self._move(HALF_OPEN, request_id)
        if self._probe_in_flight:
            return False
        self._probe_in_flight = True
        return True

    def retry_in(self) -> float:
        """Seconds until the next probe is allowed (0 if not open)."""
        if self.state != OPEN:
            return 0.0
        return max(0.0, self.cooldown_seconds - (self._clock() - self._opened_at))

    # ---------------------------------------------------------------- outcomes
    def record_success(self, request_id: Optional[str] = None) -> None:
        self._probe_in_flight = False
        self.consecutive_failures = 0
        if self.state != CLOSED:
            self._move(CLOSED, request_id)

    def record_failure(self, request_id: Optional[str] = None) -> None:
        self._probe_in_flight = False
        self.consecutive_failures += 1
        if self.state == HALF_OPEN or (self.state == CLOSED and self.consecutive_failures >= self.failure_threshold):
            self._opened_at = self._clock()
            self._move(OPEN, request_id)

    def record_neutral(self) -> None:
        """The attempt ended without telling us anything about the provider's health."""
        self._probe_in_flight = False

    def reset(self) -> None:
        self.state, self.consecutive_failures, self._probe_in_flight = CLOSED, 0, False

    # ---------------------------------------------------------------- internal
    def _move(self, state: str, request_id: Optional[str]) -> None:
        previous, self.state = self.state, state
        extra = {"event": "circuit." + state, "request_id": request_id, "provider": self.provider,
                 "previous_state": previous, "consecutive_failures": self.consecutive_failures}
        if state == OPEN:
            extra["cooldown_s"] = self.cooldown_seconds
            log.warning("circuit opened; skipping provider until cooldown ends", extra=extra)
        elif state == HALF_OPEN:
            log.info("circuit half-open; next request probes the provider", extra=extra)
        else:
            log.info("circuit closed; provider recovered", extra=extra)
