"""The gateway's single, standardized error format.

Every error the gateway returns, whether its own or an upstream failure, has
this shape, and nothing from an upstream body, exception or stack trace is
ever copied into it:

    {"error": {"type": "rate_limit_exceeded",
               "message": "Tenant token limit exceeded. Retry after 12 seconds.",
               "request_id": "req_4f1c9a0b2d3e4f56",
               "retry_after_seconds": 12}}       # only on 429
"""
from __future__ import annotations

import math
from typing import Any, Dict, Optional


class GatewayError(Exception):
    def __init__(self, status: int, error_type: str, message: str, retry_after: Optional[float] = None) -> None:
        super().__init__(message)
        self.status = status
        self.error_type = error_type
        self.message = message
        self.retry_after = None if retry_after is None else max(1, math.ceil(retry_after))


def error_body(error_type: str, message: str, request_id: str, retry_after: Optional[int] = None) -> Dict[str, Any]:
    error: Dict[str, Any] = {"type": error_type, "message": message, "request_id": request_id}
    if retry_after is not None:
        error["retry_after_seconds"] = retry_after
    return {"error": error}


# Fixed client-facing messages for upstream failures. These are factories, not
# shared instances: raising one exception object from concurrent requests
# would make them share (and grow) a single __traceback__.
def upstream_timeout() -> GatewayError:
    return GatewayError(504, "upstream_timeout", "The model provider did not respond in time. Please retry.")


def upstream_unavailable() -> GatewayError:
    return GatewayError(503, "upstream_unavailable", "All model providers are temporarily unavailable. Please retry shortly.")


def upstream_rejected() -> GatewayError:
    return GatewayError(502, "upstream_error", "The model provider could not process this request.")
