"""JSON Lines audit logging for the gateway (standard library only).

One JSON object per line. Every line has ``ts`` (UTC, ISO 8601, ms), ``level``,
``logger`` and ``message``; gateway events add an ``event`` name, the
``request_id`` of the HTTP request being handled, and their own fields:

    {"ts": "2026-11-05T19:52:10.114Z", "level": "WARNING", "logger": "mcp-gateway",
     "message": "tool call denied", "event": "mcp.deny", "request_id": "req_5b0c...",
     "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "admin_reset_key",
     "required_role": "admin", "rpc_id": 2}

Tokens and request bodies are never logged.

The request id lives in a ContextVar set at the start of each request.
ThreadingHTTPServer handles every request in its own thread, and each thread
has its own context, so concurrent requests never see each other's id, and
no function signature has to carry it.
"""
from __future__ import annotations

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import IO, List, Optional

_request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)

# Attributes every LogRecord has; anything else on a record came from ``extra=``.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


def new_request_id() -> str:
    """Start a request: create its id and make it current for this thread."""
    rid = "req_" + uuid.uuid4().hex[:16]
    _request_id.set(rid)
    return rid


def current_request_id() -> Optional[str]:
    return _request_id.get()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        rid = getattr(record, "request_id", None) or _request_id.get()
        if rid is not None:
            entry["request_id"] = rid
        for key, value in record.__dict__.items():
            if key not in _STANDARD and key not in entry and key != "request_id":
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, ensure_ascii=False)


def configure_logging(log_file: Optional[str] = None, stream: IO[str] = sys.stderr, level: int = logging.INFO) -> None:
    """JSON Lines to ``stream`` and, optionally, appended to ``log_file``."""
    handlers: List[logging.Handler] = [logging.StreamHandler(stream)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))
    for handler in handlers:
        handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = handlers
    root.setLevel(level)
