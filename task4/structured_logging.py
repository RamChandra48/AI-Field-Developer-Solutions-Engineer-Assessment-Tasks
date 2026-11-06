"""JSON Lines logging: one JSON object per log record, one record per line.

Every record has ``ts`` (UTC, ISO 8601, ms), ``level``, ``logger`` and
``message``. Records from the gateway and router also carry an ``event`` name
and structured fields passed via ``extra=``, e.g.

    {"ts": "2026-11-04T23:13:57.412Z", "level": "WARNING", "logger": "llm-router",
     "message": "primary provider failed; failing over", "event": "upstream.failover",
     "request_id": "req_0f9d8bfd8c124f8d", "provider": "primary", "reason": "rate_limited",
     "upstream_status": 429, "fallback_provider": "secondary", "latency_ms": 1}

Prompt and completion text are never logged; only their sizes are.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

# Attributes every LogRecord has; anything else on a record came from ``extra=``.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName", "color_message"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and key not in entry:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)  # server log only, never to clients
        return json.dumps(entry, default=str, ensure_ascii=False)


def configure_logging(level: Optional[str] = None) -> None:
    """Send all logs to stdout as JSON Lines (the usual container/log-shipper contract).

    Also switches uvicorn's own loggers to JSON, so the process emits one format.
    Run uvicorn with ``--no-access-log``: the gateway writes its own
    ``http.access`` event per request, with request id, tenant and latency.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level or os.environ.get("LOG_LEVEL", "INFO"))
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        for existing in logging.getLogger(name).handlers:
            existing.setFormatter(JsonFormatter())
    logging.getLogger("httpx").setLevel(logging.WARNING)  # per-request client lines are noise here
