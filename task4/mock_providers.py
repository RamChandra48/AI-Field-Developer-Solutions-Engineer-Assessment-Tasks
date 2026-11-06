"""Two mock model providers in one app, with switchable failure modes.

    uvicorn mock_providers:app --port 8301

    POST /primary/v1/chat/completions
    POST /secondary/v1/chat/completions
    POST /control/{provider}   {"mode": "ok" | "429" | "slow" | "500" | "garbage", "delay_ms": 5000}
    GET  /control

The failure responses deliberately contain internal details (an org id, a
stack trace, an internal IP) so you can check that the gateway never relays
them.
"""
from __future__ import annotations

import asyncio
import math
import os
import time
import uuid
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse

MODES = {"ok", "429", "slow", "500", "garbage"}

app = FastAPI(title="Mock model providers")
app.state.config = {
    "primary": {"mode": os.environ.get("MOCK_PRIMARY_MODE", "ok"), "delay_ms": 5000},
    "secondary": {"mode": os.environ.get("MOCK_SECONDARY_MODE", "ok"), "delay_ms": 5000},
}
app.state.calls = {"primary": 0, "secondary": 0}


def _config(provider: str) -> Dict[str, Any]:
    if provider not in app.state.config:
        raise HTTPException(404, "unknown provider")
    return app.state.config[provider]


@app.post("/{provider}/v1/chat/completions")
async def completions(provider: str, body: Dict[str, Any]):
    config = _config(provider)
    app.state.calls[provider] += 1
    mode = config["mode"]
    if mode == "slow":
        await asyncio.sleep(config["delay_ms"] / 1000)
    elif mode == "429":
        return JSONResponse({"error": {"message": "Rate limit reached for org-8f2c91 (internal) on tokens per min: Limit 10000, Used 10000"}},
                            status_code=429, headers={"Retry-After": "20"})
    elif mode == "500":
        return PlainTextResponse(
            'Traceback (most recent call last):\n  File "/srv/inference/worker.py", line 212, in generate\n'
            "psycopg2.OperationalError: could not connect to server at 10.0.3.17:5432\n", status_code=500)
    elif mode == "garbage":
        return PlainTextResponse("<html><body>upstream proxy error</body></html>", status_code=200)

    messages = body.get("messages") or []
    prompt = " ".join(str(m.get("content", "")) for m in messages if isinstance(m, dict))
    reply = "[%s:%s] You said: %s" % (provider, body.get("model"), prompt[:200])
    prompt_tokens = 3 + sum(4 + math.ceil(len(str(m.get("content", ""))) / 4) for m in messages if isinstance(m, dict))
    completion_tokens = max(1, math.ceil(len(reply) / 4))
    return {
        "id": "cmpl-" + uuid.uuid4().hex[:12],
        "object": "chat.completion",
        "created": int(time.time()),
        "model": body.get("model"),
        "choices": [{"index": 0, "message": {"role": "assistant", "content": reply}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                  "total_tokens": prompt_tokens + completion_tokens},
    }


@app.post("/control/{provider}")
async def control(provider: str, body: Dict[str, Any]):
    config = _config(provider)
    if body.get("mode") not in MODES:
        raise HTTPException(400, "mode must be one of %s" % sorted(MODES))
    config["mode"] = body["mode"]
    if "delay_ms" in body:
        config["delay_ms"] = int(body["delay_ms"])
    return config


@app.get("/control")
async def show_control():
    return {"config": app.state.config, "calls": app.state.calls}
