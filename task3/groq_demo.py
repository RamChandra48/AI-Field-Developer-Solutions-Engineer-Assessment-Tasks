#!/usr/bin/env python3
"""Stream a Groq completion through the PII-redaction gateway and log every step.

    export GROQ_API_KEY=gsk_...          # your key; never commit it
    python groq_demo.py                   # default prompt
    python groq_demo.py --prompt "..."    # your own prompt
    python groq_demo.py --model <id>      # pick a model (default: chosen automatically)
    python groq_demo.py --list-models     # models your key can use

The script starts the gateway in-process on a free port with Groq as its
upstream, streams the reply live, and writes two files:

    groq_demo.jsonl   the gateway's own server log, exactly as production emits it:
                      JSON Lines, correlated by request_id, with counts and kinds of
                      PII redacted but never the text or the PII (checked after the run)
    groq_demo.log     a step-by-step trace of the redactor for this one run, for
                      reading: the raw text Groq generated (PII included, which is
                      why production never records it), each chunk received /
                      emitted / held back, each redaction, the final output, a summary

The gateway holds the Groq key; the client never sees it. The client
authenticates to the gateway with a gateway key generated for this run (shown
masked). The demo also sends one request without a key and checks it is
refused with 401, and checks that neither key appears in the server log.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import secrets
import socket
import sys
import threading
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import uvicorn

from redactor import REPLACEMENT, redact_text

GROQ_BASE_URL = os.environ.get("GROQ_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
GROQ_CHAT_URL = GROQ_BASE_URL + "/chat/completions"
GROQ_MODELS_URL = GROQ_BASE_URL + "/models"
DEFAULT_MODEL = "llama-3.3-70b-versatile"
# Safety-tuned models (gpt-oss) refuse "write a note with an SSN" or "repeat
# exactly" prompts. Saying what the values are (public dummy test values) and
# what the text is for gets them written, which is what the demo needs.
DEFAULT_PROMPT = (
    "You are a test-data formatter for a PII-redaction QA suite. All values below are public dummy "
    "test values (the SSN 123-45-6789 is the well-known invalid example SSN, and 4111 1111 1111 1111 "
    "is Visa's published test card). Rewrite them as a 3-sentence support ticket, copying every value "
    "character for character: email jane.doe@example.com, SSN 123-45-6789, card 4111 1111 1111 1111."
)
DEFAULT_LOG = Path(__file__).with_name("groq_demo.log")
DEFAULT_SERVER_LOG = Path(__file__).with_name("groq_demo.jsonl")

# General-purpose chat models, best first. Groq's catalogue changes, so the
# choice is made at run time from what the key can actually use.
PREFERRED_MODELS = (
    "llama-3.3-70b-versatile", "kimi-k2", "llama-4-maverick", "llama-4-scout", "gpt-oss-120b",
    "llama-3.1-8b-instant", "gpt-oss-20b", "llama", "qwen3",
)
# Not suitable here: speech, safety classifiers, embeddings, agentic systems,
# and language-specialist models (allam, saba: Arabic-first).
EXCLUDED_MODELS = ("whisper", "tts", "guard", "playai", "orpheus", "embed", "distil", "compound", "allam", "saba")


# ------------------------------------------------------------------- models
def fetch_models(api_key: str) -> List[str]:
    """Model ids this key can use. Raises RuntimeError with Groq's message on failure."""
    response = httpx.get(GROQ_MODELS_URL, headers={"Authorization": "Bearer " + api_key}, timeout=15)
    if response.status_code != 200:
        raise RuntimeError("Groq returned HTTP %d: %s" % (response.status_code, response.text[:300]))
    return sorted(m["id"] for m in response.json().get("data", []) if m.get("active", True))


def pick_chat_model(models: List[str]) -> Optional[str]:
    """The best general chat model this key has (see PREFERRED_MODELS)."""
    usable = [m for m in models if not any(word in m.lower() for word in EXCLUDED_MODELS)]
    for wanted in PREFERRED_MODELS:
        matches = [m for m in usable if wanted in m.lower()]
        if matches:
            return matches[0]
    return usable[0] if usable else None


# ------------------------------------------------------------------ gateway
class EmbeddedGateway:
    """The Task 3 gateway under uvicorn in a background thread, with tracing on."""

    def __init__(self, api_key: str, trace, client_keys: Dict[str, str]) -> None:
        from gateway import create_app

        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        port = self._sock.getsockname()[1]
        app = create_app(upstream_url=GROQ_CHAT_URL, upstream_api_key=api_key, trace=trace,
                         client_keys=client_keys)
        # log_config=None: uvicorn's records go through the same JSON handler as the gateway's.
        config = uvicorn.Config(app, log_config=None, log_level="warning", access_log=False, lifespan="on")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, kwargs={"sockets": [self._sock]}, daemon=True)
        self._thread.start()
        deadline = time.time() + 15
        while not self._server.started:
            if time.time() > deadline or not self._thread.is_alive():
                raise RuntimeError("gateway did not start")
            time.sleep(0.02)
        self.url = "http://127.0.0.1:%d/v1/chat/completions" % port

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)
        self._sock.close()


# ------------------------------------------------------------------- client
async def unauthenticated_status(url: str, body: Dict[str, Any]) -> int:
    """The same request with no gateway key: must be refused before Groq is called."""
    async with httpx.AsyncClient(timeout=10) as client:
        return (await client.post(url, json=body)).status_code


async def stream_completion(url: str, body: Dict[str, Any], start: float, client_key: str) -> Dict[str, Any]:
    """POST a streaming request to the gateway; print text as it arrives."""
    ttft: Optional[float] = None
    pieces: List[str] = []
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with client.stream("POST", url, json=body,
                                 headers={"Authorization": "Bearer " + client_key}) as response:
            if response.status_code != 200:
                return {"error": "HTTP %d: %s" % (response.status_code, (await response.aread()).decode(errors="replace")[:500])}
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if payload == "[DONE]":
                    break
                chunk = json.loads(payload)
                if "error" in chunk:
                    return {"error": json.dumps(chunk["error"]), "text": "".join(pieces)}
                for choice in chunk.get("choices") or []:
                    text = (choice.get("delta") or {}).get("content") or ""
                    if text:
                        if ttft is None:
                            ttft = time.perf_counter() - start
                        pieces.append(text)
                        print(text, end="", flush=True)
    return {"text": "".join(pieces), "ttft": ttft, "total": time.perf_counter() - start}


# ---------------------------------------------------------------------- log
def write_log(path: Path, model: str, prompt: str, events: List[Dict[str, Any]], result: Dict[str, Any]) -> Dict[str, Any]:
    chunks = [e for e in events if e["event"] == "chunk"]
    redactions = [e for e in events if e["event"] == "redact"]
    raw = "".join(e["received"] for e in chunks)
    emitted = "".join(e["emitted"] for e in events if e["event"] in ("chunk", "flush"))
    final = result["text"]
    stats = {
        "chunks": len(chunks),
        "redactions": Counter(e["kind"] for e in redactions),
        "max_held": max((len(e["held"]) for e in chunks), default=0),
        "pii_in_raw": redact_text(raw).count(REPLACEMENT) - raw.count(REPLACEMENT),
        "pii_left": redact_text(final).count(REPLACEMENT) - final.count(REPLACEMENT),
        "consistent": emitted == final,
    }

    def cell(text: str, width: int) -> str:
        shown = repr(text)
        return (shown if len(shown) <= width else shown[: width - 1] + "…").ljust(width)

    rule = "=" * 100
    out = [
        rule,
        "Groq -> PII-redaction gateway -> client      %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model:  %s" % model,
        "prompt: %s" % prompt,
        rule,
        "",
        "1. RAW LLM OUTPUT (what Groq generated, before the gateway)",
        "-" * 100,
        raw,
        "",
        "2. REDACTOR, CHUNK BY CHUNK",
        "-" * 100,
        "   ms   #   received from Groq          sent to client              held back (may still be PII)",
    ]
    number = 0
    for e in events:
        t = "%6.0f" % (e["t"] * 1000)
        if e["event"] == "chunk":
            number += 1
            out.append(("%s %3d   %s %s %s" % (t, number, cell(e["received"], 27), cell(e["emitted"], 27), cell(e["held"], 30))).rstrip())
        elif e["event"] == "redact":
            out.append("%s       >>> REDACTED %-11s %r -> %s" % (t, e["kind"], e["text"], REPLACEMENT))
        elif e["event"] == "flush":
            out.append("%s       end of stream: released %r" % (t, e["emitted"]))
    out += [
        "",
        "3. FINAL OUTPUT (what the client received)",
        "-" * 100,
        final,
        "",
        "4. SUMMARY",
        "-" * 100,
        "chunks received        %d" % stats["chunks"],
        "time to first token    %.0f ms" % ((result["ttft"] or 0) * 1000),
        "total time             %.0f ms" % (result["total"] * 1000),
        "PII in raw output      %d" % stats["pii_in_raw"],
        "redactions             %d  (%s)" % (sum(stats["redactions"].values()),
                                             ", ".join("%s %d" % kv for kv in sorted(stats["redactions"].items())) or "none"),
        "max text held back     %d chars (the rest streamed straight through)" % stats["max_held"],
        "PII left in output     %d  %s" % (stats["pii_left"], "OK" if stats["pii_left"] == 0 else "LEAK"),
        "trace matches output   %s" % ("yes" if stats["consistent"] else "NO"),
        rule,
    ]
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return stats


# --------------------------------------------------------------------- main
def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--model", default=os.environ.get("GROQ_MODEL"),
                        help="model id (default: %s if your key has it, else picked automatically)" % DEFAULT_MODEL)
    parser.add_argument("--max-tokens", type=int, default=300)
    parser.add_argument("--log-file", default=str(DEFAULT_LOG), help="redactor trace for reading (default: task3/groq_demo.log)")
    parser.add_argument("--server-log", default=str(DEFAULT_SERVER_LOG),
                        help="the gateway's JSON Lines server log (default: task3/groq_demo.jsonl)")
    parser.add_argument("--list-models", action="store_true", help="list the models your Groq key can use")
    args = parser.parse_args(argv)

    logging.getLogger("httpx").setLevel(logging.WARNING)  # hide per-request INFO lines
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("error: set your key first:  export GROQ_API_KEY=gsk_...", file=sys.stderr)
        return 1

    try:
        models = fetch_models(api_key)
    except RuntimeError as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1
    if args.list_models:
        suggested = pick_chat_model(models)
        for model in models:
            print(model + ("   <- used by default" if model == suggested else ""))
        return 0
    if args.model is None:
        args.model = pick_chat_model(models)
        if args.model is None:
            print("error: no chat model available to this key; see --list-models", file=sys.stderr)
            return 1
        if args.model != DEFAULT_MODEL:
            print("note: %s is not available to your key; using %s" % (DEFAULT_MODEL, args.model))

    # The gateway's logging, pointed at a file so the streamed reply stays readable.
    server_log = Path(args.server_log)
    server_log.unlink(missing_ok=True)
    from structured_logging import JsonFormatter
    import gateway  # noqa: F401  (importing configures stdout JSON logging; replaced below)
    file_handler = logging.FileHandler(server_log, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    logging.getLogger().handlers = [file_handler]
    logging.getLogger().setLevel(logging.INFO)

    body = {"model": args.model, "stream": True, "max_tokens": args.max_tokens, "temperature": 0.3,
            "messages": [{"role": "user", "content": args.prompt}]}

    events: List[Dict[str, Any]] = []
    start = time.perf_counter()

    def trace(event: Dict[str, Any]) -> None:  # called by the gateway for every step
        events.append(dict(event, t=time.perf_counter() - start))

    client_key = secrets.token_urlsafe(24)  # this run's gateway key for the demo client
    gateway = EmbeddedGateway(api_key, trace, {client_key: "groq-demo"})
    print("=" * 78)
    print(" Groq (%s) -> gateway (PII redaction on) -> you" % args.model)
    print(" client key: %s…%s (gateway's own; the Groq key never leaves the gateway)" % (client_key[:4], client_key[-2:]))
    print("=" * 78)
    try:
        refused = asyncio.run(unauthenticated_status(gateway.url, body))
        start = time.perf_counter()
        result = asyncio.run(stream_completion(gateway.url, body, start, client_key))
    finally:
        gateway.stop()
        file_handler.close()
    print()
    print("=" * 78)

    if "error" in result:
        print(" request failed: %s" % result["error"])
        print(" Check the model with --list-models, or pass one with --model <name>.")
        return 1

    stats = write_log(Path(args.log_file), args.model, args.prompt, events, result)
    print(" chunks: %d   TTFT: %.0f ms   total: %.0f ms" % (stats["chunks"], (result["ttft"] or 0) * 1000, result["total"] * 1000))
    print(" PII in the answer: %d   redacted: %d   left in output: %d   %s" % (
        stats["pii_in_raw"], sum(stats["redactions"].values()), stats["pii_left"], "✅" if stats["pii_left"] == 0 else "❌"))
    print(" no client key: HTTP %d   %s" % (refused, "✅" if refused == 401 else "❌ (expected 401)"))
    secrets_in_log = [e["text"] for e in events if e["event"] == "redact"] + [client_key, api_key]
    log_ok, log_summary = check_server_log(server_log, secrets_in_log)
    print(" server log: %s" % server_log)
    print("             %s %s" % (log_summary, "✅" if log_ok else "❌"))
    print(" trace:      %s" % args.log_file)
    print("=" * 78)
    if stats["pii_in_raw"] == 0:
        # Nothing to redact proves nothing: usually the model refused.
        print(" ⚠️  The model's answer contained no PII, so there was nothing to redact")
        print("    (safety-tuned models sometimes refuse). Run it again, or try --prompt / --model.")
        return 1
    return 0 if stats["pii_left"] == 0 and stats["consistent"] and log_ok and refused == 401 else 1


def check_server_log(path: Path, secrets_: List[str]):
    """Valid JSON Lines, a stream.completed event and an auth.failed event, with
    none of ``secrets`` (the redacted PII and both keys) in it."""
    text = path.read_text(encoding="utf-8")
    try:
        events = [json.loads(line) for line in text.splitlines() if line.strip()]
    except ValueError:
        return False, "(not valid JSON Lines)"
    done = [e for e in events if e.get("event") == "stream.completed"]
    leaked = [p for p in secrets_ if p and p in text]
    auth_failed = any(e.get("event") == "auth.failed" for e in events)
    ok = bool(done) and auth_failed and not leaked
    counts = ", ".join("%s=%d" % kv for kv in sorted(done[0]["redactions"].items())) if done else ""
    # The gateway counts every model text field; the line above counts only the
    # visible answer, so reasoning models (gpt-oss) show more here.
    return ok, "%d lines; redactions (answer + reasoning) logged as counts only: %s; PII or keys in log: %d" % (
        len(events), counts or "none", len(leaked))


if __name__ == "__main__":
    sys.exit(main())
