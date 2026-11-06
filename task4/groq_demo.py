#!/usr/bin/env python3
"""Rate limiting and failover with the mock as the PRIMARY and Groq as the SECONDARY.

    export GROQ_API_KEY=gsk_...          # your key; never commit it
    python groq_demo.py                   # run all scenarios
    python groq_demo.py --model <id>      # pick the Groq model (default: chosen automatically)
    python groq_demo.py --list-models     # models your key can use

The script starts the mock provider and the Task 4 gateway in-process, each
on a free port over real HTTP. The gateway is configured with:

    primary   = mock provider   (its failure mode is switched before each scenario)
    secondary = Groq            (a real LLM; only the gateway holds the key)

Each scenario sends its own request(s) as a tenant API-key client:

    1. primary healthy         -> answered by the mock, Groq is not called
    2. primary returns 429     -> failover to Groq; the mock's org id is not leaked
    3. primary hangs (5 s)     -> cut off at the 3000 ms deadline, then Groq answers
    4. primary returns 500     -> failover to Groq; the mock's stack trace is not leaked
    5. primary 500, Groq 401   -> standardized 502; nothing from either upstream
                                  leaks; the tenant is refunded (charged 0 tokens)
    6. tenant burst            -> 3 concurrent 20k-token requests against a 50k/min
                                  limit: 2 admitted, 1 rejected with 429 + Retry-After;
                                  reservations then shrink to real usage
    7. circuit breaker         -> primary keeps hanging: 2 requests wait out the
                                  deadline, the circuit opens, the next request skips
                                  the primary and Groq answers with no 3 s wait; after
                                  the cooldown one probe finds the primary healthy and
                                  the circuit closes (demo: threshold 2, cooldown 3 s;
                                  production defaults: 3 and 30 s)

The breaker is reset before every scenario, so scenarios 1-6 each start with
a closed circuit.

Outputs:

    console            one line per scenario with its checks, request ids and Groq's reply
    groq_demo.jsonl    the gateway's own server log, exactly as production emits it:
                       JSON Lines, one event per line, correlated by request_id
                       (no prompt or completion text, no API keys)
    groq_demo.db       the rate limiter's on-disk SQLite file, recreated each run

    jq -c 'select(.request_id=="req_...")' groq_demo.jsonl     # one request's events
    jq -c 'select(.event=="upstream.failover")' groq_demo.jsonl
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import uvicorn

HERE = Path(__file__).resolve().parent
GROQ_BASE_URL = os.environ.get("GROQ_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
GROQ_CHAT_URL = GROQ_BASE_URL + "/chat/completions"
GROQ_MODELS_URL = GROQ_BASE_URL + "/models"
DEFAULT_LOG = HERE / "groq_demo.jsonl"
DEFAULT_DB = HERE / "groq_demo.db"

# General-purpose chat models, best first. Groq's catalogue changes, so the
# choice is made at run time from what the key can actually use.
PREFERRED_MODELS = (
    "llama-3.3-70b-versatile", "kimi-k2", "llama-4-maverick", "llama-4-scout", "gpt-oss-120b",
    "llama-3.1-8b-instant", "gpt-oss-20b", "llama", "qwen3",
)
# Not suitable here: speech, safety classifiers, embeddings, agentic systems,
# and language-specialist models (allam, saba: Arabic-first).
EXCLUDED_MODELS = ("whisper", "tts", "guard", "playai", "orpheus", "embed", "distil", "compound", "allam", "saba")

# Sent with every request that may reach Groq, so the answer language and
# length do not depend on which model is picked.
SYSTEM_PROMPT = "You are a concise technical assistant. Always answer in English, in one sentence."
GROQ_MAX_TOKENS = 400  # room for reasoning models, which spend tokens before answering
CIRCUIT_THRESHOLD, CIRCUIT_COOLDOWN_S = 2, 3.0  # small, so scenario 7 runs in seconds

TENANT_A = {"Authorization": "Bearer tenant-a-key"}
TENANT_B = {"Authorization": "Bearer tenant-b-key"}
# Strings that exist only inside the upstreams' failure responses, or name internals.
MOCK_LEAKS = ("org-8f2c91", "Traceback", "psycopg2", "10.0.3.17", "worker.py")
GROQ_LEAKS = ("invalid_api_key", "Invalid API Key", "groq", "gsk_")
INTERNAL_NAMES = ("primary", "secondary", "mock", "llama")


# ------------------------------------------------------------------- models
def fetch_models(api_key: str) -> List[str]:
    response = httpx.get(GROQ_MODELS_URL, headers={"Authorization": "Bearer " + api_key}, timeout=15)
    if response.status_code != 200:
        raise RuntimeError("Groq returned HTTP %d: %s" % (response.status_code, response.text[:300]))
    return sorted(m["id"] for m in response.json().get("data", []) if m.get("active", True))


def pick_chat_model(models: List[str]) -> Optional[str]:
    usable = [m for m in models if not any(word in m.lower() for word in EXCLUDED_MODELS)]
    for wanted in PREFERRED_MODELS:
        matches = [m for m in usable if wanted in m.lower()]
        if matches:
            return matches[0]
    return usable[0] if usable else None


# ------------------------------------------------------------------ servers
class EmbeddedServer:
    """An ASGI app under uvicorn in a background thread, on a free local port.

    ``log_config=None`` leaves logging to us, so uvicorn's own records go
    through the same JSON handler as the gateway's.
    """

    def __init__(self, app) -> None:
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self.base = "http://127.0.0.1:%d" % self._sock.getsockname()[1]
        config = uvicorn.Config(app, log_config=None, log_level="warning", access_log=False, lifespan="on")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, kwargs={"sockets": [self._sock]}, daemon=True)
        self._thread.start()
        deadline = time.time() + 15
        while not self._server.started:
            if time.time() > deadline or not self._thread.is_alive():
                raise RuntimeError("server did not start")
            time.sleep(0.02)

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)
        self._sock.close()


class EventCapture(logging.Handler):
    """Keeps the gateway's log records in memory so the checks can inspect them.

    The gateway logs before it sends the response, so a request's events are
    already here when the client receives its reply.
    """

    def __init__(self) -> None:
        super().__init__()
        self._records: List[logging.LogRecord] = []
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        with self._lock:
            self._records.append(record)

    def take(self, request_id: Optional[str]) -> List[logging.LogRecord]:
        with self._lock:
            mine = [r for r in self._records if request_id and getattr(r, "request_id", None) == request_id]
            self._records = [r for r in self._records if r not in mine]
        return mine

    def clear(self) -> None:
        with self._lock:
            self._records = []


# ---------------------------------------------------------------- scenarios
@dataclass
class Exchange:
    """One client request through the gateway: what the client saw, and the server's events for it."""
    label: str
    status: int
    headers: Dict[str, str]
    body: Any
    raw: str
    elapsed_ms: float
    events: List[str] = field(default_factory=list)
    attempts: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def request_id(self) -> str:
        return self.headers.get("x-request-id", "?")

    @property
    def provider(self) -> Optional[str]:
        return self.headers.get("x-gateway-provider")


@dataclass
class Scenario:
    number: int
    title: str
    exchanges: List[Exchange] = field(default_factory=list)
    checks: List[Tuple[str, bool]] = field(default_factory=list)

    def check(self, name: str, ok: Any) -> None:
        self.checks.append((name, bool(ok)))

    @property
    def passed(self) -> bool:
        return all(ok for _, ok in self.checks)


class Demo:
    def __init__(self, groq_key: str, model: str, db_path: Path, capture: EventCapture) -> None:
        import mock_providers
        from gateway import Settings, create_app

        self.model = model
        self.capture = capture
        self.mock = EmbeddedServer(mock_providers.app)
        self.settings = Settings(
            primary_url=self.mock.base + "/primary/v1/chat/completions",
            primary_model="mock-primary-model",
            secondary_url=GROQ_CHAT_URL,
            secondary_model=model,
            secondary_api_key=groq_key,
            db_path=str(db_path),
            circuit_failure_threshold=CIRCUIT_THRESHOLD,
            circuit_cooldown_seconds=CIRCUIT_COOLDOWN_S,
        )
        self.gateway_app = create_app(self.settings)
        self.gateway = EmbeddedServer(self.gateway_app)
        self.url = self.gateway.base + "/v1/chat/completions"
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(60.0))
        self.prompts: List[str] = []  # every prompt sent, to verify none of them reach the log

    async def close(self) -> None:
        await self.client.aclose()
        self.gateway.stop()
        self.mock.stop()

    # -- helpers
    async def set_primary(self, mode: str, delay_ms: int = 5000) -> None:
        r = await self.client.post(self.mock.base + "/control/primary", json={"mode": mode, "delay_ms": delay_ms})
        r.raise_for_status()

    async def primary_calls(self) -> int:
        return (await self.client.get(self.mock.base + "/control")).json()["calls"]["primary"]

    async def circuit(self) -> str:
        return (await self.client.get(self.gateway.base + "/healthz")).json()["primary_circuit"]

    async def used(self, headers: Dict[str, str]) -> int:
        r = await self.client.get(self.gateway.base + "/v1/rate-limit", headers=headers)
        return r.json()["used"]

    async def send(self, label: str, headers: Dict[str, str], prompt: str, max_tokens: int,
                   system: Optional[str] = None) -> Exchange:
        self.prompts.append(prompt)
        messages = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": prompt}]
        start = time.perf_counter()
        r = await self.client.post(self.url, headers=headers,
                                   json={"messages": messages, "max_tokens": max_tokens, "temperature": 0.3})
        elapsed = (time.perf_counter() - start) * 1000
        try:
            parsed = r.json()
        except ValueError:
            parsed = None
        keep = ("x-request-id", "x-gateway-provider", "x-gateway-fallback", "retry-after")
        x = Exchange(label, r.status_code, {k: r.headers[k] for k in keep if k in r.headers}, parsed, r.text, elapsed)
        for record in self.capture.take(x.request_id):
            x.events.append(getattr(record, "event", record.getMessage()))
            if getattr(record, "attempts", None) is not None:
                x.attempts = list(record.attempts)
        return x

    async def ask_groq(self, prompt: str) -> Exchange:
        return await self.send("tenant-a", TENANT_A, prompt, GROQ_MAX_TOKENS, system=SYSTEM_PROMPT)

    # -- scenarios
    async def healthy_primary(self) -> Scenario:
        s = Scenario(1, "Primary healthy (Groq not called)")
        await self.set_primary("ok")
        before = await self.used(TENANT_A)
        x = await self.send("tenant-a", TENANT_A, "Say hello in five words.", 100)
        after = await self.used(TENANT_A)
        s.exchanges.append(x)
        s.check("HTTP 200 from the primary, no failover",
                x.status == 200 and x.provider == "primary" and x.headers.get("x-gateway-fallback") == "false")
        s.check("only the primary was attempted", [a["provider"] for a in x.attempts] == ["primary"])
        s.check("charged the provider's reported usage", after - before == _total_tokens(x.body))
        s.check("logged completion.served + http.access", x.events == ["completion.served", "http.access"])
        return s

    async def failover(self, number: int, title: str, mode: str, prompt: str, failover_reason: str) -> Scenario:
        s = Scenario(number, title)
        await self.set_primary(mode, delay_ms=5000)
        before = await self.used(TENANT_A)
        x = await self.ask_groq(prompt)
        after = await self.used(TENANT_A)
        s.exchanges.append(x)
        reply = _content(x.body)
        s.check("HTTP 200 from the secondary (Groq) with X-Gateway-Fallback: true",
                x.status == 200 and x.provider == "secondary" and x.headers.get("x-gateway-fallback") == "true")
        s.check("attempts: primary %s, then secondary ok" % failover_reason,
                [(a["provider"], a["outcome"]) for a in x.attempts] == [("primary", failover_reason), ("secondary", "ok")])
        s.check("reply is a real Groq completion in English",
                reply and "You said:" not in reply and _mostly_english(reply))
        s.check("charged Groq's reported usage", after - before == _total_tokens(x.body))
        s.check("no upstream detail in the response", not any(l in x.raw for l in MOCK_LEAKS))
        s.check("logged upstream.failover + completion.served + http.access",
                x.events == ["upstream.failover", "completion.served", "http.access"])
        if mode == "slow" and x.attempts:
            ms = x.attempts[0]["latency_ms"]
            s.check("primary cut off at the 3000 ms deadline (%.0f ms)" % ms, 2900 <= ms <= 3400)
        return s

    async def all_fail(self) -> Scenario:
        s = Scenario(5, "Primary 500 and Groq rejects (revoked key)")
        await self.set_primary("500")
        # For this one request the secondary uses a key Groq will reject, so
        # Groq itself answers 401. Your real key is untouched.
        from router import Provider
        router = self.gateway_app.state.router
        real = router.secondary
        router.secondary = Provider(real.name, real.url, real.model, "gsk_revoked_demo_key")
        try:
            before = await self.used(TENANT_A)
            x = await self.ask_groq("Hello?")
            after = await self.used(TENANT_A)
        finally:
            router.secondary = real
        s.exchanges.append(x)
        error = x.body.get("error", {}) if isinstance(x.body, dict) else {}
        s.check("HTTP 502 with the standardized body (type/message/request_id only)",
                x.status == 502 and error.get("type") == "upstream_error"
                and set(error) == {"type", "message", "request_id"} and error.get("request_id") == x.request_id)
        s.check("both providers were attempted", [a["provider"] for a in x.attempts] == ["primary", "secondary"])
        leaks = MOCK_LEAKS + GROQ_LEAKS + INTERNAL_NAMES
        s.check("nothing leaked from either upstream", not any(l.lower() in x.raw.lower() for l in leaks))
        s.check("tenant refunded (charged 0)", after == before)
        s.check("logged upstream.failover + upstream.exhausted + ratelimit.refunded + http.access",
                x.events == ["upstream.failover", "upstream.exhausted", "ratelimit.refunded", "http.access"])
        return s

    async def burst(self) -> Scenario:
        s = Scenario(6, "Tenant burst against the 50k tokens/min limit")
        await self.set_primary("slow", delay_ms=1500)  # in flight long enough to overlap, under the deadline
        tasks = [asyncio.create_task(self.send("tenant-b #%d" % (i + 1), TENANT_B, "Burst request %d" % (i + 1), 20_000))
                 for i in range(3)]
        await asyncio.sleep(0.6)  # all three admitted or rejected; two still in flight
        during = await self.used(TENANT_B)
        exchanges = list(await asyncio.gather(*tasks))
        after = await self.used(TENANT_B)
        await self.set_primary("ok")
        follow = await self.send("tenant-b #4", TENANT_B, "One more, please.", 20_000)
        other = await self.send("tenant-a", TENANT_A, "Unaffected?", 100)
        s.exchanges += exchanges + [follow, other]

        ok = [x for x in exchanges if x.status == 200]
        limited = [x for x in exchanges if x.status == 429]
        error = limited[0].body.get("error", {}) if limited and isinstance(limited[0].body, dict) else {}
        s.check("2 of 3 admitted, 1 rejected with 429", len(ok) == 2 and len(limited) == 1)
        s.check("429 is standardized, Retry-After header = retry_after_seconds",
                error.get("type") == "rate_limit_exceeded"
                and limited[0].headers.get("retry-after") == str(error.get("retry_after_seconds")))
        s.check("rejected without calling a provider", limited and not limited[0].attempts)
        s.check("logged ratelimit.rejected for the 429", limited and limited[0].events == ["ratelimit.rejected", "http.access"])
        s.check("in-flight reservations counted (%d > 40000)" % during, during > 40_000)
        s.check("reservations shrink to real usage after completion (%d < 1000)" % after, after < 1000)
        s.check("next request admitted; other tenant unaffected", follow.status == 200 and other.status == 200)
        return s


    async def circuit_breaker(self) -> Scenario:
        s = Scenario(7, "Primary keeps hanging -> circuit opens -> Groq with no wait -> recovery")
        await self.set_primary("slow", delay_ms=5000)
        tripping = [await self.ask_groq("In one sentence, what is a circuit breaker in software?")
                    for _ in range(CIRCUIT_THRESHOLD)]
        state_open = await self.circuit()
        calls_before = await self.primary_calls()
        skipped = await self.ask_groq("In one sentence, why skip a failing dependency?")
        calls_after = await self.primary_calls()

        await self.set_primary("ok")
        await asyncio.sleep(CIRCUIT_COOLDOWN_S + 0.2)
        probe = await self.send("probe", TENANT_A, "Are you back?", 100)
        state_closed = await self.circuit()
        for x in tripping:
            x.label = "tripping"
        skipped.label = "skipped"
        s.exchanges += tripping + [skipped, probe]

        s.check("%d requests each waited out the 3000 ms deadline, then Groq answered" % CIRCUIT_THRESHOLD,
                all(x.status == 200 and x.attempts and x.attempts[0]["outcome"] == "timeout" for x in tripping))
        s.check("the last of them opened the circuit (circuit.open logged; /healthz says open)",
                "circuit.open" in tripping[-1].events and state_open == "open")
        s.check("next request skipped the primary: only Groq attempted, mock not called",
                skipped.status == 200 and skipped.provider == "secondary"
                and [a["provider"] for a in skipped.attempts] == ["secondary"] and calls_after == calls_before)
        s.check("no deadline wait: %.0f ms vs %.0f ms while tripping" % (skipped.elapsed_ms, tripping[-1].elapsed_ms),
                skipped.elapsed_ms < tripping[-1].elapsed_ms - 2000)
        s.check("logged upstream.skipped + completion.served + http.access",
                skipped.events == ["upstream.skipped", "completion.served", "http.access"])
        s.check("after the cooldown one probe reached the healthy primary and closed the circuit",
                probe.status == 200 and probe.provider == "primary" and state_closed == "closed"
                and probe.events[:2] == ["circuit.half_open", "circuit.closed"])
        return s


def _content(body: Any) -> str:
    try:
        return body["choices"][0]["message"]["content"] or ""
    except (TypeError, KeyError, IndexError):
        return ""


def _total_tokens(body: Any) -> Optional[int]:
    try:
        return int(body["usage"]["total_tokens"])
    except (TypeError, KeyError, ValueError):
        return None


def _mostly_english(text: str) -> bool:
    letters = [c for c in text if c.isalpha()]
    return bool(letters) and sum(c.isascii() for c in letters) / len(letters) >= 0.95


# --------------------------------------------------------------------- main
async def run(demo: Demo) -> List[Scenario]:
    steps = [
        demo.healthy_primary,
        lambda: demo.failover(2, "Primary 429 -> failover to Groq", "429",
                              "In one sentence, why do API gateways rate limit?", "rate_limited"),
        lambda: demo.failover(3, "Primary hangs -> 3000 ms deadline -> failover to Groq", "slow",
                              "In one sentence, what is a timeout?", "timeout"),
        lambda: demo.failover(4, "Primary 500 -> failover to Groq", "500",
                              "In one sentence, what is a fallback model?", "server_error"),
        demo.all_fail,
        demo.burst,
        demo.circuit_breaker,
    ]
    scenarios = []
    for step in steps:
        demo.capture.clear()
        demo.gateway_app.state.router.breaker.reset()  # each scenario starts with a closed circuit
        scenario = await step()
        print(" %s %d. %s" % ("✅" if scenario.passed else "❌", scenario.number, scenario.title))
        for name, ok in scenario.checks:
            print("      [%s] %s" % ("PASS" if ok else "FAIL", name))
        for x in scenario.exchanges:
            print("      %-11s HTTP %d  %6.0f ms  %s" % (x.label, x.status, x.elapsed_ms, x.request_id))
            if x.provider == "secondary":
                print("                  Groq: %s" % _content(x.body).strip().replace("\n", " ")[:100])
        scenarios.append(scenario)
    await demo.set_primary("ok")
    return scenarios


def verify_log(path: Path, prompts: List[str], secret: str) -> List[Tuple[str, bool]]:
    """Checks on the log file itself: valid JSON Lines, and nothing sensitive in it."""
    text = path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    try:
        events = [json.loads(line) for line in lines]
        valid = all(isinstance(e, dict) and {"ts", "level", "logger", "message"} <= set(e) for e in events)
    except ValueError:
        events, valid = [], False
    return [
        ("%d lines, every line a JSON object with ts/level/logger/message" % len(lines), valid and bool(lines)),
        ("every gateway event carries an event name and request_id",
         all("event" in e and "request_id" in e for e in events if e.get("logger") in ("llm-gateway", "llm-router"))),
        ("no prompt text in the log", not any(p in text for p in prompts)),
        ("no API key in the log", secret not in text and "gsk_revoked_demo_key" not in text),
    ]


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model", default=os.environ.get("GROQ_MODEL"),
                        help="Groq model id (default: the best general chat model your key has)")
    parser.add_argument("--log-file", default=str(DEFAULT_LOG), help="JSON Lines server log (default: task4/groq_demo.jsonl)")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite file for the demo (default: task4/groq_demo.db)")
    parser.add_argument("--list-models", action="store_true", help="list the models your Groq key can use")
    args = parser.parse_args(argv)

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("error: set your key first:  export GROQ_API_KEY=gsk_...", file=sys.stderr)
        return 1
    try:
        models = fetch_models(api_key)
    except (RuntimeError, httpx.HTTPError) as exc:
        print("error: could not list Groq models: %s" % exc, file=sys.stderr)
        return 1
    suggested = pick_chat_model(models)
    if args.list_models:
        for model in models:
            print(model + ("   <- used by default" if model == suggested else ""))
        return 0
    if args.model is None:
        if suggested is None:
            print("error: no general chat model available to this key; see --list-models", file=sys.stderr)
            return 1
        args.model = suggested

    # Fresh state each run: empty rate-limit window, empty log.
    db_path, log_path = Path(args.db), Path(args.log_file)
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    log_path.unlink(missing_ok=True)

    # The gateway's logging, pointed at a file instead of stdout.
    from structured_logging import JsonFormatter
    import gateway  # noqa: F401  (importing configures stdout JSON logging; replaced below)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(JsonFormatter())
    capture = EventCapture()
    root = logging.getLogger()
    root.handlers = [file_handler, capture]
    root.setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    print("=" * 78)
    print(" client -> gateway -> mock (primary) | Groq %s (secondary)" % args.model)
    print("=" * 78)
    start = time.perf_counter()
    demo = Demo(api_key, args.model, db_path, capture)

    async def go() -> List[Scenario]:
        try:
            return await run(demo)
        finally:
            await demo.close()

    scenarios = asyncio.run(go())
    file_handler.close()
    total = time.perf_counter() - start

    log_checks = verify_log(log_path, demo.prompts, api_key)
    print(" %s log file" % ("✅" if all(ok for _, ok in log_checks) else "❌"))
    for name, ok in log_checks:
        print("      [%s] %s" % ("PASS" if ok else "FAIL", name))

    passed = sum(s.passed for s in scenarios)
    all_ok = passed == len(scenarios) and all(ok for _, ok in log_checks)
    print("=" * 78)
    print(" %d/%d scenarios passed   (%.1f s)" % (passed, len(scenarios), total))
    print(" server log (JSON Lines): %s" % log_path)
    print("=" * 78)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
