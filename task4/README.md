# Task 4 — Rate Limiting & Model Fallback Router

An LLM gateway that:

- **enforces a per-tenant token budget:** a sliding window of 50,000
  tokens/minute per API key, stored in SQLite on disk
- **fails over** from the primary model to a secondary on 429, or after a
  hard 3000 ms deadline
- **returns one standardized error format** that never leaks upstream details

A circuit breaker stops a hanging primary from costing every request 3 s.

## Which AI models does it use?

The gateway needs **two** models: a **primary** that gets every request, and
a **secondary** (backup) that takes over when the primary fails. Which ones
you use is up to you. Each is set by its address, model name and key, and
the code doesn't change.

**What is the "mock"?** `mock_providers.py` is a small program that pretends
to be an AI model. It answers instantly with a fixed reply, and you can
switch it to misbehave on purpose: refuse with "too busy" (429), hang, or
crash (500). That is the only reliable way to show failover. Real providers
can't be made to fail on demand.

The same gateway code runs in three settings:

| Where | Primary | Secondary | Why |
| --- | --- | --- | --- |
| **Automated tests** | Mock | Mock | Free, no internet or account, same result every time. Every failure can be triggered at exactly the right moment |
| **Demo** (`groq_demo.py`) | Mock | **Groq** (real AI) | The mock fails on cue. Groq then gives a real answer, showing the user gets a proper reply even when the main model is down |
| **Production** | Your main provider | A provider from a **different company** | Set `PRIMARY_URL`, `PRIMARY_MODEL`, `PRIMARY_API_KEY` and the matching `SECONDARY_` settings. If both models came from the same company, one outage could take down both |

```bash
# production example (placeholders): two real providers, no mocks
PRIMARY_URL=https://<main-provider>/v1/chat/completions    PRIMARY_MODEL=<model>   PRIMARY_API_KEY=<key> \
SECONDARY_URL=https://<backup-provider>/v1/chat/completions SECONDARY_MODEL=<model> SECONDARY_API_KEY=<key> \
python -m uvicorn gateway:app --port 8300
```

**Why Groq for the demo:**

- **Free to try.** Anyone reviewing this can create a key and run the demo
  at no cost.
- **Fast.** The backup's reply comes back in well under a second, so the demo
  shows that failover costs very little time.
- **Standard format.** Groq accepts the same "chat completions" requests as
  other major providers, so a working Groq demo shows the gateway will work
  with them too.

Only Groq and the mock have actually been tested. Another provider that uses
the same request format should work by changing the settings.

## Flow

```
client ──Bearer tenant key──▶ gateway :8300
   1. auth (tenant key)                       401 if unknown
   2. validate + estimate tokens               400 / 413
   3. reserve the estimate in the SQLite window  429 + Retry-After if it doesn't fit
   4. primary, 3000 ms hard deadline ──429 / timeout / 5xx──▶ secondary
        (circuit open after 3 failures in a row: skip the primary entirely)
   5. reconcile the reservation to real usage, or refund it if nothing was delivered
◀── completion + X-Gateway-Provider, X-Gateway-Fallback, X-RateLimit-*, X-Request-ID
```

## Quick start

Python 3.11+ (`asyncio.timeout`; tested on 3.11, 3.12 and 3.13).

```bash
cd task4
python -m pip install -r requirements.txt   # fastapi, uvicorn, httpx, pydantic, pytest
export GROQ_API_KEY=gsk_...                 # your key; keep it out of the repo
python groq_demo.py                         # 7 scenarios: mock primary, real Groq secondary
python -m pytest -q                         # 83 passed (no API key needed)
```

## Commands

### Demo with Groq (one command, no servers to start)

The mock stays the **primary**, so its failures can be switched on demand.
Groq is the **secondary**, so every failover returns a real LLM answer.

```bash
python groq_demo.py                  # --model <id>, --list-models
```

```
 ✅ 1. Primary healthy                                     mock answers, no failover
 ✅ 2. Primary returns 429 -> failover to Groq             mock's org id not leaked
 ✅ 3. Primary hangs -> 3000 ms deadline -> failover       primary cut off at ~3000 ms
 ✅ 4. Primary returns 500 -> failover to Groq             mock's stack trace not leaked
 ✅ 5. Primary 500 and Groq rejects the request            standardized 502, tenant refunded
 ✅ 6. Tenant burst against the token limit                2 of 3 admitted, 1 gets 429
 ✅ 7. Primary keeps hanging -> circuit opens -> recovery  skipped request: no 3000 ms wait
```

How the scenarios run:

- Each scenario sends its own requests. The mock's failure mode is switched,
  and the circuit breaker is reset, before each one.
- Groq is called only in scenarios 2–5 and 7, with a one-sentence question
  and a system prompt asking for English.
- Scenario 5 gives the secondary a revoked key for that one request, so Groq
  really answers 401. Your real key is untouched.
- Scenario 7 uses a threshold of 2 and a 3 s cooldown so it finishes in
  seconds. The gateway's defaults are 3 and 30 s.

The console shows each scenario's checks, request ids and Groq's reply.

`groq_demo.jsonl` is the gateway's server log for the run, in the same JSON
Lines format it writes in production. The demo checks that log too: every
line is valid JSON, every gateway event has an `event` and a `request_id`,
and no prompt text or API key appears.

The Groq model is picked from what your key can use, preferring general chat
models and skipping speech, guard, agentic and Arabic-first models.
Rate-limit state goes in `groq_demo.db`, recreated on each run.

### Run it as servers (mocks only)

```bash
python -m uvicorn mock_providers:app --port 8301                # terminal 1: primary + secondary mocks
python -m uvicorn gateway:app --port 8300 --no-access-log       # terminal 2: gateway, JSON Lines on stdout
```

In terminal 3:

```bash
U=http://127.0.0.1:8300/v1/chat/completions
H=(-H 'Authorization: Bearer tenant-a-key' -H 'Content-Type: application/json')
D='{"messages":[{"role":"user","content":"Hello"}],"max_tokens":100}'
mode() { curl -s -X POST http://127.0.0.1:8301/control/$1 -H 'Content-Type: application/json' -d "{\"mode\":\"$2\"}"; echo; }

curl -si $U "${H[@]}" -d "$D"                        # 200, X-Gateway-Provider: primary
mode primary 429;  curl -si $U "${H[@]}" -d "$D"     # 200 from secondary, X-Gateway-Fallback: true
mode primary slow; curl -si $U "${H[@]}" -d "$D"     # secondary after ~3.0 s (primary deadline)
mode primary 500;  curl -si $U "${H[@]}" -d "$D"     # 200 from secondary; the mock's stack trace isn't leaked
mode secondary 500; curl -si $U "${H[@]}" -d "$D"    # both down: 503 upstream_unavailable, no stack trace
mode primary ok; mode secondary ok
curl -s http://127.0.0.1:8300/v1/rate-limit -H 'Authorization: Bearer tenant-a-key'   # usage in the window
curl -s http://127.0.0.1:8301/control                # the mocks' current modes and call counts
```

Mock modes: `ok`, `429`, `slow`, `500`, `garbage`. Tenant keys:
`tenant-a-key` and `tenant-b-key`, each with its own budget.

**Circuit breaker.** The 429, slow and 500 commands above are already 3
primary failures in a row, so the circuit may be open by now (check
`/healthz`). To watch it open from scratch, restart terminal 2, then:

```bash
mode primary 500; for i in 1 2 3; do curl -s -o /dev/null $U "${H[@]}" -d "$D"; done
curl -s http://127.0.0.1:8300/healthz                # {"status":"ok","primary_circuit":"open"}
curl -si $U "${H[@]}" -d "$D"                        # straight to the secondary; the primary's call count stays put
mode primary ok                                      # after 30 s, one probe closes the circuit again
```

**Tenant rate limit.** Restart terminal 2 with a small budget, then send
this twice:

```bash
RATE_LIMIT_TOKENS_PER_MINUTE=1000 python -m uvicorn gateway:app --port 8300 --no-access-log   # terminal 2

BIG=$(python -c 'import json; print(json.dumps({"messages":[{"role":"user","content":"x"*3000}],"max_tokens":100}))')
curl -si $U "${H[@]}" -d "$BIG"                      # 1st: 200; 2nd: 429 with Retry-After
```

### Run it as servers with Groq as the secondary

```bash
python -m uvicorn mock_providers:app --port 8301                # terminal 1
SECONDARY_URL=https://api.groq.com/openai/v1/chat/completions \
SECONDARY_MODEL=openai/gpt-oss-120b SECONDARY_API_KEY=$GROQ_API_KEY \
python -m uvicorn gateway:app --port 8300 --no-access-log       # terminal 2
```

The same curl commands work, with Groq answering whenever the primary fails
(look for `X-Gateway-Provider: secondary`). For `SECONDARY_MODEL`, use any
chat model from `python groq_demo.py --list-models`.

### Configuration

| Variable | Default |
| --- | --- |
| `PRIMARY_URL` / `SECONDARY_URL` | the mock's `/primary` and `/secondary` on `:8301` |
| `PRIMARY_MODEL` / `SECONDARY_MODEL`, `PRIMARY_API_KEY` / `SECONDARY_API_KEY` | mock values |
| `PRIMARY_TIMEOUT_MS` / `SECONDARY_TIMEOUT_MS` | `3000` / `10000` |
| `RATE_LIMIT_TOKENS_PER_MINUTE` | `50000` |
| `RATE_LIMIT_DB` | `task4/ratelimit.db` |
| `DEFAULT_MAX_TOKENS` | `1024` |
| `CIRCUIT_FAILURE_THRESHOLD` / `CIRCUIT_COOLDOWN_SECONDS` | `3` (`0` disables the breaker) / `30` |
| `TENANT_API_KEYS` | `tenant-a-key:tenant-a,tenant-b-key:tenant-b` |
| `LOG_LEVEL` | `INFO` |

## Tests

The tests are in `tests/`. `pytest.ini` points pytest there and puts `task4/`
on the import path, so this works from `task4/` or from the repo root
(`python -m pytest task4`):

```bash
python -m pytest -v          # 83 passed, about 3 s
```

| File | Covers |
| --- | --- |
| `test_rate_limiter.py` | Sliding window, eviction, `Retry-After`, atomic admission across two connections (200 concurrent requests admit exactly 50) |
| `test_token_accounting.py` | Usage above the reservation and above the remaining quota, absurd or invalid usage, a failed reconcile, the estimator |
| `test_router.py` | Failover rules, the 3000 ms deadline (including a provider trickling bytes), 200 requests racing the deadline |
| `test_circuit_breaker.py` | State machine, skipping the primary, a single half-open probe, `/healthz` |
| `test_gateway.py` | End to end: auth, errors that never leak, refunds, client disconnects |

The tests' strength was checked by mutation testing. Removing the refund,
removing the transaction, introducing an off-by-one in the window, or
removing the hard deadline each makes the relevant tests fail.

## Design

### Decisions beyond the spec

- **Failover triggers.** The spec names only 429 and timeout. I also fail
  over on 5xx, connection errors and malformed responses. Other 4xx errors
  don't fail over, because the secondary would reject the same request.
- **Refunds.** A request that gets no completion costs the tenant nothing,
  even if the primary used tokens before failing.
- **Circuit breaker.** Failover alone makes every request wait out the 3 s
  deadline while the primary hangs. See [Circuit breaker](#circuit-breaker).
- **Token estimate.** Admission estimates about 4 characters per token for
  ASCII text and one token per non-ASCII character, plus `max_tokens`.
  Chinese, Cyrillic or emoji tokenize far denser than English, so chars/4
  alone would under-reserve them by up to 4×. The charge is corrected to real
  usage afterwards. A real tokenizer would make admission exact.

### Rate limiter

- **Sliding log, not fixed windows.** Each admitted request is a row
  `(key, ts_ms, tokens)`, and usage is the sum over the last 60 s. There is no
  burst at minute boundaries.
- **Reserve, then reconcile.** Before calling upstream the gateway reserves
  `prompt estimate + max_tokens`, so concurrent requests can't jointly
  overshoot. Afterwards the row is updated to the provider's reported
  `usage.total_tokens`. If no provider delivered a completion, the row is
  refunded in full. That includes client disconnects: the refund is shielded
  from cancellation.
- **Atomic across processes.** Check-and-insert runs in `BEGIN IMMEDIATE`
  (SQLite's write lock), so several gateway workers sharing the file can't
  both admit the last 10k tokens.
- **Eviction.** Expired rows are deleted on every acquire, for all tenants,
  so the table holds about one minute of traffic. Indexes on `(key, ts_ms)`
  and `ts_ms` keep this cheap.
- **Usage above the reservation is charged in full, as debt.**
  - Example: a tenant at 45k reserves 5k, which fills the window, and the
    provider reports 8k. The window now holds 53k.
  - The tokens were spent, so the gateway charges them, logs
    `ratelimit.overrun`, and still delivers the completion.
  - Every later request gets a 429 until the window pays the debt back. Its
    `Retry-After` is the exact time the oldest rows expire (49 s in the test).
  - Overruns can only come from the prompt estimate, because the completion
    is capped by `max_tokens`, which is already reserved. So concurrent
    requests overshoot by at most the sum of their prompt underestimates.
- **Bounded charge.** One request is charged at most the whole window
  budget. That blocks the tenant until its row expires, which is all a larger
  figure could do. It also stops a nonsensical `total_tokens` (say 10³⁰) from
  overflowing SQLite's 64-bit INTEGER and turning a delivered completion
  into a 500. Usage that isn't a non-negative integer is ignored, and the
  estimate stays charged.
- **Bookkeeping never costs the client a completion.** If recording real
  usage fails (for example, the database is locked), the estimate stays
  charged, `ratelimit.reconcile_failed` is logged, and the completion is
  returned anyway. The tenant is never under-charged.
- **Accurate `Retry-After`.** It's computed from the oldest rows that must
  expire for this request to fit, not a flat 60 s.

### Router and timeouts

| Primary result | Action |
| --- | --- |
| 2xx with a valid body | Return it |
| 429, timeout, 5xx, connection error, malformed body | Fail over to the secondary |
| Other 4xx | Stop: 502 `upstream_error` (the secondary would reject it too) |

- **The 3000 ms deadline is `asyncio.timeout` around the whole exchange:**
  connect, headers and body. httpx's own timeouts are per read, so a provider
  trickling a body one byte at a time would never trip them. A test covers
  exactly that.
- **On the deadline the primary request is cancelled.** Its connection is
  closed, so a late response can't be used or counted twice. A race test runs
  200 requests whose primary answers within ±10 ms of the deadline. Each
  resolves exactly once: either the primary completed, or it was cancelled
  and the secondary answered. Never both, and never an error.

### Circuit breaker

```
closed ──3 consecutive failures──▶ open ──30 s cooldown──▶ half_open
  ▲                                  ▲                         │
  │                                  └───────probe fails───────┤
  └──────────────────────probe succeeds────────────────────────┘
```

- **Open:** requests skip the primary and go straight to the secondary
  (`upstream.skipped`), so a hanging primary costs nothing instead of 3 s per
  request. The response still says `X-Gateway-Fallback: true`.
- **Half-open:** exactly **one** request probes the primary. The rest keep
  using the secondary until it settles, so a recovering primary isn't hit by
  a stampede (10 concurrent requests produce 1 primary call).
- **What counts as a failure:** 429, timeouts, 5xx, connection errors and
  malformed bodies. A 4xx is the caller's fault, and a cancelled request says
  nothing about the provider's health, so neither opens the circuit. A
  cancelled probe can't leave it stuck half-open.
- **Visible:** `GET /healthz` returns `primary_circuit` (`closed`, `open`,
  `half_open` or `disabled`), and every transition is logged.
- **Per process:** each gateway replica keeps its own state.

### Standardized errors

Every non-2xx response has the same shape, plus an `X-Request-ID` header:

```json
{"error": {"type": "rate_limit_exceeded", "message": "Tenant token limit exceeded. Please retry later.",
           "request_id": "req_1ae3fb6ef6d94ab5", "retry_after_seconds": 60}}
```

| Status | `type` |
| --- | --- |
| 400 | `invalid_request` (also malformed JSON) |
| 401 | `authentication_error` |
| 404 / 405 | `not_found` / `method_not_allowed` |
| 413 | `request_too_large` (needs more than the whole per-minute budget) |
| 429 | `rate_limit_exceeded`, with `Retry-After` |
| 502 | `upstream_error` |
| 503 | `upstream_unavailable` |
| 504 | `upstream_timeout` |
| 500 | `internal_error` |

- **Messages are fixed strings.** Upstream bodies, provider names, exception
  text and stack traces are never copied into responses. Exceptions are
  re-raised `from None`, so nothing chains through.
- **Details go to the server log only,** under the request id: the failure
  reason and upstream status, plus the traceback for an unexpected exception.
- **The tests prove it.** The mock's failure responses deliberately contain a
  stack trace, an internal IP and an org id, and the tests assert none of
  them reach a client.

## Logs

The gateway writes **JSON Lines to stdout**: one JSON object per event, ready
for a log shipper (Loki, Datadog, CloudWatch, ELK). Every line has `ts` (UTC),
`level`, `logger` and `message`. Gateway and router events add an `event`
name and a `request_id`, which is also returned to the client as
`X-Request-ID`.

| `event` | Level | When | Key fields |
| --- | --- | --- | --- |
| `http.access` | INFO (ERROR for 5xx) | every request | `method`, `path`, `status`, `latency_ms`, `tenant` |
| `completion.served` | INFO | a completion was returned | `provider`, `model`, `fallback`, `attempts[]`, `tokens_estimated`, `tokens_reported`, `tokens_charged` |
| `upstream.failover` | WARNING | primary failed, trying the secondary | `reason`, `upstream_status`, `latency_ms` |
| `upstream.rejected` | WARNING | primary returned a non-retryable 4xx | `reason`, `upstream_status` |
| `upstream.exhausted` | ERROR | no provider produced a completion | `reason`, `attempts[]` |
| `upstream.transport_error` | WARNING | connection-level failure | `provider`, `error_class` |
| `upstream.skipped` | INFO | circuit open, primary not called | `reason`, `retry_primary_in_s` |
| `circuit.open` / `circuit.half_open` / `circuit.closed` | WARNING / INFO / INFO | breaker state change | `previous_state`, `consecutive_failures`, `cooldown_s` |
| `ratelimit.rejected` | INFO | tenant over its token budget (429) | `tokens_used`, `tokens_requested`, `tokens_limit`, `retry_after_s` |
| `ratelimit.too_large` | INFO | one request exceeds the whole budget (413) | `tokens_requested`, `tokens_limit` |
| `ratelimit.refunded` | INFO | reservation released, nothing delivered | `tokens_refunded` |
| `ratelimit.overrun` | WARNING | real usage exceeded the reservation; charged in full | `tokens_estimated`, `tokens_reported`, `tokens_charged`, `tokens_over`, `window_used`, `over_limit`, `capped` |
| `ratelimit.reconcile_failed` | ERROR | real usage couldn't be recorded; estimate kept, completion still returned | `tokens_estimated`, `tokens_reported` |
| `auth.failed` | WARNING | missing or unknown API key (401) | `credential_present` |
| `request.unhandled_exception` | ERROR | a bug | `exception` (traceback) |

```json
{"ts": "2026-11-05T04:19:48.912Z", "level": "WARNING", "logger": "llm-router", "message": "primary provider failed; failing over", "event": "upstream.failover", "request_id": "req_1d5f5ef4bfd34516", "provider": "primary", "reason": "rate_limited", "upstream_status": 429, "fallback_provider": "secondary", "latency_ms": 0.7}
```

Prompt and completion text, upstream response bodies and API keys are never
logged. Only sizes (`prompt_chars`, `prompt_messages`) and token counts are.
Start uvicorn with `--no-access-log`, because `http.access` replaces its
access line.

```bash
jq -c 'select(.request_id=="req_1d5f5ef4bfd34516")' groq_demo.jsonl   # one request's events
jq -c 'select(.level=="ERROR" or .level=="WARNING")' groq_demo.jsonl
```

## Files

| File | What it is for |
| --- | --- |
| `gateway.py` | **The FastAPI app:** auth, validation, reserve/reconcile, headers, error handlers, access log |
| `rate_limiter.py` | `SlidingWindowTokenLimiter`: SQLite sliding log with reserve / reconcile / release |
| `router.py` | `ModelRouter`: deadlines, cancellation, failover rules |
| `circuit_breaker.py` | `CircuitBreaker`: closed / open / half-open state for the primary |
| `errors.py` | `GatewayError` and the standardized error body |
| `structured_logging.py` | JSON Lines log formatter |
| `mock_providers.py` | `/primary` and `/secondary` mock providers with switchable failure modes |
| `groq_demo.py` | **The demo:** mock primary, real Groq secondary, 7 scenarios |
| `groq_demo.jsonl` | Server log from the last demo run |
| `tests/` | 83 tests (see [Tests](#tests)) |
| `requirements.txt`, `pytest.ini` | Packages and test settings |

## Limitations

- **Admission uses an estimate, not a tokenizer.** A prompt that tokenizes
  denser than estimated can take the tenant past its limit. The overrun is
  charged and logged, and it blocks the tenant until it's paid back.
- **SQLite coordinates processes on one host.** A multi-host deployment
  needs a shared store (Redis or Postgres) with the same reserve/reconcile
  logic.
- **Circuit state is per process,** so each replica discovers an outage on
  its own.
- **The window uses wall-clock time** (shared across processes), so a large
  clock jump shifts it.
- **Non-streaming completions only.**

## Troubleshooting

| Problem | Fix |
| --- | --- |
| 500 with `module 'asyncio' has no attribute 'timeout'` | A bare `uvicorn` resolved to an older Python. Use `python -m uvicorn ...` with Python 3.11+ |
| Still rate-limited after a restart | Usage persists on disk by design. Wait 60 s, or delete `task4/ratelimit.db*` |
| `[Errno 48] Address already in use` | Something is already on 8300/8301 (`lsof -i :8300`). Stop it, or use other ports and set `PRIMARY_URL` / `SECONDARY_URL` |
| Every request gets 401 | Send `Authorization: Bearer tenant-a-key` (or your `TENANT_API_KEYS`) |
