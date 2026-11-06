# AI Field Developer / Solutions Engineer — Assessment

[![tests](https://github.com/RamChandra48/AI-Field-Developer-Solutions-Engineer-Assessment-Tasks/actions/workflows/ci.yml/badge.svg)](https://github.com/RamChandra48/AI-Field-Developer-Solutions-Engineer-Assessment-Tasks/actions/workflows/ci.yml)

Four pieces of infrastructure that sit between AI agents, the tools they call
and the models they use. Each is a small, runnable service with its own
README, demo and tests.

```
                    ┌──────────────────────────── tools side ──────────────────────────────┐
  AI agent ──MCP──▶ │ Task 2: security gateway ──▶ MCP server      Task 1: MCP server      │
                    │ (JWT auth, blocks admin_*)                   (stdio, safe tools)     │
                    └──────────────────────────────────────────────────────────────────────┘
                    ┌──────────────────────────── models side ─────────────────────────────┐
  client  ──HTTP──▶ │ Task 3: PII-redaction gateway ──▶ Groq                               │
                    │ Task 4: rate limit + failover ──▶ primary ──(429 / 3 s)──▶ secondary │
                    └──────────────────────────────────────────────────────────────────────┘
```

| Task | What it is | Core idea | Tests |
| --- | --- | --- | --- |
| [**1. MCP customer service**](task1/README.md) | MCP server (stdio) with `get_customer_record` and `trigger_refund` | Tools that are safe for an LLM to call: strict schemas, behaviour hints, exact money, idempotent refunds | 40 |
| [**2. MCP security gateway**](task2/README.md) | Reverse proxy in front of an MCP server | Verify a JWT, then block `admin_*` tools for non-admins **before** the server sees the call | 125 |
| [**3. Streaming PII redaction**](task3/README.md) | LLM gateway in front of Groq | Redact emails, SSNs and cards **while the stream flows**, holding back only text that could still become PII | 77 |
| [**4. Rate limiting and model fallback**](task4/README.md) | LLM gateway with a primary and a secondary model | Per-tenant token budget in SQLite, 3000 ms failover, a circuit breaker, errors that never leak upstream details | 83 |

## Getting started

### 1. Set up (once)

Python 3.11+ (tested on 3.11, 3.12 and 3.13).

```bash
git clone https://github.com/RamChandra48/AI-Field-Developer-Solutions-Engineer-Assessment-Tasks.git
cd AI-Field-Developer-Solutions-Engineer-Assessment-Tasks
python -m venv .venv && source .venv/bin/activate        # or: conda activate <your env>
python -m pip install -r task1/requirements.txt -r task2/requirements.txt \
                      -r task3/requirements.txt -r task4/requirements.txt
```

### 2. Run every test

```bash
python run_tests.py              # all four suites; exit code 1 if any fail
python run_tests.py task2 task4  # just some
```

```
task     passed  failed     time
task1        40       0     1.9s  ✅
task2       125       0     1.7s  ✅
task3        77       0     7.7s  ✅
task4        83       0     3.1s  ✅
total       325
```

No API key or network is needed: every test uses local fakes. Each suite runs
from its own directory, because several tasks have a module named
`gateway.py`. GitHub Actions runs the same command on every push, on Python
3.11, 3.12 and 3.13 ([`ci.yml`](.github/workflows/ci.yml)).

### 3. Run each demo

Every demo checks its own results, so it doubles as a smoke test. The full
flow, all commands and troubleshooting for each task are in its README.

| Task | Command | Needs | What you see |
| --- | --- | --- | --- |
| 1 | `cd task1 && python demo_task1.py` | nothing | Tools and their hints, a lookup, a refund, an idempotent retry, a rejected input |
| 2 | `cd task2 && python run_demo.py` | nothing | Mock MCP server `:9000` + gateway `:8080`, tokens in `tokens.env`, a self-check, then the live audit log |
| 2 | `cd task2 && python demo_transcript.py` | nothing | Rewrites [`TRANSCRIPT.md`](task2/TRANSCRIPT.md): 12 recorded scenarios |
| 3 | `cd task3 && python groq_demo.py` | `GROQ_API_KEY` | A Groq reply streamed through the gateway with the PII redacted, plus its server log and trace |
| 4 | `cd task4 && python groq_demo.py` | `GROQ_API_KEY` | 7 failover / rate-limit / circuit-breaker scenarios, mock primary, real Groq secondary |

The ports don't overlap (task2 `8080`/`9000`, task3 `8200`, task4
`8300`/`8301`; the demos for tasks 3 and 4 pick free ports), so everything can
run at the same time.

---

## What was built

### Task 1: MCP customer service → [task1/README.md](task1/README.md)

```
MCP client (agent) ──stdio, JSON-RPC──▶ task1_server.py ──▶ mock records / simulated refunds
```

- **Tools describe themselves to the agent:** descriptions, titles and MCP
  behaviour hints (`readOnlyHint`, `destructiveHint`, `idempotentHint`). A
  client can auto-approve the lookup and ask a human before a refund.
- **Strict input and output schemas** (Pydantic, `strict`, `extra="forbid"`).
  Results come back as `structuredContent`, checked against the output schema.
- **Money is exact:** at most 2 decimal places and at most 10,000 USD.
  `0.1 + 0.2` is rejected rather than rounded. Amounts go out as `"12.50"`,
  never as floats.
- **Idempotent refunds:** retrying with the same `idempotency_key` returns the
  original refund instead of paying twice.
- **Correct JSON-RPC errors** (`-32700`, `-32600`, `-32601`, `-32602`), and
  stdout carries protocol messages only.

### Task 2: MCP security gateway → [task2/README.md](task2/README.md)

```
agent ──POST /mcp + Bearer JWT──▶ gateway :8080 ──(only if allowed)──▶ MCP server :9000
                                   └─ viewer calls admin_* → -32001, never forwarded
```

- **Authenticates every request** with an HS256 JWT. The algorithm is pinned
  (no `alg: none`), the signature is checked in constant time, and expiry is
  enforced. **There is no default secret:** without one of at least 32 bytes,
  the gateway refuses to start.
- **Authorizes tool calls by role**, and blocks the usual bypasses:
  look-alike names (`ADMIN_x`, full-width `ａｄｍｉｎ＿x`), admin calls hidden
  in a batch or sent as notifications, and duplicate JSON keys.
- **The client's token stops at the gateway.** The MCP server gets the
  verified identity in headers instead.
- **JSON Lines audit log:** one event per decision, with a `request_id` that
  is also the `X-Request-ID` and is passed on to the MCP server.
- Standard library only.

### Task 3: Streaming PII redaction → [task3/README.md](task3/README.md)

```
client ◀── redacted SSE, chunk by chunk ── gateway ◀── raw SSE ── Groq
          "jane." + "doe@e" + "xample.com"  →  "[REDACTED]"   (no prefix ever leaks)
```

- **Partial-match regex:** each chunk is checked together with only the
  held-back suffix, so the work is linear and the buffer is bounded. Ordinary
  prose streams about one token behind the provider.
- **Every model-generated text field is redacted:** `content`, `reasoning`
  and tool-call `arguments`.
- **Fails closed:** a chunk it can't parse ends the stream. Card numbers are
  Luhn-checked, and Unicode spaces and dashes (which LLMs really emit) don't
  slip past the patterns.
- **Only clients with a gateway key** can spend the gateway's Groq key. The
  check is constant-time, and with no keys configured every request is
  refused.
- **Tested properties:** output is identical however the text is split, no
  partial PII is ever sent, and the gateway adds under 150 ms to
  time-to-first-token.

### Task 4: Rate limiting and model fallback → [task4/README.md](task4/README.md)

```
client ─▶ auth ─▶ reserve tokens (SQLite sliding window) ─▶ primary (3000 ms) ─▶ secondary
                     └─ 429 + Retry-After                     └─ circuit open: skip primary
          ◀─ reconcile to real usage, or refund if nothing was delivered
```

- **Token-aware sliding log** (50,000 tokens/minute per tenant).
  - The gateway reserves an estimate before calling a model, then
    reconciles it to real usage. Usage above the reservation is charged as
    debt.
  - Nothing delivered means a full refund, even when the client disconnects.
  - `BEGIN IMMEDIATE` makes admission atomic across worker processes.
- **The 3000 ms deadline is `asyncio.timeout` around the whole exchange**, so
  even a provider trickling bytes is cut off. The late request is cancelled,
  so it is never counted twice.
- **Circuit breaker:** after 3 consecutive failures the primary is skipped
  for 30 s, then a single probe tests it.
- **One standardized error format:** fixed messages and a `request_id`. No
  upstream bodies, provider names or stack traces, and the tests prove none
  leak.

---

## Across all four

| | |
| --- | --- |
| **Tests** | 325, all offline, run by [`run_tests.py`](run_tests.py) and GitHub Actions on Python 3.11 / 3.12 / 3.13 |
| **Logs** | Tasks 2–4 write JSON Lines with an `event` name and a `request_id` that is also returned as `X-Request-ID`. A log shipper can ingest them as-is |
| **Secrets** | No default secrets anywhere. Provider keys stay in the gateway process. Tokens, keys and PII are never logged (tested). `.env` is git-ignored |
| **Errors** | Clients get stable, documented error codes. Internals go to the server log only, under the same request id |
| **Models** | Not tied to one AI company. Tests use built-in fakes (free, no network), the demos call Groq (free tier, fast), and production points the same code at any provider that accepts the standard "chat completions" request format, by changing settings only. See [Task 3](task3/README.md#which-ai-model-does-it-use) and [Task 4](task4/README.md#which-ai-models-does-it-use) |
| **Fail closed** | A missing JWT secret or missing client keys refuses service. An unparseable stream chunk ends the stream |

## What I'd do next in production

| Task | Next step |
| --- | --- |
| 1 | Store idempotency keys with the payment record (they're in memory here); require human approval above a refund threshold; add an audit trail |
| 2 | Asymmetric keys (RS256/ES256 via JWKS, with rotation) instead of a shared HS256 secret; load the tool policy from config; per-subject rate limits |
| 3 | Per-client rate limits and key rotation from a secret store; more PII types (phone numbers, IBANs, names via NER); redaction metrics |
| 4 | Redis or Postgres for a multi-host token budget (SQLite coordinates one host); a real tokenizer for exact admission; streaming; breaker state shared across replicas |

## Repository layout

```
.
├── README.md                  this overview
├── run_tests.py               runs all four suites, prints one table
├── .github/workflows/ci.yml   CI: the same tests on Python 3.11 / 3.12 / 3.13
├── task1/                     MCP customer-service server (stdio)
├── task2/                     MCP security gateway (stdlib only)
├── task3/                     streaming PII-redaction gateway (Groq)
└── task4/                     rate-limit + model-fallback gateway
```

Each `taskN/README.md` follows the same order: what it is, the flow, quick
start, commands, tests, design, logs, files, troubleshooting.
