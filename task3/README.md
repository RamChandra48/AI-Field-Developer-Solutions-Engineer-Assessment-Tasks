# Task 3 — LLM Gateway with Streaming PII Redaction

An LLM gateway in front of Groq that streams replies back to the client and
removes emails, SSNs and credit-card numbers **while the stream is flowing**,
replacing them with `[REDACTED]`. Only clients holding a gateway key can use
it, and the Groq key never leaves the gateway.

## Which AI model does it use?

**Any model you choose.** The gateway isn't tied to one AI company. It sits
between your app and the model and filters what comes back. It uses the
standard "chat completions" request format that most AI providers support,
so connecting a different provider means changing two settings, not code.

The same gateway code runs in three settings, each with a different model
behind it:

| Where | Model behind the gateway | Why |
| --- | --- | --- |
| **Automated tests** | A small fake model, built into the tests | Free, no internet or account needed, same result every time. It can also split an email across chunks at exactly the worst place, which a real model only does by chance |
| **Demo** (`groq_demo.py`) | **Groq**, a real AI provider | Shows the redaction working on real model output, not just on text written for the tests |
| **Production** | Your company's provider | Set `LLM_UPSTREAM_URL` and `LLM_UPSTREAM_API_KEY`. Nothing else changes |

**Why Groq for the demo:**

- **Free to try.** Anyone reviewing this can create a key and run the demo
  at no cost.
- **Fast.** Replies stream quickly, so the demo shows clearly that redaction
  doesn't slow the stream down.
- **Standard format.** Groq accepts the same requests as other major
  providers, so a working Groq demo shows the gateway will work with them too.
- **It found a real bug.** A Groq model wrote a test card number with an
  unusual kind of space, which the first version of the filter missed. Tests
  written by hand hadn't caught that (see below).

Only Groq and the built-in fake have actually been tested. Another provider
that uses the same request format should work by changing the two settings.

## Flow

```
client ──POST /v1/chat/completions + Bearer gateway key──▶ gateway ──Groq key──▶ Groq
       ◀──────────── redacted SSE, chunk by chunk ─────────        ◀── raw SSE ──
```

1. **Authenticate.** The client's gateway key is checked before anything
   else (401 if missing or wrong).
2. **Forward.** The request goes to Groq with the gateway's own Groq key.
3. **Redact while streaming.** Each SSE chunk from Groq goes through the
   redactor. Text that is certainly safe is sent on at once. Only the tail
   that could still turn into PII is held back.
4. **Log.** One `stream.completed` event and one `http.access` event per
   request. They record counts and kinds of PII redacted, never the text.

The hard part is PII split across chunks:

```
"... the email jane."   "doe@e"   "xampl"   "e.com and ..."
```

Checking each chunk on its own would leak `jane.`. Waiting for the whole reply
would make the user wait. The gateway does neither:

```
received "jane."            sent ""              held "jane."                  (could be an email)
received "doe@e"            sent ""              held "jane.doe@e"
received "e.com"            sent ""              held "jane.doe@example.com"
received " and"             sent "[REDACTED] "   held "and"                    (email complete: redacted)
```

Ordinary words pass through about one chunk late. Everything else streams
straight through.

| Detected | Example |
| --- | --- |
| Email | `jane.doe@example.com` |
| SSN | `123-45-6789` |
| Credit card | `4111 1111 1111 1111` (Luhn-checked, so random 16-digit numbers are left alone) |

Separators can be any Unicode space or dash, not just ASCII `" "` and `-`, and
the `@` can be full-width (`＠`). This isn't theoretical: a real `gpt-oss-120b`
reply wrote the test card with narrow no-break spaces (`4111\u202f1111…`),
which an ASCII-only pattern let through. That reply is now a regression test.

Every piece of model text the client receives is redacted, not just the
answer: `content`, `reasoning` (where Groq's reasoning models such as
`gpt-oss` and `qwen3` stream their thinking) and tool-call `arguments`. Each
is its own stream with its own buffer, and a redacted tool call is still
valid JSON.

## Quick start

Python 3.11+ (tested on 3.11, 3.12 and 3.13). The demo needs a Groq API key;
the tests don't.

```bash
cd task3
python -m pip install -r requirements.txt   # fastapi, uvicorn, httpx, regex, pytest
export GROQ_API_KEY=gsk_...                 # your key; keep it out of the repo
python groq_demo.py                         # Groq -> gateway -> you, streamed live with PII redacted
python -m pytest -q                         # 77 passed (no API key needed)
```

## Commands

### Demo (one command, no servers to start)

```bash
python groq_demo.py
python groq_demo.py --prompt "Write a note that mentions bob@example.com"
python groq_demo.py --list-models        # models your key can use
python groq_demo.py --model <id>         # by default a model is picked automatically
```

The script:

1. starts the gateway in-process on a free port, with Groq as its upstream
2. generates a gateway key for this run
3. checks that a request without a key is refused with 401
4. streams the reply on screen, with the PII already replaced
5. checks the result and writes two files

```
 chunks: 68   TTFT: 705 ms   total: 720 ms
 PII in the answer: 3   redacted: 3   left in output: 0   ✅
 no client key: HTTP 401   ✅
 server log: task3/groq_demo.jsonl
             4 lines; redactions (answer + reasoning) logged as counts only: credit_card=1, email=1, ssn=1; PII or keys in log: 0 ✅
 trace:      task3/groq_demo.log
```

| File | What it is |
| --- | --- |
| `groq_demo.jsonl` | The gateway's **server log**, exactly as production writes it: counts and kinds of PII, never the text or any key (checked after the run) |
| `groq_demo.log` | A **trace** for reading: the raw Groq text, the redactor chunk by chunk, the final output, a summary |

The model is picked from what your key can use. The demo prefers general chat
models (Llama 3.3 70B, Kimi K2, Llama 4, GPT-OSS) and skips speech, guard,
agentic and Arabic-first models such as `allam-2-7b`.

### Run the gateway as a server

```bash
# terminal 1
export GROQ_API_KEY=gsk_...
export GATEWAY_KEY=$(openssl rand -hex 24)             # a key for one client (not the Groq key)
export GATEWAY_API_KEYS="$GATEWAY_KEY:my-app"          # without this, every request gets 401
echo $GATEWAY_KEY                                      # copy it for terminal 2
python -m uvicorn gateway:app --port 8200 --no-access-log   # JSON Lines log on stdout

# terminal 2
export GATEWAY_KEY=<value from terminal 1>
curl -N http://127.0.0.1:8200/v1/chat/completions -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' -d \
  '{"model":"llama-3.3-70b-versatile","stream":true,"messages":[{"role":"user","content":"Say hi to jane@example.com"}]}'

curl -si http://127.0.0.1:8200/v1/chat/completions -d '{}'   # no key: 401, WWW-Authenticate: Bearer
```

For the model, use any id from `python groq_demo.py --list-models`. Leave out
`"stream": true` to get a single JSON reply, redacted the same way.

| Variable | Default | Meaning |
| --- | --- | --- |
| `GATEWAY_API_KEYS` | (none: refuse all) | `key:client,key2:client2`; keys of at least 16 characters |
| `GROQ_API_KEY` / `LLM_UPSTREAM_API_KEY` | | The provider key, held only by the gateway |
| `LLM_UPSTREAM_URL` | Groq's chat completions URL | Any OpenAI-compatible streaming endpoint |
| `LOG_LEVEL` | `INFO` | |

## Tests

```bash
python -m pytest -v          # 77 passed, about 8 s, no API key or network
```

The tests use a built-in fake provider, never a real LLM. They prove that:

- **Splitting doesn't matter:** output is identical however the text is split
  into chunks (thousands of random splits).
- **No partial PII is ever sent**, in `content`, `reasoning` or tool-call
  arguments.
- **Typographic look-alikes are caught:** no-break, narrow and thin spaces,
  en dashes, non-breaking hyphens, minus signs and full-width `＠`, at every
  split point.
- **Streaming is real:** chunks arrive one by one, not all at the end, and
  the gateway adds less than 150 ms to time-to-first-token.
- **Failures fail closed:** an unparseable chunk ends the stream rather than
  forwarding text that wasn't inspected.
- **Auth works:** six kinds of bad credential get 401 before Groq is called.
  The client's key never reaches Groq and is never logged.
- **The server log** has counts and kinds only, never PII.

## Design

### How it meets the evaluation criteria

| Criterion | How |
| --- | --- |
| **Efficient async stream chunking and buffer state management** | The gateway reads the provider's stream line by line (`httpx` `aiter_lines`) and yields each redacted chunk straight into a `StreamingResponse`. Each reply has its own small buffer: the held-back text plus one character of context |
| **Performant regex matching over partial text streams** | Uses the `regex` library's partial matching to find the earliest point where PII could start. Each new chunk is checked together with only the held-back text, not the whole reply, so the work grows linearly with the reply length (1.2 MB redacts in about 1.5 s) |
| **Memory efficiency and low-latency proxying** | The full reply is never stored. The buffer is capped by the longest possible match (20 chars in the demo run, at most about 64 for a long word). TTFT is about one chunk behind the provider. Connections to Groq are pooled, and proxy buffering is turned off |

### Client authentication

The gateway spends its own Groq key on every request, so it doesn't serve
anonymous callers.

- Each client gets its own **gateway key** and sends it as
  `Authorization: Bearer <key>`. Keys map to client names in
  `GATEWAY_API_KEYS`. Keys shorter than 16 characters stop startup.
- **Fails closed:** with no keys configured, every request is refused, and a
  `config.no_client_keys` warning is logged at startup.
- The key is checked **before** the body is read or Groq is called. Each
  configured key is compared in constant time (`hmac.compare_digest`).
- A rejected request gets `401` with `WWW-Authenticate: Bearer`, in the same
  error shape as every other error.
- The client's key is **never forwarded** to Groq and **never logged**. The log
  records the client's *name*.

## Logs

### Server log (stdout in production, `groq_demo.jsonl` in the demo)

**JSON Lines**: one JSON object per event, ready for a log shipper (Loki,
Datadog, CloudWatch, ELK). Every request gets a `request_id`, which is also
returned as `X-Request-ID` and included in error bodies.

```json
{"ts": "2026-11-05T19:46:15.932Z", "level": "INFO", "logger": "llm-gateway", "message": "stream completed", "event": "stream.completed", "request_id": "req_190a4a4c68044261", "outcome": "done", "chunks": 56, "first_chunk_ms": 0.2, "duration_ms": 535.8, "redactions": {"email": 2, "ssn": 2, "credit_card": 1}, "redaction_total": 5, "max_held_chars": 19}
{"ts": "2026-11-05T19:46:15.937Z", "level": "INFO", "logger": "llm-gateway", "message": "request completed", "event": "http.access", "request_id": "req_190a4a4c68044261", "client": "groq-demo", "method": "POST", "path": "/v1/chat/completions", "status": 200, "latency_ms": 548.4}
```

| `event` | Level | When | Key fields |
| --- | --- | --- | --- |
| `http.access` | INFO (ERROR for 5xx) | every request; for streams, when the stream ends | `client` (name, `null` if unauthenticated), `method`, `path`, `status`, `latency_ms` |
| `auth.failed` | WARNING | missing or invalid gateway key (401) | `credential_present`, `keys_configured`; never the presented value |
| `config.no_client_keys` | WARNING | startup with no `GATEWAY_API_KEYS`: every request will be refused | |
| `stream.completed` | INFO, WARNING if not `done` | a stream ended | `outcome` (`done`, `upstream_closed`, `client_disconnected`, `invalid_chunk`, `interrupted`), `chunks`, `first_chunk_ms`, `redactions` by kind, `max_held_chars` |
| `completion.redacted` | INFO | non-streaming reply redacted | `redactions` by kind |
| `upstream.unavailable` / `upstream.error` | ERROR | Groq unreachable / returned non-200 | `error_class` / `upstream_status` |
| `stream.invalid_chunk` / `stream.interrupted` | ERROR | unparseable chunk (stream closed, fail safe) / transport failure mid-stream | `payload_chars` / `error_class` |
| `redaction.dropped_field` | WARNING | a text field that wasn't a string (not forwarded) | `field`, `value_type` |

**The server log never contains the prompt, the reply, any PII or any
key.** It holds only the counts and kinds of what was redacted, across
`content`, `reasoning` and tool-call arguments. The tests check this, and so
does the demo after each run.

### Trace (`groq_demo.log`, demo only)

The trace is not a server log. It's a readable account of one run, and it
holds the raw LLM output **with** its PII. That's why the gateway produces
it only when the demo turns tracing on. It shows:

1. **Raw LLM output:** what Groq generated, PII included (the prompt asks for fake data)
2. **Redactor, chunk by chunk:** for each chunk, what arrived, what was sent to the client, what was held back, and each `REDACTED` event
3. **Final output:** what the client received
4. **Summary:** chunks, time to first token, redactions by type, the largest amount of text held back, and confirmation that no PII is left

## Files

| File | What it is for |
| --- | --- |
| `redactor.py` | **The redaction engine:** chunks in, safe text out, holds back only possible PII |
| `gateway.py` | **The gateway** (FastAPI): client auth, reads Groq's stream, redacts each chunk, sends it on immediately |
| `structured_logging.py` | JSON Lines log formatter |
| `groq_demo.py` | **The demo:** a prompt to Groq through the gateway, streamed live, with both log files written |
| `groq_demo.jsonl`, `groq_demo.log` | Output of the last demo run |
| `test_redactor.py` | Tests for the redaction engine |
| `test_gateway.py` | Tests for streaming, auth and the server log, against a built-in fake provider |
| `requirements.txt`, `pytest.ini` | Packages and test settings |

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `error: set your key first` | `export GROQ_API_KEY=gsk_...` in the same terminal |
| Every request gets 401 | `GATEWAY_API_KEYS` isn't set in the gateway's terminal, or the client sends a different key |
| Gateway won't start: `gateway API keys must be at least 16 characters` | Use a longer key: `openssl rand -hex 24` |
| Demo ends with `The model's answer contained no PII` | The model refused (safety-tuned models sometimes do), so there was nothing to redact. Run it again, or try `--prompt` / `--model` |
| `upstream_error` ... `HTTP401` from a running gateway | Groq rejected the key in the **gateway's** terminal. Load it there with `set -a; source ../.env; set +a` (if `.env` holds `GROQ_API_KEY=...`) and restart |
| `request failed: HTTP 502` in the demo | Groq rejected the model or key. Check `python groq_demo.py --list-models`, or pass `--model <id>` |
| `ModuleNotFoundError: regex` / `fastapi` | Wrong Python: activate the project env (`conda activate ai-assessment`), or `python -m pip install -r requirements.txt` |
