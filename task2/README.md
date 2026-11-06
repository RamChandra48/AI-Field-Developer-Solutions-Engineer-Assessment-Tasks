# Task 2 — MCP Security Gateway

A reverse proxy between AI agents and an MCP server. It verifies each caller's
token, then blocks `admin_*` tools for anyone who isn't an admin, **before**
the MCP server ever sees the call. Standard library only.

## Flow

```
AI agent ──POST /mcp + Bearer JWT──▶ gateway :8080 ──(only if allowed)──▶ MCP server :9000
                                        │
                                        └─ viewer calls admin_* → -32001, never forwarded
```

For every request, the gateway:

1. **Checks the token.** It reads `Authorization: Bearer <token>`, verifies the
   HS256 JWT, and gets the caller's role, `admin` or `viewer`. A missing,
   forged or expired token is rejected with HTTP 401.
2. **Parses the JSON-RPC message.** Broken JSON or a malformed message gets a
   proper JSON-RPC error from the gateway.
3. **Decides:**
   - `tools/list` and other methods: forwarded to the MCP server.
   - `tools/call` with a normal tool: forwarded for any authenticated user.
   - `tools/call` with an `admin_` tool: forwarded only for the `admin` role.
     Anyone else gets `-32001 Unauthorized Tool Call` from the gateway, and
     the MCP server is never called.
4. **Forwards** the message it inspected, re-serialized, never the raw bytes.
   The client's token stops at the gateway. The MCP server gets
   `X-Gateway-Subject` and `X-Gateway-Role`, taken from the verified token.
5. **Logs the decision** as one JSON Lines audit event, with a `request_id`.
   The same id goes back to the client as `X-Request-ID` and on to the MCP
   server.

## Quick start

Python 3.9+ (tested on 3.11, 3.12 and 3.13). Nothing to install except
`pytest` for the tests.

```bash
cd task2
python -m pip install -r requirements.txt   # only pytest
python run_demo.py                          # terminal 1: mock MCP server + gateway, tokens, self-check
python -m pytest -q                         # 125 passed
```

These are plain Python scripts, not ASGI apps. Start them with `python`, not
`uvicorn`.

## Commands

### Option 1: one command (recommended)

```bash
python run_demo.py
```

This one command:

1. starts the mock MCP server on `:9000` and the gateway on `:8080`
2. generates a random signing secret for this run
3. mints a viewer token (bob) and an admin token (alice), and writes them to
   `tokens.env`
4. sends three self-check requests: viewer lists tools, viewer is blocked
   from an admin tool, admin is allowed
5. keeps running and prints every gateway decision live. **Ctrl+C** stops both
   servers.

```
 Self-check through the gateway:
   ✅ viewer  tools/list       -> 4 tools (search_docs, get_status, admin_reset_key, admin_delete_user)
   ✅ viewer  admin_reset_key  -> error -32001 Unauthorized Tool Call
   ✅ admin   admin_reset_key  -> 'API key k-42 has been reset (simulated).'
```

Then send your own requests from a second terminal:

```bash
cd task2
source tokens.env                      # loads $VIEWER_TOKEN and $ADMIN_TOKEN
call() { curl -s -H "Authorization: Bearer $1" -H 'Content-Type: application/json' -d "$2" http://127.0.0.1:8080/mcp; echo; }

call $VIEWER_TOKEN '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'                                                                        # forwarded
call $VIEWER_TOKEN '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"k-42"}}}'      # -32001
call $ADMIN_TOKEN  '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"key_id":"k-42"}}}'      # allowed
call ""            '{"jsonrpc":"2.0","id":4,"method":"tools/list"}'                                                                        # no token: -32003 (401)
```

If a port is in use, the script says so. Stop the old process, or run
`python run_demo.py --port 8081 --mock-port 9001`.

### Option 2: each piece by hand

There is no default signing secret, so generate one and share it between
the gateway and the token script:

```bash
openssl rand -hex 32                     # copy the value

# terminal 1: the MCP server
python mock_mcp_server.py --port 9000

# terminal 2: the gateway (refuses to start without a secret of at least 32 bytes)
export GATEWAY_JWT_SECRET=<value>
python gateway.py --port 8080 --upstream http://127.0.0.1:9000/mcp --log-file gateway.jsonl

# terminal 3: tokens, then the same `call` commands as above
export GATEWAY_JWT_SECRET=<value>
eval "$(python create_demo_tokens.py)"   # exports $ADMIN_TOKEN and $VIEWER_TOKEN (valid 1 h)
```

### Record the transcript

```bash
python demo_transcript.py      # rewrites TRANSCRIPT.md and TRANSCRIPT.jsonl
```

This sends 12 scenarios through the gateway and records each JSON-RPC
request, response and audit event in [`TRANSCRIPT.md`](TRANSCRIPT.md):
allowed calls, blocked admin calls, a missing and a forged token, a look-alike
name, invalid JSON and JSON-RPC, a batch, and a notification. It starts its own servers on free ports, and
fails if any scenario gives an unexpected result.

## Tests

```bash
python -m pytest -v            # 125 passed, about 2 s
```

The suite covers:
- **Tokens:** valid, forged, expired, `alg: none`, malformed.
- **Policy:** role checks, and bypass attempts (case, full-width Unicode,
  batches, notifications, duplicate JSON keys).
- **Wire format** and **downstream failures:** down, slow, oversized.
- **The audit log:** one event per request, ids that never mix under
  concurrency, and no token ever logged.
- **The signing-secret checks.**

## Design

### Security decisions

- **Algorithm pinned.** Only HS256 is accepted, so `alg: none` and algorithm
  confusion are rejected. The signature is compared in constant time, and
  expiry is enforced.
- **No default signing secret.** `gateway.py` exits at startup (code 2,
  `config.invalid_secret`) unless `GATEWAY_JWT_SECRET` is set to at least 32
  bytes, the HS256 minimum (RFC 7518 §3.2). A built-in fallback would let
  anyone who has read this repo mint admin tokens. The demos generate a
  random secret for each run instead.
- **Bypasses are blocked:**
  - look-alike tool names (`ADMIN_reset_key`, full-width `ａｄｍｉｎ＿x`),
    caught by NFKC normalization plus casefold
  - admin calls hidden in a batch
  - admin calls sent as notifications
  - duplicate JSON keys
- **What is inspected is what is forwarded.** The gateway re-serializes the
  parsed message rather than passing on the raw bytes, so the MCP server
  can't parse something different from what was checked.

### Error codes

| Code | When |
| --- | --- |
| `-32001` | Unauthorized Tool Call: a non-admin called an `admin_` tool |
| `-32003` | Missing or invalid token (HTTP 401) |
| `-32700` | Body is not valid JSON |
| `-32600` | Not a valid JSON-RPC request, or a batch |
| `-32602` | `tools/call` without a tool name |
| `-32603` | MCP server unreachable (HTTP 502) or too slow (HTTP 504) |

## Logs

The gateway writes a **JSON Lines audit log**: one JSON object per line, on
stderr, and appended to a file with `--log-file`. A log shipper (Loki,
Datadog, CloudWatch, ELK) or a SIEM can ingest it unchanged.

```json
{"ts": "2026-11-05T19:42:05.731Z", "level": "INFO", "logger": "mcp-gateway", "message": "request allowed", "request_id": "req_acb033b13a0e407a", "event": "mcp.allow", "sub": "bob", "role": "viewer", "method": "tools/list", "tool": null, "rpc_id": 1, "notification": false}
{"ts": "2026-11-05T19:42:05.732Z", "level": "WARNING", "logger": "mcp-gateway", "message": "tool call denied", "request_id": "req_d9f85057dc8f4143", "event": "mcp.deny", "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "admin_reset_key", "required_role": "admin", "rpc_id": 2, "notification": false}
```

| `event` | Level | When | Key fields |
| --- | --- | --- | --- |
| `mcp.allow` | INFO | request forwarded | `sub`, `role`, `method`, `tool`, `rpc_id`, `notification` |
| `mcp.deny` | WARNING | `admin_*` tool called without the admin role | `sub`, `role`, `tool`, `required_role`, `rpc_id` |
| `mcp.reject` | WARNING | malformed JSON-RPC (answered by the gateway) | `sub`, `role`, `rpc_code`, `reason`, `rpc_id` |
| `auth.failed` | WARNING | missing, forged, malformed or expired token (401) | `client`, `reason` |
| `downstream.timeout` / `downstream.error` / `downstream.too_large` | ERROR | MCP server slow, down, or answered too much (504 / 502) | `rpc_id`, `error_class` |
| `gateway.started` | INFO | startup | `url`, `upstream` |
| `config.invalid_secret` | CRITICAL | startup refused: secret missing or under 32 bytes (exit code 2) | reason in `message`; never the value |

- **One id, three places.** The `request_id` is in the log line, in the
  `X-Request-ID` response header, and in the `X-Request-ID` header sent to the
  MCP server, so one call can be traced from client to server. Each request's
  id lives in a `ContextVar` for its own thread, so concurrent requests never
  mix ids (tested with 20 at once).
- **Tokens are never logged**, not even their signature (tested). Request
  bodies aren't logged either, only the method and tool name.

```bash
tail -f gateway.jsonl                                      # follow live
jq -c 'select(.event=="mcp.deny")' gateway.jsonl          # every blocked admin call
jq -c 'select(.request_id=="req_d9f85057dc8f4143")' gateway.jsonl
```

## Files

| File | What it is for |
| --- | --- |
| `gateway.py` | **The gateway:** token check, JSON-RPC parsing, admin-tool blocking, forwarding |
| `mock_mcp_server.py` | The MCP server behind the gateway. Four simulated tools: `search_docs`, `get_status`, `admin_reset_key`, `admin_delete_user` |
| `run_demo.py` | **One-command demo:** both servers, both tokens, a self-check, then the live log |
| `create_demo_tokens.py` | Mints a viewer token and an admin token (needs `GATEWAY_JWT_SECRET`) |
| `demo_transcript.py` | Records the 12 scenarios in `TRANSCRIPT.md` and `TRANSCRIPT.jsonl` |
| `TRANSCRIPT.md`, `TRANSCRIPT.jsonl` | The recorded transcript and its audit log |
| `structured_logging.py` | JSON Lines formatter and the per-request id |
| `test_gateway.py` | 125 tests |
| `requirements.txt` | Only `pytest`. Everything else is the standard library |

Created when you run the demos, and git-ignored: `tokens.env` (the two
tokens, permissions 600) and `gateway.jsonl` (your audit log).

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `Attribute "app" not found in module ...` | These aren't ASGI apps. Run `python mock_mcp_server.py` / `python gateway.py`, not `uvicorn` |
| `gateway.py` exits with `config.invalid_secret` | Set `GATEWAY_JWT_SECRET` to at least 32 bytes: `export GATEWAY_JWT_SECRET=$(openssl rand -hex 32)` |
| Every token gets 401 | The token was signed with a different secret (`run_demo.py` makes a new one each run, so `source tokens.env` again), or it expired after 1 h |
| `[Errno 48] Address already in use` | Something is already on that port (`lsof -i :8080`). Stop it, or pass `--port` / `--mock-port` |
