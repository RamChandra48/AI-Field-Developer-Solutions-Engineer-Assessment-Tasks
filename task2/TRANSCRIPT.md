# Task 2 — JSON-RPC Transcript

Real traffic recorded by `python demo_transcript.py` on 2026-11-06 05:57 UTC.
The gateway and mock MCP server ran in-process on free ports. JSON bodies are
pretty-printed for reading; on the wire they were compact JSON with the same content
(scenario 8 is shown as the raw bytes sent). Status lines and gateway audit events are
verbatim. Tokens are shown as labels; the real ones are freshly minted HS256 JWTs.

| # | Scenario | HTTP | JSON-RPC | Reached downstream | Check |
| --- | --- | --- | --- | --- | --- |
| 1 | Viewer lists tools | 200 | result | yes | ✅ |
| 2 | Viewer calls an admin tool | 200 | `-32001` | no | ✅ |
| 3 | Admin calls the same admin tool | 200 | result | yes | ✅ |
| 4 | Viewer calls an ordinary tool | 200 | result | yes | ✅ |
| 5 | Viewer tries a look-alike admin name | 200 | `-32001` | no | ✅ |
| 6 | No token | 401 | `-32003` | no | ✅ |
| 7 | Forged admin token | 401 | `-32003` | no | ✅ |
| 8 | Invalid JSON | 400 | `-32700` | no | ✅ |
| 9 | Invalid JSON-RPC envelope | 400 | `-32600` | no | ✅ |
| 10 | tools/call without a tool name | 200 | `-32602` | no | ✅ |
| 11 | Batch hiding an admin call | 400 | `-32600` | no | ✅ |
| 12 | Admin call sent as a notification | 202 | (none) | no | ✅ |

Error codes: `-32700` parse error, `-32600` invalid request, `-32602` invalid params,
`-32001` Unauthorized Tool Call, `-32003` missing/invalid bearer token.

## 1. Viewer lists tools

`tools/list` is forwarded transparently, for any role.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/list"
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_3aff39b9b81e4e45`

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "tools": [
      {
        "name": "search_docs",
        "description": "Search the documentation (simulated).",
        "inputSchema": {
          "type": "object",
          "properties": {
            "query": {
              "type": "string"
            }
          },
          "required": [
            "query"
          ],
          "additionalProperties": false
        }
      },
      {
        "name": "get_status",
        "description": "Report service health (simulated).",
        "inputSchema": {
          "type": "object",
          "properties": {},
          "required": [],
          "additionalProperties": false
        }
      },
      {
        "name": "admin_reset_key",
        "description": "Reset an API key (simulated).",
        "inputSchema": {
          "type": "object",
          "properties": {
            "key_id": {
              "type": "string"
            }
          },
          "required": [
            "key_id"
          ],
          "additionalProperties": false
        }
      },
      {
        "name": "admin_delete_user",
        "description": "Delete a user account (simulated).",
        "inputSchema": {
          "type": "object",
          "properties": {
            "user_id": {
              "type": "string"
            }
          },
          "required": [
            "user_id"
          ],
          "additionalProperties": false
        }
      }
    ]
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.862Z", "level": "INFO", "logger": "mcp-gateway", "message": "request allowed", "request_id": "req_3aff39b9b81e4e45", "event": "mcp.allow", "sub": "bob", "role": "viewer", "method": "tools/list", "tool": null, "rpc_id": 1, "notification": false}
```

**Downstream MCP server**: received `tools/list`

**Check** (HTTP 200, result, forwarded): ✅ as expected

## 2. Viewer calls an admin tool

`admin_*` needs the admin role, so the gateway answers itself.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "admin_reset_key",
    "arguments": {
      "key_id": "k-42"
    }
  }
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_92a92a9e96dc4e7b`

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "error": {
    "code": -32001,
    "message": "Unauthorized Tool Call",
    "data": {
      "tool": "admin_reset_key",
      "requiredRole": "admin"
    }
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.863Z", "level": "WARNING", "logger": "mcp-gateway", "message": "tool call denied", "request_id": "req_92a92a9e96dc4e7b", "event": "mcp.deny", "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "admin_reset_key", "required_role": "admin", "rpc_id": 2, "notification": false}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 200, error `-32001`, not forwarded): ✅ as expected

## 3. Admin calls the same admin tool

Same request, admin token: forwarded and executed.

**Request**: `POST /mcp`, `Authorization: Bearer <admin token (sub=alice, role=admin)>`

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "admin_reset_key",
    "arguments": {
      "key_id": "k-42"
    }
  }
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_c13bb31f59e8442d`

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "API key k-42 has been reset (simulated)."
      }
    ],
    "isError": false
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.864Z", "level": "INFO", "logger": "mcp-gateway", "message": "request allowed", "request_id": "req_c13bb31f59e8442d", "event": "mcp.allow", "sub": "alice", "role": "admin", "method": "tools/call", "tool": "admin_reset_key", "rpc_id": 3, "notification": false}
```

**Downstream MCP server**: received `tools/call` → `admin_reset_key`

**Check** (HTTP 200, result, forwarded): ✅ as expected

## 4. Viewer calls an ordinary tool

Tools outside the `admin_` prefix are open to any authenticated role.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "id": "req-4",
  "method": "tools/call",
  "params": {
    "name": "search_docs",
    "arguments": {
      "query": "rate limits"
    }
  }
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_aae321003097443b`

```json
{
  "jsonrpc": "2.0",
  "id": "req-4",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "Found 3 results for 'rate limits' (mock data)."
      }
    ],
    "isError": false
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.865Z", "level": "INFO", "logger": "mcp-gateway", "message": "request allowed", "request_id": "req_aae321003097443b", "event": "mcp.allow", "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "search_docs", "rpc_id": "req-4", "notification": false}
```

**Downstream MCP server**: received `tools/call` → `search_docs`

**Check** (HTTP 200, result, forwarded): ✅ as expected

## 5. Viewer tries a look-alike admin name

Names are normalised before the check, so `ADMIN_` is still admin.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "tools/call",
  "params": {
    "name": "ADMIN_delete_user",
    "arguments": {
      "user_id": "u-1"
    }
  }
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_13fec78146f946a7`

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "error": {
    "code": -32001,
    "message": "Unauthorized Tool Call",
    "data": {
      "tool": "ADMIN_delete_user",
      "requiredRole": "admin"
    }
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.866Z", "level": "WARNING", "logger": "mcp-gateway", "message": "tool call denied", "request_id": "req_13fec78146f946a7", "event": "mcp.deny", "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "ADMIN_delete_user", "required_role": "admin", "rpc_id": 5, "notification": false}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 200, error `-32001`, not forwarded): ✅ as expected

## 6. No token

Unauthenticated requests are rejected before the body is inspected.

**Request**: `POST /mcp`, no `Authorization` header

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "tools/list"
}
```

**Response**: `HTTP 401 Unauthorized`, `X-Request-ID: req_8be4630ffec04ba9`

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32003,
    "message": "Missing or invalid bearer token"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.866Z", "level": "WARNING", "logger": "mcp-gateway", "message": "authentication failed", "request_id": "req_8be4630ffec04ba9", "event": "auth.failed", "client": "127.0.0.1", "reason": "no Authorization header"}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 401, error `-32003`, not forwarded): ✅ as expected

## 7. Forged admin token

A token claiming `role: admin` but signed with the wrong secret.

**Request**: `POST /mcp`, `Authorization: Bearer <forged token (claims role=admin, wrong signature)>`

```json
{
  "jsonrpc": "2.0",
  "id": 7,
  "method": "tools/call",
  "params": {
    "name": "admin_reset_key",
    "arguments": {
      "key_id": "k-42"
    }
  }
}
```

**Response**: `HTTP 401 Unauthorized`, `X-Request-ID: req_c69b970cc34b40e9`

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32003,
    "message": "Missing or invalid bearer token"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.866Z", "level": "WARNING", "logger": "mcp-gateway", "message": "authentication failed", "request_id": "req_c69b970cc34b40e9", "event": "auth.failed", "client": "127.0.0.1", "reason": "bad signature"}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 401, error `-32003`, not forwarded): ✅ as expected

## 8. Invalid JSON

The body is not JSON at all.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```text
{"jsonrpc": "2.0", "id": 8, "method": 
```

**Response**: `HTTP 400 Bad Request`, `X-Request-ID: req_7dfddaf7407a4e8d`

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32700,
    "message": "Parse error"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.867Z", "level": "WARNING", "logger": "mcp-gateway", "message": "malformed request rejected", "request_id": "req_7dfddaf7407a4e8d", "event": "mcp.reject", "sub": "bob", "role": "viewer", "rpc_code": -32700, "reason": "Parse error", "rpc_id": null}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 400, error `-32700`, not forwarded): ✅ as expected

## 9. Invalid JSON-RPC envelope

`jsonrpc` must be exactly "2.0". The valid `id` is still echoed.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "1.0",
  "id": 9,
  "method": "tools/list"
}
```

**Response**: `HTTP 400 Bad Request`, `X-Request-ID: req_4d0cd9821c1f4b23`

```json
{
  "jsonrpc": "2.0",
  "id": 9,
  "error": {
    "code": -32600,
    "message": "Invalid Request"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.867Z", "level": "WARNING", "logger": "mcp-gateway", "message": "malformed request rejected", "request_id": "req_4d0cd9821c1f4b23", "event": "mcp.reject", "sub": "bob", "role": "viewer", "rpc_code": -32600, "reason": "Invalid Request", "rpc_id": 9}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 400, error `-32600`, not forwarded): ✅ as expected

## 10. tools/call without a tool name

Without `params.name` the call cannot be authorized, so it is refused.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "method": "tools/call",
  "params": {
    "arguments": {}
  }
}
```

**Response**: `HTTP 200 OK`, `X-Request-ID: req_da0493003ab84998`

```json
{
  "jsonrpc": "2.0",
  "id": 10,
  "error": {
    "code": -32602,
    "message": "params.name must be a non-empty string"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.867Z", "level": "WARNING", "logger": "mcp-gateway", "message": "malformed request rejected", "request_id": "req_da0493003ab84998", "event": "mcp.reject", "sub": "bob", "role": "viewer", "rpc_code": -32602, "reason": "params.name must be a non-empty string", "rpc_id": 10}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 200, error `-32602`, not forwarded): ✅ as expected

## 11. Batch hiding an admin call

Batches are refused outright, so nothing can be smuggled inside one.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
[
  {
    "jsonrpc": "2.0",
    "id": 11,
    "method": "tools/list"
  },
  {
    "jsonrpc": "2.0",
    "id": 12,
    "method": "tools/call",
    "params": {
      "name": "admin_reset_key",
      "arguments": {
        "key_id": "k-42"
      }
    }
  }
]
```

**Response**: `HTTP 400 Bad Request`, `X-Request-ID: req_36117544316945f9`

```json
{
  "jsonrpc": "2.0",
  "id": null,
  "error": {
    "code": -32600,
    "message": "Batch requests are not supported by this gateway"
  }
}
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.868Z", "level": "WARNING", "logger": "mcp-gateway", "message": "malformed request rejected", "request_id": "req_36117544316945f9", "event": "mcp.reject", "sub": "bob", "role": "viewer", "rpc_code": -32600, "reason": "Batch requests are not supported by this gateway", "rpc_id": null}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 400, error `-32600`, not forwarded): ✅ as expected

## 12. Admin call sent as a notification

No `id`, so no reply is allowed. It is still blocked: empty 202.

**Request**: `POST /mcp`, `Authorization: Bearer <viewer token (sub=bob, role=viewer)>`

```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "admin_delete_user",
    "arguments": {
      "user_id": "u-1"
    }
  }
}
```

**Response**: `HTTP 202 Accepted`, `X-Request-ID: req_761650310cf54c99`

```text
(empty body)
```

**Gateway audit log** (same `request_id`):

```json
{"ts": "2026-11-06T05:57:20.868Z", "level": "WARNING", "logger": "mcp-gateway", "message": "tool call denied", "request_id": "req_761650310cf54c99", "event": "mcp.deny", "sub": "bob", "role": "viewer", "method": "tools/call", "tool": "admin_delete_user", "required_role": "admin", "rpc_id": null, "notification": true}
```

**Downstream MCP server**: **not called**

**Check** (HTTP 202, empty body, not forwarded): ✅ as expected

## What the downstream server actually received (scenario 1)

The gateway forwards the message it inspected, re-serialized. It replaces the
client's credentials with identity headers derived from the verified token.

```text
content-type: application/json
accept: application/json, text/event-stream
x-gateway-subject: bob
x-gateway-role: viewer
authorization: (absent: the client token is not passed on)

{"jsonrpc":"2.0","id":1,"method":"tools/list"}
```
