# Task 1 — MCP Customer Service Server

An MCP server, built on the official MCP Python SDK (`mcp` 2.x), that gives an
AI agent two customer-service tools over stdio: look up a customer, and issue
a refund. The tools are designed to be safe for an LLM to call: strict
schemas, exact money, behaviour hints, and refunds that can't be paid twice.

## Flow

```
AI agent / MCP client ──spawns──▶ task1_server.py
        │                              │
        │  stdin:  JSON-RPC requests   │  1. initialize        (protocol handshake)
        │  stdout: JSON-RPC responses  │  2. tools/list        (names, descriptions, schemas, hints)
        │  stderr: logs only           │  3. tools/call        (validate → run → structured result)
        ◀──────────────────────────────┘
```

1. **The client starts the server** as a subprocess and talks MCP over
   stdin/stdout. There is no port.
2. **`tools/list`** returns each tool's description, input schema, output
   schema and behaviour hints. The agent decides how to use a tool, and how
   careful to be with it, from these.
3. **`tools/call`** validates the arguments strictly (Pydantic). It either runs
   the tool and returns `structuredContent`, or answers with a JSON-RPC error.

| Tool | Arguments | Hints sent to the agent |
| --- | --- | --- |
| `get_customer_record` | `customer_id` | read-only, idempotent, not destructive |
| `trigger_refund` | `customer_id`, `amount`, `reason`, optional `idempotency_key` | destructive, not idempotent without a key |

Customer records are mock data, and refunds are simulated.

## Quick start

Python 3.11+ (tested on 3.11, 3.12 and 3.13).

```bash
cd task1
python -m pip install -r requirements.txt     # mcp (official SDK), pydantic, pytest
python demo_task1.py                          # run the demo
python -m pytest -q test_task1.py test_stdio.py   # 40 passed
```

## Commands

### Demo

```bash
python demo_task1.py
```

The demo starts the server as a real MCP client would, then:

1. lists the tools with their hints and descriptions
2. looks up a customer
3. issues a refund with an idempotency key
4. retries the refund with the same key, and gets the same refund back
5. sends invalid input, which is rejected

```
Tool: get_customer_record (read-only=True, destructive=False)
  Look up one customer's record by customer ID. Read-only and safe to retry.
Tool: trigger_refund (read-only=False, destructive=True)
  Issue a refund to a customer (simulated). This moves money: confirm the customer ID and amount first, ...
Customer: {"customer_id": "CUST-12345", "name": "Demo Customer", "mock": true}
Refund: {"refund_id": "14020f7a-...", "customer_id": "CUST-12345", "amount": "12.50", "currency": "USD", "reason": "Duplicate payment", "status": "simulated", "replayed": false, "mock": true}
Retry with same idempotency key: replayed refund 14020f7a-... (no second payment)
Invalid input rejected: -32602 Invalid tool arguments
```

### Use it from an MCP client (optional)

The server is a normal stdio MCP server, so any MCP client can start it. For
example, with the MCP Inspector (needs Node.js 22.23+; check with `node --version`):

```bash
npx @modelcontextprotocol/inspector "$(which python)" task1_server.py
```

This opens a browser UI. Click **Connect**, then **Tools → List Tools**, pick
a tool, fill in the arguments and click **Run Tool**. `"$(which python)"`
makes the Inspector use the Python that has `mcp` installed.

Or add it to a client's server config (Claude Desktop, IDEs). Use the
absolute path:

```json
{"mcpServers": {"customer-service": {"command": "python", "args": ["/absolute/path/to/task1/task1_server.py"]}}}
```

## Tests

```bash
python -m pytest -v test_task1.py test_stdio.py   # 40 passed, about 2 s
```

- `test_task1.py` tests the tools directly: schema validation, exact money
  handling, idempotent replay and conflicts, the bounded key cache, tool
  metadata, and results matching the output schema.
- `test_stdio.py` starts the real server over stdio. It covers the MCP
  handshake, every JSON-RPC error code, recovery after malformed input, and
  checks that nothing but protocol messages reaches stdout.

## Design

### What each tool tells the agent

- **A description and a title.** The refund tool says it moves money and
  asks for an idempotency key.
- **Behaviour hints** (`readOnlyHint`, `destructiveHint`, `idempotentHint`,
  `openWorldHint`). With these, a client can auto-approve the lookup and ask
  a human before a refund.
- **A strict input schema**, with a description and examples on every field.
- **An output schema.** Results come back as `structuredContent`, checked
  against that schema, and as the same JSON in a text block for older clients.

### Validation

- Customer IDs must be `CUST-` followed by exactly five ASCII digits.
- `amount` must be a positive, finite JSON number with **at most 2 decimal
  places**, and at most **10,000 USD**. `0.1 + 0.2`
  (`0.30000000000000004`) is rejected, not rounded. The amount becomes an
  exact `Decimal` before use, and is returned as a string (`"12.50"`) with a
  currency, never as a float.
- `reason` must be 10 to 500 characters after trimming.
- Unexpected arguments and type coercions (`"12.5"`, `true`) are rejected.

### Idempotent refunds

Agents retry after a timeout, a lost connection or a confused plan. Without
protection, every retry of a refund pays out again.

| Call | Result |
| --- | --- |
| Same `idempotency_key`, same arguments | The original refund, with `"replayed": true`. Nothing is paid twice |
| Same key, different arguments | Rejected with `-32602`: a key belongs to one refund |
| No key | A new refund each call |

The last 10,000 keys are held in memory, because refunds here are simulated.
A real server would store them with the payment record.

### Errors

| Code | When |
| --- | --- |
| `-32700` | Malformed JSON |
| `-32600` | Invalid request envelope |
| `-32601` | Unknown JSON-RPC method |
| `-32602` | Invalid tool arguments, an unknown tool, or an idempotency key reused for a different refund |

Protocol messages go to stdout and application logs to stderr, so a log line
can never corrupt the protocol stream.

## Files

| File | What it is for |
| --- | --- |
| `task1_server.py` | **The MCP server:** tool definitions, schemas, validation, idempotency, error mapping |
| `demo_task1.py` | The demo client: starts the server over stdio and exercises both tools |
| `test_task1.py` | Unit tests for the tools and their schemas |
| `test_stdio.py` | End-to-end tests against the real server process over stdio |
| `requirements.txt`, `pytest.ini` | Packages and test settings |

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `ImportError: cannot import name 'MCPError' from 'mcp'` | An old 1.x SDK is installed. Run `python -m pip install -r requirements.txt` (needs `mcp>=2.0`) |
| `ModuleNotFoundError: mcp` | Install the requirements into the Python you run with |
| Running `python task1_server.py` seems to hang | That's expected: it waits for an MCP client on stdin. Use `demo_task1.py` or an MCP client |
