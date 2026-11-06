#!/usr/bin/env python3
"""Record real JSON-RPC traffic through the gateway into TRANSCRIPT.md.

Starts the mock MCP server and the gateway in-process on free ports, sends one
request per scenario, and writes down for each:

  * the exact request body and which token was used
  * the HTTP status and response body
  * the gateway's audit event (JSON Lines: mcp.allow / mcp.deny / mcp.reject / auth.failed)
  * what the downstream server received (proof that blocked calls never arrive)

Every scenario is also checked against its expected result. The script exits
non-zero if any check fails, so it doubles as a smoke test.

    python demo_transcript.py                  # writes TRANSCRIPT.md and TRANSCRIPT.jsonl
    python demo_transcript.py --stdout         # print instead
"""
from __future__ import annotations

import argparse
import http.client
import json
import logging
import sys
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

from gateway import issue_token, make_server
from mock_mcp_server import MockMCPServer
from structured_logging import JsonFormatter

SECRET = secrets.token_hex(32)  # per run; the servers live only as long as this script


@dataclass
class Scenario:
    title: str
    why: str
    token: Optional[str]  # "viewer", "admin", "forged" or None
    body: Union[bytes, Any]  # JSON-serializable, or raw bytes sent as-is
    expect_status: int
    expect_code: Optional[int]  # JSON-RPC error code, None for a successful result
    expect_forwarded: bool


SCENARIOS: List[Scenario] = [
    Scenario("Viewer lists tools", "`tools/list` is forwarded transparently, for any role.",
             "viewer", {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, 200, None, True),
    Scenario("Viewer calls an admin tool", "`admin_*` needs the admin role, so the gateway answers itself.",
             "viewer", {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                        "params": {"name": "admin_reset_key", "arguments": {"key_id": "k-42"}}}, 200, -32001, False),
    Scenario("Admin calls the same admin tool", "Same request, admin token: forwarded and executed.",
             "admin", {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                       "params": {"name": "admin_reset_key", "arguments": {"key_id": "k-42"}}}, 200, None, True),
    Scenario("Viewer calls an ordinary tool", "Tools outside the `admin_` prefix are open to any authenticated role.",
             "viewer", {"jsonrpc": "2.0", "id": "req-4", "method": "tools/call",
                        "params": {"name": "search_docs", "arguments": {"query": "rate limits"}}}, 200, None, True),
    Scenario("Viewer tries a look-alike admin name", "Names are normalised before the check, so `ADMIN_` is still admin.",
             "viewer", {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                        "params": {"name": "ADMIN_delete_user", "arguments": {"user_id": "u-1"}}}, 200, -32001, False),
    Scenario("No token", "Unauthenticated requests are rejected before the body is inspected.",
             None, {"jsonrpc": "2.0", "id": 6, "method": "tools/list"}, 401, -32003, False),
    Scenario("Forged admin token", "A token claiming `role: admin` but signed with the wrong secret.",
             "forged", {"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                        "params": {"name": "admin_reset_key", "arguments": {"key_id": "k-42"}}}, 401, -32003, False),
    Scenario("Invalid JSON", "The body is not JSON at all.",
             "viewer", b'{"jsonrpc": "2.0", "id": 8, "method": ', 400, -32700, False),
    Scenario("Invalid JSON-RPC envelope", "`jsonrpc` must be exactly \"2.0\". The valid `id` is still echoed.",
             "viewer", {"jsonrpc": "1.0", "id": 9, "method": "tools/list"}, 400, -32600, False),
    Scenario("tools/call without a tool name", "Without `params.name` the call cannot be authorized, so it is refused.",
             "viewer", {"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": {"arguments": {}}}, 200, -32602, False),
    Scenario("Batch hiding an admin call", "Batches are refused outright, so nothing can be smuggled inside one.",
             "viewer", [{"jsonrpc": "2.0", "id": 11, "method": "tools/list"},
                        {"jsonrpc": "2.0", "id": 12, "method": "tools/call",
                         "params": {"name": "admin_reset_key", "arguments": {"key_id": "k-42"}}}], 400, -32600, False),
    Scenario("Admin call sent as a notification", "No `id`, so no reply is allowed. It is still blocked: empty 202.",
             "viewer", {"jsonrpc": "2.0", "method": "tools/call",
                        "params": {"name": "admin_delete_user", "arguments": {"user_id": "u-1"}}}, 202, None, False),
]


class LogCapture(logging.Handler):
    """Keeps each audit event exactly as the gateway writes it (one JSON line)."""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JsonFormatter())
        self.lines: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))


def start(server) -> None:
    threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()


def post(port: int, body: bytes, token: Optional[str]) -> Tuple[int, str, bytes, str]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request("POST", "/mcp", body=body, headers=headers)
        response = conn.getresponse()
        return response.status, response.reason, response.read(), response.getheader("X-Request-ID", "")
    finally:
        conn.close()


def pretty(data: bytes) -> Tuple[str, Any]:
    """(text for display, parsed JSON or None)."""
    try:
        parsed = json.loads(data)
    except ValueError:
        return data.decode("utf-8", "replace"), None
    return json.dumps(parsed, indent=2, ensure_ascii=False), parsed


def check(s: Scenario, status: int, parsed: Any, forwarded: bool) -> List[str]:
    problems = []
    if status != s.expect_status:
        problems.append("HTTP %d, expected %d" % (status, s.expect_status))
    code = parsed.get("error", {}).get("code") if isinstance(parsed, dict) else None
    if s.expect_code is not None and code != s.expect_code:
        problems.append("error code %r, expected %d" % (code, s.expect_code))
    if s.expect_code is None and s.expect_status == 200 and (not isinstance(parsed, dict) or "result" not in parsed):
        problems.append("expected a JSON-RPC result")
    if forwarded != s.expect_forwarded:
        problems.append("downstream %s, expected %s" % ("called" if forwarded else "not called",
                                                         "called" if s.expect_forwarded else "not called"))
    return problems


def run(log_path: Optional[str] = None) -> Tuple[str, bool]:
    mock = MockMCPServer(("127.0.0.1", 0))
    gw = make_server("127.0.0.1", 0, "http://127.0.0.1:%d/mcp" % mock.server_address[1], SECRET)
    start(mock)
    start(gw)

    capture = LogCapture()
    gateway_log = logging.getLogger("mcp-gateway")
    gateway_log.addHandler(capture)
    gateway_log.setLevel(logging.INFO)
    gateway_log.propagate = False
    raw_log = None
    if log_path:
        # The gateway's own log output, unedited, in the same JSON Lines format as `gateway.py`.
        raw_log = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        raw_log.setFormatter(JsonFormatter())
        gateway_log.addHandler(raw_log)

    tokens = {
        "viewer": issue_token("bob", "viewer", SECRET),
        "admin": issue_token("alice", "admin", SECRET),
        "forged": issue_token("mallory", "admin", "attacker-guessed-secret"),
    }
    token_labels = {
        "viewer": "viewer token (sub=bob, role=viewer)",
        "admin": "admin token (sub=alice, role=admin)",
        "forged": "forged token (claims role=admin, wrong signature)",
    }

    sections, summary, all_ok = [], [], True
    first_forwarded = None
    try:
        for number, s in enumerate(SCENARIOS, 1):
            raw = s.body if isinstance(s.body, bytes) else json.dumps(s.body).encode()
            capture.lines.clear()
            calls_before = len(mock.calls)
            status, reason, data, request_id = post(gw.server_address[1], raw, tokens.get(s.token) if s.token else None)
            received = mock.calls[calls_before:]
            response_text, parsed = pretty(data)
            problems = check(s, status, parsed, bool(received))
            if not capture.lines or any(json.loads(line).get("request_id") != request_id for line in capture.lines):
                problems.append("audit event missing or not tagged with this request's X-Request-ID")
            all_ok &= not problems
            if received and first_forwarded is None:
                first_forwarded = (number, received[0])

            auth = "`Authorization: Bearer <%s>`" % token_labels[s.token] if s.token else "no `Authorization` header"
            request_text = raw.decode() if isinstance(s.body, bytes) else json.dumps(s.body, indent=2)
            downstream = ("received `%s`%s" % (received[0]["method"], " → `%s`" % received[0]["tool"] if received[0]["tool"] else "")
                          if received else "**not called**")
            verdict = "✅ as expected" if not problems else "❌ " + "; ".join(problems)
            expected_desc = "HTTP %d, %s, %s" % (
                s.expect_status,
                "error `%d`" % s.expect_code if s.expect_code is not None else ("empty body" if s.expect_status == 202 else "result"),
                "forwarded" if s.expect_forwarded else "not forwarded")

            sections.append("\n".join([
                "## %d. %s" % (number, s.title),
                "",
                s.why,
                "",
                "**Request**: `POST /mcp`, %s" % auth,
                "",
                "```%s" % ("text" if isinstance(s.body, bytes) else "json"),
                request_text,
                "```",
                "",
                "**Response**: `HTTP %d %s`, `X-Request-ID: %s`" % (status, reason, request_id),
                "",
                "```json" if parsed is not None else "```text",
                response_text if data else "(empty body)",
                "```",
                "",
                "**Gateway audit log** (same `request_id`):",
                "",
                "```json",
                "\n".join(capture.lines) or "(none)",
                "```",
                "",
                "**Downstream MCP server**: " + downstream,
                "",
                "**Check** (%s): %s" % (expected_desc, verdict),
            ]))
            summary.append("| %d | %s | %d | %s | %s | %s |" % (
                number, s.title, status,
                "`%s`" % parsed["error"]["code"] if isinstance(parsed, dict) and "error" in parsed else "result" if data else "(none)",
                "yes" if received else "no",
                "✅" if not problems else "❌"))
    finally:
        gw.shutdown()
        mock.shutdown()
        gw.server_close()
        mock.server_close()
        gateway_log.removeHandler(capture)
        if raw_log is not None:
            gateway_log.removeHandler(raw_log)
            raw_log.close()

    if first_forwarded:
        number, call = first_forwarded
        hdrs = call["headers"]
        shown = {k: hdrs[k] for k in ("content-type", "accept", "x-gateway-subject", "x-gateway-role") if k in hdrs}
        shown["authorization"] = hdrs.get("authorization", "(absent: the client token is not passed on)")
        sections.append("\n".join([
            "## What the downstream server actually received (scenario %d)" % number,
            "",
            "The gateway forwards the message it inspected, re-serialized. It replaces the",
            "client's credentials with identity headers derived from the verified token.",
            "",
            "```text",
            "\n".join("%s: %s" % (k, v) for k, v in shown.items()),
            "",
            call["body"].decode(),
            "```",
        ]))

    header = "\n".join([
        "# Task 2 — JSON-RPC Transcript",
        "",
        "Real traffic recorded by `python demo_transcript.py` on %s." % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "The gateway and mock MCP server ran in-process on free ports. JSON bodies are",
        "pretty-printed for reading; on the wire they were compact JSON with the same content",
        "(scenario 8 is shown as the raw bytes sent). Status lines and gateway audit events are",
        "verbatim. Tokens are shown as labels; the real ones are freshly minted HS256 JWTs.",
        "",
        "| # | Scenario | HTTP | JSON-RPC | Reached downstream | Check |",
        "| --- | --- | --- | --- | --- | --- |",
        *summary,
        "",
        "Error codes: `-32700` parse error, `-32600` invalid request, `-32602` invalid params,",
        "`-32001` Unauthorized Tool Call, `-32003` missing/invalid bearer token.",
    ])
    return header + "\n\n" + "\n\n".join(sections) + "\n", all_ok


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Record gateway JSON-RPC traffic into a Markdown transcript")
    parser.add_argument("--output", default=str(Path(__file__).with_name("TRANSCRIPT.md")))
    parser.add_argument("--log", default=str(Path(__file__).with_name("TRANSCRIPT.jsonl")),
                        help="the gateway's JSON Lines log for the same run (default: %(default)s)")
    parser.add_argument("--stdout", action="store_true", help="print the transcript instead of writing files")
    args = parser.parse_args(argv)

    text, ok = run(None if args.stdout else args.log)
    if args.stdout:
        print(text)
    else:
        Path(args.output).write_text(text, encoding="utf-8")
        print("wrote %s and %s (%d scenarios, %s)" % (args.output, args.log, len(SCENARIOS),
                                                     "all checks passed" if ok else "SOME CHECKS FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
