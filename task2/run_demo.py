#!/usr/bin/env python3
"""Run the whole Task 2 demo with one command.

    python run_demo.py

This script:
  1. starts the mock MCP server (downstream, :9000) and the gateway (:8080)
  2. mints two tokens, viewer (sub=bob) and admin (sub=alice), and writes them
     to tokens.env
  3. runs a quick self-check through the gateway (viewer is denied an admin
     tool, admin is allowed)
  4. keeps running and prints every gateway decision live, also appending it
     to gateway.jsonl (JSON Lines), until you press Ctrl+C

Then, from any other terminal:

    source task2/tokens.env
    curl -s -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \\
         -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' http://127.0.0.1:8080/mcp
"""
from __future__ import annotations

import argparse
import errno
import http.client
import json
import os
import secrets
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

from gateway import GATEWAY_PATH, issue_token, make_server
from mock_mcp_server import MCP_PATH, MockMCPServer
from structured_logging import configure_logging

HERE = Path(__file__).resolve().parent


def display(path: Path) -> str:
    """Path relative to the current directory if it is inside it, else absolute."""
    try:
        relative = os.path.relpath(path)
    except ValueError:
        return str(path)
    return str(path.resolve()) if relative.startswith("..") else relative


def bind(factory, what: str, port: int, flag: str):
    try:
        return factory()
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            sys.exit(
                "error: port %d (%s) is already in use. Something, maybe an earlier run, is listening there.\n"
                "       Find it with `lsof -nP -iTCP:%d -sTCP:LISTEN` and stop it, or pick another port: %s <port>"
                % (port, what, port, flag)
            )
        raise


def call(port: int, token: str, body: Any) -> Tuple[int, Any]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        conn.request("POST", GATEWAY_PATH, body=json.dumps(body),
                     headers={"Content-Type": "application/json", "Authorization": "Bearer " + token})
        response = conn.getresponse()
        return response.status, json.loads(response.read() or b"null")
    finally:
        conn.close()


def self_check(port: int, viewer: str, admin: str) -> List[str]:
    admin_call = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                  "params": {"name": "admin_reset_key", "arguments": {"key_id": "k-42"}}}
    results = []

    _, body = call(port, viewer, {"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tools = [t["name"] for t in body.get("result", {}).get("tools", [])]
    results.append(("%s viewer  tools/list       -> %d tools (%s)" % ("✅" if tools else "❌", len(tools), ", ".join(tools))))

    _, body = call(port, viewer, admin_call)
    code = body.get("error", {}).get("code")
    results.append("%s viewer  admin_reset_key  -> %s" % ("✅" if code == -32001 else "❌", "error %s Unauthorized Tool Call" % code if code else body))

    _, body = call(port, admin, admin_call)
    text = (body.get("result", {}).get("content") or [{}])[0].get("text")
    results.append("%s admin   admin_reset_key  -> %s" % ("✅" if text else "❌", repr(text) if text else body))
    return results


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Start the mock MCP server and the gateway, and mint demo tokens")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080, help="gateway port (default: %(default)s)")
    parser.add_argument("--mock-port", type=int, default=9000, help="mock MCP server port (default: %(default)s)")
    parser.add_argument("--ttl", type=int, default=3600, help="token lifetime in seconds (default: %(default)s)")
    parser.add_argument("--log-file", default=str(HERE / "gateway.jsonl"), help="JSON Lines audit log, appended (default: task2/gateway.jsonl)")
    parser.add_argument("--tokens-file", default=str(HERE / "tokens.env"), help="where to write the tokens (default: task2/tokens.env)")
    parser.add_argument("--no-check", action="store_true", help="skip the startup self-check")
    args = parser.parse_args(argv)

    configure_logging(args.log_file)  # JSON Lines, the same format as gateway.py
    # No shared default: a fresh random secret per run unless you supply one.
    # The tokens it signs are written to tokens.env, so nothing else needs it.
    secret = os.environ.get("GATEWAY_JWT_SECRET") or secrets.token_hex(32)

    # 1. servers (each binds its port immediately, so conflicts surface here)
    mock = bind(lambda: MockMCPServer((args.host, args.mock_port)), "mock MCP server", args.mock_port, "--mock-port")
    upstream = "http://%s:%d%s" % (args.host, args.mock_port, MCP_PATH)
    gw = bind(lambda: make_server(args.host, args.port, upstream, secret), "gateway", args.port, "--port")
    for server in (mock, gw):
        threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True).start()

    # 2. tokens
    viewer = issue_token("bob", "viewer", secret, args.ttl)
    admin = issue_token("alice", "admin", secret, args.ttl)
    tokens_file = Path(args.tokens_file)
    tokens_file.write_text("export VIEWER_TOKEN=%s\nexport ADMIN_TOKEN=%s\n" % (viewer, admin), encoding="utf-8")
    tokens_file.chmod(0o600)
    expires = time.strftime("%H:%M", time.localtime(time.time() + args.ttl))

    # 3. self-check
    checks = [] if args.no_check else self_check(args.port, viewer, admin)

    gateway_url = "http://%s:%d%s" % (args.host, args.port, GATEWAY_PATH)
    lines = [
        "",
        "=" * 78,
        " MCP security gateway demo is running",
        "=" * 78,
        " gateway      %s   <- point clients here" % gateway_url,
        " downstream   %s   (mock MCP server, where the tools live)" % upstream,
        " audit log    %s" % display(Path(args.log_file)),
        " tokens       %s   (expire at %s)" % (display(tokens_file), expires),
        "",
        " VIEWER token  (sub=bob, role=viewer):",
        "   " + viewer,
        "",
        " ADMIN token   (sub=alice, role=admin):",
        "   " + admin,
    ]
    if checks:
        lines += ["", " Self-check through the gateway:"] + ["   " + c for c in checks]
    lines += [
        "",
        " Try it from another terminal:",
        "   source %s" % display(tokens_file),
        "   curl -s -H \"Authorization: Bearer $VIEWER_TOKEN\" -H 'Content-Type: application/json' \\",
        "        -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"admin_reset_key\",\"arguments\":{\"key_id\":\"k-42\"}}}' \\",
        "        %s" % gateway_url,
        "",
        "   MCP Inspector UI:  npx @modelcontextprotocol/inspector",
        "     Transport: Streamable HTTP   URL: %s" % gateway_url,
        "     Header:    Authorization: Bearer <paste a token from above>",
        "",
        " Gateway audit events (mcp.allow / mcp.deny / mcp.reject / auth.failed) appear below",
        " as JSON Lines, one per request. Ctrl+C to stop.",
        "=" * 78,
        "",
    ]
    print("\n".join(lines), flush=True)

    # 4. run until Ctrl+C (SIGINT) or `kill` (SIGTERM); both shut down cleanly
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda signum, frame: stop.set())
    try:
        while not stop.wait(0.5):
            pass
    except KeyboardInterrupt:
        print("\nstopping...", flush=True)
    finally:
        gw.shutdown()
        mock.shutdown()
        gw.server_close()
        mock.server_close()
    print("stopped. Tokens in %s stay valid until %s." % (display(tokens_file), expires))
    return 1 if any(c.startswith("❌") for c in checks) else 0


if __name__ == "__main__":
    sys.exit(main())
