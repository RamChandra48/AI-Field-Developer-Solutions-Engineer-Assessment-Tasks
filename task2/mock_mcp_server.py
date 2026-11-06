#!/usr/bin/env python3
"""Mock downstream MCP server (JSON-RPC 2.0 over HTTP POST).

Stands in for a real MCP server behind the security gateway. It implements
just enough of the protocol to exercise the gateway:

    initialize, notifications/initialized, ping, tools/list, tools/call

All tools are simulated and have no side effects. Every POST it receives is
recorded in ``MockMCPServer.calls`` so tests can assert whether the gateway
did, or did not, reach the downstream server.

    python mock_mcp_server.py --port 9000
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

from structured_logging import configure_logging

log = logging.getLogger("mock-mcp")

MCP_PATH = "/mcp"
PROTOCOL_VERSION = "2025-06-18"
MAX_BODY = 1024 * 1024

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602


def _tool(name: str, description: str, properties: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": list(properties),
            "additionalProperties": False,
        },
    }


TOOLS: List[Dict[str, Any]] = [
    _tool("search_docs", "Search the documentation (simulated).", {"query": {"type": "string"}}),
    _tool("get_status", "Report service health (simulated).", {}),
    _tool("admin_reset_key", "Reset an API key (simulated).", {"key_id": {"type": "string"}}),
    _tool("admin_delete_user", "Delete a user account (simulated).", {"user_id": {"type": "string"}}),
]
_TOOLS_BY_NAME = {t["name"]: t for t in TOOLS}

_RESULTS = {
    "search_docs": lambda a: "Found 3 results for %r (mock data)." % a["query"],
    "get_status": lambda a: "All systems nominal.",
    "admin_reset_key": lambda a: "API key %s has been reset (simulated)." % a["key_id"],
    "admin_delete_user": lambda a: "User %s has been deleted (simulated)." % a["user_id"],
}


def _error(req_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _call_tool(req_id: Any, params: Any) -> Dict[str, Any]:
    if not isinstance(params, dict) or not isinstance(params.get("name"), str):
        return _error(req_id, INVALID_PARAMS, "params.name must be a string")
    name = params["name"]
    tool = _TOOLS_BY_NAME.get(name)
    if tool is None:
        return _error(req_id, INVALID_PARAMS, "Unknown tool: %s" % name)
    args = params.get("arguments", {})
    if not isinstance(args, dict):
        return _error(req_id, INVALID_PARAMS, "params.arguments must be an object")
    required = tool["inputSchema"]["required"]
    if set(args) != set(required) or not all(isinstance(args[k], str) and args[k] for k in required):
        return _error(req_id, INVALID_PARAMS, "Invalid arguments for %s; expected %s" % (name, required))
    text = _RESULTS[name](args)
    return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [{"type": "text", "text": text}], "isError": False}}


def handle_message(msg: Any) -> Optional[Dict[str, Any]]:
    """Return the JSON-RPC response for ``msg``, or None for a notification."""
    if isinstance(msg, list):
        return _error(None, INVALID_REQUEST, "Batch requests are not supported")
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0" or not isinstance(msg.get("method"), str):
        return _error(None, INVALID_REQUEST, "Invalid Request")
    if "id" not in msg:
        return None  # notification: never answered
    req_id, method = msg["id"], msg["method"]
    if method == "initialize":
        result: Dict[str, Any] = {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mock-mcp-server", "version": "1.0.0"},
        }
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": TOOLS}
    elif method == "tools/call":
        return _call_tool(req_id, msg.get("params"))
    else:
        return _error(req_id, METHOD_NOT_FOUND, "Method not found: %s" % method)
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


class MockMCPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: Any) -> None:
        super().__init__(address, _Handler)
        self.calls: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(self, call: Dict[str, Any]) -> None:
        with self._lock:
            self.calls.append(call)


class _Handler(BaseHTTPRequestHandler):
    server_version = "MockMCP/1.0"
    sys_version = ""
    timeout = 15

    def log_message(self, fmt: str, *args: Any) -> None:
        log.debug("%s - %s", self.client_address[0], fmt % args)

    def _send(self, status: int, payload: Optional[Dict[str, Any]]) -> None:
        body = b"" if payload is None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != MCP_PATH:
            return self._send(404, _error(None, INVALID_REQUEST, "Not found"))
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            return self._send(411, _error(None, INVALID_REQUEST, "Content-Length required"))
        if not 0 <= length <= MAX_BODY:
            return self._send(413, _error(None, INVALID_REQUEST, "Payload too large"))
        raw = self.rfile.read(length)

        headers = {k.lower(): v for k, v in self.headers.items()}
        try:
            msg = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError):
            self.server.record({"method": None, "tool": None, "headers": headers, "body": raw})
            return self._send(400, _error(None, PARSE_ERROR, "Parse error"))

        params = msg.get("params") if isinstance(msg, dict) else None
        self.server.record({
            "method": msg.get("method") if isinstance(msg, dict) else None,
            "tool": params.get("name") if isinstance(params, dict) else None,
            "headers": headers,
            "body": raw,
        })
        response = handle_message(msg)
        if response is None:
            return self._send(202, None)
        self._send(400 if "error" in response and response["id"] is None else 200, response)

    def do_GET(self) -> None:
        self.send_response(405)
        self.send_header("Allow", "POST")
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.end_headers()

    do_PUT = do_DELETE = do_PATCH = do_GET


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Mock downstream MCP server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args(argv)
    configure_logging()  # JSON Lines on stderr, like the gateway
    server = MockMCPServer((args.host, args.port))
    log.info("mock MCP server listening", extra={
        "event": "mock.started", "url": "http://%s:%d%s" % (args.host, server.server_address[1], MCP_PATH)})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
