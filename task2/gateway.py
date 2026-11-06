#!/usr/bin/env python3
"""MCP security gateway: a reverse proxy with bearer-token auth and per-tool authorization.

    AI agent client --POST /mcp--> gateway --POST /mcp--> downstream MCP server
                      Bearer <jwt>           (only if allowed)

For every request the gateway:

  1. authenticates the ``Authorization: Bearer <token>`` header (HS256 JWT) and
     extracts the caller's role;
  2. parses the body as a JSON-RPC 2.0 message;
  3. if ``method == "tools/call"``, authorizes ``params.name`` against
     TOOL_ROLE_POLICY (``admin_*`` tools require the ``admin`` role). Denied calls
     are answered by the gateway with JSON-RPC error -32001 and never reach the
     downstream server;
  4. forwards everything else (tools/list, initialize, ping, ...) unchanged.

Every decision is written as one JSON Lines audit event (structured_logging.py)
tagged with a request id, which is also returned as ``X-Request-ID`` and passed
to the downstream server so one call can be traced across both.

Only the standard library is used. Run ``python gateway.py --help``.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import http.client
import json
import logging
import os
import re
import socket
import sys
import time
import unicodedata
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from structured_logging import configure_logging, current_request_id, new_request_id

log = logging.getLogger("mcp-gateway")

GATEWAY_PATH = "/mcp"
MAX_BODY = 1024 * 1024  # request bytes
MAX_RESPONSE = 10 * 1024 * 1024  # downstream response bytes
# RFC 7518 §3.2: an HS256 key must be at least as long as the hash output (256 bits).
MIN_SECRET_BYTES = 32

# JSON-RPC error codes. -32700..-32600 are defined by JSON-RPC 2.0; -32000..-32099
# is the implementation-defined range.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
UNAUTHORIZED_TOOL_CALL = -32001  # required by the spec of this task
UNAUTHENTICATED = -32003  # gateway-specific: missing/invalid bearer token

# Authorization policy: (tool-name prefix, role required to call it).
# Tools matching no prefix are callable by any authenticated user.
TOOL_ROLE_POLICY: Tuple[Tuple[str, str], ...] = (("admin_", "admin"),)

# Request headers copied to the downstream server. Authorization is deliberately
# absent: the client's token is for the gateway, not for the downstream server.
_FORWARDED_REQUEST_HEADERS = ("Accept", "Mcp-Session-Id", "MCP-Protocol-Version")
REQUEST_ID_HEADER = "X-Request-ID"
_FORWARDED_RESPONSE_HEADERS = ("Content-Type", "Mcp-Session-Id")
_SAFE_HEADER_VALUE = re.compile(r"[\x21-\x7e][\x20-\x7e]{0,511}")


# --------------------------------------------------------------------------- tokens
class TokenError(Exception):
    """The bearer token is missing, malformed, forged, or expired."""


class Claims(NamedTuple):
    sub: str
    role: str
    exp: float


_B64URL = re.compile(r"[A-Za-z0-9_-]+")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(segment: str) -> bytes:
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def _sign(signing_input: bytes, secret: str) -> bytes:
    return hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()


def issue_token(sub: str, role: str, secret: str, ttl_seconds: int = 3600, now: Optional[float] = None) -> str:
    """Create an HS256 JWT carrying ``sub``, ``role`` and an expiry."""
    issued = int(time.time() if now is None else now)
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"sub": sub, "role": role, "iat": issued, "exp": issued + ttl_seconds}
    signing_input = "%s.%s" % tuple(
        _b64url_encode(json.dumps(part, separators=(",", ":")).encode("utf-8")) for part in (header, payload)
    )
    return "%s.%s" % (signing_input, _b64url_encode(_sign(signing_input.encode("ascii"), secret)))


def verify_token(token: str, secret: str, now: Optional[float] = None) -> Claims:
    """Validate ``token`` and return its claims, or raise TokenError."""
    parts = token.split(".")
    if len(parts) != 3 or not all(_B64URL.fullmatch(p) for p in parts):
        raise TokenError("malformed token")
    header_b64, payload_b64, signature_b64 = parts
    try:
        header = json.loads(_b64url_decode(header_b64))
        # Pin the algorithm: never honour the token's own claim of "none" or a
        # different algorithm.
        if not isinstance(header, dict) or header.get("alg") != "HS256":
            raise TokenError("unsupported algorithm")
        expected = _sign(("%s.%s" % (header_b64, payload_b64)).encode("ascii"), secret)
        if not hmac.compare_digest(expected, _b64url_decode(signature_b64)):
            raise TokenError("bad signature")
        payload = json.loads(_b64url_decode(payload_b64))  # parsed only after the signature checks out
    except ValueError as exc:  # bad base64, bad UTF-8, bad JSON
        raise TokenError("malformed token") from exc

    if not isinstance(payload, dict):
        raise TokenError("malformed payload")
    sub, role, exp = payload.get("sub"), payload.get("role"), payload.get("exp")
    if not isinstance(sub, str) or not sub:
        raise TokenError("missing sub claim")
    if not isinstance(role, str) or not role:
        raise TokenError("missing role claim")
    if isinstance(exp, bool) or not isinstance(exp, (int, float)):
        raise TokenError("missing exp claim")
    if not exp > (time.time() if now is None else now):  # also rejects NaN
        raise TokenError("token expired")
    return Claims(sub=sub, role=role, exp=float(exp))


# ------------------------------------------------------------------ authorization
def required_role(tool_name: str) -> Optional[str]:
    """Return the role needed to call ``tool_name``, or None if any user may.

    The name is NFKC-normalised, stripped and case-folded before matching, so
    look-alikes such as "ADMIN_reset_key", " admin_x" or full-width "ａｄｍｉｎ＿x"
    cannot slip past a downstream server that is lenient about tool names.
    This errs on the side of denying.
    """
    normalized = unicodedata.normalize("NFKC", tool_name).strip().casefold()
    for prefix, role in TOOL_ROLE_POLICY:
        if normalized.startswith(prefix):
            return role
    return None


# ----------------------------------------------------------------- JSON-RPC layer
def rpc_error(req_id: Any, code: int, message: str, data: Any = None) -> Dict[str, Any]:
    error: Dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": error}


def _dumps(obj: Any) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def _reject_constant(name: str) -> Any:
    raise ValueError("invalid JSON constant: %s" % name)  # NaN / Infinity


class Outcome(NamedTuple):
    """What to do with one inspected request.

    ``forward`` set   -> send these bytes downstream and relay the answer.
    ``forward`` None  -> the gateway answers itself with ``status``/``body``.
    """

    forward: Optional[bytes] = None
    status: int = 200
    body: Optional[bytes] = None
    request_id: Any = None


def _reply(status: int, req_id: Any, code: int, message: str, data: Any = None) -> Outcome:
    return Outcome(None, status, _dumps(rpc_error(req_id, code, message, data)), req_id)


def _reject(claims: Claims, status: int, req_id: Any, code: int, message: str) -> Outcome:
    """A malformed request: answered by the gateway and audited like allow/deny (event mcp.reject)."""
    log.warning("malformed request rejected", extra={
        "event": "mcp.reject", "sub": claims.sub, "role": claims.role,
        "rpc_code": code, "reason": message, "rpc_id": req_id})
    return _reply(status, req_id, code, message)


def _valid_id(value: Any) -> bool:
    return isinstance(value, (str, int)) and not isinstance(value, bool)


def inspect_request(raw: bytes, claims: Claims) -> Outcome:
    """Parse one JSON-RPC message, apply the tool policy, decide what happens next.

    This is a pure function of its inputs (apart from logging), so the whole
    policy can be unit-tested without sockets.
    """
    try:
        msg = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return _reject(claims, 400, None, PARSE_ERROR, "Parse error")

    if isinstance(msg, list):
        # A batch could hide an admin_* call among innocent ones; rather than
        # half-support it, refuse (MCP 2025-06-18 removed batching anyway).
        return _reject(claims, 400, None, INVALID_REQUEST, "Batch requests are not supported by this gateway")
    if not isinstance(msg, dict):
        return _reject(claims, 400, None, INVALID_REQUEST, "Invalid Request")

    is_notification = "id" not in msg
    req_id = None if is_notification else msg["id"]
    if (
        msg.get("jsonrpc") != "2.0"
        or not isinstance(msg.get("method"), str)
        or not (is_notification or _valid_id(req_id))
        or ("params" in msg and not isinstance(msg["params"], (dict, list)))
    ):
        return _reject(claims, 400, req_id if _valid_id(req_id) else None, INVALID_REQUEST, "Invalid Request")

    method: str = msg["method"]
    tool: Optional[str] = None

    if method == "tools/call":
        params = msg.get("params")
        tool = params.get("name") if isinstance(params, dict) else None
        if not isinstance(tool, str) or not tool:
            return _no_reply_for_notifications(
                is_notification, _reject(claims, 200, req_id, INVALID_PARAMS, "params.name must be a non-empty string")
            )
        needed = required_role(tool)
        if needed is not None and claims.role != needed:
            log.warning("tool call denied", extra={
                "event": "mcp.deny", "sub": claims.sub, "role": claims.role, "method": method,
                "tool": tool, "required_role": needed, "rpc_id": req_id, "notification": is_notification})
            return _no_reply_for_notifications(
                is_notification,
                _reply(200, req_id, UNAUTHORIZED_TOOL_CALL, "Unauthorized Tool Call",
                       {"tool": tool, "requiredRole": needed}),
            )

    log.info("request allowed", extra={
        "event": "mcp.allow", "sub": claims.sub, "role": claims.role, "method": method,
        "tool": tool, "rpc_id": req_id, "notification": is_notification})
    # Forward what was inspected, re-serialised, never the raw bytes. A body with
    # duplicate keys ({"method":"tools/list","method":"tools/call"}) is parsed
    # one way here and might be parsed another way downstream; this removes the
    # ambiguity.
    return Outcome(forward=_dumps(msg), request_id=req_id)


def _no_reply_for_notifications(is_notification: bool, outcome: Outcome) -> Outcome:
    """JSON-RPC forbids answering a notification, so a denied one gets an empty 202."""
    return Outcome(None, 202, None, None) if is_notification else outcome


# ------------------------------------------------------------------- HTTP layer
class GatewayConfig(NamedTuple):
    upstream_scheme: str
    upstream_host: str
    upstream_port: int
    upstream_path: str
    secret: str
    timeout: float
    max_body: int


def parse_upstream(url: str) -> Tuple[str, str, int, str]:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ValueError("upstream must be an http(s) URL, got %r" % url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = parts.path or "/"
    return parts.scheme, parts.hostname, port, path + ("?" + parts.query if parts.query else "")


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: Tuple[str, int], config: GatewayConfig) -> None:
        super().__init__(address, GatewayHandler)
        self.config = config


class GatewayHandler(BaseHTTPRequestHandler):
    server: GatewayServer
    server_version = "MCPGateway/1.0"
    sys_version = ""
    timeout = 15  # per-socket read timeout, so a stalled client cannot pin a thread

    def log_message(self, fmt: str, *args: Any) -> None:
        log.debug("%s - %s", self.client_address[0], fmt % args)

    def parse_request(self) -> bool:
        new_request_id()  # every request, whatever its method, gets an id first
        return super().parse_request()

    def end_headers(self) -> None:
        rid = current_request_id()
        if rid is not None:
            self.send_header(REQUEST_ID_HEADER, rid)
        super().end_headers()

    # -- responses
    def _send(self, status: int, body: Optional[bytes], headers: Sequence[Tuple[str, str]] = ()) -> None:
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json")
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body or b"")))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # client went away

    def _send_error(self, status: int, code: int, message: str, headers: Sequence[Tuple[str, str]] = ()) -> None:
        self._send(status, _dumps(rpc_error(None, code, message)), headers)

    def _method_not_allowed(self) -> None:
        self._send_error(405, INVALID_REQUEST, "Method not allowed; use POST", [("Allow", "POST")])

    do_GET = do_PUT = do_DELETE = do_PATCH = _method_not_allowed

    # -- request pipeline
    def do_POST(self) -> None:
        config = self.server.config
        if urlsplit(self.path).path != GATEWAY_PATH:
            return self._send_error(404, INVALID_REQUEST, "Not found")

        body = self._read_body(config.max_body)
        if body is None:
            return  # _read_body already responded

        claims = self._authenticate(config.secret)
        if claims is None:
            return self._send_error(
                401, UNAUTHENTICATED, "Missing or invalid bearer token",
                [("WWW-Authenticate", 'Bearer realm="mcp-gateway"')],
            )

        outcome = inspect_request(body, claims)
        if outcome.forward is None:
            return self._send(outcome.status, outcome.body)
        self._forward(config, outcome, claims)

    def _read_body(self, limit: int) -> Optional[bytes]:
        if "Transfer-Encoding" in self.headers:
            self._send_error(400, INVALID_REQUEST, "Transfer-Encoding is not supported")
            return None
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            self._send_error(411, INVALID_REQUEST, "Content-Length required")
            return None
        if not re.fullmatch(r"[0-9]{1,12}", length_header):
            self._send_error(400, INVALID_REQUEST, "Invalid Content-Length")
            return None
        length = int(length_header)
        if length > limit:
            self._send_error(413, INVALID_REQUEST, "Payload too large")
            return None
        try:
            body = self.rfile.read(length)
        except OSError:  # includes read timeout
            self.close_connection = True
            return None
        if len(body) != length:
            self._send_error(400, INVALID_REQUEST, "Incomplete request body")
            return None
        return body

    def _authenticate(self, secret: str) -> Optional[Claims]:
        values = self.headers.get_all("Authorization") or []
        if len(values) != 1:
            self._auth_failed("no Authorization header" if not values else "multiple Authorization headers")
            return None
        parts = values[0].split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            self._auth_failed("not a Bearer credential")
            return None
        try:
            return verify_token(parts[1], secret)
        except TokenError as exc:
            self._auth_failed(str(exc))
            return None

    def _auth_failed(self, reason: str) -> None:
        log.warning("authentication failed", extra={
            "event": "auth.failed", "client": self.client_address[0], "reason": reason})

    def _forward(self, config: GatewayConfig, outcome: Outcome, claims: Claims) -> None:
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        for name in _FORWARDED_REQUEST_HEADERS:
            value = self.headers.get(name)
            if value is not None and _SAFE_HEADER_VALUE.fullmatch(value):
                headers[name] = value
        # Tell the downstream server who is calling (derived from the verified token).
        for name, value in (("X-Gateway-Subject", claims.sub), ("X-Gateway-Role", claims.role),
                            (REQUEST_ID_HEADER, current_request_id() or "")):
            if _SAFE_HEADER_VALUE.fullmatch(value):
                headers[name] = value

        connection_class = http.client.HTTPSConnection if config.upstream_scheme == "https" else http.client.HTTPConnection
        conn = connection_class(config.upstream_host, config.upstream_port, timeout=config.timeout)
        try:
            conn.request("POST", config.upstream_path, body=outcome.forward, headers=headers)
            response = conn.getresponse()
            data = response.read(MAX_RESPONSE + 1)
            status = response.status
            relay = [(n, v) for n in _FORWARDED_RESPONSE_HEADERS
                     if (v := response.getheader(n)) is not None and _SAFE_HEADER_VALUE.fullmatch(v)]
        except socket.timeout:
            log.error("downstream MCP server timed out", extra={
                "event": "downstream.timeout", "timeout_s": config.timeout, "rpc_id": outcome.request_id})
            return self._send(504, _dumps(rpc_error(outcome.request_id, INTERNAL_ERROR, "Downstream MCP server timed out")))
        except (OSError, http.client.HTTPException) as exc:
            log.error("downstream MCP server unavailable", extra={
                "event": "downstream.error", "error_class": type(exc).__name__, "error": str(exc),
                "rpc_id": outcome.request_id})
            return self._send(502, _dumps(rpc_error(outcome.request_id, INTERNAL_ERROR, "Downstream MCP server unavailable")))
        finally:
            conn.close()

        if len(data) > MAX_RESPONSE:
            log.error("downstream response too large", extra={
                "event": "downstream.too_large", "limit_bytes": MAX_RESPONSE, "rpc_id": outcome.request_id})
            return self._send(502, _dumps(rpc_error(outcome.request_id, INTERNAL_ERROR, "Downstream response too large")))

        # Relay status and body as-is, including downstream 4xx/5xx.
        self.send_response(status)
        for name, value in relay:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        if data:
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass


# --------------------------------------------------------------------------- main
class SecretError(ValueError):
    """The signing secret is unusable; the gateway must not start with it."""


def check_secret(secret: Optional[str]) -> str:
    """Return ``secret`` if it is fit to sign tokens, else raise SecretError.
    There is deliberately no fallback secret: a gateway with a guessable key
    accepts forged admin tokens, so a missing or weak secret stops startup."""
    if not secret:
        raise SecretError("no signing secret: set GATEWAY_JWT_SECRET (e.g. $(openssl rand -hex 32))")
    if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
        raise SecretError("signing secret is %d bytes; HS256 needs at least %d (e.g. $(openssl rand -hex 32))"
                          % (len(secret.encode("utf-8")), MIN_SECRET_BYTES))
    return secret


def make_server(
    host: str,
    port: int,
    upstream: str,
    secret: str,
    timeout: float = 10.0,
    max_body: int = MAX_BODY,
) -> GatewayServer:
    scheme, up_host, up_port, up_path = parse_upstream(upstream)
    config = GatewayConfig(scheme, up_host, up_port, up_path, check_secret(secret), timeout, max_body)
    return GatewayServer((host, port), config)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MCP security gateway (bearer auth + per-tool authorization)")
    parser.add_argument("--host", default="127.0.0.1", help="listen address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=8080, help="listen port (default: %(default)s)")
    parser.add_argument("--upstream", default="http://127.0.0.1:9000/mcp", help="downstream MCP URL (default: %(default)s)")
    parser.add_argument("--timeout", type=float, default=10.0, help="downstream timeout in seconds (default: %(default)s)")
    parser.add_argument("--secret", help="HS256 signing secret, at least 32 bytes (default: $GATEWAY_JWT_SECRET; "
                                         "prefer the variable: arguments are visible in ps)")
    parser.add_argument("--log-file", help="also append the JSON Lines audit log to this file (e.g. gateway.jsonl)")
    args = parser.parse_args(argv)

    configure_logging(args.log_file)
    if args.log_file:
        log.info("audit log also written to a file", extra={
            "event": "gateway.log_file", "path": os.path.abspath(args.log_file)})
    try:
        secret = check_secret(args.secret or os.environ.get("GATEWAY_JWT_SECRET"))
    except SecretError as exc:  # fail closed: no default secret to fall back to
        log.critical("refusing to start: %s" % exc, extra={"event": "config.invalid_secret"})
        return 2

    server = make_server(args.host, args.port, args.upstream, secret, timeout=args.timeout)
    log.info("gateway listening", extra={
        "event": "gateway.started", "url": "http://%s:%d%s" % (args.host, server.server_address[1], GATEWAY_PATH),
        "upstream": args.upstream})
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
