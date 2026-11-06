"""Tests for the MCP security gateway.

Unit tests cover tokens, the tool policy and JSON-RPC inspection without any
sockets. Integration tests run the real gateway in front of the real mock
server on ephemeral localhost ports, and use the mock's call log to prove
whether a request did or did not reach the downstream server.
"""
from __future__ import annotations

import http.client
import json
import logging
import socket
import threading
from typing import Any, Dict, List, Optional, Tuple

import pytest

from create_demo_tokens import main as create_tokens_main
from gateway import main as gateway_main
from gateway import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    PARSE_ERROR,
    UNAUTHORIZED_TOOL_CALL,
    MIN_SECRET_BYTES,
    Claims,
    SecretError,
    TokenError,
    check_secret,
    inspect_request,
    issue_token,
    make_server,
    required_role,
    verify_token,
)
from mock_mcp_server import MockMCPServer
from structured_logging import JsonFormatter

SECRET = "test-secret-that-is-long-enough-for-hs256"
ADMIN = Claims(sub="alice", role="admin", exp=0)
VIEWER = Claims(sub="bob", role="viewer", exp=0)


def rpc(method: str, params: Optional[Dict[str, Any]] = None, req_id: Any = 1) -> Dict[str, Any]:
    msg: Dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if req_id is not None:
        msg["id"] = req_id
    if params is not None:
        msg["params"] = params
    return msg


def call(name: str, req_id: Any = 1, **arguments: Any) -> Dict[str, Any]:
    return rpc("tools/call", {"name": name, "arguments": arguments}, req_id)


# ----------------------------------------------------------------------- tokens
class TestTokens:
    def test_roundtrip(self):
        claims = verify_token(issue_token("alice", "admin", SECRET), SECRET)
        assert (claims.sub, claims.role) == ("alice", "admin")

    def test_wrong_secret(self):
        with pytest.raises(TokenError, match="bad signature"):
            verify_token(issue_token("alice", "admin", SECRET), "another-secret")

    def test_expired(self):
        token = issue_token("alice", "admin", SECRET, ttl_seconds=60, now=1_000)
        assert verify_token(token, SECRET, now=1_059).role == "admin"
        with pytest.raises(TokenError, match="expired"):
            verify_token(token, SECRET, now=1_060)

    def test_tampered_payload_is_rejected(self):
        head, _, sig = issue_token("bob", "viewer", SECRET).split(".")
        forged = issue_token("bob", "admin", "attacker-secret").split(".")[1]
        with pytest.raises(TokenError, match="bad signature"):
            verify_token(".".join((head, forged, sig)), SECRET)

    def test_alg_none_is_rejected(self):
        import base64

        def b64(obj):
            return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

        token = "%s.%s.%s" % (b64({"alg": "none"}), b64({"sub": "x", "role": "admin", "exp": 4_000_000_000}), "AAAA")
        with pytest.raises(TokenError, match="algorithm"):
            verify_token(token, SECRET)

    @pytest.mark.parametrize("token", ["", "abc", "a.b", "a.b.c.d", "a..c", "a.b.c", "a b.c.d", "é.é.é"])
    def test_malformed(self, token):
        with pytest.raises(TokenError):
            verify_token(token, SECRET)

    @pytest.mark.parametrize("claims", [
        {"role": "admin", "exp": 4_000_000_000},               # no sub
        {"sub": "x", "exp": 4_000_000_000},                    # no role
        {"sub": "x", "role": "", "exp": 4_000_000_000},        # empty role
        {"sub": "x", "role": 7, "exp": 4_000_000_000},         # role not a string
        {"sub": "x", "role": "admin"},                         # no exp
        {"sub": "x", "role": "admin", "exp": True},            # bool exp
        {"sub": "x", "role": "admin", "exp": float("nan")},    # NaN never expires if compared naively
    ])
    def test_bad_claims(self, claims):
        import base64
        import gateway

        def b64(raw):
            return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

        head = b64(b'{"alg":"HS256"}')
        body = b64(json.dumps(claims).encode())
        sig = b64(gateway._sign(("%s.%s" % (head, body)).encode(), SECRET))
        with pytest.raises(TokenError):
            verify_token("%s.%s.%s" % (head, body, sig), SECRET)


# ----------------------------------------------------------------------- policy
class TestPolicy:
    @pytest.mark.parametrize("name", ["admin_reset_key", "admin_", "ADMIN_reset_key", "Admin_x", " admin_x", "admin_x\n", "ａｄｍｉｎ＿x"])
    def test_admin_tools_need_admin(self, name):
        assert required_role(name) == "admin"

    @pytest.mark.parametrize("name", ["search_docs", "get_status", "admin", "administrator", "x_admin_y", "reset_admin_key"])
    def test_other_tools_open_to_all(self, name):
        assert required_role(name) is None


# -------------------------------------------------------------- inspect_request
class TestInspectRequest:
    def body(self, msg) -> bytes:
        return json.dumps(msg).encode()

    def test_tools_list_forwarded_for_viewer(self):
        out = inspect_request(self.body(rpc("tools/list")), VIEWER)
        assert out.forward is not None and json.loads(out.forward)["method"] == "tools/list"

    def test_admin_call_by_viewer_denied_with_32001(self):
        out = inspect_request(self.body(call("admin_reset_key", req_id="abc", key_id="k1")), VIEWER)
        assert out.forward is None and out.status == 200
        reply = json.loads(out.body)
        assert reply["jsonrpc"] == "2.0" and reply["id"] == "abc"
        assert reply["error"]["code"] == UNAUTHORIZED_TOOL_CALL == -32001
        assert reply["error"]["message"] == "Unauthorized Tool Call"
        assert "result" not in reply

    def test_admin_call_by_admin_forwarded(self):
        assert inspect_request(self.body(call("admin_reset_key", key_id="k1")), ADMIN).forward is not None

    def test_non_admin_call_by_viewer_forwarded(self):
        assert inspect_request(self.body(call("search_docs", query="x")), VIEWER).forward is not None

    def test_unknown_role_is_not_admin(self):
        out = inspect_request(self.body(call("admin_reset_key", key_id="k")), Claims("eve", "Admin", 0))
        assert json.loads(out.body)["error"]["code"] == -32001

    def test_denied_notification_gets_no_reply(self):
        out = inspect_request(self.body(rpc("tools/call", {"name": "admin_reset_key"}, req_id=None)), VIEWER)
        assert (out.forward, out.status, out.body) == (None, 202, None)

    @pytest.mark.parametrize("raw", [b"", b"{", b"not json", b"\xff\xfe", b'{"a": NaN}', b"[" * 100000])
    def test_parse_errors(self, raw):
        out = inspect_request(raw, ADMIN)
        assert out.status == 400 and json.loads(out.body)["error"]["code"] == PARSE_ERROR
        assert json.loads(out.body)["id"] is None

    def test_batches_rejected_even_when_empty(self):
        for batch in ([], [rpc("tools/list")], [rpc("tools/list"), call("admin_reset_key", key_id="k")]):
            out = inspect_request(self.body(batch), ADMIN)
            assert out.forward is None and json.loads(out.body)["error"]["code"] == INVALID_REQUEST

    @pytest.mark.parametrize("msg", [
        42, "str", None, {},
        {"jsonrpc": "1.0", "id": 1, "method": "tools/list"},
        {"id": 1, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 1},
        {"jsonrpc": "2.0", "id": 1, "method": 5},
        {"jsonrpc": "2.0", "id": None, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": True, "method": "tools/list"},
        {"jsonrpc": "2.0", "id": [1], "method": "tools/list"},
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": "x"},
    ])
    def test_invalid_envelope(self, msg):
        out = inspect_request(self.body(msg), ADMIN)
        assert out.forward is None and out.status == 400
        assert json.loads(out.body)["error"]["code"] == INVALID_REQUEST

    @pytest.mark.parametrize("params", [None, [], {}, {"name": None}, {"name": 5}, {"name": ""}, {"arguments": {}}])
    def test_tools_call_without_valid_name_is_invalid_params(self, params):
        msg = {"jsonrpc": "2.0", "id": 9, "method": "tools/call"}
        if params is not None:
            msg["params"] = params
        out = inspect_request(self.body(msg), ADMIN)
        assert out.forward is None
        assert json.loads(out.body)["error"]["code"] == INVALID_PARAMS
        assert json.loads(out.body)["id"] == 9

    def test_duplicate_keys_cannot_smuggle_a_call(self):
        # Parsed last-wins here -> tools/list; the forwarded body must say exactly that.
        raw = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key"},"method":"tools/list"}'
        out = inspect_request(raw, VIEWER)
        assert out.forward is not None
        assert out.forward.count(b'"method"') == 1
        assert json.loads(out.forward)["method"] == "tools/list"

    def test_other_methods_pass_through(self):
        for method in ("initialize", "ping", "notifications/initialized", "resources/list"):
            assert inspect_request(self.body(rpc(method)), VIEWER).forward is not None


# ------------------------------------------------------------------ integration
class Stack:
    def __init__(self, mock: MockMCPServer, gateway_port: int):
        self.mock = mock
        self.port = gateway_port

    def request(
        self,
        payload: Any = None,
        token: Optional[str] = None,
        *,
        raw: Optional[bytes] = None,
        method: str = "POST",
        path: str = "/mcp",
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[str] = None,
    ) -> Tuple[int, Dict[str, str], Any]:
        body = raw if raw is not None else json.dumps(payload).encode()
        hdrs = {"Content-Type": "application/json"}
        if token is not None:
            hdrs["Authorization"] = "Bearer " + token
        if auth is not None:
            hdrs["Authorization"] = auth
        hdrs.update(headers or {})
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request(method, path, body=body if method == "POST" else None, headers=hdrs)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, {k.lower(): v for k, v in resp.getheaders()}, (json.loads(data) if data else None)
        finally:
            conn.close()


@pytest.fixture()
def make_stack():
    servers = []

    def _make(upstream: Optional[str] = None, **gateway_kwargs) -> Stack:
        mock = MockMCPServer(("127.0.0.1", 0))
        gateway = make_server(
            "127.0.0.1", 0, upstream or "http://127.0.0.1:%d/mcp" % mock.server_address[1], SECRET, **gateway_kwargs
        )
        for server in (mock, gateway):
            # Short poll interval so shutdown() returns promptly in teardown.
            threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
            servers.append(server)
        return Stack(mock, gateway.server_address[1])

    yield _make
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture()
def stack(make_stack) -> Stack:
    return make_stack()


@pytest.fixture()
def admin() -> str:
    return issue_token("alice", "admin", SECRET)


@pytest.fixture()
def viewer() -> str:
    return issue_token("bob", "viewer", SECRET)


class TestForwarding:
    def test_tools_list_is_proxied_transparently(self, stack, viewer):
        status, _, body = stack.request(rpc("tools/list", req_id=7), viewer)
        assert status == 200 and body["id"] == 7
        assert {t["name"] for t in body["result"]["tools"]} == {
            "search_docs", "get_status", "admin_reset_key", "admin_delete_user"
        }
        assert [c["method"] for c in stack.mock.calls] == ["tools/list"]

    def test_initialize_handshake_passes_through(self, stack, viewer):
        status, _, body = stack.request(rpc("initialize", {"protocolVersion": "2025-06-18"}), viewer)
        assert status == 200 and body["result"]["serverInfo"]["name"] == "mock-mcp-server"
        status, _, body = stack.request(rpc("notifications/initialized", req_id=None), viewer)
        assert (status, body) == (202, None)

    def test_viewer_can_call_ordinary_tool(self, stack, viewer):
        status, _, body = stack.request(call("search_docs", query="mcp"), viewer)
        assert status == 200 and body["result"]["isError"] is False
        assert "mcp" in body["result"]["content"][0]["text"]
        assert [c["tool"] for c in stack.mock.calls] == ["search_docs"]

    def test_admin_can_call_admin_tool(self, stack, admin):
        status, _, body = stack.request(call("admin_reset_key", req_id="r-1", key_id="k-42"), admin)
        assert status == 200 and body["id"] == "r-1"
        assert "k-42" in body["result"]["content"][0]["text"]
        assert [c["tool"] for c in stack.mock.calls] == ["admin_reset_key"]

    def test_downstream_error_is_relayed_unchanged(self, stack, viewer):
        status, _, body = stack.request(call("no_such_tool"), viewer)
        assert status == 200 and body["error"]["code"] == -32602 and "Unknown tool" in body["error"]["message"]

    def test_client_token_is_not_leaked_but_identity_is_propagated(self, stack, admin):
        stack.request(rpc("tools/list"), admin, headers={"Mcp-Session-Id": "sess-1"})
        headers = stack.mock.calls[0]["headers"]
        assert "authorization" not in headers
        assert headers["x-gateway-subject"] == "alice" and headers["x-gateway-role"] == "admin"
        assert headers["mcp-session-id"] == "sess-1"

    def test_forwarded_body_matches_what_was_inspected(self, stack, viewer):
        raw = b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key"},"method":"tools/list"}'
        status, _, body = stack.request(raw=raw, token=viewer)
        assert status == 200 and "tools" in body["result"]
        assert stack.mock.calls[0]["method"] == "tools/list"
        assert stack.mock.calls[0]["body"].count(b'"method"') == 1


class TestAuthorization:
    def test_viewer_denied_admin_tool_and_downstream_not_called(self, stack, viewer):
        status, _, body = stack.request(call("admin_reset_key", req_id=3, key_id="k-42"), viewer)
        assert status == 200
        assert body == {
            "jsonrpc": "2.0",
            "id": 3,
            "error": {
                "code": -32001,
                "message": "Unauthorized Tool Call",
                "data": {"tool": "admin_reset_key", "requiredRole": "admin"},
            },
        }
        assert stack.mock.calls == []

    @pytest.mark.parametrize("name", ["admin_delete_user", "ADMIN_reset_key", " admin_reset_key", "admin_unlisted"])
    def test_every_admin_variant_is_blocked_before_downstream(self, stack, viewer, name):
        _, _, body = stack.request(call(name, user_id="u"), viewer)
        assert body["error"]["code"] == -32001
        assert stack.mock.calls == []

    def test_denied_notification_does_not_reach_downstream(self, stack, viewer):
        status, _, body = stack.request(rpc("tools/call", {"name": "admin_delete_user"}, req_id=None), viewer)
        assert (status, body) == (202, None)
        assert stack.mock.calls == []

    def test_batch_cannot_smuggle_admin_call(self, stack, viewer):
        status, _, body = stack.request([rpc("tools/list", req_id=1), call("admin_reset_key", req_id=2, key_id="k")], viewer)
        assert status == 400 and body["error"]["code"] == -32600
        assert stack.mock.calls == []

    def test_unknown_role_cannot_call_admin_tool(self, stack):
        token = issue_token("eve", "superuser", SECRET)
        _, _, body = stack.request(call("admin_reset_key", key_id="k"), token)
        assert body["error"]["code"] == -32001
        assert stack.mock.calls == []


class TestAuthentication:
    def test_missing_token(self, stack):
        status, headers, body = stack.request(rpc("tools/list"))
        assert status == 401 and body["error"]["code"] == -32003
        assert headers["www-authenticate"].startswith("Bearer")
        assert stack.mock.calls == []

    @pytest.mark.parametrize("auth", ["Bearer", "Bearer ", "Bearer a b", "Basic dXNlcjpwYXNz", "Bearer not.a.jwt", "token abc"])
    def test_malformed_authorization_header(self, stack, auth):
        status, _, _ = stack.request(rpc("tools/list"), auth=auth)
        assert status == 401 and stack.mock.calls == []

    def test_scheme_is_case_insensitive(self, stack, viewer):
        status, _, _ = stack.request(rpc("tools/list"), auth="bearer " + viewer)
        assert status == 200

    def test_token_signed_with_other_secret(self, stack):
        status, _, _ = stack.request(rpc("tools/list"), issue_token("alice", "admin", "attacker-secret"))
        assert status == 401 and stack.mock.calls == []

    def test_expired_token(self, stack):
        status, _, _ = stack.request(rpc("tools/list"), issue_token("alice", "admin", SECRET, ttl_seconds=60, now=1_000))
        assert status == 401 and stack.mock.calls == []

    def test_unauthenticated_request_is_rejected_before_inspection(self, stack):
        status, _, body = stack.request(raw=b"not json")
        assert status == 401 and body["error"]["code"] == -32003

    def test_duplicate_authorization_headers(self, stack, viewer, admin):
        conn = http.client.HTTPConnection("127.0.0.1", stack.port, timeout=5)
        conn.putrequest("POST", "/mcp")
        body = json.dumps(call("admin_reset_key", key_id="k")).encode()
        for token in (viewer, admin):
            conn.putheader("Authorization", "Bearer " + token)
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders(body)
        assert conn.getresponse().status == 401
        conn.close()
        assert stack.mock.calls == []


class TestWireFormat:
    def test_invalid_json(self, stack, viewer):
        status, _, body = stack.request(raw=b"{not json", token=viewer)
        assert status == 400 and body == {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
        assert stack.mock.calls == []

    def test_invalid_utf8(self, stack, viewer):
        status, _, body = stack.request(raw=b'{"jsonrpc":"2.0","method":"\xff"}', token=viewer)
        assert status == 400 and body["error"]["code"] == -32700

    def test_invalid_request_echoes_valid_id(self, stack, viewer):
        status, _, body = stack.request({"jsonrpc": "2.0", "id": "x-1", "method": 5}, viewer)
        assert status == 400 and body["id"] == "x-1" and body["error"]["code"] == -32600

    def test_tools_call_missing_name(self, stack, viewer):
        status, _, body = stack.request(rpc("tools/call", {"arguments": {}}, req_id=4), viewer)
        assert status == 200 and body["id"] == 4 and body["error"]["code"] == -32602
        assert stack.mock.calls == []

    def test_string_and_integer_ids_round_trip(self, stack, viewer):
        for req_id in ("abc", 0, 2**40):
            _, _, body = stack.request(rpc("ping", req_id=req_id), viewer)
            assert body["id"] == req_id and type(body["id"]) is type(req_id)

    @pytest.mark.parametrize("method", ["GET", "PUT", "DELETE"])
    def test_only_post_is_allowed(self, stack, viewer, method):
        status, headers, _ = stack.request(token=viewer, method=method)
        assert status == 405 and headers["allow"] == "POST"

    def test_unknown_path(self, stack, viewer):
        status, _, _ = stack.request(rpc("tools/list"), viewer, path="/other")
        assert status == 404 and stack.mock.calls == []

    def test_query_string_on_mcp_path_is_accepted(self, stack, viewer):
        status, _, _ = stack.request(rpc("tools/list"), viewer, path="/mcp?x=1")
        assert status == 200

    def test_missing_content_length(self, stack, viewer):
        conn = http.client.HTTPConnection("127.0.0.1", stack.port, timeout=5)
        conn.putrequest("POST", "/mcp")
        conn.putheader("Authorization", "Bearer " + viewer)
        conn.endheaders()
        assert conn.getresponse().status == 411
        conn.close()

    def test_oversized_body_rejected_from_declared_length(self, make_stack, viewer):
        stack = make_stack(max_body=1024)
        conn = http.client.HTTPConnection("127.0.0.1", stack.port, timeout=5)
        conn.putrequest("POST", "/mcp")
        conn.putheader("Authorization", "Bearer " + viewer)
        conn.putheader("Content-Length", "2048")
        conn.endheaders()  # declared length only; nothing is sent, nothing to read
        assert conn.getresponse().status == 413
        conn.close()
        assert stack.mock.calls == []


class TestDownstreamFailures:
    def test_downstream_down_returns_502_with_request_id(self, make_stack, viewer):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            dead_port = s.getsockname()[1]
        stack = make_stack(upstream="http://127.0.0.1:%d/mcp" % dead_port)
        status, _, body = stack.request(rpc("tools/list", req_id=11), viewer)
        assert status == 502 and body["id"] == 11 and body["error"]["code"] == -32603

    def test_downstream_timeout_returns_504(self, make_stack, viewer):
        with socket.socket() as hung:  # accepts TCP connections (backlog) but never answers
            hung.bind(("127.0.0.1", 0))
            hung.listen(1)
            stack = make_stack(upstream="http://127.0.0.1:%d/mcp" % hung.getsockname()[1], timeout=0.3)
            status, _, body = stack.request(rpc("tools/list", req_id=12), viewer)
        assert status == 504 and body["id"] == 12

    def test_denied_call_still_works_when_downstream_is_down(self, make_stack, viewer):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            dead_port = s.getsockname()[1]
        stack = make_stack(upstream="http://127.0.0.1:%d/mcp" % dead_port)
        _, _, body = stack.request(call("admin_reset_key", key_id="k"), viewer)
        assert body["error"]["code"] == -32001  # decided locally, no downstream needed


class TestDemoTokens:
    def test_cli_emits_verifiable_admin_and_viewer_tokens(self, capsys):
        assert create_tokens_main(["--secret", SECRET, "--ttl", "120"]) == 0
        lines = dict(line[len("export "):].split("=", 1) for line in capsys.readouterr().out.splitlines())
        assert verify_token(lines["ADMIN_TOKEN"], SECRET).role == "admin"
        assert verify_token(lines["VIEWER_TOKEN"], SECRET).role == "viewer"

    def test_cli_refuses_to_sign_without_a_secret(self, capsys, monkeypatch):
        monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
        assert create_tokens_main([]) == 2
        out = capsys.readouterr()
        assert out.out == "" and "GATEWAY_JWT_SECRET" in out.err  # no token printed


# --------------------------------------------------------------- signing secret
class TestSigningSecret:
    """No default secret: the gateway won't start with a missing or weak key."""

    @pytest.mark.parametrize("secret", [None, "", "short", "x" * (MIN_SECRET_BYTES - 1)])
    def test_missing_or_short_secret_is_rejected(self, secret):
        with pytest.raises(SecretError):
            check_secret(secret)

    def test_length_is_counted_in_bytes(self):
        assert check_secret("x" * MIN_SECRET_BYTES)
        assert check_secret("é" * (MIN_SECRET_BYTES // 2))  # 2 bytes each in UTF-8
        with pytest.raises(SecretError):
            check_secret("é" * (MIN_SECRET_BYTES // 2 - 1))

    def test_make_server_enforces_it_too(self):
        with pytest.raises(SecretError):
            make_server("127.0.0.1", 0, "http://127.0.0.1:9/mcp", "short")

    @pytest.mark.parametrize("env, argv", [
        (None, []),                                  # nothing configured
        ("too-short", []),                           # weak value in the environment
        (None, ["--secret", "too-short"]),           # weak value on the command line
    ])
    def test_gateway_exits_before_listening(self, monkeypatch, audit, env, argv):
        if env is None:
            monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
        else:
            monkeypatch.setenv("GATEWAY_JWT_SECRET", env)
        monkeypatch.setattr("gateway.make_server", lambda *a, **k: pytest.fail("must not bind a port"))
        monkeypatch.setattr("gateway.configure_logging", lambda *a, **k: None)  # keep pytest's root handlers
        assert gateway_main(["--port", "0", *argv]) == 2
        event = audit.events[-1]
        assert (event["event"], event["level"]) == ("config.invalid_secret", "CRITICAL")
        assert "too-short" not in "\n".join(audit.lines)  # the rejected value is never logged


# -------------------------------------------------------------------- audit log
class JsonLines(logging.Handler):
    """Formats the gateway's records exactly as production does and keeps the lines."""

    def __init__(self) -> None:
        super().__init__()
        self.setFormatter(JsonFormatter())
        self.lines: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))

    @property
    def events(self) -> List[Dict[str, Any]]:
        return [json.loads(line) for line in self.lines]


@pytest.fixture()
def audit():
    handler = JsonLines()
    logger = logging.getLogger("mcp-gateway")
    logger.addHandler(handler)
    previous = logger.level
    logger.setLevel(logging.INFO)
    yield handler
    logger.removeHandler(handler)
    logger.setLevel(previous)


class TestAuditLog:
    def test_each_decision_is_one_json_event(self, stack, viewer, admin, audit):
        stack.request(rpc("tools/list"), viewer)
        stack.request(call("admin_reset_key", req_id=2, key_id="k"), viewer)
        stack.request(call("admin_reset_key", req_id=3, key_id="k"), admin)
        stack.request(raw=b"{nope", token=viewer)
        stack.request(rpc("tools/list"), "not-a-token")
        events = audit.events
        assert [e["event"] for e in events] == ["mcp.allow", "mcp.deny", "mcp.allow", "mcp.reject", "auth.failed"]
        assert all({"ts", "level", "logger", "message", "event", "request_id"} <= set(e) for e in events)
        deny = events[1]
        assert (deny["level"], deny["sub"], deny["role"], deny["tool"], deny["required_role"], deny["rpc_id"]) == (
            "WARNING", "bob", "viewer", "admin_reset_key", "admin", 2)
        assert events[3]["rpc_code"] == PARSE_ERROR
        assert events[4]["reason"] == "malformed token"

    def test_request_id_links_log_response_and_downstream(self, stack, viewer, audit):
        _, headers, _ = stack.request(rpc("tools/list"), viewer)
        rid = headers["x-request-id"]
        assert rid.startswith("req_")
        assert audit.events[-1]["request_id"] == rid
        assert stack.mock.calls[-1]["headers"]["x-request-id"] == rid

    def test_rejections_carry_a_request_id_too(self, stack, audit):
        _, headers, _ = stack.request(rpc("tools/list"))  # no token: 401
        assert audit.events[-1]["event"] == "auth.failed"
        assert audit.events[-1]["request_id"] == headers["x-request-id"]

    def test_concurrent_requests_never_share_an_id(self, stack, viewer, audit):
        results: List[str] = []

        def one(i: int) -> None:
            results.append(stack.request(rpc("tools/list", req_id=i), viewer)[1]["x-request-id"])

        threads = [threading.Thread(target=one, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(results)) == 20
        by_rpc = {e["rpc_id"]: e["request_id"] for e in audit.events}
        assert set(by_rpc.values()) == set(results)  # each log line has its own request's id

    def test_tokens_are_never_logged(self, stack, viewer, admin, audit):
        stack.request(rpc("tools/list"), viewer)
        stack.request(call("admin_reset_key", key_id="k"), admin)
        stack.request(rpc("tools/list"), viewer + "tampered")
        text = "\n".join(audit.lines)
        assert viewer not in text and admin not in text
        assert viewer.split(".")[2] not in text  # not even the signature
