import asyncio
import json
from pathlib import Path
import sys

import pytest


VALID_REFUND = {
    "customer_id": "CUST-12345",
    "amount": 12.5,
    "reason": "Duplicate payment",
}


@pytest.mark.asyncio
async def test_stdio_protocol():
    server = Path(__file__).with_name("task1_server.py")

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(server),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def send(message):
        line = json.dumps(message) + "\n"
        process.stdin.write(line.encode())
        await process.stdin.drain()

    async def reply(expected_id):
        line = await asyncio.wait_for(
            process.stdout.readline(),
            timeout=5,
        )

        assert line, "Server closed stdout before replying"

        message = json.loads(line)
        assert message["jsonrpc"] == "2.0"
        assert message["id"] == expected_id

        return message

    async def request(request_id, method, params):
        await send({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        })
        return await reply(request_id)

    try:
        initialized = await request(
            1,
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {
                    "name": "task1-test",
                    "version": "1.0",
                },
            },
        )
        assert "result" in initialized

        await send({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        })

        listed = await request(2, "tools/list", {})

        by_name = {tool["name"]: tool for tool in listed["result"]["tools"]}
        refund_tool = by_name["trigger_refund"]
        assert refund_tool["description"]
        assert refund_tool["annotations"]["destructiveHint"] is True
        assert by_name["get_customer_record"]["annotations"]["readOnlyHint"] is True
        assert "outputSchema" in refund_tool

        assert {
            tool["name"]
            for tool in listed["result"]["tools"]
        } == {
            "get_customer_record",
            "trigger_refund",
        }

        record = await request(
            3,
            "tools/call",
            {
                "name": "get_customer_record",
                "arguments": {
                    "customer_id": "CUST-12345",
                },
            },
        )

        data = json.loads(
            record["result"]["content"][0]["text"]
        )
        assert data["customer_id"] == "CUST-12345"

        refund = await request(
            4,
            "tools/call",
            {
                "name": "trigger_refund",
                "arguments": VALID_REFUND,
            },
        )

        data = json.loads(
            refund["result"]["content"][0]["text"]
        )
        assert data["status"] == "simulated"
        assert data["amount"] == "12.50"
        assert refund["result"]["structuredContent"] == data

        invalid = await request(
            5,
            "tools/call",
            {
                "name": "get_customer_record",
                "arguments": {"customer_id": "BAD"},
            },
        )
        assert invalid["error"]["code"] == -32602

        unknown = await request(
            6,
            "unknown/method",
            {},
        )
        assert unknown["error"]["code"] == -32601

        process.stdin.write(b"{invalid json}\n")
        await process.stdin.drain()

        malformed = await reply(None)
        assert malformed["error"]["code"] == -32700

        await send({})

        invalid_request = await reply(None)
        assert invalid_request["error"]["code"] == -32600

        recovered = await request(7, "tools/list", {})
        assert len(recovered["result"]["tools"]) == 2

    finally:
        if process.returncode is None:
            process.terminate()

        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=5,
            )
        except TimeoutError:
            process.kill()
            await process.wait()

    assert not (await process.stdout.read()).strip()

    logs = await process.stderr.read()
    assert b"Starting MCP stdio server" in logs