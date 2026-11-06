import asyncio
import json
from pathlib import Path
import sys

from mcp import (
    ClientSession,
    MCPError,
    StdioServerParameters,
    stdio_client,
)


async def main():
    server_file = Path(__file__).with_name("task1_server.py")

    params = StdioServerParameters(
        command=sys.executable,
        args=[str(server_file)],
    )

    async with asyncio.timeout(20):
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as client:
                await client.initialize()

                tools = await client.list_tools()

                assert {tool.name for tool in tools.tools} == {
                    "get_customer_record",
                    "trigger_refund",
                }

                for tool in tools.tools:
                    hints = tool.annotations
                    print(
                        "Tool: %s (read-only=%s, destructive=%s)\n  %s"
                        % (
                            tool.name,
                            hints.read_only_hint,
                            hints.destructive_hint,
                            tool.description,
                        )
                    )

                record = await client.call_tool(
                    "get_customer_record",
                    {"customer_id": "CUST-12345"},
                )

                assert (
                    json.loads(record.content[0].text)["customer_id"]
                    == "CUST-12345"
                )

                print("Customer:", record.content[0].text)

                refund_args = {
                    "customer_id": "CUST-12345",
                    "amount": 12.50,
                    "reason": "Duplicate payment",
                    "idempotency_key": "demo-order-881",
                }

                refund = await client.call_tool("trigger_refund", refund_args)

                assert refund.structured_content["status"] == "simulated"
                assert refund.structured_content["amount"] == "12.50"

                print("Refund:", refund.content[0].text)

                # The agent retries (say, after a timeout): same key, same refund.
                retry = await client.call_tool("trigger_refund", refund_args)

                assert (
                    retry.structured_content["refund_id"]
                    == refund.structured_content["refund_id"]
                )
                assert retry.structured_content["replayed"] is True

                print(
                    "Retry with same idempotency key: replayed refund",
                    retry.structured_content["refund_id"],
                    "(no second payment)",
                )

                try:
                    await client.call_tool(
                        "get_customer_record",
                        {"customer_id": "BAD"},
                    )
                except MCPError as exc:
                    assert exc.code == -32602
                    print(
                        "Invalid input rejected:",
                        exc.code,
                        exc.message,
                    )
                else:
                    raise AssertionError(
                        "Invalid customer ID was accepted"
                    )


if __name__ == "__main__":
    asyncio.run(main())