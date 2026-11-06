import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from mcp import MCPError

import task1_server
from task1_server import (
    TOOLS,
    CustomerInput,
    RefundInput,
    RefundResult,
    list_tools,
    trigger_refund,
)


VALID_REFUND = {
    "customer_id": "CUST-12345",
    "amount": 12.5,
    "reason": "Duplicate payment",
}


def test_valid_inputs():
    customer = CustomerInput(customer_id="CUST-12345")
    refund = RefundInput(**VALID_REFUND)

    assert customer.customer_id == "CUST-12345"
    assert refund.amount == 12.5


@pytest.mark.parametrize(
    "value",
    [
        "BAD",
        "cust-12345",
        "CUST-1234",
        "CUST-123456",
        "CUST-ABCDE",
        "CUST-12345\n",
        12345,
        None,
    ],
)
def test_invalid_customer_ids(value):
    with pytest.raises(ValidationError):
        CustomerInput(customer_id=value)


@pytest.mark.parametrize(
    "change",
    [
        {"amount": 0},
        {"amount": -1},
        {"amount": True},
        {"amount": "12.5"},
        {"amount": float("nan")},
        {"amount": float("inf")},
        {"amount": None},
        {"amount": 12.345},
        {"amount": 0.1 + 0.2},
        {"amount": 0.001},
        {"amount": 10_000.01},
        {"reason": "x" * 501},
        {"idempotency_key": "short"},
        {"idempotency_key": "has spaces in it"},
        {"reason": "short"},
        {"reason": "          "},
        {"reason": 1234567890},
        {"unexpected": "field"},
    ],
)
def test_invalid_refund_arguments(change):
    with pytest.raises(ValidationError):
        RefundInput.model_validate(VALID_REFUND | change)


def test_missing_amount():
    with pytest.raises(ValidationError):
        RefundInput(
            customer_id="CUST-12345",
            reason="Duplicate payment",
        )


def test_demo_roundtrip():
    demo = Path(__file__).with_name("demo_task1.py")

    result = subprocess.run(
        [sys.executable, str(demo)],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert "Invalid input rejected: -32602" in result.stdout
    assert "Starting MCP stdio server" in result.stderr


@pytest.mark.parametrize("amount, exact", [(12.5, "12.50"), (0.1, "0.10"), (10_000, "10000.00"), (19.99, "19.99")])
def test_amount_is_exact_decimal(amount, exact):
    refund = trigger_refund(RefundInput.model_validate(VALID_REFUND | {"amount": amount}))
    assert refund.amount == exact
    assert refund.currency == "USD"


@pytest.fixture(autouse=True)
def clear_refunds():
    task1_server._refunds.clear()


def test_idempotency_key_replays_the_original_refund():
    args = RefundInput.model_validate(VALID_REFUND | {"idempotency_key": "order-881-refund"})
    first = trigger_refund(args)
    second = trigger_refund(args)
    assert second.refund_id == first.refund_id
    assert (first.replayed, second.replayed) == (False, True)


def test_idempotency_key_reused_for_a_different_refund_is_rejected():
    trigger_refund(RefundInput.model_validate(VALID_REFUND | {"idempotency_key": "order-881-refund"}))
    with pytest.raises(MCPError) as caught:
        trigger_refund(RefundInput.model_validate(VALID_REFUND | {"idempotency_key": "order-881-refund", "amount": 99}))
    assert caught.value.code == -32602


def test_without_a_key_each_call_is_a_new_refund():
    args = RefundInput.model_validate(VALID_REFUND)
    assert trigger_refund(args).refund_id != trigger_refund(args).refund_id


def test_idempotency_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(task1_server, "IDEMPOTENCY_CACHE_SIZE", 3)
    for i in range(5):
        trigger_refund(RefundInput.model_validate(VALID_REFUND | {"idempotency_key": "key-%08d" % i}))
    assert list(task1_server._refunds) == ["key-%08d" % i for i in (2, 3, 4)]


@pytest.mark.asyncio
async def test_tools_describe_themselves_to_the_agent():
    tools = {tool.name: tool for tool in (await list_tools(None, None)).tools}
    assert set(tools) == set(TOOLS)
    for tool in tools.values():
        assert tool.description and tool.title
        assert tool.output_schema["type"] == "object"
        props = tool.input_schema["properties"]
        assert all(prop.get("description") for prop in props.values())
    lookup, refund = tools["get_customer_record"].annotations, tools["trigger_refund"].annotations
    assert lookup.read_only_hint and lookup.idempotent_hint and not lookup.destructive_hint
    assert refund.destructive_hint and not refund.read_only_hint
    assert tools["trigger_refund"].input_schema["required"] == ["customer_id", "amount", "reason"]


def test_refund_output_matches_its_schema():
    refund = trigger_refund(RefundInput.model_validate(VALID_REFUND))
    assert set(refund.model_dump()) == set(RefundResult.model_json_schema()["properties"])
