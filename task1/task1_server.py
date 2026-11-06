"""MCP customer-service server (stdio): get_customer_record and trigger_refund.

Each tool has a description and behaviour hints for the calling agent, a strict
input schema, and an output schema; results are returned both as structured
content and as JSON text. Records are mock data and refunds are simulated.

    python task1_server.py      # speaks MCP on stdin/stdout, logs to stderr
"""
import asyncio
import json
import logging
import sys
from collections import OrderedDict
from decimal import Decimal
from typing import Annotated, Dict, Literal, NamedTuple, Optional, Tuple, Type
from uuid import uuid4

import anyio
from mcp import MCPError, stdio_server, types
from mcp.server import Server
from mcp.shared.message import SessionMessage
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
)


logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("customer-mcp")

INVALID_PARAMS = -32602
MAX_REFUND = 10_000  # USD; larger refunds need a human, not an agent
IDEMPOTENCY_CACHE_SIZE = 10_000


# ------------------------------------------------------------------ inputs
class CustomerInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    customer_id: str = Field(
        pattern=r"^CUST-[0-9]{5}$",
        min_length=10,
        max_length=10,
        description="Customer ID: 'CUST-' followed by exactly five digits.",
        examples=["CUST-12345"],
    )


class RefundInput(CustomerInput):
    amount: float = Field(
        gt=0,
        le=MAX_REFUND,
        allow_inf_nan=False,
        description=(
            "Refund amount in USD: positive, at most %d, "
            "and at most 2 decimal places." % MAX_REFUND
        ),
        examples=[12.5],
    )

    reason: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=10,
            max_length=500,
        ),
    ] = Field(description="Why the refund is issued (10-500 characters).")

    idempotency_key: Optional[str] = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_-]{8,64}$",
        description=(
            "Optional but recommended: a unique key for this refund. Retrying "
            "with the same key returns the original refund instead of paying twice."
        ),
    )

    @field_validator("amount")
    @classmethod
    def _whole_cents(cls, value: float) -> float:
        # repr() gives the shortest decimal that round-trips, e.g. 0.1 + 0.2
        # -> "0.30000000000000004", so sub-cent amounts cannot sneak through.
        if Decimal(repr(value)).as_tuple().exponent < -2:
            raise ValueError("amount must have at most 2 decimal places")
        return value

    @property
    def amount_decimal(self) -> Decimal:
        """Exact amount for money arithmetic; never use the float for that."""
        return Decimal(repr(self.amount)).quantize(Decimal("0.01"))


# ----------------------------------------------------------------- outputs
class CustomerRecord(BaseModel):
    customer_id: str
    name: str
    mock: bool


class RefundResult(BaseModel):
    refund_id: str
    customer_id: str
    amount: str = Field(description="Exact decimal string, e.g. '12.50'.")
    currency: Literal["USD"]
    reason: str
    status: Literal["simulated"]
    replayed: bool = Field(description="True if this idempotency key was already used.")
    mock: bool


# ------------------------------------------------------------------- tools
class ToolSpec(NamedTuple):
    input_model: Type[BaseModel]
    output_model: Type[BaseModel]
    title: str
    description: str
    annotations: types.ToolAnnotations


TOOLS: Dict[str, ToolSpec] = {
    "get_customer_record": ToolSpec(
        CustomerInput,
        CustomerRecord,
        "Get customer record",
        "Look up one customer's record by customer ID. Read-only and safe to retry.",
        types.ToolAnnotations(
            read_only_hint=True,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=False,
        ),
    ),
    "trigger_refund": ToolSpec(
        RefundInput,
        RefundResult,
        "Trigger refund",
        (
            "Issue a refund to a customer (simulated). This moves money: confirm "
            "the customer ID and amount first, and pass an idempotency_key so "
            "a retry cannot refund twice. Maximum %d USD." % MAX_REFUND
        ),
        types.ToolAnnotations(
            read_only_hint=False,
            destructive_hint=True,
            idempotent_hint=False,  # only with an idempotency_key
            open_world_hint=True,  # a real refund calls a payment provider
        ),
    ),
}

# Kept for callers that only need the input models.
MODELS = {name: spec.input_model for name, spec in TOOLS.items()}

# idempotency_key -> (request fingerprint, refund). In memory, because refunds
# are simulated; a real server would keep this in its payments database.
_refunds: "OrderedDict[str, Tuple[Tuple, RefundResult]]" = OrderedDict()


async def list_tools(ctx, params):
    return types.ListToolsResult(
        tools=[
            types.Tool(
                name=name,
                title=spec.title,
                description=spec.description,
                input_schema=spec.input_model.model_json_schema(),
                output_schema=spec.output_model.model_json_schema(),
                annotations=spec.annotations,
            )
            for name, spec in TOOLS.items()
        ]
    )


def get_customer_record(args: CustomerInput) -> CustomerRecord:
    return CustomerRecord(
        customer_id=args.customer_id,
        name="Demo Customer",
        mock=True,
    )


def trigger_refund(args: RefundInput) -> RefundResult:
    fingerprint = (args.customer_id, args.amount_decimal, args.reason)
    key = args.idempotency_key

    if key is not None and key in _refunds:
        previous_fingerprint, previous = _refunds[key]
        if previous_fingerprint != fingerprint:
            raise MCPError(
                code=INVALID_PARAMS,
                message="idempotency_key was already used for a different refund",
            )
        logger.info("Replayed refund %s for idempotency key", previous.refund_id)
        return previous.model_copy(update={"replayed": True})

    refund = RefundResult(
        refund_id=str(uuid4()),
        customer_id=args.customer_id,
        amount=str(args.amount_decimal),
        currency="USD",
        reason=args.reason,
        status="simulated",
        replayed=False,
        mock=True,
    )

    if key is not None:
        _refunds[key] = (fingerprint, refund)
        if len(_refunds) > IDEMPOTENCY_CACHE_SIZE:
            _refunds.popitem(last=False)

    return refund


HANDLERS = {
    "get_customer_record": get_customer_record,
    "trigger_refund": trigger_refund,
}


async def call_tool(ctx, params):
    spec = TOOLS.get(params.name)

    if spec is None:
        raise MCPError(
            code=INVALID_PARAMS,
            message="Unknown tool",
        )

    try:
        args = spec.input_model.model_validate(params.arguments or {})
    except ValidationError as exc:
        raise MCPError(
            code=INVALID_PARAMS,
            message="Invalid tool arguments",
            data=exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ),
        ) from None

    result = HANDLERS[params.name](args).model_dump(mode="json")

    logger.info("Completed tool %s", params.name)

    return types.CallToolResult(
        content=[
            types.TextContent(
                type="text",
                text=json.dumps(result),
            )
        ],
        structured_content=result,
    )


server = Server(
    "customer-service",
    on_list_tools=list_tools,
    on_call_tool=call_tool,
)

async def forward_input(read, write, sender):
    async with sender:
        async for item in read:
            if isinstance(item, ValidationError):
                invalid_json = any(
                    error["type"] == "json_invalid"
                    for error in item.errors()
                )

                code = -32700 if invalid_json else -32600
                message = (
                    "Parse error"
                    if invalid_json
                    else "Invalid Request"
                )

                response = types.JSONRPCError(
                    jsonrpc="2.0",
                    id=None,
                    error=types.ErrorData(
                        code=code,
                        message=message,
                    ),
                )

                await write.send(SessionMessage(response))

            elif isinstance(item, Exception):
                raise item

            else:
                await sender.send(item)


async def main():
    logger.info("Starting MCP stdio server")

    async with stdio_server() as (read, write):
        sender, incoming = anyio.create_memory_object_stream(0)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(
                forward_input,
                read,
                write,
                sender,
            )

            async with incoming:
                await server.run(
                    incoming,
                    write,
                    server.create_initialization_options(),
                )

            tasks.cancel_scope.cancel()



if __name__ == "__main__":
    asyncio.run(main())
