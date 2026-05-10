"""LangChain / deepagents integration — wraps ToolDefs as a list of BaseTool.

The returned list plugs directly into:

- ``deepagents.create_deep_agent(tools=...)``
- ``langchain.agents.create_agent(tools=...)``
- any LangChain runnable that accepts ``list[BaseTool]``.

Two integration paths are offered:

1. **In-tool enforcement (default)** — ``create_langchain_tools(...)`` returns
   ``StructuredTool``s whose coroutine pre-checks ``ContractSession`` limits
   and converts ``BLOCKED —`` envelopes from the underlying callables into
   ``ToolException``. The agent runtime renders those as
   ``ToolMessage(status="error")``.

2. **Graph-level enforcement** — ``ContractMiddleware`` subclasses
   ``langchain.agents.middleware.AgentMiddleware`` and intercepts tool calls
   at the graph boundary. Pair with ``apply_middleware=False`` to avoid
   double work.

Important divergence from ``contract_middleware`` in ``tools.middleware``:
that decorator validates SQL on *every* tool that has an ``args["sql"]``
key — including ``inspect_query``, whose explicit purpose is to *report*
violations as JSON without blocking. The in-tool path here therefore only
runs ``session.check_limits()`` and the ``BLOCKED —`` prefix sniff; SQL
validation is left to the underlying tools (``run_query`` self-validates
at ``factory.py:632-702``).

Requires the ``[langchain]`` extra: ``pip install agentic-data-contracts[langchain]``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ToolCallRequest
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, StructuredTool, ToolException
from langgraph.types import Command

from agentic_data_contracts.adapters.base import DatabaseAdapter, SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools
from agentic_data_contracts.validation.validator import Validator

_BLOCKED_PREFIX = "BLOCKED —"


def _unwrap_mcp_text(envelope: dict[str, Any]) -> str:
    """Pull the first text block out of an MCP-style content envelope.

    Defensive: tolerates missing keys, non-text blocks, and empty content.
    Falls back to ``""`` so the agent always sees a stable string type
    rather than a stringified dict.
    """
    try:
        content = envelope.get("content") or []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", ""))
        return ""
    except (AttributeError, TypeError):
        return ""


def create_langchain_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    tools: list[ToolDef] | None = None,
    apply_middleware: bool = True,
) -> list[BaseTool]:
    """Create a list of LangChain ``BaseTool``s from a ``DataContract``.

    Args:
        contract: The data contract to enforce.
        adapter: Optional database adapter for query execution.
        semantic_source: Optional semantic source (auto-loaded if not given).
        session: Optional ``ContractSession`` for tracking enforcement state.
            One is created automatically if omitted.
        tools: Pre-built ``ToolDef`` list (if ``None``, created via
            ``create_tools``).
        apply_middleware: When ``True`` (default), each tool pre-checks
            ``session.check_limits()``. Set ``False`` if you are pairing
            this with ``ContractMiddleware`` to avoid duplicate
            limit-check work — note that the ``BLOCKED —`` prefix sniff
            is always active regardless of this flag, so error semantics
            (raising ``ToolException`` on a blocked envelope) are
            preserved either way.

    Returns:
        A list of ``BaseTool`` instances; order matches the underlying
        ``create_tools()`` output.
    """
    if session is None:
        session = ContractSession(contract)

    if tools is None:
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
        )

    return [_to_structured_tool(t, session, apply_middleware) for t in tools]


def _to_structured_tool(
    tool_def: ToolDef,
    session: ContractSession,
    apply_middleware: bool,
) -> BaseTool:
    """Wrap one ``ToolDef`` into a ``StructuredTool``.

    The coroutine returns ``(content_str, raw_envelope)`` so the original
    MCP dict survives on ``ToolMessage.artifact`` while the model sees
    plain text on ``ToolMessage.content``.
    """
    inner = tool_def.callable

    async def _coroutine(**kwargs: Any) -> tuple[str, dict[str, Any]]:
        if apply_middleware:
            try:
                session.check_limits()
            except LimitExceededError as e:
                raise ToolException(
                    f"{_BLOCKED_PREFIX} Session limit exceeded: {e}"
                ) from e

        envelope = await inner(kwargs)
        text = _unwrap_mcp_text(envelope)

        # Every BLOCKED path in tools/factory.py and tools/middleware.py
        # uses the canonical "BLOCKED —" em-dash prefix; sniffing it lets
        # us surface enforcement decisions as ToolException, which the
        # agent runtime renders as ToolMessage(status="error").
        if text.startswith(_BLOCKED_PREFIX):
            raise ToolException(text)

        return text, envelope

    return StructuredTool.from_function(
        name=tool_def.name,
        description=tool_def.description,
        coroutine=_coroutine,
        args_schema=tool_def.input_schema,
        infer_schema=False,
        response_format="content_and_artifact",
        # ``handle_tool_error=False`` lets ``ToolException`` propagate to the
        # agent loop's ``ToolNode``, which converts it to
        # ``ToolMessage(status="error")``. We don't do the conversion here.
        handle_tool_error=False,
    )


class ContractMiddleware(AgentMiddleware):
    """Graph-level contract enforcement for LangChain / deepagents.

    Intercepts every tool call and, when ``args["sql"]`` is present, runs
    ``Validator.validate(sql)`` plus ``ContractSession.check_limits()``
    before the tool is invoked. On violation, short-circuits with
    ``ToolMessage(status="error")``; otherwise delegates to the next
    handler in the chain.

    Pair with ``create_langchain_tools(..., apply_middleware=False)`` to
    avoid duplicate enforcement work.
    """

    def __init__(
        self,
        contract: DataContract,
        *,
        adapter: DatabaseAdapter | None = None,
        session: ContractSession | None = None,
    ) -> None:
        super().__init__()
        self._contract = contract
        self._session = session if session is not None else ContractSession(contract)
        sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
        self._validator = Validator(
            contract,
            dialect=adapter.dialect if adapter is not None else None,
            explain_adapter=adapter,
            sql_normalizer=sql_normalizer,
        )

    def _check(self, request: ToolCallRequest) -> ToolMessage | None:
        """Run enforcement against a request. Returns a short-circuit
        ``ToolMessage`` on violation, ``None`` to continue."""
        tool_call = request.tool_call
        name = tool_call.get("name", "")
        args = tool_call.get("args") or {}
        tool_call_id = tool_call.get("id", "")

        # Session-limit breach: do NOT call ``record_retry()`` here. The
        # session is already past its cap; recording another retry would
        # increment past it for no benefit and risks double-counting if a
        # future ceiling is added. This mirrors ``factory.py:631-636`` where
        # ``run_query`` similarly skips ``record_retry`` on limit-exceeded
        # but does record on validation-block (next branch).
        try:
            self._session.check_limits()
        except LimitExceededError as e:
            return ToolMessage(
                content=f"{_BLOCKED_PREFIX} Session limit exceeded: {e}",
                name=name,
                tool_call_id=tool_call_id,
                status="error",
            )

        # Defensive ``isinstance`` guard: a malformed agent or a hand-built
        # ToolCallRequest could send ``args`` as a non-dict, or ``sql`` as a
        # non-string; either would crash inside ``Validator.validate`` /
        # sqlglot. ``langchain``'s args_schema validation should normally
        # prevent this, but the cost of guarding is one keyword.
        sql = args.get("sql") if isinstance(args, dict) else None
        if isinstance(sql, str) and sql:
            result = self._validator.validate(sql)
            if result.blocked:
                self._session.record_retry()
                return ToolMessage(
                    content=(
                        f"{_BLOCKED_PREFIX} Violations:\n"
                        + "\n".join(f"- {r}" for r in result.reasons)
                    ),
                    name=name,
                    tool_call_id=tool_call_id,
                    status="error",
                )
        return None

    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        blocked = self._check(request)
        if blocked is not None:
            return blocked
        return handler(request)

    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        blocked = self._check(request)
        if blocked is not None:
            return blocked
        return await handler(request)
