"""Claude Agent SDK integration — wraps ToolDefs into an SDK MCP server.

By default (since v0.20.0) every wrapped tool pre-checks
``ContractSession.check_limits()`` and short-circuits with a canonical
``BLOCKED — Session limit exceeded`` envelope on overrun. This aligns the
SDK adapter with ``create_langchain_tools`` so a single contract YAML
behaves the same way under both adapters — in particular,
``max_duration_seconds`` measures wall-clock from the first tool call,
not just from the first ``run_query``.

SQL validation is intentionally **not** auto-applied. Doing so would
block ``inspect_query`` from reporting violations as JSON; the canonical
``run_query`` self-validation at ``factory.py:632-702`` already covers
the cost path.

Pass ``apply_middleware=False`` to opt out (preserves the pre-0.20.0
behavior where only ``run_query`` self-checked limits).
"""

from __future__ import annotations

import functools
import json
from collections.abc import Awaitable, Callable
from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools

_BLOCKED_PREFIX = "BLOCKED —"


def _with_remaining(message: str, session: ContractSession) -> str:
    """Append the canonical ``Remaining: {budget}`` suffix used by
    ``run_query`` (factory.py:627-628) so wrapper-emitted blocks carry
    the same diagnostic footprint as run_query's own blocks."""
    return f"{message}\nRemaining: {json.dumps(session.remaining(), default=str)}"


def _wrap_with_session_check(
    inner: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    session: ContractSession,
) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
    """Wrap an MCP-style tool callable with a pre-call session-limit check.

    Returns the canonical ``BLOCKED — Session limit exceeded`` envelope on
    overrun without invoking the inner function. SQL validation is
    intentionally NOT applied here — that would short-circuit
    ``inspect_query`` whose purpose is to *report* violations as JSON.
    """

    @functools.wraps(inner)
    async def wrapped(args: dict[str, Any]) -> dict[str, Any]:
        try:
            session.check_limits()
        except LimitExceededError as e:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": _with_remaining(
                            f"{_BLOCKED_PREFIX} Session limit exceeded: {e}",
                            session,
                        ),
                    }
                ]
            }
        return await inner(args)

    return wrapped


def create_sdk_mcp_server(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    tools: list[ToolDef] | None = None,
    apply_middleware: bool = True,
    server_name: str = "data-contracts",
    server_version: str = "1.0.0",
) -> Any:
    """Create a Claude Agent SDK MCP server from a DataContract.

    Wraps all 9 contract tools with the SDK's @tool decorator and
    bundles them into an MCP server ready for ClaudeAgentOptions.mcp_servers.

    Args:
        contract: The data contract to enforce.
        adapter: Optional database adapter for query execution.
        semantic_source: Optional semantic source (auto-loaded if not given).
        session: Optional session for tracking enforcement state. One is
            created automatically if omitted.
        tools: Pre-built ToolDefs (if None, created via create_tools).
        apply_middleware: When ``True`` (default since v0.20.0), every
            wrapped tool pre-checks ``session.check_limits()`` and
            short-circuits on overrun. Aligned with ``create_langchain_tools``
            on enforcement *timing* (clock starts at first tool call), but
            error transport differs by design — see note below. Set
            ``False`` to restore pre-0.20.0 behavior in which only
            ``run_query`` self-checks limits (lookup tools bypass).

            Note on cross-adapter parity: with ``apply_middleware=False``,
            this adapter passes a tool's BLOCKED envelope through to the
            agent as-is (the SDK MCP transport carries error context as
            text content; there is no ``status="error"`` field). The
            LangChain adapter additionally sniffs the ``BLOCKED —``
            prefix and converts it into a ``ToolException``. Both surface
            the same text to the agent; only the structured-error signal
            differs.
        server_name: Name for the MCP server.
        server_version: Version for the MCP server.

    Returns:
        McpSdkServerConfig ready for ClaudeAgentOptions.mcp_servers.

    Raises:
        ImportError: If claude-agent-sdk is not installed.
    """
    try:
        from claude_agent_sdk import create_sdk_mcp_server as _create_server
        from claude_agent_sdk import tool as sdk_tool
    except ImportError:
        msg = (
            "claude-agent-sdk is required for SDK integration. "
            "Install with: pip install agentic-data-contracts[agent-sdk]"
        )
        raise ImportError(msg) from None

    if session is None:
        session = ContractSession(contract)

    if tools is None:
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
        )

    sdk_tools = []
    for t in tools:
        callable_to_register = (
            _wrap_with_session_check(t.callable, session)
            if apply_middleware
            else t.callable
        )
        decorated = sdk_tool(t.name, t.description, t.input_schema)(
            callable_to_register
        )
        sdk_tools.append(decorated)

    return _create_server(
        name=server_name,
        version=server_version,
        tools=sdk_tools,
    )
