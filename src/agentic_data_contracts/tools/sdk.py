"""Claude Agent SDK integration — wraps ToolDefs into an SDK MCP server."""

from __future__ import annotations

from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools


def create_sdk_mcp_server(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    tools: list[ToolDef] | None = None,
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
        session: Optional session for tracking enforcement state.
        tools: Pre-built ToolDefs (if None, created via create_tools).
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

    if tools is None:
        tools = create_tools(
            contract,
            adapter=adapter,
            semantic_source=semantic_source,
            session=session,
        )

    sdk_tools = []
    for t in tools:
        decorated = sdk_tool(t.name, t.description, t.input_schema)(t.callable)
        sdk_tools.append(decorated)

    return _create_server(
        name=server_name,
        version=server_version,
        tools=sdk_tools,
    )
