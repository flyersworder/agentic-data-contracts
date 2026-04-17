"""Revenue analysis agent — demonstrates agentic-data-contracts with Claude Agent SDK.

Usage:
    uv run python examples/revenue_agent/setup_db.py
    uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"

Requires: claude-agent-sdk (optional - falls back to demo mode)
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    prompt = (
        " ".join(sys.argv[1:])
        if len(sys.argv) > 1
        else "What was total revenue by region in Q1 2025?"
    )

    dc = DataContract.from_yaml(EXAMPLE_DIR / "contract.yml")
    semantic = YamlSource(EXAMPLE_DIR / "semantic.yml")

    db_path = EXAMPLE_DIR / "sample_data.duckdb"
    if not db_path.exists():
        sys.path.insert(0, str(EXAMPLE_DIR))
        from setup_db import setup  # type: ignore[import]

        setup(str(db_path))
        sys.path.pop(0)
    adapter = DuckDBAdapter(str(db_path))

    tools = create_tools(dc, adapter=adapter, semantic_source=semantic)

    try:
        asyncio.run(_run_with_sdk(dc, tools, prompt))
    except (ImportError, AttributeError):
        print("claude-agent-sdk not available or incompatible. Running demo mode.\n")
        asyncio.run(_run_demo(tools, prompt))


async def _run_with_sdk(dc: DataContract, tools: list, prompt: str) -> None:
    from claude_agent_sdk import (  # type: ignore[import]
        AssistantMessage,
        ClaudeAgentOptions,
        TextBlock,
        create_sdk_mcp_server,
        query,
    )

    server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)
    user_prompt = "You are a revenue analytics assistant for Acme Corp."
    options = ClaudeAgentOptions(
        model="claude-sonnet-4-6",
        system_prompt=f"{user_prompt}\n\n{dc.to_system_prompt()}",
        mcp_servers={"dc": server},
        allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
    )

    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)


async def _run_demo(tools: list, prompt: str) -> None:
    print(f"Query: {prompt}\n")

    tool = next(t for t in tools if t.name == "list_schemas")
    result = await tool.callable({})
    print("=== Available Schemas ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "list_tables")
    result = await tool.callable({})
    print("\n=== Available Tables ===")
    print(result["content"][0]["text"])

    # Domain discovery: understand the business context before querying
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "revenue"})
    print("\n=== Lookup Domain (revenue) ===")
    print(result["content"][0]["text"])

    # Metric lookup: get the SQL definition + tier + impact edges for a metric
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": "total_revenue"})
    print("\n=== Lookup Metric (total_revenue) ===")
    print(result["content"][0]["text"])

    # Impact graph: walk upstream to find drivers of total_revenue
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(
        {"metric_name": "total_revenue", "direction": "upstream"}
    )
    print("\n=== Trace Metric Impacts (upstream drivers of total_revenue) ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "validate_query")
    sql = (
        "SELECT c.region, SUM(o.amount) as revenue "
        "FROM analytics.orders o "
        "JOIN analytics.customers c ON o.customer_id = c.id "
        "WHERE o.tenant_id = 'acme' AND o.status = 'completed' "
        "AND o.created_at BETWEEN '2025-01-01' AND '2025-03-31' "
        "GROUP BY c.region"
    )
    result = await tool.callable({"sql": sql})
    print("\n=== Validate Query ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "run_query")
    result = await tool.callable({"sql": sql})
    print("\n=== Query Results ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "analytics.orders"})
    print("\n=== Lookup Relationships (orders) ===")
    print(result["content"][0]["text"])

    result = await tool.callable(
        {"table": "analytics.orders", "target_table": "analytics.subscriptions"}
    )
    print("\n=== Find Join Path (orders → subscriptions, 2 hops) ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "validate_query")
    join_sql = (
        "SELECT o.id, c.name "
        "FROM analytics.orders o "
        "JOIN analytics.customers c ON o.customer_id = c.id "
        "WHERE o.tenant_id = 'acme'"
    )
    result = await tool.callable({"sql": join_sql})
    print("\n=== Relationship Warning (missing required filter) ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "validate_query")
    result = await tool.callable({"sql": "SELECT * FROM analytics.orders"})
    print("\n=== Blocked Query ===")
    print(result["content"][0]["text"])

    tool = next(t for t in tools if t.name == "get_contract_info")
    result = await tool.callable({})
    print("\n=== Contract Info ===")
    print(result["content"][0]["text"])


if __name__ == "__main__":
    main()
