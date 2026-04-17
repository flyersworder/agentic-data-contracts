"""Revenue analysis agent — demonstrates agentic-data-contracts with Claude Agent SDK.

Usage:
    uv run python examples/revenue_agent/setup_db.py
    uv run python examples/revenue_agent/agent.py "What was Q1 revenue by region?"

Requires: claude-agent-sdk (optional - falls back to demo mode)
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource

EXAMPLE_DIR = Path(__file__).parent


def _parse_run_query_body(text: str) -> dict | None:
    """run_query may prepend a `WARNINGS:\n...\n\n` preamble before the JSON body.

    Returns the parsed JSON dict, or None if the response is a plain-text
    BLOCKED/error message.
    """
    if text.startswith("BLOCKED") or text.startswith("No database adapter"):
        return None
    body = text
    if body.startswith("WARNINGS:"):
        # Strip preamble up to the blank line separating it from the JSON body.
        _, _, body = body.partition("\n\n")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


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

    # ── Discovery ─────────────────────────────────────────────────────────────
    # Note: allowed schemas/tables are now injected into the system prompt by
    # ClaudePromptRenderer, so the agent reads its allowlist directly from the
    # prompt rather than calling a discovery tool. Column-level discovery is
    # still available via describe_table.
    print("=== Discovery ===")
    print(
        "Allowed schemas/tables are injected into the system prompt by "
        "ClaudePromptRenderer. Agents see them without a discovery tool call.\n"
        "Column-level discovery is still available via describe_table:"
    )
    describe = next(t for t in tools if t.name == "describe_table")
    result = await describe.callable({"schema": "analytics", "table": "orders"})
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

    # ── Validation (inspect_query) ────────────────────────────────────────────
    inspect = next(t for t in tools if t.name == "inspect_query")
    sql = (
        "SELECT c.region, SUM(o.amount) as revenue "
        "FROM analytics.orders o "
        "JOIN analytics.customers c ON o.customer_id = c.id "
        "WHERE o.tenant_id = 'acme' AND o.status = 'completed' "
        "AND o.created_at BETWEEN '2025-01-01' AND '2025-03-31' "
        "GROUP BY c.region"
    )
    result = await inspect.callable({"sql": sql})
    data = json.loads(result["content"][0]["text"])
    print("\n=== Inspect Query (valid revenue-by-region SQL) ===")
    print(f"  valid: {data['valid']}, violations: {data['violations']}")
    print(
        f"  cost: ${data.get('estimated_cost_usd', 'n/a')}, "
        f"rows: {data.get('estimated_rows', 'n/a')}"
    )

    # ── Execution ─────────────────────────────────────────────────────────────
    run = next(t for t in tools if t.name == "run_query")
    result = await run.callable({"sql": sql})
    print("\n=== Query Results ===")
    print(result["content"][0]["text"])
    # Parse the JSON body so later steps can read session budget.

    # ── Relationship discovery ───────────────────────────────────────────────
    tool = next(t for t in tools if t.name == "lookup_relationships")
    result = await tool.callable({"table": "analytics.orders"})
    print("\n=== Lookup Relationships (orders) ===")
    print(result["content"][0]["text"])

    result = await tool.callable(
        {"table": "analytics.orders", "target_table": "analytics.subscriptions"}
    )
    print("\n=== Find Join Path (orders → subscriptions, 2 hops) ===")
    print(result["content"][0]["text"])

    # Missing-required-filter warning surfaces via inspect_query
    join_sql = (
        "SELECT o.id, c.name "
        "FROM analytics.orders o "
        "JOIN analytics.customers c ON o.customer_id = c.id "
        "WHERE o.tenant_id = 'acme'"
    )
    result = await inspect.callable({"sql": join_sql})
    data = json.loads(result["content"][0]["text"])
    print("\n=== Relationship Warning (missing required filter) ===")
    print(f"  valid: {data['valid']}, violations: {data['violations']}")
    print(f"  warnings: {data.get('warnings', [])}")

    # Blocked query — inspect_query returns valid=False with violations
    result = await inspect.callable({"sql": "SELECT * FROM analytics.orders"})
    data = json.loads(result["content"][0]["text"])
    print("\n=== Blocked Query ===")
    print(f"  valid: {data['valid']}, violations: {data['violations']}")

    # ── Session budget ───────────────────────────────────────────────────────
    # Contract-wide info (name, allowed tables, rules) now lives in the system
    # prompt via DataContract.to_system_prompt(). Session budget state travels
    # on every run_query response under data["session"]["remaining"].
    result = await run.callable(
        {"sql": "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    data = _parse_run_query_body(result["content"][0]["text"])
    print("\n=== Session Budget (from run_query response) ===")
    if data is not None:
        print(f"  session remaining: {data.get('session', {}).get('remaining', {})}")
    else:
        # Query was blocked or adapter unavailable — print the raw message so
        # the demo still surfaces the reason rather than silently failing.
        print(result["content"][0]["text"])


if __name__ == "__main__":
    main()
