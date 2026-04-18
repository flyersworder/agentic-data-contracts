"""Growth analytics agent — demonstrates governance patterns distinct from
revenue_agent:

- `verified` / `correlated` / `hypothesized` metric impacts (revenue_agent only
  has `hypothesized`)
- `log`-level rule for PII auditing (not advertised in the system prompt)
- Time-bounded event queries (a block rule that prevents unbounded event scans)
- Stale-review detection flagging an un-reviewed impact edge

Usage:
    uv run python examples/growth_agent/setup_db.py
    uv run python examples/growth_agent/agent.py "Which onboarding variant lifted activation?"

Requires claude-agent-sdk for the LLM path; falls back to a demo mode that
exercises the tools directly.
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
    """run_query may prepend WARNINGS:/LOG: preambles before the JSON body."""
    if text.startswith("BLOCKED") or text.startswith("No database adapter"):
        return None
    body = text
    for preamble in ("WARNINGS:", "LOG:"):
        if body.startswith(preamble):
            _, _, body = body.partition("\n\n")
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def main() -> None:
    prompt = (
        " ".join(sys.argv[1:])
        if len(sys.argv) > 1
        else "Which onboarding variant lifted activation in Q3 2025?"
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
        print("claude-agent-sdk not available. Running demo mode.\n")
        asyncio.run(_run_demo(dc, semantic, tools, prompt))


async def _run_with_sdk(dc: DataContract, tools: list, prompt: str) -> None:
    from claude_agent_sdk import (  # type: ignore[import]
        AssistantMessage,
        ClaudeAgentOptions,
        TextBlock,
        create_sdk_mcp_server,
        query,
    )

    server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)

    # Growth-specific agent guidance: prefer concluded experiments, cite evidence.
    growth_policy = (
        "You are a growth analytics assistant for Acme Corp. When analysing"
        " experiments, use only concluded experiments (status='concluded'). When"
        " reasoning about drivers of a metric, cite the evidence string from"
        " trace_metric_impacts verbatim — particularly confidence level and"
        " sample size. Do not invent causal claims beyond what the evidence says."
    )
    options = ClaudeAgentOptions(
        model="claude-sonnet-4-6",
        system_prompt=f"{growth_policy}\n\n{dc.to_system_prompt()}",
        mcp_servers={"dc": server},
        allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
    )

    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)


async def _run_demo(
    dc: DataContract, semantic: YamlSource, tools: list, prompt: str
) -> None:
    print(f"Query: {prompt}\n")

    # ── 1. Domain + metric discovery ──────────────────────────────────────────
    lookup_domain = next(t for t in tools if t.name == "lookup_domain")
    result = await lookup_domain.callable({"name": "experimentation"})
    print("=== Lookup Domain (experimentation) ===")
    print(result["content"][0]["text"])

    lookup_metric = next(t for t in tools if t.name == "lookup_metric")
    result = await lookup_metric.callable({"metric_name": "activation_rate"})
    print("\n=== Lookup Metric (activation_rate) ===")
    print(result["content"][0]["text"])

    # ── 2. Impact graph — shows verified / correlated / hypothesized ─────────
    trace = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await trace.callable(
        {"metric_name": "conversion_rate", "direction": "upstream"}
    )
    print("\n=== Trace Metric Impacts (upstream drivers of conversion_rate) ===")
    print(result["content"][0]["text"])

    # ── 3. Time-bounded events block rule fires ──────────────────────────────
    inspect = next(t for t in tools if t.name == "inspect_query")
    unbounded_sql = (
        "SELECT event_name, COUNT(id) AS n FROM analytics.events "
        "WHERE tenant_id = 'acme' GROUP BY event_name"
    )
    result = await inspect.callable({"sql": unbounded_sql})
    data = json.loads(result["content"][0]["text"])
    print("\n=== Blocked: events scan without time bound ===")
    print(f"  valid: {data['valid']}, violations: {data['violations']}")

    # ── 4. Log-level PII audit fires on email selection ──────────────────────
    # The agent's system prompt does NOT mention this rule. It fires anyway
    # and prepends a LOG: preamble to the run_query output. The agent cannot
    # adapt its behavior to avoid it.
    run = next(t for t in tools if t.name == "run_query")
    pii_sql = (
        "SELECT email, acquisition_source FROM analytics.users WHERE tenant_id = 'acme'"
    )
    result = await run.callable({"sql": pii_sql})
    print("\n=== Log-level audit fires (query DOES run, governance is notified) ===")
    print(result["content"][0]["text"][:400])

    # ── 5. Valid experiment-lift query runs ──────────────────────────────────
    lift_sql = (
        "SELECT variant, "
        "       COUNT(DISTINCT user_id) AS users, "
        "       COUNT(DISTINCT user_id) FILTER (WHERE event_name = 'activation') AS activations "
        "FROM analytics.events "
        "WHERE tenant_id = 'acme' "
        "  AND experiment_id = 'onboarding-042' "
        "  AND created_at BETWEEN '2025-07-01' AND '2025-09-30' "
        "GROUP BY variant"
    )
    result = await run.callable({"sql": lift_sql})
    print("\n=== Experiment lift query (onboarding-042) ===")
    print(result["content"][0]["text"])

    # ── 6. Staleness check — the un-reviewed impact edge is flagged ──────────
    findings = dc.find_stale(semantic, threshold_days=90)
    print("\n=== Stale-review findings ===")
    if not findings:
        print("  (none — every artefact is within review threshold)")
    for f in findings:
        print(f"  [{f.kind}] {f.name} — age_days={f.age_days}, context={f.context}")


if __name__ == "__main__":
    main()
