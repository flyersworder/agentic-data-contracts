"""Ops reliability agent — demonstrates governance patterns orthogonal to
revenue_agent and growth_agent:

- `blocked_columns` protecting PII in incident triage data
- Multiple `log`-level rules (governance audit trail, invisible to the agent)
- `require_limit` forcing explicit caps on dashboard-driven queries
- `max_joins` capping query complexity
- A `negative` metric impact (deploy frequency ↓ incident count — counter-intuitive DORA pattern)
- Tight resource limits (max_duration=30s) for real-time dashboards
- `blocked_principals` on `sre.deploys`: interns and contractors can't see
  commit authorship — demonstrates per-caller access control. Default caller
  is `sre_lead@co.com`; override with `--caller <email>` to see a denial.

Usage:
    uv run python examples/ops_agent/setup_db.py
    uv run python examples/ops_agent/agent.py "What's our MTTR by severity this week?"
    uv run python examples/ops_agent/agent.py --caller intern@co.com "Show recent deploys"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource

EXAMPLE_DIR = Path(__file__).parent


def _parse_run_query_body(text: str) -> dict | None:
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
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument(
        "prompt",
        nargs="*",
        help="The question to ask the agent. Defaults to an MTTR query.",
    )
    parser.add_argument(
        "--caller",
        default="sre_lead@co.com",
        help=(
            "Caller identity used for per-table principal gates. "
            "Try --caller intern@co.com to see deploy-table queries denied. "
            "(default: sre_lead@co.com)"
        ),
    )
    args = parser.parse_args()

    prompt = (
        " ".join(args.prompt)
        if args.prompt
        else "What's our MTTR by severity for incidents this week?"
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

    tools = create_tools(
        dc, adapter=adapter, semantic_source=semantic, caller_principal=args.caller
    )

    print(f"Caller: {args.caller}\n")

    try:
        asyncio.run(_run_with_sdk(dc, tools, prompt))
    except (ImportError, AttributeError):
        print("claude-agent-sdk not available. Running demo mode.\n")
        asyncio.run(_run_demo(dc, semantic, tools, prompt, args.caller, adapter))


async def _run_with_sdk(dc: DataContract, tools: list, prompt: str) -> None:
    from claude_agent_sdk import (  # type: ignore[import]
        AssistantMessage,
        ClaudeAgentOptions,
        TextBlock,
        create_sdk_mcp_server,
        query,
    )

    server = create_sdk_mcp_server(name="data-contracts", version="1.0.0", tools=tools)

    ops_policy = (
        "You are an SRE reliability assistant for Acme Corp. Favor concise,"
        " actionable answers — on-call engineers are time-pressed. When"
        " reporting incident counts, always include severity breakdown."
        " For any metric that is `lagging`, flag whether the trend is"
        " improving or degrading vs the prior period. When reasoning about"
        " deploy/incident causation, cite the confidence level from"
        " trace_metric_impacts — correlated is NOT causal."
    )
    options = ClaudeAgentOptions(
        model="claude-sonnet-4-6",
        system_prompt=f"{ops_policy}\n\n{dc.to_system_prompt()}",
        mcp_servers={"dc": server},
        allowed_tools=[f"mcp__dc__{t.name}" for t in tools],
    )

    async for message in query(prompt=prompt, options=options):
        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    print(block.text)


async def _run_demo(
    dc: DataContract,
    semantic: YamlSource,
    tools: list,
    prompt: str,
    caller: str,
    adapter: DuckDBAdapter,
) -> None:
    print(f"Query: {prompt}\n")

    # ── 1. Domain + metric discovery ──────────────────────────────────────────
    lookup_domain = next(t for t in tools if t.name == "lookup_domain")
    result = await lookup_domain.callable({"name": "reliability"})
    print("=== Lookup Domain (reliability) ===")
    print(result["content"][0]["text"])

    lookup_metric = next(t for t in tools if t.name == "lookup_metric")
    result = await lookup_metric.callable({"metric_name": "mttr_minutes"})
    print("\n=== Lookup Metric (mttr_minutes) ===")
    print(result["content"][0]["text"])

    # ── 2. Impact graph — shows the negative-direction edge ──────────────────
    trace = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await trace.callable(
        {"metric_name": "incident_count_24h", "direction": "upstream"}
    )
    print("\n=== Trace Metric Impacts (upstream drivers of incident_count_24h) ===")
    print(result["content"][0]["text"])

    # ── 3. Block-level `require_limit` fires ─────────────────────────────────
    inspect = next(t for t in tools if t.name == "inspect_query")
    unlimited_sql = (
        "SELECT id, severity, opened_at FROM sre.incidents WHERE tenant_id = 'acme'"
    )
    result = await inspect.callable({"sql": unlimited_sql})
    data = json.loads(result["content"][0]["text"])
    print("\n=== Blocked: incidents query without LIMIT ===")
    print(f"  valid: {data['valid']}, violations: {data['violations']}")

    # ── 4. Log-level PII audit fires when user_email is selected ─────────────
    run = next(t for t in tools if t.name == "run_query")
    pii_sql = (
        "SELECT id, severity, user_email FROM sre.incidents "
        "WHERE tenant_id = 'acme' "
        "  AND opened_at >= CURRENT_DATE - INTERVAL 7 DAY "
        "LIMIT 20"
    )
    result = await run.callable({"sql": pii_sql})
    print("\n=== Log-level PII audit fires (query runs; governance notified) ===")
    print(result["content"][0]["text"][:400])

    # ── 5. Second log-level audit — deploy metadata ──────────────────────────
    deploy_sql = (
        "SELECT id, service_id, commit_sha, success FROM sre.deploys "
        "WHERE tenant_id = 'acme' "
        "  AND deployed_at >= CURRENT_DATE - INTERVAL 7 DAY "
        "LIMIT 20"
    )
    result = await run.callable({"sql": deploy_sql})
    print("\n=== Second log-level audit (deploy metadata) ===")
    print(result["content"][0]["text"][:400])

    # ── 6. Valid MTTR-by-severity query runs ─────────────────────────────────
    mttr_sql = (
        "SELECT severity, "
        "       ROUND(AVG(EXTRACT(EPOCH FROM (resolved_at - opened_at)) / 60.0), 1) AS mttr_min, "
        "       COUNT(id) AS n "
        "FROM sre.incidents "
        "WHERE tenant_id = 'acme' "
        "  AND resolved_at IS NOT NULL "
        "  AND opened_at >= CURRENT_DATE - INTERVAL 7 DAY "
        "GROUP BY severity "
        "LIMIT 10"
    )
    result = await run.callable({"sql": mttr_sql})
    print("\n=== MTTR by severity (last 7 days) ===")
    print(result["content"][0]["text"])

    # ── 7. Staleness check — old `incident_count_24h -> mttr_minutes` edge ───
    findings = dc.find_stale(semantic, threshold_days=90)
    print("\n=== Stale-review findings (threshold=90 days) ===")
    if not findings:
        print("  (none — every artefact is within review threshold)")
    for f in findings:
        print(f"  [{f.kind}] {f.name} — age_days={f.age_days}")

    # ── 8. Principal gate — sre.deploys has blocked_principals = [intern] ────
    # Uses two fresh tools instances with fixed identities (regardless of the
    # --caller CLI flag, which drives the primary agent persona above). Shows
    # the same deploy query succeeding for an authorized SRE and being blocked
    # for an intern before reaching DuckDB. Then shows that the intern can
    # still query sre.incidents (no principal gate there) — principal access
    # is per-table, not contract-wide.
    _ = caller  # the CLI caller is captured in `tools` above; step 8 is didactic
    deploy_sql = (
        "SELECT id, service_id, success FROM sre.deploys "
        "WHERE tenant_id = 'acme' LIMIT 3"
    )
    incident_sql = (
        "SELECT id, severity FROM sre.incidents WHERE tenant_id = 'acme' LIMIT 3"
    )
    print("\n=== Principal gate: sre.deploys blocks intern@co.com ===")

    authorized_tools = create_tools(
        dc,
        adapter=adapter,
        semantic_source=semantic,
        caller_principal="sre_lead@co.com",
    )
    authorized_run = next(t for t in authorized_tools if t.name == "run_query")
    authorized_result = await authorized_run.callable({"sql": deploy_sql})
    authorized_text = authorized_result["content"][0]["text"]
    print("\nAs sre_lead@co.com on sre.deploys:")
    if authorized_text.startswith("BLOCKED"):
        print(f"  {authorized_text[:200]}")
    else:
        body = _parse_run_query_body(authorized_text)
        row_count = body.get("row_count") if body else "?"
        print(f"  allowed — {row_count} rows returned")

    intern_tools = create_tools(
        dc,
        adapter=adapter,
        semantic_source=semantic,
        caller_principal="intern@co.com",
    )
    intern_run = next(t for t in intern_tools if t.name == "run_query")
    intern_deploy_result = await intern_run.callable({"sql": deploy_sql})
    print("\nAs intern@co.com on sre.deploys (blocklisted):")
    print(f"  {intern_deploy_result['content'][0]['text'][:300]}")

    intern_incident_result = await intern_run.callable({"sql": incident_sql})
    intern_incident_text = intern_incident_result["content"][0]["text"]
    print("\nAs intern@co.com on sre.incidents (no principal gate — per-table scope):")
    if intern_incident_text.startswith("BLOCKED"):
        print(f"  {intern_incident_text[:200]}")
    else:
        body = _parse_run_query_body(intern_incident_text)
        row_count = body.get("row_count") if body else "?"
        print(f"  allowed — {row_count} rows returned")


if __name__ == "__main__":
    main()
