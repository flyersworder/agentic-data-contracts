"""Growth analytics agent — demonstrates governance patterns distinct from
revenue_agent:

- `verified` / `correlated` / `hypothesized` metric impacts (revenue_agent only
  has `hypothesized`)
- `log`-level rule for PII auditing (not advertised in the system prompt)
- Time-bounded event queries (a block rule that prevents unbounded event scans)
- Stale-review detection flagging an un-reviewed impact edge

It also doubles as the reference template for layering Anthropic's **`data`
plugin** (analyst skills) on top of contract-governed tools. The governed
in-process MCP server stays the *only* path to the warehouse — the plugin's
own `.mcp.json` warehouse servers are suppressed with ``strict_mcp_config=True``
so there is no ungoverned side door. See ``_run_with_sdk`` below.

Usage:
    uv run python examples/growth_agent/setup_db.py
    uv run python examples/growth_agent/agent.py "Which onboarding variant lifted activation?"

To enable the data-plugin skills overlay (optional), clone the plugins repo and
point the agent at the ``data`` sub-plugin:

    git clone https://github.com/anthropics/knowledge-work-plugins /tmp/kwp
    DATA_PLUGIN_PATH=/tmp/kwp/data \\
        uv run python examples/growth_agent/agent.py "Which variant lifted activation?"

Requires claude-agent-sdk for the LLM path; falls back to a demo mode that
exercises the tools directly. The plugin overlay needs claude-agent-sdk>=0.1.x
with ``plugins`` / ``skills`` support; older versions degrade gracefully to the
governed-tools-only path.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import sys
from pathlib import Path

from agentic_data_contracts import DataContract, create_tools
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.semantic.yaml_source import YamlSource

EXAMPLE_DIR = Path(__file__).parent

# Curated subset of the `data` plugin's skills that complement governed tools.
# These do analytical *craft* (the contract supplies metric *meaning*).
#   - data-context-extractor is deliberately OMITTED: it generates a parallel
#     semantic skill that would compete with this contract as the source of
#     metric truth.
#   - create-viz / build-dashboard are omitted too: they need code-execution
#     tools (Bash/file), which this governed agent does not grant.
DATA_PLUGIN_SKILLS = [
    "validate-data",  # pre-share methodology / bias QA on the analysis
    "statistical-analysis",  # significance testing + descriptive rigor
    "explore-data",  # profiling / data-quality checks (routes via run_query)
    "sql-queries",  # window-function & dialect craft for funnels/cohorts
]


def _resolve_data_plugin() -> Path | None:
    """Return the local `data` plugin directory if configured and valid.

    The plugin is an external checkout, so it is opt-in via ``DATA_PLUGIN_PATH``.
    Returns None (with a hint) when unset or not a plugin dir, keeping the
    example runnable with zero external setup.
    """
    raw = os.environ.get("DATA_PLUGIN_PATH")
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not (path / ".claude-plugin").is_dir() and not (path / ".mcp.json").is_file():
        print(
            f"DATA_PLUGIN_PATH={raw!r} is not a plugin directory "
            "(no .claude-plugin/ or .mcp.json) — skipping plugin overlay.\n"
        )
        return None
    return path


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
        " Always resolve business metrics via lookup_metric / lookup_domain before"
        " writing SQL; use the data-plugin SQL/stats skills only to shape and QA"
        " the analysis around the contract's validated metric definitions."
    )

    # Base options: the governed in-process server is the ONLY data path, and its
    # tools are the only ones auto-allowed.
    opts_kwargs: dict = {
        "model": "claude-sonnet-4-6",
        "mcp_servers": {"dc": server},
        "allowed_tools": [f"mcp__dc__{t.name}" for t in tools],
    }

    # ── Optional: overlay the `data` plugin's analyst skills ──────────────────
    # Only when (a) the SDK supports it and (b) a plugin checkout is configured.
    option_fields = {f.name for f in dataclasses.fields(ClaudeAgentOptions)}
    sdk_supports_plugins = {"plugins", "skills", "strict_mcp_config"} <= option_fields
    plugin_path = _resolve_data_plugin()

    if sdk_supports_plugins and plugin_path is not None:
        opts_kwargs["plugins"] = [{"type": "local", "path": str(plugin_path)}]
        opts_kwargs["skills"] = DATA_PLUGIN_SKILLS
        # THE GUARD: load the plugin's skills but IGNORE its bundled .mcp.json
        # warehouse servers — so the agent cannot bypass the contract. Only the
        # "dc" server above is reachable.
        opts_kwargs["strict_mcp_config"] = True
        # Skills/commands need the claude_code system-prompt harness; append the
        # growth policy + contract governance on top of it.
        opts_kwargs["system_prompt"] = {
            "type": "preset",
            "preset": "claude_code",
            "append": f"{growth_policy}\n\n{dc.to_system_prompt()}",
        }
        print(
            f"[plugin overlay] data-plugin skills enabled from {plugin_path} "
            f"({', '.join(DATA_PLUGIN_SKILLS)}); warehouse access stays governed.\n"
        )
    else:
        # Governed-tools-only path (default). Plain-string prompt works on any SDK.
        opts_kwargs["system_prompt"] = f"{growth_policy}\n\n{dc.to_system_prompt()}"
        if plugin_path is None:
            print(
                "[plugin overlay] disabled — set DATA_PLUGIN_PATH to a local "
                "`data` plugin checkout to enable the analyst skills.\n"
            )
        elif not sdk_supports_plugins:
            print(
                "[plugin overlay] skipped — installed claude-agent-sdk lacks "
                "plugins/skills support; upgrade to enable.\n"
            )

    options = ClaudeAgentOptions(**opts_kwargs)

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
