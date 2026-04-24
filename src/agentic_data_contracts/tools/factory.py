"""Tool factory — creates 9 agent tools from a DataContract."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter, SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.principal import Principal, resolve_principal
from agentic_data_contracts.core.schema import Domain
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    SemanticSource,
    build_metric_impact_index,
    build_relationship_index,
    find_join_path,
    walk_metric_impacts,
)
from agentic_data_contracts.validation.validator import Validator

logger = logging.getLogger(__name__)


@dataclass
class ToolDef:
    """A tool definition compatible with Claude Agent SDK's @tool decorator."""

    name: str
    description: str
    input_schema: dict[str, Any]
    callable: Any  # async function(args: dict) -> dict


def _text_response(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def _effective_domains(
    metric: MetricDefinition,
    contract_domains: list[Domain],
) -> list[str]:
    """Union of metric.domains (self-declared) and reverse-lookup from Domain.metrics.

    Preserves order: self-declared domains come first, then any additional
    domains discovered via the contract's ``Domain.metrics`` lists.  This
    back-compat shim lets old domain-first YAML and new metric-first YAML
    coexist without duplicating declarations.
    """
    result = list(metric.domains)
    for d in contract_domains:
        if metric.name in d.metrics and d.name not in result:
            result.append(d.name)
    return result


def _format_impact_edge(edge: MetricImpact, *, perspective: str) -> str:
    """Render a MetricImpact as a one-line, citation-ready string.

    ``perspective="outgoing"`` emits ``"<direction> impact on <to>
    (<confidence>): <evidence>"``; ``perspective="incoming"`` flips
    the preposition and shows the driver's name.
    """
    if perspective == "outgoing":
        target, prep = edge.to_metric, "on"
    else:
        target, prep = edge.from_metric, "from"
    summary = f"{edge.direction} impact {prep} {target} ({edge.confidence})"
    if edge.evidence:
        summary += f": {edge.evidence}"
    return summary


def _metric_details(
    metric: MetricDefinition,
    contract_domains: list[Domain],
    impact_index: dict[str, list[MetricImpact]],
) -> dict[str, Any]:
    """Serialize a metric with all enrichment fields for tool responses."""
    data: dict[str, Any] = {
        "name": metric.name,
        "description": metric.description,
        "sql_expression": metric.sql_expression,
        "source_model": metric.source_model,
        "filters": metric.filters,
    }
    effective = _effective_domains(metric, contract_domains)
    if effective:
        data["domains"] = effective
    if metric.tier:
        data["tier"] = metric.tier
    if metric.indicator_kind:
        data["indicator_kind"] = metric.indicator_kind

    outgoing: list[str] = []
    incoming: list[str] = []
    for edge in impact_index.get(metric.name, []):
        if edge.from_metric == metric.name:
            outgoing.append(_format_impact_edge(edge, perspective="outgoing"))
        if edge.to_metric == metric.name:
            incoming.append(_format_impact_edge(edge, perspective="incoming"))
    if outgoing:
        data["impacts"] = outgoing
    if incoming:
        data["impacted_by"] = incoming
    return data


def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
    caller_principal: Principal = None,
) -> list[ToolDef]:
    if session is None:
        session = ContractSession(contract)

    # Auto-load semantic source from contract config if not provided
    if semantic_source is None:
        semantic_source = contract.load_semantic_source()

    # Resolve wildcard tables if adapter is available
    if contract.has_wildcard_tables():
        if adapter is not None:
            contract.resolve_tables(adapter)
        else:
            logger.warning(
                "Contract has wildcard tables (tables: ['*']) but no database"
                " adapter was provided to create_tools(). Wildcards will remain"
                " unresolved, and tools like describe_table will treat"
                " wildcard-schema tables as 'not in allowed tables list'. Pass a"
                " DatabaseAdapter to enable resolution."
            )

    dialect = adapter.dialect if adapter else None
    sql_normalizer = adapter if isinstance(adapter, SqlNormalizer) else None
    validator = Validator(
        contract,
        dialect=dialect,
        explain_adapter=adapter,
        sql_normalizer=sql_normalizer,
        semantic_source=semantic_source,
        caller_principal=caller_principal,
    )

    # Build relationship index for BFS path-finding in lookup_relationships.
    # This is a snapshot, same pattern as Validator (validator.py:70) which also
    # captures relationships at construction time.  Direct table lookups go
    # through semantic_source.get_relationships_for_table() instead.
    _rel_index = (
        build_relationship_index(semantic_source.get_relationships())
        if semantic_source is not None
        else {}
    )

    # Build metric-impact index for lookup_metric enrichment and trace_metric_impacts.
    _metric_impacts: list[MetricImpact] = (
        list(semantic_source.get_metric_impacts())
        if semantic_source is not None
        else []
    )
    _impact_index = build_metric_impact_index(_metric_impacts)

    _contract_domains = list(contract.schema.semantic.domains)
    metric_names_set = (
        {m.name for m in semantic_source.get_metrics()}
        if semantic_source is not None
        else set()
    )

    # Validate domain references
    if contract.schema.semantic.domains:
        allowed_tables_set = set(contract.allowed_table_names())
        for domain in contract.schema.semantic.domains:
            if semantic_source is not None:
                for metric_name in domain.metrics:
                    if metric_name not in metric_names_set:
                        logger.warning(
                            "Domain '%s' references unknown metric '%s'",
                            domain.name,
                            metric_name,
                        )
            for table in domain.tables:
                if table not in allowed_tables_set:
                    logger.warning(
                        "Domain '%s' references table '%s' not in allowed_tables",
                        domain.name,
                        table,
                    )

    # Validate metric-impact references — mirrors the domain validation above.
    if _metric_impacts and semantic_source is not None:
        for impact in _metric_impacts:
            if impact.from_metric not in metric_names_set:
                logger.warning(
                    "Metric impact references unknown from_metric '%s' (-> '%s')",
                    impact.from_metric,
                    impact.to_metric,
                )
            if impact.to_metric not in metric_names_set:
                logger.warning(
                    "Metric impact references unknown to_metric '%s' (from '%s')",
                    impact.to_metric,
                    impact.from_metric,
                )

    # ── Tool 1: describe_table ────────────────────────────────────────────────
    async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
        schema_name = args.get("schema", "")
        table_name = args.get("table", "")
        qualified = f"{schema_name}.{table_name}"
        if qualified not in contract.allowed_table_names():
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
            )
        principal = resolve_principal(caller_principal)
        if qualified not in contract.allowed_table_names_for(principal):
            who = principal if principal else "<no caller identified>"
            return _text_response(
                f"Table {qualified} is restricted; not available to {who!r}."
            )
        if adapter is None:
            return _text_response(
                f"No database adapter configured — table description unavailable"
                f" for {qualified}."
            )
        ts = adapter.describe_table(schema_name, table_name)
        cols = [
            {"name": c.name, "type": c.type, "nullable": c.nullable} for c in ts.columns
        ]
        return _text_response(
            json.dumps({"schema": schema_name, "table": table_name, "columns": cols})
        )

    # ── Tool 2: preview_table ─────────────────────────────────────────────────
    async def preview_table(args: dict[str, Any]) -> dict[str, Any]:
        schema = args.get("schema", "")
        table = args.get("table", "")
        try:
            limit = max(1, min(int(args.get("limit", 5)), 100))
        except (ValueError, TypeError):
            limit = 5
        qualified = f"{schema}.{table}"
        if qualified not in contract.allowed_table_names():
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
            )
        principal = resolve_principal(caller_principal)
        if qualified not in contract.allowed_table_names_for(principal):
            who = principal if principal else "<no caller identified>"
            return _text_response(
                f"Table {qualified} is restricted; not available to {who!r}."
            )
        if adapter is None:
            return _text_response(
                "No database adapter configured — preview unavailable."
            )
        # preview_table intentionally uses SELECT * — it's a discovery tool
        # and the table has already been verified against the allowlist above.
        result = adapter.execute(f"SELECT * FROM {qualified} LIMIT {limit}")
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        return _text_response(
            json.dumps({"schema": schema, "table": table, "rows": rows}, default=str)
        )

    # ── Tool 3: list_metrics ──────────────────────────────────────────────────
    async def list_metrics(args: dict[str, Any]) -> dict[str, Any]:
        if semantic_source is None:
            return _text_response("No semantic source configured.")
        metrics = semantic_source.get_metrics()
        domain_filter = args.get("domain")
        if domain_filter:
            domain_obj = contract.get_domain(domain_filter)
            declared_in_metrics = any(domain_filter in m.domains for m in metrics)
            if domain_obj is None and not declared_in_metrics:
                all_doms = contract.schema.semantic.domains
                declared_names = {d for m in metrics for d in m.domains}
                available = (
                    sorted({d.name for d in all_doms} | declared_names)
                    if all_doms or declared_names
                    else []
                )
                return _text_response(
                    f"Domain '{domain_filter}' not found."
                    f" Available domains: {available}"
                )
            # Union: contract's Domain.metrics AND self-declared metric.domains.
            contract_names = set(domain_obj.metrics) if domain_obj else set()
            metrics = [
                m
                for m in metrics
                if m.name in contract_names or domain_filter in m.domains
            ]

        tier_filter = args.get("tier")
        if tier_filter:
            metrics = [m for m in metrics if tier_filter in m.tier]

        indicator_filter = args.get("indicator_kind")
        if indicator_filter:
            metrics = [m for m in metrics if m.indicator_kind == indicator_filter]

        data: list[dict[str, Any]] = []
        for m in metrics:
            entry: dict[str, Any] = {
                "name": m.name,
                "description": m.description,
                "source_model": m.source_model,
            }
            if m.tier:
                entry["tier"] = m.tier
            if m.indicator_kind:
                entry["indicator_kind"] = m.indicator_kind
            data.append(entry)
        return _text_response(json.dumps({"metrics": data}))

    # ── Tool 4: lookup_metric ─────────────────────────────────────────────────
    async def lookup_metric(args: dict[str, Any]) -> dict[str, Any]:
        metric_name = args.get("metric_name", "")
        if semantic_source is None:
            return _text_response("No semantic source configured.")
        # Try exact match first
        metric = semantic_source.get_metric(metric_name)
        if metric is not None:
            return _text_response(
                json.dumps(_metric_details(metric, _contract_domains, _impact_index))
            )
        # Fuzzy fallback
        candidates = semantic_source.search_metrics(metric_name)
        if not candidates:
            return _text_response(f"Metric '{metric_name}' not found.")
        data = [
            _metric_details(m, _contract_domains, _impact_index) for m in candidates
        ]
        return _text_response(
            json.dumps(
                {
                    "query": metric_name,
                    "exact_match": False,
                    "candidates": data,
                }
            )
        )

    # ── Tool 5: lookup_domain ───────────────────────────────────────────
    async def lookup_domain(args: dict[str, Any]) -> dict[str, Any]:
        name = args.get("name", "")
        domain = contract.get_domain(name)

        if domain is not None:
            # Exact match — enrich metrics with descriptions from semantic source
            if semantic_source is not None:
                metric_data: list[Any] = []
                for metric_name in domain.metrics:
                    m = semantic_source.get_metric(metric_name)
                    if m is not None:
                        metric_data.append(
                            {"name": m.name, "description": m.description}
                        )
                    else:
                        metric_data.append({"name": metric_name, "description": ""})
            else:
                metric_data = list(domain.metrics)

            data: dict[str, Any] = {
                "name": domain.name,
                "summary": domain.summary,
                "description": domain.description,
                "metrics": metric_data,
            }
            if domain.tables:
                data["tables"] = domain.tables
            return _text_response(json.dumps(data))

        # Fuzzy fallback over domain names
        all_domains = contract.schema.semantic.domains
        if not all_domains:
            return _text_response(f"Domain '{name}' not found. No domains defined.")

        from thefuzz import fuzz, process

        choices = {d.name: d.name for d in all_domains}
        results = process.extractBests(
            name,
            choices,
            scorer=fuzz.token_set_ratio,
            score_cutoff=50,
            limit=3,
        )
        if not results:
            available = [d.name for d in all_domains]
            return _text_response(
                f"Domain '{name}' not found. Available domains: {available}"
            )

        candidates = []
        for _, _, key in results:
            d = contract.get_domain(key)
            if d is not None:
                candidates.append(
                    {
                        "name": d.name,
                        "summary": d.summary,
                        "metric_count": len(d.metrics),
                    }
                )
        return _text_response(
            json.dumps(
                {
                    "query": name,
                    "exact_match": False,
                    "candidates": candidates,
                }
            )
        )

    # ── Tool 6: lookup_relationships ────────────────────────────────────────
    async def lookup_relationships(args: dict[str, Any]) -> dict[str, Any]:
        table = args.get("table", "")
        target_table = args.get("target_table")
        if semantic_source is None:
            return _text_response("No semantic source configured.")

        if target_table:
            # Graph walk: find join path between two tables
            path = find_join_path(_rel_index, table, target_table)
            if path is None:
                return _text_response(
                    f"No join path found between '{table}' and '{target_table}'"
                    " within 3 hops."
                )
            data = [
                {
                    "from": r.from_,
                    "to": r.to,
                    "type": r.type,
                    "description": r.description,
                    **(
                        {"required_filter": r.required_filter}
                        if r.required_filter
                        else {}
                    ),
                }
                for r in path
            ]
            return _text_response(
                json.dumps(
                    {
                        "table": table,
                        "target_table": target_table,
                        "join_path": data,
                        "hops": len(data),
                    }
                )
            )

        # Direct lookup: all relationships involving this table
        rels = semantic_source.get_relationships_for_table(table)
        if not rels:
            return _text_response(f"No relationships found for table '{table}'.")
        data = [
            {
                "from": r.from_,
                "to": r.to,
                "type": r.type,
                "description": r.description,
                **({"required_filter": r.required_filter} if r.required_filter else {}),
            }
            for r in rels
        ]
        return _text_response(json.dumps({"table": table, "relationships": data}))

    # ── Tool 7: trace_metric_impacts ──────────────────────────────────────────
    async def trace_metric_impacts(args: dict[str, Any]) -> dict[str, Any]:
        metric_name = args.get("metric_name", "")
        direction = args.get("direction", "upstream")
        try:
            max_depth = max(1, min(int(args.get("max_depth", 2)), 10))
        except (ValueError, TypeError):
            max_depth = 2

        if direction not in ("upstream", "downstream"):
            return _text_response(
                f"direction must be 'upstream' or 'downstream', got {direction!r}."
            )
        if semantic_source is None:
            return _text_response("No semantic source configured.")
        if semantic_source.get_metric(metric_name) is None:
            return _text_response(f"Metric '{metric_name}' not found.")

        walk = walk_metric_impacts(
            _impact_index, metric_name, direction=direction, max_depth=max_depth
        )
        edges = [
            {
                "depth": depth,
                "from": edge.from_metric,
                "to": edge.to_metric,
                "direction": edge.direction,
                "confidence": edge.confidence,
                **({"evidence": edge.evidence} if edge.evidence else {}),
                **({"description": edge.description} if edge.description else {}),
            }
            for depth, edge in walk
        ]
        return _text_response(
            json.dumps(
                {
                    "metric_name": metric_name,
                    "direction": direction,
                    "max_depth": max_depth,
                    "edges": edges,
                }
            )
        )

    # ── Tool 8: inspect_query ─────────────────────────────────────────────────
    async def inspect_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")
        result = validator.validate(sql)
        data: dict[str, Any] = {
            "valid": not result.blocked,
            "violations": list(result.reasons),
            "warnings": list(result.warnings),
            "log_messages": list(result.log_messages),
            "schema_valid": result.schema_valid,
            "explain_errors": list(result.explain_errors),
            "pending_result_checks": list(validator.pending_result_check_names()),
        }
        if result.estimated_cost_usd is not None:
            data["estimated_cost_usd"] = result.estimated_cost_usd
        if result.estimated_rows is not None:
            data["estimated_rows"] = result.estimated_rows
        return _text_response(json.dumps(data, default=str))

    # ── Tool 9: run_query ─────────────────────────────────────────────────────
    async def run_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")

        def _with_remaining(msg: str) -> str:
            return f"{msg}\nRemaining: {json.dumps(session.remaining(), default=str)}"

        # Check session limits first
        try:
            session.check_limits()
        except LimitExceededError as e:
            return _text_response(
                _with_remaining(f"BLOCKED — Session limit exceeded: {e}")
            )

        # Phase 1 + 2: query checks + EXPLAIN
        vresult = validator.validate(sql)
        if vresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Violations:\n" + "\n".join(
                f"- {r}" for r in vresult.reasons
            )
            return _text_response(_with_remaining(msg))

        # Record estimated cost from EXPLAIN — charged before execution because
        # the cost budget tracks database resource consumption, not successful
        # operations. Even if result checks later block the output, the database
        # work was performed.
        if vresult.estimated_cost_usd is not None:
            session.record_cost(vresult.estimated_cost_usd)

        if adapter is None:
            return _text_response(
                "No database adapter configured — cannot execute query."
            )

        try:
            qresult = adapter.execute(sql)
        except Exception as e:  # noqa: BLE001
            session.record_retry()
            return _text_response(
                _with_remaining(f"BLOCKED — Query execution failed: {e}")
            )

        # Phase 3: result checks
        rresult = validator.validate_results(
            sql, qresult.columns, [tuple(r) for r in qresult.rows]
        )
        if rresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Result check violations:\n" + "\n".join(
                f"- {r}" for r in rresult.reasons
            )
            return _text_response(_with_remaining(msg))

        rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
        data = {
            "columns": qresult.columns,
            "rows": rows,
            "row_count": qresult.row_count,
            "session": {"remaining": session.remaining()},
        }
        response_text = json.dumps(data, default=str)

        # Prepend warnings and log-enforcement messages from both query checks
        # and result checks. log_messages surface rules with enforcement=log
        # that triggered during validation — symmetric with inspect_query.
        all_warnings = vresult.warnings + rresult.warnings
        all_logs = vresult.log_messages + rresult.log_messages
        preamble_parts: list[str] = []
        if all_warnings:
            preamble_parts.append(
                "WARNINGS:\n" + "\n".join(f"- {w}" for w in all_warnings)
            )
        if all_logs:
            preamble_parts.append("LOG:\n" + "\n".join(f"- {m}" for m in all_logs))
        if preamble_parts:
            response_text = "\n\n".join(preamble_parts) + "\n\n" + response_text

        return _text_response(response_text)

    # ── Assemble ToolDef list ─────────────────────────────────────────────────
    return [
        ToolDef(
            name="describe_table",
            description=(
                "Get full column details for a specific table from the database."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                },
                "required": ["schema", "table"],
            },
            callable=describe_table,
        ),
        ToolDef(
            name="preview_table",
            description="Preview sample rows from an allowed table.",
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {"type": "string", "description": "Schema name"},
                    "table": {"type": "string", "description": "Table name"},
                    "limit": {
                        "type": "integer",
                        "description": "Number of rows to return (default 5)",
                    },
                },
                "required": ["schema", "table"],
            },
            callable=preview_table,
        ),
        ToolDef(
            name="list_metrics",
            description=(
                "List metric definitions from the semantic source,"
                " optionally filtered by domain, tier, or indicator_kind."
                " Entries include tier and indicator_kind when available —"
                " use these to prioritize north-stars, department/team KPIs,"
                " or leading vs lagging indicators."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "domain": {
                        "type": "string",
                        "description": "Optional domain to filter metrics by",
                    },
                    "tier": {
                        "type": "string",
                        "description": (
                            "Optional tier to filter by"
                            " (e.g. 'north_star', 'department_kpi', 'team_kpi')"
                        ),
                    },
                    "indicator_kind": {
                        "type": "string",
                        "description": (
                            "Optional indicator kind to filter by:"
                            " 'leading' or 'lagging'"
                        ),
                    },
                },
                "required": [],
            },
            callable=list_metrics,
        ),
        ToolDef(
            name="lookup_metric",
            description=(
                "Get the full definition of a specific metric including SQL"
                " expression, tier (north_star / department_kpi / team_kpi),"
                " indicator_kind (leading / lagging), and any metric-impact"
                " edges (impacts and impacted_by, each with direction,"
                " confidence, and evidence citation). Use the impact fields"
                " to reason about drivers and downstream effects."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "metric_name": {
                        "type": "string",
                        "description": "Name of the metric to look up",
                    }
                },
                "required": ["metric_name"],
            },
            callable=lookup_metric,
        ),
        ToolDef(
            name="lookup_domain",
            description=(
                "Look up a business domain by name to get its full description,"
                " associated metrics, and tables. Use this to understand"
                " business context before querying."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the domain to look up",
                    }
                },
                "required": ["name"],
            },
            callable=lookup_domain,
        ),
        ToolDef(
            name="inspect_query",
            description=(
                "Inspect a SQL query without executing it. Returns structured JSON"
                " with: `valid` (bool), `violations` (block-enforcement rule"
                " messages), `warnings` (warn-enforcement rule messages),"
                " `log_messages` (log-enforcement rule messages), `schema_valid`"
                " and `explain_errors` (from the EXPLAIN layer), and"
                " `pending_result_checks` (result checks that would run after"
                " execution). When a database adapter is configured, also"
                " includes `estimated_cost_usd` and `estimated_rows` from EXPLAIN."
                " Use this to iterate on SQL before spending retry budget on run_query."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to inspect"}
                },
                "required": ["sql"],
            },
            callable=inspect_query,
        ),
        ToolDef(
            name="run_query",
            description="Validate and execute a SQL query, returning the results.",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to execute"}
                },
                "required": ["sql"],
            },
            callable=run_query,
        ),
        ToolDef(
            name="lookup_relationships",
            description=(
                "Look up table relationships (join paths) involving a specific"
                " table. Returns join columns, types, descriptions, and required"
                " filters. When target_table is provided, finds the shortest"
                " multi-hop join path between the two tables (up to 3 hops)."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": (
                            'Fully qualified table name, e.g. "schema.table"'
                        ),
                    },
                    "target_table": {
                        "type": "string",
                        "description": (
                            "Optional: find the shortest join path from table"
                            " to this target table (multi-hop supported)"
                        ),
                    },
                },
                "required": ["table"],
            },
            callable=lookup_relationships,
        ),
        ToolDef(
            name="trace_metric_impacts",
            description=(
                "Walk the metric-impact graph from a starting metric."
                " direction='upstream' returns metrics that drive the target"
                " (useful for root-cause analyses like 'why did revenue"
                " drop?'); direction='downstream' returns metrics the target"
                " affects (useful for 'what does this KPI move?')."
                " Each edge includes direction, confidence, and evidence for"
                " grounded reasoning. Cycles are handled via visited tracking."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "metric_name": {
                        "type": "string",
                        "description": "Metric to walk the impact graph from",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream"],
                        "description": (
                            "'upstream' for drivers, 'downstream' for"
                            " affected metrics. Default 'upstream'."
                        ),
                    },
                    "max_depth": {
                        "type": "integer",
                        "description": "Max BFS depth (default 2)",
                    },
                },
                "required": ["metric_name"],
            },
            callable=trace_metric_impacts,
        ),
    ]
