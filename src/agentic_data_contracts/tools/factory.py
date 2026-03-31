"""Tool factory — creates 10 agent tools from a DataContract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.validation.validator import Validator


@dataclass
class ToolDef:
    """A tool definition compatible with Claude Agent SDK's @tool decorator."""

    name: str
    description: str
    input_schema: dict[str, Any]
    callable: Any  # async function(args: dict) -> dict


def _text_response(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def create_tools(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    semantic_source: SemanticSource | None = None,
    session: ContractSession | None = None,
) -> list[ToolDef]:
    if session is None:
        session = ContractSession(contract)

    # Auto-load semantic source from contract config if not provided
    if semantic_source is None:
        semantic_source = contract.load_semantic_source()

    # Resolve wildcard tables if adapter is available
    if adapter is not None and contract.has_wildcard_tables():
        contract.resolve_tables(adapter)

    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect, explain_adapter=adapter)

    # ── Tool 1: list_schemas ──────────────────────────────────────────────────
    async def list_schemas(args: dict[str, Any]) -> dict[str, Any]:
        schemas = [
            entry.schema_
            for entry in contract.schema.semantic.allowed_tables
            if entry.tables
        ]
        return _text_response(json.dumps({"schemas": schemas}))

    # ── Tool 2: list_tables ───────────────────────────────────────────────────
    async def list_tables(args: dict[str, Any]) -> dict[str, Any]:
        schema_filter = args.get("schema")
        try:
            limit = max(1, int(args.get("limit", 50)))
        except (ValueError, TypeError):
            limit = 50
        try:
            offset = max(0, int(args.get("offset", 0)))
        except (ValueError, TypeError):
            offset = 0
        all_tables: list[dict[str, Any]] = []
        for entry in contract.schema.semantic.allowed_tables:
            if schema_filter and entry.schema_ != schema_filter:
                continue
            if "*" in entry.tables:
                return _text_response(
                    f"Schema '{entry.schema_}' uses wildcard tables"
                    " but no database adapter is available"
                    " to resolve them."
                )
            for table in entry.tables:
                info: dict[str, Any] = {
                    "schema": entry.schema_,
                    "table": table,
                }
                if semantic_source is not None:
                    ts = semantic_source.get_table_schema(entry.schema_, table)
                    if ts is not None:
                        info["columns"] = [c.name for c in ts.columns]
                all_tables.append(info)
        total = len(all_tables)
        page = all_tables[offset : offset + limit]
        result: dict[str, Any] = {"tables": page, "total": total}
        if offset + limit < total:
            result["next_offset"] = offset + limit
        return _text_response(json.dumps(result))

    # ── Tool 3: describe_table ────────────────────────────────────────────────
    async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
        schema_name = args.get("schema", "")
        table_name = args.get("table", "")
        qualified = f"{schema_name}.{table_name}"
        if qualified not in contract.allowed_table_names():
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
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

    # ── Tool 4: preview_table ─────────────────────────────────────────────────
    async def preview_table(args: dict[str, Any]) -> dict[str, Any]:
        schema = args.get("schema", "")
        table = args.get("table", "")
        try:
            limit = max(1, min(int(args.get("limit", 5)), 100))
        except (ValueError, TypeError):
            limit = 5
        allowed = contract.allowed_table_names()
        qualified = f"{schema}.{table}"
        if qualified not in allowed:
            return _text_response(
                f"Table {qualified} is not in the allowed tables list."
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

    # ── Tool 5: list_metrics ──────────────────────────────────────────────────
    async def list_metrics(args: dict[str, Any]) -> dict[str, Any]:
        if semantic_source is None:
            return _text_response("No semantic source configured.")
        metrics = semantic_source.get_metrics()
        domain_filter = args.get("domain")
        if domain_filter:
            domains = contract.schema.semantic.domains
            allowed_names = set(domains.get(domain_filter, []))
            if not allowed_names:
                available = list(domains.keys()) if domains else []
                return _text_response(
                    f"Domain '{domain_filter}' not found."
                    f" Available domains: {available}"
                )
            metrics = [m for m in metrics if m.name in allowed_names]
        data = [
            {
                "name": m.name,
                "description": m.description,
                "source_model": m.source_model,
            }
            for m in metrics
        ]
        return _text_response(json.dumps({"metrics": data}))

    # ── Tool 6: lookup_metric ─────────────────────────────────────────────────
    async def lookup_metric(args: dict[str, Any]) -> dict[str, Any]:
        metric_name = args.get("metric_name", "")
        if semantic_source is None:
            return _text_response("No semantic source configured.")
        # Try exact match first
        metric = semantic_source.get_metric(metric_name)
        if metric is not None:
            data = {
                "name": metric.name,
                "description": metric.description,
                "sql_expression": metric.sql_expression,
                "source_model": metric.source_model,
                "filters": metric.filters,
            }
            return _text_response(json.dumps(data))
        # Fuzzy fallback
        candidates = semantic_source.search_metrics(metric_name)
        if not candidates:
            return _text_response(f"Metric '{metric_name}' not found.")
        data = [
            {
                "name": m.name,
                "description": m.description,
                "sql_expression": m.sql_expression,
                "source_model": m.source_model,
                "filters": m.filters,
            }
            for m in candidates
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

    # ── Tool 7: validate_query ────────────────────────────────────────────────
    async def validate_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")
        result = validator.validate(sql)
        if result.blocked:
            msg = "BLOCKED — Violations:\n" + "\n".join(
                f"- {r}" for r in result.reasons
            )
            if result.warnings:
                msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in result.warnings)
        else:
            msg = "VALID — Query passed all pre-execution checks."
            if result.warnings:
                msg += "\nWarnings:\n" + "\n".join(f"- {w}" for w in result.warnings)
            # Note pending result checks
            pending = validator.pending_result_check_names()
            if pending:
                msg += (
                    f"\nNote: {len(pending)} result check(s) will run after execution: "
                    + ", ".join(pending)
                )
        return _text_response(msg)

    # ── Tool 8: query_cost_estimate ───────────────────────────────────────────
    async def query_cost_estimate(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")
        if adapter is None:
            return _text_response(
                "No database adapter configured — cost estimate unavailable."
            )
        explain = adapter.explain(sql)
        data: dict[str, Any] = {
            "schema_valid": explain.schema_valid,
            "errors": explain.errors,
        }
        if explain.estimated_cost_usd is not None:
            data["estimated_cost_usd"] = explain.estimated_cost_usd
        if explain.estimated_rows is not None:
            data["estimated_rows"] = explain.estimated_rows
        return _text_response(json.dumps(data))

    # ── Tool 9: run_query ─────────────────────────────────────────────────────
    async def run_query(args: dict[str, Any]) -> dict[str, Any]:
        sql = args.get("sql", "")

        # Check session limits first
        try:
            session.check_limits()
        except LimitExceededError as e:
            return _text_response(f"BLOCKED — Session limit exceeded: {e}")

        # Phase 1 + 2: query checks + EXPLAIN
        vresult = validator.validate(sql)
        if vresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Violations:\n" + "\n".join(
                f"- {r}" for r in vresult.reasons
            )
            return _text_response(msg)

        # Record estimated cost from EXPLAIN
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
            return _text_response(f"BLOCKED — Query execution failed: {e}")

        # Phase 3: result checks
        rresult = validator.validate_results(
            sql, qresult.columns, [tuple(r) for r in qresult.rows]
        )
        if rresult.blocked:
            session.record_retry()
            msg = "BLOCKED — Result check violations:\n" + "\n".join(
                f"- {r}" for r in rresult.reasons
            )
            return _text_response(msg)

        rows = [dict(zip(qresult.columns, row)) for row in qresult.rows]
        data = {
            "columns": qresult.columns,
            "rows": rows,
            "row_count": qresult.row_count,
        }
        response_text = json.dumps(data, default=str)

        # Prepend warnings from both query checks and result checks
        all_warnings = vresult.warnings + rresult.warnings
        if all_warnings:
            warning_text = "WARNINGS:\n" + "\n".join(f"- {w}" for w in all_warnings)
            response_text = warning_text + "\n\n" + response_text

        return _text_response(response_text)

    # ── Tool 10: get_contract_info ────────────────────────────────────────────
    async def get_contract_info(args: dict[str, Any]) -> dict[str, Any]:
        info: dict[str, Any] = {
            "name": contract.name,
            "allowed_tables": contract.allowed_table_names(),
        }

        rules = []
        for rule in contract.schema.semantic.rules:
            rules.append(
                {
                    "name": rule.name,
                    "description": rule.description,
                    "enforcement": rule.enforcement.value,
                }
            )
        info["rules"] = rules

        if contract.schema.semantic.forbidden_operations:
            info["forbidden_operations"] = contract.schema.semantic.forbidden_operations

        res = contract.schema.resources
        if res:
            limits: dict[str, Any] = {}
            if res.cost_limit_usd is not None:
                limits["cost_limit_usd"] = res.cost_limit_usd
            if res.max_retries is not None:
                limits["max_retries"] = res.max_retries
            if res.token_budget is not None:
                limits["token_budget"] = res.token_budget
            if res.max_query_time_seconds is not None:
                limits["max_query_time_seconds"] = res.max_query_time_seconds
            if res.max_rows_scanned is not None:
                limits["max_rows_scanned"] = res.max_rows_scanned
            info["resource_limits"] = limits

        info["session_remaining"] = session.remaining()

        return _text_response(json.dumps(info, default=str))

    # ── Assemble ToolDef list ─────────────────────────────────────────────────
    return [
        ToolDef(
            name="list_schemas",
            description=(
                "List all allowed database schemas defined in the data contract."
            ),
            input_schema={"type": "object", "properties": {}, "required": []},
            callable=list_schemas,
        ),
        ToolDef(
            name="list_tables",
            description=(
                "List allowed tables, optionally filtered by schema. "
                "Includes column names when semantic source is available. "
                "Paginated \u2014 use limit/offset for large schemas."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "string",
                        "description": "Optional schema name to filter by",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max tables to return (default 50)",
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Skip first N tables (default 0)",
                    },
                },
                "required": [],
            },
            callable=list_tables,
        ),
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
                " optionally filtered by domain."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "domain": {
                        "type": "string",
                        "description": "Optional domain to filter metrics by",
                    }
                },
                "required": [],
            },
            callable=list_metrics,
        ),
        ToolDef(
            name="lookup_metric",
            description=(
                "Get the full definition of a specific metric including SQL expression."
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
            name="validate_query",
            description=(
                "Validate a SQL query against the data contract rules "
                "without executing it."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to validate"}
                },
                "required": ["sql"],
            },
            callable=validate_query,
        ),
        ToolDef(
            name="query_cost_estimate",
            description=(
                "Estimate the cost and row count for a SQL query using EXPLAIN."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to estimate"}
                },
                "required": ["sql"],
            },
            callable=query_cost_estimate,
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
            name="get_contract_info",
            description=(
                "Get the full data contract information including rules, limits, "
                "and session status."
            ),
            input_schema={"type": "object", "properties": {}, "required": []},
            callable=get_contract_info,
        ),
    ]
