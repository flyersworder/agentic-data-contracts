"""Verifies every non-run_query contract tool logs a ToolCall when a recorder
is attached, and behaves byte-identically when it is not.

Fixtures are defined module-locally (not shared) per the task-7 brief: the
`contract` fixture's semantic source points at a dbt manifest that does not
resolve, so `semantic_source` must be passed explicitly to `create_tools`,
mirroring the idiom at the top of tests/test_tools/test_factory.py.
"""

from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def contract() -> DataContract:
    return DataContract.from_yaml(FIXTURES_DIR / "valid_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (
            id INTEGER, amount DECIMAL(10,2), tenant_id VARCHAR, created_at DATE
        );
        INSERT INTO analytics.orders VALUES
            (1, 100.00, 'acme', '2026-08-01'),
            (2, 200.00, 'acme', '2026-08-15');
        CREATE TABLE analytics.customers (id INTEGER, name VARCHAR, tenant_id VARCHAR);
        CREATE TABLE analytics.subscriptions (
            id INTEGER, plan VARCHAR, tenant_id VARCHAR
        );
        """
    )
    return db


@pytest.fixture
def semantic() -> YamlSource:
    return YamlSource(FIXTURES_DIR / "semantic_source.yml")


@pytest.fixture
def no_semantic_contract() -> DataContract:
    """A contract with no `semantic.source` declared at all.

    Unlike `contract` (valid_contract.yml), whose source points at a dbt
    manifest that does not resolve on disk (raising fail-closed), this one
    has no source key, so `create_tools` legitimately builds tools with
    `semantic_source=None` — the scenario the "no semantic source" outcome
    paths actually exercise.
    """
    return DataContract.from_yaml(FIXTURES_DIR / "minimal_contract.yml")


def _tools(contract, recorder, adapter=None, semantic=None):
    session = ContractSession(contract, recorder=recorder)
    return {
        t.name: t.callable
        for t in create_tools(
            contract, adapter=adapter, semantic_source=semantic, session=session
        )
    }


_MINIMAL_ARGS = {
    "describe_table": {"schema": "analytics", "table": "orders"},
    "preview_table": {"schema": "analytics", "table": "orders"},
    "list_metrics": {},
    "lookup_metric": {"metric_name": "total_revenue"},
    "lookup_domain": {"name": "revenue"},
    "lookup_relationships": {"table": "analytics.orders"},
    "trace_metric_impacts": {"metric_name": "total_revenue"},
    "inspect_query": {
        "sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"
    },
    "run_query": {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"},
}


# ── Brief's four required tests ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_exact_metric_lookup_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_metric"]({"metric_name": "total_revenue"})

    assert [(c.tool, c.outcome) for c in rec.calls] == [("lookup_metric", "ok")]


@pytest.mark.asyncio
async def test_fuzzy_metric_lookup_records_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_metric"]({"metric_name": "revenu"})

    assert rec.calls[0].outcome == "miss"


@pytest.mark.asyncio
async def test_no_recorder_records_nothing_and_still_works(contract, semantic):
    session = ContractSession(contract)
    tools = {
        t.name: t.callable
        for t in create_tools(contract, semantic_source=semantic, session=session)
    }
    result = await tools["lookup_metric"]({"metric_name": "total_revenue"})

    assert session.recorder is None
    assert result["content"]


@pytest.mark.asyncio
async def test_every_tool_records_at_least_one_call(contract, adapter, semantic):
    """A wholly uninstrumented new tool must fail CI."""
    for name in [
        "describe_table",
        "preview_table",
        "list_metrics",
        "lookup_metric",
        "lookup_domain",
        "lookup_relationships",
        "trace_metric_impacts",
        "inspect_query",
        "run_query",
    ]:
        rec = ToolRecorder()
        tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
        await tools[name](_MINIMAL_ARGS[name])
        assert rec.calls, f"{name} recorded nothing"
        assert rec.calls[0].tool == name


# ── Outcome-mapping coverage: one test per distinct return path ────────────


@pytest.mark.asyncio
async def test_describe_table_not_allowed_records_blocked(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["describe_table"]({"schema": "raw", "table": "nope"})
    assert rec.calls[0].outcome == "blocked"


@pytest.mark.asyncio
async def test_describe_table_no_adapter_records_error(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["describe_table"]({"schema": "analytics", "table": "orders"})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_describe_table_success_records_ok(contract, adapter, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["describe_table"]({"schema": "analytics", "table": "orders"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_preview_table_not_allowed_records_blocked(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["preview_table"]({"schema": "raw", "table": "nope"})
    assert rec.calls[0].outcome == "blocked"


@pytest.mark.asyncio
async def test_preview_table_no_adapter_records_error(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["preview_table"]({"schema": "analytics", "table": "orders"})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_preview_table_success_records_ok(contract, adapter, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["preview_table"]({"schema": "analytics", "table": "orders"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_list_metrics_no_semantic_source_records_error(no_semantic_contract):
    rec = ToolRecorder()
    tools = _tools(no_semantic_contract, rec)
    await tools["list_metrics"]({})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_list_metrics_unknown_domain_records_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["list_metrics"]({"domain": "nonexistent"})
    assert rec.calls[0].outcome == "miss"
    assert rec.calls[0].detail == "nonexistent"


@pytest.mark.asyncio
async def test_list_metrics_success_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["list_metrics"]({})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_lookup_metric_no_semantic_source_records_error(no_semantic_contract):
    rec = ToolRecorder()
    tools = _tools(no_semantic_contract, rec)
    await tools["lookup_metric"]({"metric_name": "total_revenue"})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_lookup_metric_no_candidates_records_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_metric"]({"metric_name": "zzz_totally_unrelated_qqq"})
    assert rec.calls[0].outcome == "miss"
    assert rec.calls[0].detail == "zzz_totally_unrelated_qqq"


@pytest.mark.asyncio
async def test_lookup_domain_no_domains_records_miss(no_semantic_contract):
    rec = ToolRecorder()
    tools = _tools(no_semantic_contract, rec)
    await tools["lookup_domain"]({"name": "anything"})
    assert rec.calls[0].outcome == "miss"


@pytest.mark.asyncio
async def test_lookup_domain_exact_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_domain"]({"name": "revenue"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_lookup_domain_fuzzy_candidates_record_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_domain"]({"name": "revenu"})
    assert rec.calls[0].outcome == "miss"


@pytest.mark.asyncio
async def test_lookup_domain_no_fuzzy_match_records_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_domain"]({"name": "zzz_totally_unrelated_qqq"})
    assert rec.calls[0].outcome == "miss"


@pytest.mark.asyncio
async def test_lookup_relationships_no_semantic_source_records_error(
    no_semantic_contract,
):
    rec = ToolRecorder()
    tools = _tools(no_semantic_contract, rec)
    await tools["lookup_relationships"]({"table": "analytics.orders"})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_lookup_relationships_found_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_relationships"]({"table": "analytics.orders"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_lookup_relationships_none_found_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_relationships"]({"table": "analytics.subscriptions"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_lookup_relationships_no_join_path_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["lookup_relationships"](
        {"table": "analytics.orders", "target_table": "analytics.subscriptions"}
    )
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_trace_metric_impacts_bad_direction_records_error(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["trace_metric_impacts"](
        {"metric_name": "total_revenue", "direction": "sideways"}
    )
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_trace_metric_impacts_no_semantic_source_records_error(
    no_semantic_contract,
):
    rec = ToolRecorder()
    tools = _tools(no_semantic_contract, rec)
    await tools["trace_metric_impacts"]({"metric_name": "total_revenue"})
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_trace_metric_impacts_unknown_metric_records_miss(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["trace_metric_impacts"]({"metric_name": "zzz_nope"})
    assert rec.calls[0].outcome == "miss"
    assert rec.calls[0].detail == "zzz_nope"


@pytest.mark.asyncio
async def test_trace_metric_impacts_bad_kinds_records_error(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["trace_metric_impacts"](
        {"metric_name": "total_revenue", "kinds": "bogus"}
    )
    assert rec.calls[0].outcome == "error"


@pytest.mark.asyncio
async def test_trace_metric_impacts_success_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["trace_metric_impacts"]({"metric_name": "total_revenue"})
    assert rec.calls[0].outcome == "ok"


@pytest.mark.asyncio
async def test_inspect_query_records_ok(contract, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, semantic=semantic)
    await tools["inspect_query"](
        {"sql": "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"}
    )
    assert rec.calls[0].outcome == "ok"


# ── run_query's six return paths ────────────────────────────────────────────


@pytest.mark.asyncio
async def test_successful_query_records_scalar_and_row_count(
    contract, adapter, semantic
):
    # COUNT(id), not COUNT(*): the fixture's no_select_star rule blocks any
    # exp.Star node, and sqlglot represents COUNT(*) with one.
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["run_query"](
        {"sql": "SELECT COUNT(id) FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    call = rec.calls[-1]
    assert call.outcome == "ok"
    assert call.scalar is not None
    assert call.row_count == 1


@pytest.mark.asyncio
async def test_multi_row_result_records_no_scalar_but_is_still_ok(
    contract, adapter, semantic
):
    """A table-shaped answer is ordinary for run_query, not a failure."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["run_query"](
        {"sql": "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    call = rec.calls[-1]
    assert call.outcome == "ok"
    assert call.scalar is None


@pytest.mark.asyncio
async def test_blocked_query_records_blocked(contract, adapter, semantic):
    """Query-check validation blocked (SELECT * is forbidden by the fixture)."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["run_query"]({"sql": "SELECT * FROM analytics.orders"})

    assert rec.calls[-1].outcome == "blocked"
    assert rec.calls[-1].detail


@pytest.mark.asyncio
async def test_missing_adapter_records_error_not_ok(contract, semantic):
    """The regression that would certify a governed path that never ran."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=None, semantic=semantic)
    await tools["run_query"](
        {"sql": "SELECT amount FROM analytics.orders WHERE tenant_id = 'acme'"}
    )

    assert rec.calls[-1].outcome == "error"


@pytest.mark.asyncio
async def test_relative_time_window_is_recorded(contract, adapter, semantic):
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["run_query"](
        {
            "sql": (
                "SELECT amount FROM analytics.orders"
                " WHERE tenant_id = 'acme' AND created_at > CURRENT_DATE - 7"
            )
        }
    )

    assert rec.calls[-1].relative_time is not None


@pytest.mark.asyncio
async def test_session_limit_exceeded_records_blocked(contract, adapter, semantic):
    """Fourth call trips max_retries=3 before validation even runs."""
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    blocked_sql = "SELECT * FROM analytics.orders"
    for _ in range(3):
        await tools["run_query"]({"sql": blocked_sql})

    await tools["run_query"]({"sql": blocked_sql})

    call = rec.calls[-1]
    assert call.outcome == "blocked"
    assert call.detail is not None
    assert "retries" in call.detail.lower()


@pytest.mark.asyncio
async def test_query_execution_error_records_error(contract, adapter, semantic):
    """Passes the EXPLAIN dry-run (Layer 2) but fails on real execution.

    An unknown column is caught by DuckDB's EXPLAIN plan already -- that
    lands on the query-check-blocked path, not this one. A bad CAST is
    schema-valid (the column exists and the type is legal) but only fails
    once DuckDB tries to convert the actual runtime value, so it is a
    genuine execution-time failure the dry-run cannot see coming.
    """
    rec = ToolRecorder()
    tools = _tools(contract, rec, adapter=adapter, semantic=semantic)
    await tools["run_query"](
        {
            "sql": (
                "SELECT CAST(tenant_id AS INTEGER) FROM analytics.orders"
                " WHERE tenant_id = 'acme'"
            )
        }
    )

    call = rec.calls[-1]
    assert call.outcome == "error"
    assert call.detail


@pytest.mark.asyncio
async def test_result_check_blocked_records_blocked():
    """Result-check enforcement=block discards data and records blocked."""
    from agentic_data_contracts.core.schema import (
        AllowedTable,
        DataContractSchema,
        Enforcement,
        ResultCheck,
        SemanticConfig,
        SemanticRule,
    )

    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            rules=[
                SemanticRule(
                    name="no_negative",
                    description="No negative amounts",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="amount", min_value=0),
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 100.00), (2, -50.00);
        """
    )

    rec = ToolRecorder()
    session = ContractSession(dc, recorder=rec)
    tools = {t.name: t.callable for t in create_tools(dc, adapter=db, session=session)}
    await tools["run_query"]({"sql": "SELECT amount FROM analytics.orders"})

    call = rec.calls[-1]
    assert call.outcome == "blocked"
    assert call.detail
