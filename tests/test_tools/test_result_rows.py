"""run_query / preview_table cap the rows they fetch and return (#116)."""

import json
from typing import Any

import pytest

from agentic_data_contracts.adapters.base import QueryResult, TableSchema
from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.recorder import ToolRecorder
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    ResultCheck,
    SemanticConfig,
    SemanticRule,
)
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.tools import factory
from agentic_data_contracts.tools.factory import create_tools
from agentic_data_contracts.validation.explain import ExplainResult


def _contract(*rules: SemanticRule) -> DataContract:
    return DataContract(
        DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate(
                        {"schema": "analytics", "tables": ["orders"]}
                    )
                ],
                rules=list(rules),
            ),
        )
    )


def _row_rule(**check: int) -> SemanticRule:
    return SemanticRule(
        name="rows_" + "_".join(check),  # distinct per rule
        description="row bound",
        enforcement="block",
        result_check=ResultCheck(**check),  # ty: ignore[invalid-argument-type]
    )


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(100);"
    )
    return db


def _tool(tools: list[Any], name: str) -> Any:
    return next(t for t in tools if t.name == name).callable


def _payload(result: dict[str, Any]) -> dict[str, Any]:
    return json.loads(result["content"][0]["text"])


SQL = "SELECT id FROM analytics.orders ORDER BY id"


async def test_default_cap_is_1000() -> None:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(5000);"
    )
    run_query = _tool(create_tools(_contract(), adapter=db), "run_query")
    data = _payload(await run_query({"sql": SQL}))
    assert len(data["rows"]) == 1000
    assert data["row_count"] == 1000
    assert data["truncated"] is True


async def test_truncated_payload_key_order(adapter: DuckDBAdapter) -> None:
    run_query = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=10), "run_query"
    )
    data = _payload(await run_query({"sql": SQL}))
    assert list(data) == ["columns", "rows", "row_count", "truncated", "session"]
    assert data["rows"] == [[i] for i in range(10)]


async def test_untruncated_payload_is_unchanged(adapter: DuckDBAdapter) -> None:
    capped = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=100), "run_query"
    )
    uncapped = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=None), "run_query"
    )
    a = await capped({"sql": SQL})
    b = await uncapped({"sql": SQL})
    assert a["content"][0]["text"] == b["content"][0]["text"]
    assert "truncated" not in a["content"][0]["text"]


async def test_none_returns_every_row(adapter: DuckDBAdapter) -> None:
    run_query = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=None), "run_query"
    )
    assert len(_payload(await run_query({"sql": SQL}))["rows"]) == 100


async def test_truncated_result_records_no_scalar(adapter: DuckDBAdapter) -> None:
    """A truncated single-column result must not be recorded as a scalar:
    conformance.py's scalar_calls/sole_scalar treat a recorded scalar as an
    answer candidate, so a cap of 1 turning a 100-row result into a 1x1 shape
    must not look like the query's answer (#116)."""
    contract = _contract()
    recorder = ToolRecorder()
    session = ContractSession(contract, recorder=recorder)
    run_query = _tool(
        create_tools(contract, adapter=adapter, session=session, max_result_rows=1),
        "run_query",
    )
    data = _payload(await run_query({"sql": SQL}))
    assert data["truncated"] is True
    assert len(data["rows"]) == 1

    call = recorder.calls[-1]
    assert call.tool == "run_query"
    assert call.outcome == "ok"
    assert call.scalar is None
    assert call.row_count == 1


@pytest.mark.parametrize("bad", [0, -1])
def test_non_positive_cap_raises_at_wiring(adapter: DuckDBAdapter, bad: int) -> None:
    with pytest.raises(ValueError, match="max_result_rows"):
        create_tools(_contract(), adapter=adapter, max_result_rows=bad)


async def test_row_thresholds_above_the_cap_stay_exact(adapter: DuckDBAdapter) -> None:
    # 100 rows exist; the cap is 5. Without widening the fetch, min_rows would
    # see 5 rows and block, and max_rows would see 5 and pass.
    passes = _tool(
        create_tools(
            _contract(_row_rule(min_rows=20)), adapter=adapter, max_result_rows=5
        ),
        "run_query",
    )
    result = await passes({"sql": SQL})
    assert "is_error" not in result
    assert len(_payload(result)["rows"]) == 5

    blocks = _tool(
        create_tools(
            _contract(_row_rule(max_rows=20)), adapter=adapter, max_result_rows=5
        ),
        "run_query",
    )
    result = await blocks({"sql": SQL})
    assert result["_kind"] == "blocked"
    assert "maximum is 20" in result["content"][0]["text"]


class _SpyAdapter(DuckDBAdapter):
    def __init__(self) -> None:
        super().__init__(":memory:")
        self.limits: list[int] = []

    def execute_limited(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        self.limits.append(max_rows)
        return super().execute_limited(sql, max_rows, timeout_seconds)


async def test_fetch_is_widened_only_to_the_threshold() -> None:
    db = _SpyAdapter()
    db.connection.execute(
        "CREATE SCHEMA analytics;"
        " CREATE TABLE analytics.orders AS SELECT range AS id FROM range(100);"
    )
    rules = (_row_rule(min_rows=20), _row_rule(max_rows=30))
    run_query = _tool(
        create_tools(_contract(*rules), adapter=db, max_result_rows=5), "run_query"
    )
    await run_query({"sql": SQL})
    assert db.limits == [31]


class _PlainAdapter:
    """A DatabaseAdapter with neither execute_limited nor execute_with_timeout."""

    dialect = "duckdb"

    def __init__(self, inner: DuckDBAdapter) -> None:
        self._inner = inner

    def execute(self, sql: str) -> QueryResult:
        return self._inner.execute(sql)

    def explain(self, sql: str) -> ExplainResult:
        return self._inner.explain(sql)

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return self._inner.describe_table(schema, table)

    def list_tables(self, schema: str) -> list[str]:
        return self._inner.list_tables(schema)


async def test_fallback_slices_and_warns_once(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(factory, "_WARNED_UNBOUNDED_FETCH", set())
    plain = _PlainAdapter(adapter)
    tools = create_tools(_contract(), adapter=plain, max_result_rows=10)
    create_tools(_contract(), adapter=plain, max_result_rows=10)
    assert caplog.text.count("execute_limited") == 1

    data = _payload(await _tool(tools, "run_query")({"sql": SQL}))
    assert len(data["rows"]) == 10
    assert data["truncated"] is True


def test_no_warning_when_uncapped_or_capable(
    adapter: DuckDBAdapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(factory, "_WARNED_UNBOUNDED_FETCH", set())
    create_tools(_contract(), adapter=_PlainAdapter(adapter), max_result_rows=None)
    create_tools(_contract(), adapter=adapter)
    assert "execute_limited" not in caplog.text


async def test_preview_table_is_capped(adapter: DuckDBAdapter) -> None:
    preview = _tool(
        create_tools(_contract(), adapter=adapter, max_result_rows=3), "preview_table"
    )
    result = await preview({"schema": "analytics", "table": "orders", "limit": 50})
    assert len(json.loads(result["content"][0]["text"])["rows"]) == 3


@pytest.mark.parametrize(
    ("module", "builder"),
    [
        ("agentic_data_contracts.tools.pydantic_ai", "create_pydantic_ai_tools"),
        ("agentic_data_contracts.tools.langchain", "create_langchain_tools"),
        ("agentic_data_contracts.tools.sdk", "create_sdk_mcp_server"),
    ],
)
def test_entry_points_forward_max_result_rows(
    module: str, builder: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib

    seen: dict[str, Any] = {}

    def _spy(*args: Any, **kwargs: Any) -> list[Any]:
        seen.update(kwargs)
        return []

    try:
        mod = importlib.import_module(module)
        monkeypatch.setattr(mod, "create_tools", _spy)
        getattr(mod, builder)(_contract(), max_result_rows=7)
    except ImportError:
        pytest.skip(f"{module}'s optional dependency is not installed")
    assert seen["max_result_rows"] == 7


def test_toolset_rejects_a_bad_cap_at_construction() -> None:
    from agentic_data_contracts.tools.pydantic_ai import create_pydantic_ai_toolset

    with pytest.raises(ValueError, match="max_result_rows"):
        create_pydantic_ai_toolset(_contract(), max_result_rows=0)
