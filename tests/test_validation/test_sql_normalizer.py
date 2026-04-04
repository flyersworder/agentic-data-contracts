"""Tests for the SqlNormalizer protocol hook."""

from __future__ import annotations

import re

from agentic_data_contracts.adapters.base import (
    DatabaseAdapter,
    QueryResult,
    SqlNormalizer,
    TableSchema,
)
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.validation.explain import ExplainResult
from agentic_data_contracts.validation.validator import Validator


class VqlNormalizingAdapter:
    """Mock adapter that normalizes VQL CAST('type', col) to CAST(col AS type)."""

    @property
    def dialect(self) -> str:
        return "postgres"

    def normalize_sql(self, sql: str) -> str:
        # Rewrite CAST('type', col) -> CAST(col AS type)
        return re.sub(
            r"CAST\('(\w+)',\s*(\w+)\)",
            r"CAST(\2 AS \1)",
            sql,
        )

    def execute(self, sql: str) -> QueryResult:
        return QueryResult(columns=["id"], rows=[(1,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=0.01,
            estimated_rows=100,
            schema_valid=True,
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema()

    def list_tables(self, schema: str) -> list[str]:
        return []


class PlainAdapter:
    """Mock adapter without SqlNormalizer — standard dialect."""

    @property
    def dialect(self) -> str:
        return "duckdb"

    def execute(self, sql: str) -> QueryResult:
        return QueryResult(columns=["id"], rows=[(1,)])

    def explain(self, sql: str) -> ExplainResult:
        return ExplainResult(
            estimated_cost_usd=0.01,
            estimated_rows=100,
            schema_valid=True,
        )

    def describe_table(self, schema: str, table: str) -> TableSchema:
        return TableSchema()

    def list_tables(self, schema: str) -> list[str]:
        return []


def _make_contract() -> DataContract:
    schema = DataContractSchema(
        version="1.0",
        name="normalizer-test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["users"]})
            ],
            forbidden_operations=[],
            rules=[],
        ),
    )
    return DataContract(schema=schema)


# --- Protocol detection ---


def test_normalizing_adapter_is_sql_normalizer() -> None:
    adapter = VqlNormalizingAdapter()
    assert isinstance(adapter, SqlNormalizer)


def test_normalizing_adapter_is_database_adapter() -> None:
    adapter = VqlNormalizingAdapter()
    assert isinstance(adapter, DatabaseAdapter)


def test_plain_adapter_is_not_sql_normalizer() -> None:
    adapter = PlainAdapter()
    assert not isinstance(adapter, SqlNormalizer)


# --- Validation with normalization ---


def test_vql_cast_passes_with_normalizer() -> None:
    """VQL CAST('varchar', col) is invalid sqlglot input but normalizes to valid SQL."""
    adapter = VqlNormalizingAdapter()
    contract = _make_contract()
    validator = Validator(
        contract,
        dialect=adapter.dialect,
        sql_normalizer=adapter,
    )
    result = validator.validate("SELECT CAST('varchar', id) FROM public.users")
    assert not result.blocked


def test_vql_cast_fails_without_normalizer() -> None:
    """Without normalization, VQL CAST syntax causes a parse error."""
    contract = _make_contract()
    validator = Validator(contract, dialect="postgres")
    result = validator.validate("SELECT CAST('varchar', id) FROM public.users")
    assert result.blocked
    assert any("parse error" in r.lower() or "CAST" in r for r in result.reasons)


# --- Original SQL preserved for explain ---


def test_explain_receives_original_sql() -> None:
    """The explain adapter must receive the original SQL, not the normalized form."""
    received_sql: list[str] = []

    class SpyNormalizingAdapter(VqlNormalizingAdapter):
        def explain(self, sql: str) -> ExplainResult:
            received_sql.append(sql)
            return super().explain(sql)

    adapter = SpyNormalizingAdapter()
    contract = _make_contract()
    validator = Validator(
        contract,
        dialect=adapter.dialect,
        explain_adapter=adapter,
        sql_normalizer=adapter,
    )
    original_sql = "SELECT CAST('varchar', id) FROM public.users"
    validator.validate(original_sql)
    assert received_sql == [original_sql]
