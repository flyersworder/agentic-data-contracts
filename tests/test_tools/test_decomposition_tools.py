"""Integration tests for decomposition surfacing on the tool layer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools

FIXTURE = Path(__file__).parent.parent / "fixtures" / "decomposition_source.yml"


def _tools() -> dict:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["dim_customer"]}
                ),
            ],
        ),
    )
    contract = DataContract(schema)
    source = YamlSource(FIXTURE)
    tools = create_tools(contract, semantic_source=source)
    return {t.name: t.callable for t in tools}


async def _call(fn, **args) -> dict:
    result = await fn(args)
    return json.loads(result["content"][0]["text"])


class TestLookupMetricSurfacesDecomposition:
    @pytest.mark.asyncio
    async def test_includes_decompositions_and_drill_by(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        assert {"operator": "product", "operands": ["paying_users", "arpu"]} in data[
            "decompositions"
        ]
        assert {
            "dimension": "region",
            "column": "analytics.dim_customer.region",
        } in data["drill_by"]

    @pytest.mark.asyncio
    async def test_leaf_metric_omits_fields(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="paying_users")
        assert "decompositions" not in data
        assert "drill_by" not in data


class TestTraceWalksIdentity:
    @pytest.mark.asyncio
    async def test_downstream_all_includes_identity_and_influence(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="downstream",
        )
        kinds = {e["kind"] for e in data["edges"]}
        assert "identity" in kinds
        identity_edges = [e for e in data["edges"] if e["kind"] == "identity"]
        assert any(
            e["to"] == "arpu" and e["operator"] == "product" for e in identity_edges
        )

    @pytest.mark.asyncio
    async def test_kinds_identity_excludes_influence(self) -> None:
        # Walk upstream from arpu: identity edge revenue->arpu exists; arpu has
        # no influence edges, so kinds="identity" must return a non-empty,
        # all-identity set.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="arpu",
            direction="upstream",
            kinds="identity",
        )
        assert data["edges"]  # non-empty
        assert all(e["kind"] == "identity" for e in data["edges"])
        assert any(e["from"] == "revenue" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_kinds_influence_excludes_identity(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="revenue",
            direction="upstream",
            kinds="influence",
        )
        assert all(e["kind"] == "influence" for e in data["edges"])

    @pytest.mark.asyncio
    async def test_invalid_kinds_returns_message(self) -> None:
        result = await _tools()["trace_metric_impacts"](
            {"metric_name": "revenue", "kinds": "bogus"}
        )
        assert "kinds must be" in result["content"][0]["text"]
