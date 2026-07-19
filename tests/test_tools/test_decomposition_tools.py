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
