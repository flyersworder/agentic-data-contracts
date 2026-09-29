"""`MetricDefinition.untranslated` reaches the agent and survives a freeze.

A source that cannot express a metric as one SQL expression leaves
`sql_expression` empty and says why (#123). An empty expression with no reason
would read to the agent as "this metric has no SQL", so the reason must reach
`lookup_metric`, and must survive `dump_semantic_source` -> `YamlSource`, the
path a frozen contract takes.
"""

import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import SemanticSource, dump_semantic_source
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "dbt_metricflow_manifest.json")


async def _lookup(source: SemanticSource, name: str, fixtures_dir: Path) -> dict:
    contract = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    tools = create_tools(
        contract, adapter=DuckDBAdapter(":memory:"), semantic_source=source
    )
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": name})
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_lookup_metric_carries_the_reason(
    source: DbtSource, fixtures_dir: Path
) -> None:
    payload = await _lookup(source, "order_value", fixtures_dir)
    assert "ratio" in payload["untranslated"]
    assert payload["sql_expression"] == ""


@pytest.mark.asyncio
async def test_lookup_metric_omits_it_for_a_translated_metric(
    source: DbtSource, fixtures_dir: Path
) -> None:
    payload = await _lookup(source, "revenue", fixtures_dir)
    assert "untranslated" not in payload
    assert payload["sql_expression"] == "SUM(amount)"


def test_it_survives_dump_and_rebuild(source: DbtSource) -> None:
    rebuilt = YamlSource.from_raw(dump_semantic_source(source))
    for metric in source.get_metrics():
        again = rebuilt.get_metric(metric.name)
        assert again is not None
        assert again.untranslated == metric.untranslated
        assert again.sql_expression == metric.sql_expression


def test_a_translated_metric_dumps_without_the_key(source: DbtSource) -> None:
    # Omit-when-None keeps a frozen contract's digest stable across the
    # upgrade for every source that never sets it (YAML, Ossie).
    dumped = {m["name"]: m for m in dump_semantic_source(source)["metrics"]}
    assert "untranslated" not in dumped["revenue"]
    assert "untranslated" in dumped["order_value"]
