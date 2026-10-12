"""`MetricDefinition.from_sql` and `grain`: a metric over several tables (#136).

A metric's `sql_expression` may read several tables through aliases. Without
a declared FROM clause, the join lives only in prose: nothing can build the
query that checks it, and the agent cannot see it. `from_sql` declares that
FROM clause and `grain` what one of its rows is. Both are optional, reach the
agent through `lookup_metric`, and survive `dump_semantic_source` ->
`YamlSource`, the path a frozen contract takes.
"""

import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import SemanticSource, dump_semantic_source
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import create_tools

FROM = (
    "main.payments p JOIN main.fees f"
    " ON f.card_scheme IS NULL OR f.card_scheme = p.card_scheme"
)
GRAIN = "one row per (payment, fee rule that applies to it)"


def _source(**extra: object) -> YamlSource:
    return YamlSource.from_raw(
        {
            "metrics": [
                {
                    "name": "transaction_fee",
                    "description": "Fee for one payment under one rule",
                    "sql_expression": "f.fixed_amount + f.rate * p.eur_amount / 10000",
                    "source_model": "main.fees",
                    **extra,
                },
                {
                    "name": "volume",
                    "description": "Total volume",
                    "sql_expression": "SUM(eur_amount)",
                    "source_model": "main.payments",
                },
            ]
        }
    )


def test_yaml_reads_from_sql_and_grain() -> None:
    metric = _source(from_sql=FROM, grain=GRAIN).get_metric("transaction_fee")
    assert metric is not None
    assert metric.from_sql == FROM
    assert metric.grain == GRAIN


def test_a_metric_without_them_is_unchanged() -> None:
    metric = _source().get_metric("volume")
    assert metric is not None
    assert metric.from_sql is None
    assert metric.grain is None


@pytest.mark.parametrize("key", ["from_sql", "grain"])
def test_a_blank_value_is_refused(key: str) -> None:
    # A blank FROM clause would read as "declared" while declaring nothing.
    with pytest.raises(ValueError, match=key):
        _source(**{key: "  "})


def test_they_survive_dump_and_rebuild() -> None:
    source = _source(from_sql=FROM, grain=GRAIN)
    rebuilt = YamlSource.from_raw(dump_semantic_source(source))
    metric = rebuilt.get_metric("transaction_fee")
    assert metric is not None
    assert (metric.from_sql, metric.grain) == (FROM, GRAIN)


def test_a_metric_without_them_dumps_without_the_keys() -> None:
    # Omit-when-None keeps every existing frozen contract's digest stable.
    dumped = {m["name"]: m for m in dump_semantic_source(_source())["metrics"]}
    assert "from_sql" not in dumped["volume"]
    assert "grain" not in dumped["volume"]
    assert "from_sql" not in dumped["transaction_fee"]


async def _lookup(source: SemanticSource, name: str, fixtures_dir: Path) -> dict:
    contract = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    tools = create_tools(
        contract, adapter=DuckDBAdapter(":memory:"), semantic_source=source
    )
    tool = next(t for t in tools if t.name == "lookup_metric")
    result = await tool.callable({"metric_name": name})
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_lookup_metric_shows_the_declared_join(fixtures_dir: Path) -> None:
    payload = await _lookup(
        _source(from_sql=FROM, grain=GRAIN), "transaction_fee", fixtures_dir
    )
    assert payload["from_sql"] == FROM
    assert payload["grain"] == GRAIN


@pytest.mark.asyncio
async def test_lookup_metric_omits_them_when_undeclared(fixtures_dir: Path) -> None:
    payload = await _lookup(_source(), "volume", fixtures_dir)
    assert "from_sql" not in payload
    assert "grain" not in payload
