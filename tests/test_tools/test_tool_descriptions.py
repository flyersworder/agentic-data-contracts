"""Tool descriptions as contracts in plain register (#139, design §2).

Every clause appears only when the capability it names exists. Exact
composition of the run_query description is pinned in test_tool_protocol.py;
this file checks the gating and the register.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.tools.factory import ToolDef, create_tools

_BASE = """
version: "1.0"
name: t
semantic:
  allowed_tables:
    - schema: analytics
      tables: [orders]
  domains:
    - name: revenue
      summary: Revenue
      description: Revenue is recognized at fulfillment.
  rules: {rules}
{resources}
"""

_RESULT_CHECK_RULE = """
    - name: non_negative
      description: Amounts are never negative
      enforcement: block
      result_check:
        column: amount
        min_value: 0
"""


def _contract(*, rules: str = "[]", resources: str = "") -> DataContract:
    return DataContract.from_yaml_string(_BASE.format(rules=rules, resources=resources))


def _limits(**limits: object) -> str:
    return "resources:\n" + "".join(f"  {k}: {v}\n" for k, v in limits.items())


@pytest.fixture
def semantic(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    return DuckDBAdapter(":memory:")


def _desc(tools: list[ToolDef], name: str) -> str:
    return next(t for t in tools if t.name == name).description


def _all_configs(semantic: YamlSource, adapter: DuckDBAdapter) -> list[list[ToolDef]]:
    full = _contract(
        rules=_RESULT_CHECK_RULE,
        resources=_limits(max_retries=3, max_query_time_seconds=30),
    )
    return [
        create_tools(full, adapter=adapter, semantic_source=semantic),
        create_tools(full, semantic_source=semantic),
        create_tools(_contract(), adapter=adapter),
        create_tools(_contract()),
    ]


# ── Register ─────────────────────────────────────────────────────────────────


def test_no_shouted_imperatives(semantic: YamlSource, adapter: DuckDBAdapter) -> None:
    # Current models follow instructions literally; MUST / NEVER / CRITICAL
    # over-trigger. A constraint is stated once, at normal volume.
    for tools in _all_configs(semantic, adapter):
        for tool in tools:
            assert not re.search(
                r"\b(MUST|NEVER|CRITICAL|ALWAYS)\b", tool.description
            ), tool.name


def test_full_capability_descriptions_are_detailed(
    semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    # Descriptions are contracts: what it does, when to use it, what it does
    # not return. Three sentences at least once every capability is present.
    tools = _all_configs(semantic, adapter)[0]
    for tool in tools:
        sentences = re.findall(r"[.!?](?:\s|$)", tool.description)
        assert len(sentences) >= 3, (tool.name, tool.description)


# ── describe_table / preview_table ───────────────────────────────────────────


def test_describe_table_states_return_shape_only_with_adapter(
    semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    with_adapter = _desc(create_tools(_contract(), adapter=adapter), "describe_table")
    without = _desc(create_tools(_contract()), "describe_table")
    assert "nullab" in with_adapter
    assert "nullab" not in without
    assert "no rows" in with_adapter


def test_describe_table_names_governed_descriptions_only_with_table_entries(
    semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    governed = _desc(
        create_tools(_contract(), adapter=adapter, semantic_source=semantic),
        "describe_table",
    )
    plain = _desc(create_tools(_contract(), adapter=adapter), "describe_table")
    assert "semantic source" in governed
    assert "semantic source" not in plain


@pytest.mark.parametrize(
    ("max_result_rows", "cap"), [(1000, 100), (None, 100), (10, 10)]
)
def test_preview_table_states_its_row_cap(
    adapter: DuckDBAdapter, max_result_rows: int | None, cap: int
) -> None:
    desc = _desc(
        create_tools(_contract(), adapter=adapter, max_result_rows=max_result_rows),
        "preview_table",
    )
    assert f"at most {cap} rows" in desc


def test_preview_table_return_shape_only_with_adapter() -> None:
    assert "at most" not in _desc(create_tools(_contract()), "preview_table")


# ── Metric and domain lookups ────────────────────────────────────────────────


def test_lookup_metric_carries_point_of_use_routing_only_with_metrics(
    semantic: YamlSource,
) -> None:
    with_metrics = _desc(
        create_tools(_contract(), semantic_source=semantic), "lookup_metric"
    )
    without = _desc(
        create_tools(_contract(), semantic_source=YamlSource.from_raw({})),
        "lookup_metric",
    )
    assert "before writing SQL that computes a metric" in with_metrics
    assert "governed" in with_metrics
    assert "before writing SQL" not in without


def test_list_metrics_is_for_browsing(semantic: YamlSource) -> None:
    desc = _desc(create_tools(_contract(), semantic_source=semantic), "list_metrics")
    assert "browse" in desc
    assert "not know" in desc


def test_lookup_domain_says_what_the_description_holds(semantic: YamlSource) -> None:
    desc = _desc(create_tools(_contract(), semantic_source=semantic), "lookup_domain")
    assert "rules that apply across the domain's metrics" in desc
    assert "before writing SQL that computes one of the domain's metrics" in desc
    # Today's "before querying" read as every query.
    assert "before querying" not in desc


def test_lookup_domain_point_of_use_clause_needs_metrics() -> None:
    desc = _desc(create_tools(_contract()), "lookup_domain")
    assert "before writing SQL" not in desc


# ── inspect_query / run_query ────────────────────────────────────────────────


def test_inspect_query_says_inspecting_is_optional(
    semantic: YamlSource, adapter: DuckDBAdapter
) -> None:
    desc = _desc(
        create_tools(_contract(), adapter=adapter, semantic_source=semantic),
        "inspect_query",
    )
    assert "optional" in desc
    assert "run_query applies the same checks" in desc
    assert "cost estimate" in desc
    # The return-shape text stays.
    assert "`pending_result_checks`" in desc


def test_inspect_query_cost_estimate_only_with_adapter(semantic: YamlSource) -> None:
    desc = _desc(create_tools(_contract(), semantic_source=semantic), "inspect_query")
    assert "cost estimate" not in desc


def test_retry_clause_only_when_max_retries_is_set() -> None:
    with_retries = _desc(
        create_tools(_contract(resources=_limits(max_retries=3))), "inspect_query"
    )
    without = _desc(create_tools(_contract()), "inspect_query")
    assert "spends one of the session's retries" in with_retries
    assert "retries" not in without


def test_run_query_says_it_validates_first(adapter: DuckDBAdapter) -> None:
    desc = _desc(create_tools(_contract(), adapter=adapter), "run_query")
    assert "The checks run first" in desc
    assert "blocked query is not run" in desc


def test_run_query_timeout_clause_only_when_declared(adapter: DuckDBAdapter) -> None:
    timed = _desc(
        create_tools(
            _contract(resources=_limits(max_query_time_seconds=30)), adapter=adapter
        ),
        "run_query",
    )
    untimed = _desc(create_tools(_contract(), adapter=adapter), "run_query")
    assert "30 seconds" in timed
    assert "seconds" not in untimed


def test_run_query_result_check_clause_only_when_declared(
    adapter: DuckDBAdapter,
) -> None:
    checked = _desc(
        create_tools(_contract(rules=_RESULT_CHECK_RULE), adapter=adapter),
        "run_query",
    )
    unchecked = _desc(create_tools(_contract(), adapter=adapter), "run_query")
    assert "result checks" in checked
    assert "result checks" not in unchecked


# ── Relationships ────────────────────────────────────────────────────────────


def test_lookup_relationships_says_when_to_use_it(semantic: YamlSource) -> None:
    desc = _desc(
        create_tools(_contract(), semantic_source=semantic), "lookup_relationships"
    )
    assert "join" in desc
    assert "Use it" in desc


# ── Review round 1: capability gating and accuracy ───────────────────────────


def test_execution_clauses_need_an_adapter() -> None:
    # Without an adapter run_query returns "No database adapter configured",
    # so a timeout or result-check sentence would describe something that
    # never happens.
    desc = _desc(
        create_tools(
            _contract(
                rules=_RESULT_CHECK_RULE, resources=_limits(max_query_time_seconds=30)
            )
        ),
        "run_query",
    )
    assert "seconds" not in desc
    assert "result checks" not in desc


def test_describe_and_preview_usage_sentences_need_an_adapter() -> None:
    tools = create_tools(_contract())
    assert "Use it" not in _desc(tools, "describe_table")
    assert "Use it" not in _desc(tools, "preview_table")


def test_timeout_wording_without_a_cancellable_adapter(adapter: DuckDBAdapter) -> None:
    class _Plain:
        """A DatabaseAdapter that is not a TimeoutAdapter."""

        dialect = "duckdb"

        def execute(self, sql: str):  # noqa: ANN201
            return adapter.execute(sql)

        def explain(self, sql: str):  # noqa: ANN201
            return adapter.explain(sql)

        def describe_table(self, schema: str, table: str):  # noqa: ANN201
            return adapter.describe_table(schema, table)

        def list_tables(self, schema: str) -> list[str]:
            return adapter.list_tables(schema)

    contract = _contract(resources=_limits(max_query_time_seconds=30))
    plain = _desc(create_tools(contract, adapter=_Plain()), "run_query")  # type: ignore[arg-type]
    cancellable = _desc(create_tools(contract, adapter=adapter), "run_query")
    # The plain adapter's statement keeps running in the database; only the
    # tool stops waiting for it.
    assert "stops waiting" in plain
    assert "is stopped" not in plain
    assert "is stopped" in cancellable


def test_timeout_is_stated_as_declared(adapter: DuckDBAdapter) -> None:
    for declared, shown in (("1000000", "1000000"), ("2.5", "2.5"), ("30", "30")):
        desc = _desc(
            create_tools(
                _contract(resources=_limits(max_query_time_seconds=declared)),
                adapter=adapter,
            ),
            "run_query",
        )
        assert f"after {shown} seconds" in desc


def test_preview_default_never_exceeds_the_cap(adapter: DuckDBAdapter) -> None:
    desc = _desc(
        create_tools(_contract(), adapter=adapter, max_result_rows=3), "preview_table"
    )
    assert "default 3, at most 3 rows" in desc


def test_describe_table_governed_clause_follows_the_source_not_the_bulk_index(
    adapter: DuckDBAdapter, fixtures_dir: Path
) -> None:
    class _ExactOnly(YamlSource):
        """Serves get_table_schema but not the bulk get_table_schemas."""

        def get_table_schemas(self) -> dict:
            return {}

    desc = _desc(
        create_tools(
            _contract(),
            adapter=adapter,
            semantic_source=_ExactOnly(fixtures_dir / "semantic_source.yml"),
        ),
        "describe_table",
    )
    assert "semantic source" in desc


def test_impact_sentence_only_when_impacts_are_declared(semantic: YamlSource) -> None:
    no_impacts = YamlSource.from_raw(
        {"metrics": [{"name": "m", "description": "M.", "sql_expression": "1"}]}
    )
    without = _desc(
        create_tools(_contract(), semantic_source=no_impacts), "lookup_metric"
    )
    with_impacts = _desc(
        create_tools(_contract(), semantic_source=semantic), "lookup_metric"
    )
    assert "Use the impact edges" not in without
    assert "Use the impact edges" in with_impacts


def test_run_query_names_no_other_tool(adapter: DuckDBAdapter) -> None:
    # A host may pass a subset of the tools; without metrics, run_query's
    # description must stand alone.
    desc = _desc(create_tools(_contract(), adapter=adapter), "run_query")
    assert "inspect_query" not in desc


def test_warn_only_result_checks_are_not_said_to_block(adapter: DuckDBAdapter) -> None:
    warn_rule = _RESULT_CHECK_RULE.replace("enforcement: block", "enforcement: warn")
    warned = _desc(
        create_tools(_contract(rules=warn_rule), adapter=adapter), "run_query"
    )
    blocking = _desc(
        create_tools(_contract(rules=_RESULT_CHECK_RULE), adapter=adapter),
        "run_query",
    )
    assert "result checks" in warned
    assert "block the result" not in warned
    assert "block the result" in blocking


def test_lookup_descriptions_do_not_overstate(semantic: YamlSource) -> None:
    tools = create_tools(_contract(), semantic_source=semantic)
    # A miss can return "not found" rather than candidates.
    assert "close candidates when there are any" in _desc(tools, "lookup_metric")
    # Owners and freshness are present only where the domain declares them.
    assert "owners and review freshness where declared" in _desc(tools, "lookup_domain")
