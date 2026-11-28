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
    assert "same checks as inspect_query" in desc
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
