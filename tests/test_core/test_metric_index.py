"""Metric names in the prompt index (#139, design §1).

Applies only when the caller passes a semantic source with metrics; the
no-source rendering is pinned separately by ``test_no_source_golden.py``.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.prompt import XmlPromptRenderer
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
)

_PROMPT_LOGGER = "agentic_data_contracts.core.prompt"

LISTED_DOMAIN_HINT = (
    "<hint>Each domain's description holds rules that apply to its metrics,"
    " and each listed metric has a governed SQL definition. Look up the"
    " domain and the metric before computing it.</hint>"
)
LISTED_PLAIN_HINT = (
    "<hint>Each listed metric has a governed SQL definition; look it up"
    " before computing the metric.</hint>"
)


def _dropped_domain_hint(m: int) -> str:
    return (
        f"<hint>{m} metrics have governed SQL definitions; their names are"
        " omitted here. Each domain's description holds rules that apply to"
        " its metrics. Look up the domain and the metric before computing"
        " it.</hint>"
    )


def _dropped_plain_hint(m: int) -> str:
    return (
        f"<hint>{m} metrics have governed SQL definitions; their names are"
        " omitted here. Look a metric up before computing it.</hint>"
    )


class _Source:
    def __init__(self, metrics: list[MetricDefinition]) -> None:
        self._metrics = metrics

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        return next((m for m in self._metrics if m.name == name), None)

    def get_table_schema(self, schema: str, table: str) -> None:
        return None

    def get_table_schemas(self) -> dict:
        return {}

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_relationships(self) -> list[Relationship]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []

    def get_metric_impacts(self) -> list[MetricImpact]:
        return []

    def get_extras(self) -> dict[str, Any]:
        return {}


def _metric(name: str, *domains: str) -> MetricDefinition:
    return MetricDefinition(
        name=name,
        description=f"About {name}.",
        sql_expression="SUM(x)",
        domains=list(domains),
    )


def _plain(count: int) -> _Source:
    return _Source([_metric(f"m{i}") for i in range(count)])


def _contract(*domain_names: str) -> DataContract:
    return DataContract(
        DataContractSchema(
            name="t",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate({"schema": "s", "tables": ["t"]})
                ],
                domains=[
                    Domain(name=d, summary=f"{d} summary", description=f"{d}.")
                    for d in domain_names
                ],
            ),
        )
    )


def _logs(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _PROMPT_LOGGER]


# ---------------------------------------------------------------------------
# With domains
# ---------------------------------------------------------------------------


class TestDomainsListNames:
    def test_members_listed_under_their_domain(self) -> None:
        source = _Source([_metric("avg_fee", "fees"), _metric("rule_match", "fees")])
        prompt = _contract("fees", "fraud").to_system_prompt(source)
        assert (
            '  <domain name="fees" summary="fees summary" metric_count="2">\n'
            '    <metric name="avg_fee" />\n'
            '    <metric name="rule_match" />\n'
            "  </domain>\n"
        ) in prompt
        # A domain with no members keeps today's self-closing element.
        assert '  <domain name="fraud" summary="fraud summary" />' in prompt

    def test_metric_in_several_domains_listed_under_each(self) -> None:
        source = _Source([_metric("shared", "a", "b")])
        prompt = _contract("a", "b").to_system_prompt(source)
        assert prompt.count('<metric name="shared" />') == 2

    def test_metrics_outside_cataloged_domains_get_one_group(self) -> None:
        source = _Source(
            [
                _metric("in_a", "a"),
                _metric("no_domain"),
                _metric("uncataloged", "ghost"),
            ]
        )
        prompt = _contract("a").to_system_prompt(source)
        assert (
            "  <other_metrics>\n"
            '    <metric name="no_domain" />\n'
            '    <metric name="uncataloged" />\n'
            "  </other_metrics>\n"
        ) in prompt
        assert "<available_metrics>" not in prompt

    def test_no_other_group_when_every_metric_has_a_domain(self) -> None:
        prompt = _contract("a").to_system_prompt(_Source([_metric("x", "a")]))
        assert "<other_metrics>" not in prompt

    def test_hint_names_no_tool(self) -> None:
        prompt = _contract("a").to_system_prompt(_Source([_metric("x", "a")]))
        assert LISTED_DOMAIN_HINT in prompt
        assert "lookup_domain" not in prompt
        assert "lookup_metric" not in prompt

    def test_descriptions_never_added(self) -> None:
        prompt = _contract("a").to_system_prompt(_Source([_metric("x", "a")]))
        assert "About x." not in prompt

    def test_detail_threshold_does_not_drop_domain_names(self) -> None:
        source = _Source([_metric(f"m{i}", "a") for i in range(30)])
        prompt = _contract("a").to_system_prompt(
            source, renderer=XmlPromptRenderer(metric_detail_threshold=5)
        )
        assert '<metric name="m29" />' in prompt


class TestDomainsDropNames:
    def test_above_index_threshold_renders_todays_domains(self) -> None:
        source = _Source([_metric(f"m{i}", "a") for i in range(4)])
        prompt = _contract("a").to_system_prompt(
            source, renderer=XmlPromptRenderer(metric_index_threshold=3)
        )
        assert '  <domain name="a" summary="a summary" metric_count="4" />' in prompt
        assert "<metric name=" not in prompt
        assert _dropped_domain_hint(4) in prompt

    def test_entries_count_each_listing(self) -> None:
        # 2 distinct metrics, each in 2 domains: 4 entries > 3.
        source = _Source([_metric("x", "a", "b"), _metric("y", "a", "b")])
        prompt = _contract("a", "b").to_system_prompt(
            source, renderer=XmlPromptRenderer(metric_index_threshold=3)
        )
        assert "<metric name=" not in prompt
        # {m} is distinct metrics, never the threshold or the entry count.
        assert _dropped_domain_hint(2) in prompt

    def test_at_threshold_still_lists(self) -> None:
        source = _Source([_metric(f"m{i}", "a") for i in range(3)])
        prompt = _contract("a").to_system_prompt(
            source, renderer=XmlPromptRenderer(metric_index_threshold=3)
        )
        assert '<metric name="m2" />' in prompt

    def test_other_group_counts_toward_entries(self) -> None:
        source = _Source([_metric("x", "a"), _metric("y"), _metric("z")])
        prompt = _contract("a").to_system_prompt(
            source, renderer=XmlPromptRenderer(metric_index_threshold=2)
        )
        assert "<other_metrics>" not in prompt
        assert _dropped_domain_hint(3) in prompt


# ---------------------------------------------------------------------------
# Without domains
# ---------------------------------------------------------------------------


class TestNoDomains:
    def test_small_set_keeps_descriptions_with_new_hint(self) -> None:
        prompt = _contract().to_system_prompt(_plain(3))
        assert '  <metric name="m0">About m0.</metric>' in prompt
        assert LISTED_PLAIN_HINT in prompt
        assert "lookup_metric" not in prompt

    def test_above_detail_threshold_lists_names_only(self) -> None:
        prompt = _contract().to_system_prompt(_plain(30))
        assert '  <metric name="m0" />' in prompt
        assert '  <metric name="m29" />' in prompt
        assert "About m0." not in prompt
        assert "<count>" not in prompt
        assert LISTED_PLAIN_HINT in prompt

    def test_above_index_threshold_renders_count_line(self) -> None:
        prompt = _contract().to_system_prompt(
            _plain(30), renderer=XmlPromptRenderer(metric_index_threshold=25)
        )
        assert (
            "<count>30 metrics available; names omitted above 25 index entries.</count>"
        ) in prompt
        assert "<metric name=" not in prompt
        assert _dropped_plain_hint(30) in prompt

    def test_default_index_threshold_is_200(self) -> None:
        assert XmlPromptRenderer.METRIC_INDEX_THRESHOLD == 200
        assert '<metric name="m199" />' in _contract().to_system_prompt(_plain(200))
        prompt = _contract().to_system_prompt(_plain(201))
        assert "names omitted above 200 index entries" in prompt

    def test_index_threshold_none_never_drops(self) -> None:
        prompt = _contract().to_system_prompt(
            _plain(300), renderer=XmlPromptRenderer(metric_index_threshold=None)
        )
        assert '<metric name="m299" />' in prompt


# ---------------------------------------------------------------------------
# Threshold resolution
# ---------------------------------------------------------------------------


class TestIndexThresholdResolution:
    def test_detail_none_implies_never_drop_names(self) -> None:
        prompt = _contract().to_system_prompt(
            _plain(300), renderer=XmlPromptRenderer(metric_detail_threshold=None)
        )
        assert '<metric name="m299">About m299.</metric>' in prompt

    def test_detail_none_set_after_construction_still_applies(self) -> None:
        renderer = XmlPromptRenderer()
        renderer.metric_detail_threshold = None
        prompt = _contract("a").to_system_prompt(
            _Source([_metric(f"m{i}", "a") for i in range(300)]), renderer=renderer
        )
        assert '<metric name="m299" />' in prompt

    def test_explicit_index_threshold_beats_detail_none(self) -> None:
        prompt = _contract().to_system_prompt(
            _plain(30),
            renderer=XmlPromptRenderer(
                metric_detail_threshold=None, metric_index_threshold=10
            ),
        )
        assert "names omitted above 10 index entries" in prompt

    def test_subclass_constant_wins_over_default(self) -> None:
        class Tight(XmlPromptRenderer):
            METRIC_INDEX_THRESHOLD = 5

        prompt = _contract().to_system_prompt(_plain(6), renderer=Tight())
        assert "names omitted above 5 index entries" in prompt

    def test_legacy_subclass_without_super_init_uses_class_constant(self) -> None:
        class Legacy(XmlPromptRenderer):
            METRIC_INDEX_THRESHOLD = 5

            def __init__(self) -> None:  # deliberately no super().__init__()
                pass

        prompt = Legacy().render(_contract(), _plain(6))
        assert "names omitted above 5 index entries" in prompt

    def test_legacy_subclass_with_detail_none_never_drops(self) -> None:
        class Legacy(XmlPromptRenderer):
            METRIC_DETAIL_THRESHOLD = None  # type: ignore[assignment]

            def __init__(self) -> None:  # deliberately no super().__init__()
                pass

        prompt = Legacy().render(_contract(), _plain(300))
        assert '<metric name="m299">About m299.</metric>' in prompt


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestLogging:
    def test_names_only_logs_descriptions_omitted(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
            _contract().to_system_prompt(_plain(30))
        (msg,) = _logs(caplog)
        assert "30 metrics exceeds metric_detail_threshold=20" in msg
        assert "descriptions omitted" in msg
        assert "metric_detail_threshold=None" in msg
        assert "list_metrics" not in msg

    def test_names_dropped_logs_which_argument_restores(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
            _contract("a").to_system_prompt(
                _Source([_metric(f"m{i}", "a") for i in range(5)]),
                renderer=XmlPromptRenderer(metric_index_threshold=3),
            )
        (msg,) = _logs(caplog)
        assert "5 metric index entries exceed metric_index_threshold=3" in msg
        assert "names omitted" in msg
        assert "metric_index_threshold=None" in msg

    def test_no_log_when_everything_listed(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
            _contract("a").to_system_prompt(_Source([_metric("x", "a")]))
            _contract().to_system_prompt(_plain(3))
        assert _logs(caplog) == []
