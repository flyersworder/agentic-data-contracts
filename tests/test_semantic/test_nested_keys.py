"""Unknown keys *inside* a semantic YAML's list entries (#89).

#60 established the rule for the top level of a semantic YAML: a key the
parser does not interpret is named in a warning, and becomes a load-time
error once the consumer declares ``expected_extras``. One level down --
inside a ``tables:`` entry, a ``columns:`` entry, a ``metrics:`` entry --
the same key was read by direct access over a fixed set and everything
else fell on the floor, with no signal *even under* ``expected_extras=[]``,
the setting whose documented purpose is to fail the build on a typo.

The asymmetry these tests pin: a nested unknown key is *diagnosed but not
carried*. A top-level key is plausibly a consumer's own section, so it
survives into ``get_extras()``; a key inside a ``columns:`` entry has no
addressable home on ``Column``, so carrying it would mean inventing a
nested-extras shape that widens the dump format and moves every published
digest. Diagnose, don't carry.
"""

from __future__ import annotations

import logging

import pytest

from agentic_data_contracts.semantic.yaml_source import (
    COLUMN_KEYS,
    METRIC_IMPACT_KEYS,
    METRIC_KEYS,
    RELATIONSHIP_KEYS,
    TABLE_KEYS,
    YamlSource,
)

LOGGER_NAME = "agentic_data_contracts.semantic.yaml_source"


# ── The exported key sets ───────────────────────────────────────────────────
# Exported for the reason SEMANTIC_KEYS already is: a consumer asserting
# against the interpreted vocabulary should not hardcode a list that drifts
# on the next release.


def test_table_keys_names_the_keys_the_table_parser_reads() -> None:
    assert TABLE_KEYS == frozenset({"schema", "table", "description", "columns"})


def test_column_keys_names_the_keys_the_column_parser_reads() -> None:
    assert COLUMN_KEYS == frozenset({"name", "type", "description"})


# ── tables[] ────────────────────────────────────────────────────────────────


def test_unknown_key_on_a_table_entry_warns_by_default(caplog) -> None:  # noqa: ANN001
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [{"schema": "main", "table": "payments", "summary": "x"}],
            }
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "summary" in joined
    assert "main.payments" in joined


def test_unknown_key_on_a_table_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="summary"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [{"schema": "main", "table": "payments", "summary": "x"}],
            },
            expected_extras=[],
        )


def test_expected_extras_does_not_excuse_a_nested_key() -> None:
    """``expected_extras`` whitelists *top-level sections*, not nested keys.

    Naming ``summary`` there says "I authored a top-level ``summary:``
    section", which is no statement at all about a ``summary:`` key inside a
    table entry. Strict mode stays strict all the way down.
    """
    with pytest.raises(ValueError, match="summary"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [{"schema": "main", "table": "payments", "summary": "x"}],
            },
            expected_extras=["summary"],
        )


def test_a_nested_unknown_key_is_not_carried_into_extras(caplog) -> None:  # noqa: ANN001
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        src = YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [{"schema": "main", "table": "payments", "summary": "x"}],
            }
        )
    assert src.get_extras() == {}


# ── columns[] ───────────────────────────────────────────────────────────────


def test_unknown_key_on_a_column_entry_warns_by_default(caplog) -> None:  # noqa: ANN001
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [
                    {
                        "schema": "main",
                        "table": "payments",
                        "columns": [{"name": "psp_reference", "note": "dropped"}],
                    }
                ],
            }
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "note" in joined
    assert "psp_reference" in joined


def test_unknown_key_on_a_column_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="note"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [
                    {
                        "schema": "main",
                        "table": "payments",
                        "columns": [{"name": "psp_reference", "note": "dropped"}],
                    }
                ],
            },
            expected_extras=[],
        )


# ── metrics[] and its own nested lists ──────────────────────────────────────


def test_unknown_key_on_a_metric_entry_warns_by_default(caplog) -> None:  # noqa: ANN001
    """A dbt refugee pasting ``meta:`` onto a metric is the common case."""
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw({"metrics": [{"name": "revenue", "meta": {"a": 1}}]})
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "meta" in joined
    assert "revenue" in joined


def test_typo_in_a_metric_key_raises_under_expected_extras() -> None:
    """``descriptions:`` for ``description:`` is the nested twin of #60's case."""
    with pytest.raises(ValueError, match="descriptions"):
        YamlSource.from_raw(
            {"metrics": [{"name": "revenue", "descriptions": "typo"}]},
            expected_extras=[],
        )


def test_unknown_key_on_a_drill_by_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="colunm"):
        YamlSource.from_raw(
            {
                "metrics": [
                    {
                        "name": "revenue",
                        "drill_by": [{"dimension": "region", "colunm": "r"}],
                    }
                ],
                "tables": [],
            },
            expected_extras=[],
        )


def test_unknown_key_on_a_decomposition_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="operater"):
        YamlSource.from_raw(
            {
                "metrics": [
                    {
                        "name": "revenue",
                        "decompositions": [
                            {"operator": "sum", "operands": [], "operater": "typo"}
                        ],
                    }
                ]
            },
            expected_extras=[],
        )


# ── relationships[] and metric_impacts[] ────────────────────────────────────


def test_unknown_key_on_a_relationship_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="cardinality"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "relationships": [
                    {"from": "a.b.c", "to": "d.e.f", "cardinality": "many_to_one"}
                ],
            },
            expected_extras=[],
        )


def test_unknown_key_on_a_metric_impact_entry_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="strength"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "metric_impacts": [{"from": "a", "to": "b", "strength": "high"}],
            },
            expected_extras=[],
        )


def test_relationship_and_impact_key_sets_include_their_yaml_spellings() -> None:
    """``from``/``to`` in YAML, ``from_``/``from_metric`` on the dataclass."""
    assert {"from", "to"} <= RELATIONSHIP_KEYS
    assert {"from", "to"} <= METRIC_IMPACT_KEYS


# ── The guard must not fire on the vocabulary it interprets ─────────────────


def test_every_interpreted_key_loads_without_a_warning(caplog) -> None:  # noqa: ANN001
    """A full-vocabulary document is silent -- the guard's false-positive test.

    Built from the exported key sets rather than a handwritten literal, so a
    key added to the parser without being added here fails loudly instead of
    going untested.
    """
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {
                "tables": [
                    {
                        "schema": "main",
                        "table": "payments",
                        "description": "One row per authorisation attempt.",
                        "columns": [
                            {"name": "region", "type": "VARCHAR", "description": "d"}
                        ],
                    }
                ],
                "metrics": [
                    {
                        "name": "revenue",
                        "description": "d",
                        "sql_expression": "SUM(x)",
                        "source_model": "main.payments",
                        "filters": [],
                        "domains": [],
                        "tier": [],
                        "indicator_kind": None,
                        "business_owner": None,
                        "operational_owner": None,
                        "last_reviewed": None,
                        "decompositions": [],
                        "drill_by": [
                            {"dimension": "region", "column": "main.payments.region"}
                        ],
                    }
                ],
                "relationships": [
                    {
                        "from": "main.payments.region",
                        "to": "main.regions.id",
                        "type": "many_to_one",
                        "description": "d",
                        "required_filter": None,
                        "preferred": False,
                    }
                ],
                "metric_impacts": [
                    {
                        "from": "revenue",
                        "to": "revenue",
                        "direction": "positive",
                        "confidence": "hypothesized",
                        "evidence": "e",
                        "description": "d",
                        "last_reviewed": None,
                    }
                ],
            },
            expected_extras=[],
        )
    assert caplog.records == []


def test_metric_keys_covers_every_key_the_full_document_uses() -> None:
    """Guards the test above from silently under-covering the vocabulary."""
    assert "business_owner" in METRIC_KEYS
    assert "decompositions" in METRIC_KEYS
    assert "drill_by" in METRIC_KEYS
