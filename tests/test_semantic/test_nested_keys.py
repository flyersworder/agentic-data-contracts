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
from typing import Any

import pytest
import yaml

from agentic_data_contracts.semantic.yaml_source import (
    COLUMN_KEYS,
    DECOMPOSITION_CONVENTION_KEYS,
    DECOMPOSITION_KEYS,
    DRILL_BY_KEYS,
    METRIC_IMPACT_KEYS,
    METRIC_KEYS,
    RELATIONSHIP_KEYS,
    SEMANTIC_KEYS,
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


#: A document using every interpreted key. Asserted key-for-key against the
#: exported frozensets below, which is what makes the silence test meaningful:
#: a key added to a parser without being added to its key set fails there
#: rather than going quietly untested -- the exact regression #89 was.
_FULL_DOCUMENT: dict[str, Any] = {
    "tables": [
        {
            "schema": "main",
            "table": "payments",
            "description": "One row per authorisation attempt.",
            "columns": [{"name": "region", "type": "VARCHAR", "description": "d"}],
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
            "decompositions": [
                {
                    "operator": "product",
                    "operands": ["a", "b"],
                    "convention": "split_evenly",
                    "convention_operand": None,
                }
            ],
            "drill_by": [{"dimension": "region", "column": "main.payments.region"}],
        },
        # Leaf operands, so the decomposition above resolves. Their sparser key
        # sets are a subset of METRIC_KEYS, so they raise nothing; only
        # `metrics[0]` is asserted equal to the full vocabulary.
        {"name": "a", "sql_expression": "SUM(a)"},
        {"name": "b", "sql_expression": "SUM(b)"},
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
    "decomposition_convention": {"convention": "split_evenly"},
}


def test_the_full_document_uses_every_interpreted_key() -> None:
    """Makes the silence test below a real guard rather than a claim.

    Each assertion is equality, not membership: a key added to a parser (and so
    to its frozenset) without being added to the document fails here, and the
    silence test can never go stale by covering less than the vocabulary.
    """
    metric = _FULL_DOCUMENT["metrics"][0]
    table = _FULL_DOCUMENT["tables"][0]
    assert set(table) == TABLE_KEYS
    assert set(table["columns"][0]) == COLUMN_KEYS
    assert set(metric) == METRIC_KEYS
    assert set(metric["decompositions"][0]) == DECOMPOSITION_KEYS
    assert set(metric["drill_by"][0]) == DRILL_BY_KEYS
    assert set(_FULL_DOCUMENT["relationships"][0]) == RELATIONSHIP_KEYS
    assert set(_FULL_DOCUMENT["metric_impacts"][0]) == METRIC_IMPACT_KEYS
    assert (
        set(_FULL_DOCUMENT["decomposition_convention"]) == DECOMPOSITION_CONVENTION_KEYS
    )
    # And the document's own top level is exactly the interpreted sections.
    assert set(_FULL_DOCUMENT) == SEMANTIC_KEYS


def test_every_interpreted_key_loads_without_a_warning(caplog) -> None:  # noqa: ANN001
    """A full-vocabulary document is silent -- the guard's false-positive test."""
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(_FULL_DOCUMENT, expected_extras=[])
    assert caplog.records == []


# ── Keys YAML does not resolve to strings ───────────────────────────────────


def test_mixed_type_unknown_keys_are_reported_not_crashed(caplog) -> None:  # noqa: ANN001
    """``yaml.safe_load`` resolves bare ``2024:`` to an int, ``on:`` to a bool.

    Two unknown keys of different types in one entry made the diagnostic sort a
    heterogeneous set and die with an opaque ``TypeError`` -- turning a silent
    drop into a crash, which is worse than the defect #89 set out to fix. One
    such key never tripped it, since a single-element sort compares nothing.
    """
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [
                    {
                        "schema": "main",
                        "table": "t",
                        "columns": [{"name": "x", 2024: "backfilled", "note": "n"}],
                    }
                ],
            }
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "2024" in joined
    assert "note" in joined


def test_mixed_type_unknown_keys_still_raise_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="2024"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "tables": [{"schema": "main", "table": "t", 2024: "x", "note": "n"}],
            },
            expected_extras=[],
        )


def test_mixed_type_unknown_keys_at_the_top_level_do_not_crash(caplog) -> None:  # noqa: ANN001
    """The same hazard in `_apply_extras_policy`, which has the same shape.

    Loaded through ``yaml.safe_load`` rather than written as a dict literal
    because that is the only way the key arises: ``from_raw`` is annotated
    ``dict[str, Any]``, and an unquoted ``2024:`` in a real file is precisely
    the case the annotation does not describe.
    """
    raw = yaml.safe_load("metrics: []\n2024: x\nwidget_hints: []\n")
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(raw)
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "2024" in joined


# ── decomposition_convention: a mapping, not a list ─────────────────────────


def test_unknown_key_in_decomposition_convention_warns(caplog) -> None:  # noqa: ANN001
    """The one interpreted section the list-walk could not reach.

    `_parse_convention_default` reads exactly one key from it, so a second was
    dropped in the way #89 complains about -- and a claim that the rule holds
    "at every depth" was not quite true while it was.
    """
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {
                "metrics": [],
                "decomposition_convention": {
                    "convention": "split_evenly",
                    "fold_into": "revenue",
                },
            }
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "fold_into" in joined


def test_unknown_key_in_decomposition_convention_raises_under_expected_extras() -> None:
    with pytest.raises(ValueError, match="typo_key"):
        YamlSource.from_raw(
            {
                "metrics": [],
                "decomposition_convention": {
                    "convention": "split_evenly",
                    "typo_key": 1,
                },
            },
            expected_extras=[],
        )


def test_a_valid_convention_block_is_silent(caplog) -> None:  # noqa: ANN001
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        YamlSource.from_raw(
            {"metrics": [], "decomposition_convention": {"convention": "split_evenly"}},
            expected_extras=[],
        )
    assert caplog.records == []
