"""A key present with no *list* value, and identity fields that cannot be blank.

The sibling of `test_null_text_fields.py`. That file covers a null landing in a
field annotated ``str``; this one covers the two cases it left open.

A bare **section header** -- ``metrics:`` with nothing under it -- is the single
most common way a YAML carries "present but empty", and it reached the parse
loop as ``None`` and died with an uncontextualised ``TypeError``. The new nested
guard was already defensive about exactly this (``_check_nested_keys._entries``
returns ``[]`` for a non-list), so the guard passed and the loop two lines later
crashed.

A null **identity** is different in kind from a null description. Coercing a
missing ``name:`` to ``""`` produces a metric that loads clean, renders into the
prompt and is unfindable by any name -- #89's own complaint, reintroduced by the
fix for it. Identity fields raise instead.
"""

from __future__ import annotations

import pytest
import yaml

from agentic_data_contracts.semantic.yaml_source import YamlSource


def _load(doc: str) -> YamlSource:
    return YamlSource.from_raw(yaml.safe_load(doc) or {})


# ── Bare section headers ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "doc",
    [
        "metrics:\n",
        "tables:\n",
        "relationships:\n",
        "metric_impacts:\n",
        "metrics:\ntables:\nrelationships:\nmetric_impacts:\n",
    ],
)
def test_a_bare_section_header_loads_as_empty(doc: str) -> None:
    src = _load(doc)
    assert src.get_metrics() == []
    assert src.get_table_schemas() == {}
    assert src.get_relationships() == []
    assert src.get_metric_impacts() == []


def test_a_bare_columns_key_on_a_table_loads_as_empty() -> None:
    src = _load("metrics: []\ntables:\n  - schema: main\n    table: t\n    columns:\n")
    assert src.get_table_schemas()["main.t"].columns == []


def test_a_bare_filters_key_leaves_an_empty_list_not_none() -> None:
    """``filters`` is annotated ``list[str]``, and a null there is not loud.

    ``domains`` and ``tier`` fail at load; ``filters`` was carried as ``None``
    into the metric, rendered to the agent as ``"filters": null``, and blew up
    later inside ``dump_semantic_source`` -- so a freeze or a digest crashed far
    from the malformed key that caused it.
    """
    src = _load("metrics:\n  - name: revenue\n    filters:\n")
    assert src.get_metrics()[0].filters == []


def test_a_bare_filters_key_does_not_break_a_freeze() -> None:
    from agentic_data_contracts.semantic.base import dump_semantic_source

    src = _load("metrics:\n  - name: revenue\n    filters:\n")
    assert dump_semantic_source(src)["metrics"][0]["filters"] == []


# ── Identity fields refuse to be blank ──────────────────────────────────────


def test_a_null_metric_name_is_refused() -> None:
    """Coerced to ``""`` it loaded clean and was unfindable by any real name."""
    with pytest.raises(ValueError, match="name"):
        _load("metrics:\n  - name:\n    description: no name\n")


def test_an_empty_metric_name_is_refused() -> None:
    with pytest.raises(ValueError, match="name"):
        _load('metrics:\n  - name: ""\n')


def test_a_null_column_name_is_refused() -> None:
    with pytest.raises(ValueError, match="name"):
        _load(
            "metrics: []\ntables:\n  - schema: main\n    table: t\n"
            "    columns:\n      - name:\n        type: VARCHAR\n"
        )


def test_a_null_relationship_endpoint_is_refused() -> None:
    with pytest.raises(ValueError, match="from"):
        _load("metrics: []\nrelationships:\n  - from:\n    to: a.b.c\n")


def test_a_null_metric_impact_endpoint_is_refused() -> None:
    with pytest.raises(ValueError, match="to"):
        _load("metrics: []\nmetric_impacts:\n  - from: a\n    to:\n")


def test_the_error_names_where_the_blank_is() -> None:
    """A parser error a consumer cannot locate is barely better than silence."""
    with pytest.raises(ValueError, match="tables"):
        _load(
            "metrics: []\ntables:\n  - schema: main\n    table: t\n"
            "    columns:\n      - name:\n"
        )


def test_a_named_metric_still_loads() -> None:
    assert _load("metrics:\n  - name: revenue\n").get_metrics()[0].name == "revenue"


# ── Cube schemas are hand-authored YAML, so they carry the same shape ───────


@pytest.mark.parametrize("bare", ["measures", "columns"])
def test_a_bare_cube_container_loads_as_empty(tmp_path, bare: str) -> None:  # noqa: ANN001
    from agentic_data_contracts.semantic.cube import CubeSource

    schema = tmp_path / "cube.yml"
    schema.write_text(f"cubes:\n  - name: O\n    sql_table: a.orders\n    {bare}:\n")
    src = CubeSource(schema)
    assert src.get_table_schemas()["a.orders"].columns == []
    assert src.get_metrics() == []
