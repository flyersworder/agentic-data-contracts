"""The malformed-document property, held for every semantic source.

`test_malformed_documents.py` states the property and gates `YamlSource`. Round
5 of review found six more instances of it in the *other three* sources -- a
bare `sql_table:` yielding `source_model=None`, a null `relationship_type:`
defeating its own fallback, `"columns": null` in a dbt manifest raising
`AttributeError` -- which is the same lesson one level up: a gate that covers
one of four parsers leaves three unswept, and hand-patching those three is what
the previous four rounds already showed does not converge.

So the same generator runs against a full-vocabulary document per source. Each
document is written to a real file and loaded through the source's real
constructor, since these three parse files rather than dicts.

The property is unchanged: loading either succeeds or raises ``ValueError`` --
never ``TypeError``, ``KeyError``, or ``AttributeError``.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.ossie import OssieSource

from .test_malformed_documents import _INTERNAL, _at, _paths

_CUBE: dict[str, Any] = {
    "cubes": [
        {
            "name": "Orders",
            "sql_table": "analytics.orders",
            "description": "One row per order.",
            "measures": [
                {
                    "name": "revenue",
                    "sql": "SUM(amount)",
                    "type": "sum",
                    "description": "Revenue.",
                    "meta": {
                        "tier": ["north_star"],
                        "domains": ["revenue"],
                        "indicator_kind": "lagging",
                    },
                }
            ],
            "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
            "columns": [
                {"name": "id", "type": "INTEGER", "description": "Order id."},
                {"name": "region", "type": "VARCHAR", "description": "Region."},
            ],
            "joins": [
                {
                    "name": "Customers",
                    "sql": "{Orders}.customer_id = {Customers}.id",
                    "relationship": "many_to_one",
                    "description": "Order to customer.",
                    "meta": {
                        "relationship_type": "many_to_one",
                        "required_filter": None,
                        "preferred": True,
                    },
                }
            ],
        },
        {
            "name": "Customers",
            "sql_table": "analytics.customers",
            "columns": [{"name": "id", "type": "INTEGER"}],
        },
    ]
}

_DBT: dict[str, Any] = {
    "nodes": {
        "model.p.orders": {
            "resource_type": "model",
            "schema": "analytics",
            "name": "orders",
            "description": "All orders.",
            "columns": {
                "id": {"name": "id", "data_type": "INTEGER", "description": "Id."},
                "customer_id": {"name": "customer_id", "data_type": "INTEGER"},
            },
        },
        "model.p.customers": {
            "resource_type": "model",
            "schema": "analytics",
            "name": "customers",
            "columns": {"id": {"name": "id", "data_type": "INTEGER"}},
        },
        "test.p.rel": {
            "resource_type": "test",
            "description": "FK.",
            "attached_node": "model.p.orders",
            "depends_on": {"nodes": ["model.p.orders", "model.p.customers"]},
            "meta": {"relationship_type": "many_to_one", "preferred": True},
            "test_metadata": {
                "name": "relationships",
                "kwargs": {"column_name": "customer_id", "field": "id"},
            },
        },
    },
    "metrics": {
        "metric.p.revenue": {
            "name": "revenue",
            "description": "Revenue.",
            "model": "analytics.orders",
            "type_params": {"measure": {"expr": "SUM(amount)"}},
            "filters": [{"field": "status", "operator": "=", "value": "'done'"}],
            "meta": {
                "tier": ["north_star"],
                "domains": ["revenue"],
                "indicator_kind": "lagging",
            },
        }
    },
}

_OSSIE: dict[str, Any] = {
    "semantic_model": [
        {
            "name": "retail",
            "description": "Retail model.",
            "datasets": [
                {
                    "name": "orders",
                    "source": "analytics.orders",
                    "description": "All orders.",
                    "primary_key": ["id"],
                    "fields": [
                        {"name": "id", "datatype": "INTEGER", "description": "Id."},
                        {"name": "region", "datatype": "VARCHAR"},
                        {"name": "customer_id", "datatype": "INTEGER"},
                    ],
                },
                {
                    "name": "customers",
                    "source": "analytics.customers",
                    "primary_key": ["id"],
                    "fields": [{"name": "id", "datatype": "INTEGER"}],
                },
            ],
            "relationships": [
                {
                    "name": "orders_to_customers",
                    "from": "orders",
                    "to": "customers",
                    "from_columns": ["customer_id"],
                    "to_columns": ["id"],
                    "description": "Order to customer.",
                }
            ],
            "custom_extensions": [
                {
                    "vendor_name": "AGENTIC_DATA_CONTRACTS",
                    "data": {
                        "metrics": {
                            "revenue": {
                                "source_model": "analytics.orders",
                                "filters": ["status = 'done'"],
                                "domains": ["revenue"],
                                "tier": ["north_star"],
                                "indicator_kind": "lagging",
                                "business_owner": "revenue-platform",
                                "operational_owner": "data-eng",
                                "last_reviewed": "2026-05-15",
                                "decompositions": [
                                    {"operator": "sum", "operands": ["a", "b"]}
                                ],
                                "drill_by": [
                                    {
                                        "dimension": "region",
                                        "column": "analytics.orders.region",
                                    }
                                ],
                            },
                            "a": {"source_model": "analytics.orders"},
                            "b": {"source_model": "analytics.orders"},
                        },
                        "metric_impacts": [
                            {
                                "from": "revenue",
                                "to": "revenue",
                                "direction": "positive",
                                "confidence": "verified",
                                "evidence": "e",
                            }
                        ],
                    },
                }
            ],
            "metrics": [
                {
                    "name": "revenue",
                    "description": "Revenue.",
                    "expression": {
                        "dialects": [
                            {"dialect": "duckdb", "expression": "SUM(a)"},
                        ]
                    },
                },
                {"name": "a", "expression": {"dialects": []}},
                {"name": "b", "expression": {"dialects": []}},
            ],
        }
    ]
}


def _load_cube(path: Path, doc: Any) -> None:
    path.write_text(yaml.safe_dump(doc))
    CubeSource(path)


def _load_dbt(path: Path, doc: Any) -> None:
    path.write_text(json.dumps(doc))
    DbtSource(path)


def _load_ossie(path: Path, doc: Any) -> None:
    path.write_text(yaml.safe_dump(doc))
    OssieSource(path)


_SOURCES: dict[str, tuple[dict[str, Any], Callable[[Path, Any], None], str]] = {
    "cube": (_CUBE, _load_cube, "schema.yml"),
    "dbt": (_DBT, _load_dbt, "manifest.json"),
    "ossie": (_OSSIE, _load_ossie, "model.yml"),
}

_CASES = [
    (name, path, how)
    for name, (doc, _, _) in _SOURCES.items()
    for path in _paths(doc)
    for how in ("null", "delete", "scalar")
    if not (how == "delete" and isinstance(path[-1], int))
]


@pytest.mark.parametrize(
    ("source", "path", "how"),
    _CASES,
    ids=[f"{n}-{h}-{'.'.join(map(str, p))}" for n, p, h in _CASES],
)
def test_a_malformed_document_fails_locatably(
    tmp_path: Path, source: str, path: tuple[Any, ...], how: str
) -> None:
    base, loader, filename = _SOURCES[source]
    doc = copy.deepcopy(base)
    parent = _at(doc, path[:-1]) if len(path) > 1 else doc
    last = path[-1]
    if how == "null":
        parent[last] = None
    elif how == "scalar":
        parent[last] = "not-a-mapping"
    else:
        del parent[last]
    try:
        loader(tmp_path / filename, doc)
    except ValueError:
        return
    except _INTERNAL as exc:
        pytest.fail(
            f"{source}: {how} at {path} raised {type(exc).__name__}: {exc}\n"
            "A malformed document must produce a located ValueError."
        )


@pytest.mark.parametrize("source", sorted(_SOURCES))
def test_the_baseline_document_loads(tmp_path: Path, source: str) -> None:
    """Without this, a broken baseline would make every mutation pass."""
    base, loader, filename = _SOURCES[source]
    loader(tmp_path / filename, copy.deepcopy(base))


def test_enough_cases_to_be_a_real_gate() -> None:
    assert len(_CASES) > 150, len(_CASES)


@pytest.mark.parametrize("source", sorted(_SOURCES))
def test_the_baseline_reaches_every_parser_output(tmp_path: Path, source: str) -> None:
    """A section nothing parses makes every mutation of it a no-op.

    Round 6 found the Cube join written as ``${Orders}.col``, which
    ``_JOIN_EQ_RE`` never matches, so the relationship body was unreachable by
    the entire generator while the file claimed to gate it. `YamlSource`'s
    document is anchored to the exported frozensets; these three have no such
    anchor, so this is the equivalent interlock.
    """
    base, _, filename = _SOURCES[source]
    path = tmp_path / filename
    if source == "dbt":
        path.write_text(json.dumps(base))
        src: Any = DbtSource(path)
    else:
        path.write_text(yaml.safe_dump(base))
        src = CubeSource(path) if source == "cube" else OssieSource(path)
    assert src.get_metrics(), f"{source}: baseline parses no metric"
    assert src.get_table_schemas(), f"{source}: baseline parses no table"
    assert src.get_relationships(), f"{source}: baseline parses no relationship"


def test_the_ossie_baseline_reaches_the_vendor_overlay(tmp_path: Path) -> None:
    """Decompositions, drill_by and impacts all live behind the vendor block."""
    path = tmp_path / "model.yml"
    path.write_text(yaml.safe_dump(_OSSIE))
    src = OssieSource(path)
    revenue = src.get_metric("revenue")
    assert revenue is not None
    assert revenue.decompositions, "overlay decompositions unreachable"
    assert revenue.drill_by, "overlay drill_by unreachable"
    assert src.get_metric_impacts(), "overlay metric_impacts unreachable"
