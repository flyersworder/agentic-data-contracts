"""A malformed semantic document must fail *locatably*, never internally.

Three review rounds in a row found sites this PR's null-tolerance sweep had
missed -- a bare `tier:`, a null `drill_by` dimension, an unguarded `t['schema']`
-- each fixed one at a time, each round finding more. That pattern says the
per-site sweep is the wrong instrument: it covers the sites a hand-written test
happens to touch, and the vocabulary is far larger than any such set.

So this file asserts a *whole-output property* instead of enumerating sites:

    Loading a malformed document either succeeds, or raises ``ValueError``.
    It never raises ``TypeError``, ``KeyError``, or ``AttributeError``.

That distinction is the contract. A ``ValueError`` from this parser names the
offending entry and is actionable. A ``TypeError: 'NoneType' object is not
iterable`` names nothing, points at library internals, and is indistinguishable
from a library bug -- which is what a consumer reasonably reports it as.

The variants are *derived from* `_FULL_DOCUMENT`, the same literal that
`test_nested_keys.py` asserts equal to the exported key frozensets. The two
tests interlock: a key added to a parser must be added to its frozenset (or the
equality test fails), which puts it in the document, which generates malformed
variants here automatically. Neither test needs editing when the vocabulary
grows, so this cannot decay into covering less than the parser reads.
"""

from __future__ import annotations

import copy
from typing import Any

import pytest

from agentic_data_contracts.semantic.yaml_source import YamlSource

from .test_nested_keys import _FULL_DOCUMENT

#: What a malformed document is allowed to do. `ValueError` is the parser's own
#: vocabulary for "your document is wrong, here is where"; the rest are Python
#: telling you the parser fell over.
_INTERNAL = (TypeError, KeyError, AttributeError, IndexError)


def _paths(node: Any, trail: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    """Every addressable position in the document, depth-first."""
    found: list[tuple[Any, ...]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            found.append((*trail, key))
            found.extend(_paths(value, (*trail, key)))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found.append((*trail, index))
            found.extend(_paths(value, (*trail, index)))
    return found


def _at(doc: Any, path: tuple[Any, ...]) -> Any:
    for step in path:
        doc = doc[step]
    return doc


def _mutated(path: tuple[Any, ...], how: str) -> dict[str, Any]:
    """A copy of the full document with one position broken."""
    doc = copy.deepcopy(_FULL_DOCUMENT)
    parent = _at(doc, path[:-1]) if len(path) > 1 else doc
    last = path[-1]
    if how == "null":
        parent[last] = None
    elif how == "scalar":
        parent[last] = "not-a-mapping"
    elif how == "delete":
        del parent[last]
    return doc


_PATHS = _paths(_FULL_DOCUMENT)
_NULLABLE = [p for p in _PATHS if not isinstance(p[-1], int)]
_ENTRIES = [p for p in _PATHS if isinstance(p[-1], int)]


def _assert_locatable(doc: dict[str, Any], label: str) -> None:
    try:
        YamlSource.from_raw(doc)
    except ValueError:
        return  # The parser's own "your document is wrong" channel.
    except _INTERNAL as exc:
        pytest.fail(
            f"{label} raised {type(exc).__name__}: {exc}\n"
            "A malformed document must produce a located ValueError, not an"
            " internal error naming library internals."
        )


def test_the_document_yields_enough_variants_to_be_a_real_gate() -> None:
    """Guards the generator itself: an empty path list would pass everything."""
    assert len(_NULLABLE) > 40, len(_NULLABLE)
    assert len(_ENTRIES) > 5, len(_ENTRIES)


@pytest.mark.parametrize("path", _NULLABLE, ids=lambda p: ".".join(map(str, p)))
def test_a_null_value_anywhere_fails_locatably(path: tuple[Any, ...]) -> None:
    """Every key set to null -- the "present but empty" shape YAML makes easy."""
    _assert_locatable(_mutated(path, "null"), f"null at {path}")


@pytest.mark.parametrize("path", _NULLABLE, ids=lambda p: ".".join(map(str, p)))
def test_a_missing_key_anywhere_fails_locatably(path: tuple[Any, ...]) -> None:
    """Every key deleted -- an author who omitted a field the parser indexes."""
    _assert_locatable(_mutated(path, "delete"), f"missing {path}")


@pytest.mark.parametrize("path", _ENTRIES, ids=lambda p: ".".join(map(str, p)))
def test_a_scalar_where_a_mapping_belongs_fails_locatably(
    path: tuple[Any, ...],
) -> None:
    """`metrics:\\n  - revenue` -- a list of names where mappings were meant."""
    _assert_locatable(_mutated(path, "scalar"), f"scalar at {path}")


def test_a_section_authored_as_a_mapping_fails_locatably() -> None:
    """`metrics:` written as a mapping keyed by name, not a list of entries."""
    _assert_locatable(
        {"metrics": {"revenue": {"name": "revenue"}}}, "metrics as a mapping"
    )


def test_a_scalar_where_a_section_belongs_fails_locatably() -> None:
    _assert_locatable({"metrics": "revenue"}, "metrics as a scalar")
