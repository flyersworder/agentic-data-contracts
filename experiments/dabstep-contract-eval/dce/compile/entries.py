"""The compiler's neutral view of a knowledge base.

Each benchmark's adapter maps its documentation onto `KBEntry`s; nothing
past this module knows which benchmark an entry came from. LiveSQLBench's
KB types map to `calculation`, `predicate` and `illustration`; DABStep's
manual sections are `section`s, which may yield several metrics or none.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass

KINDS: tuple[str, ...] = ("calculation", "predicate", "illustration", "section")

#: The domain each kind's output lands in. A section is its own domain.
DOMAIN_BY_KIND: dict[str, str] = {
    "calculation": "calculations",
    "predicate": "definitions",
    "illustration": "reference",
}


@dataclass(frozen=True)
class KBEntry:
    id: int
    kind: str
    name: str
    text: str  # verbatim: LiveSQLBench's description and definition, or a section
    children: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"unknown kind {self.kind!r}; expected one of {KINDS}")


def slug(text: str) -> str:
    value = re.sub(r"[^0-9a-z]+", "_", text.lower()).strip("_")
    return value or "entry"


def metric_names(entries) -> dict[int, str]:
    """entry id -> metric name, for every calculation and predicate. The
    slug of the entry's name; entries that share a slug ALL get `_<id>`,
    so no name depends on which entry happened to come first."""
    eligible = [e for e in entries if e.kind in ("calculation", "predicate")]
    names = {e.id: slug(e.name) for e in eligible}
    # Repeat until unique: a suffixed name can meet a natural one ("Foo" as
    # id 12 and "Foo 12" are both `foo_12`). Ids are unique, so it ends.
    while True:
        counts = Counter(names.values())
        clashing = [i for i, name in names.items() if counts[name] > 1]
        if not clashing:
            return names
        for i in clashing:
            names[i] = f"{names[i]}_{i}"


def compile_order(entries) -> list[KBEntry]:
    """Children before parents, so a parent's SQL can build on its children's
    metrics; otherwise ascending id."""
    by_id: dict[int, KBEntry] = {}
    for entry in entries:
        if entry.id in by_id:
            raise ValueError(f"duplicate entry id {entry.id}")
        by_id[entry.id] = entry
    for entry in by_id.values():
        missing = [c for c in entry.children if c not in by_id]
        if missing:
            raise ValueError(f"entry {entry.id} names missing children {missing}")
    order: list[KBEntry] = []
    state: dict[int, str] = {}

    def visit(entry: KBEntry, path: list[int]) -> None:
        if state.get(entry.id) == "done":
            return
        if state.get(entry.id) == "open":
            raise ValueError(f"cycle in children: {path + [entry.id]}")
        state[entry.id] = "open"
        for child in sorted(entry.children):
            visit(by_id[child], path + [entry.id])
        state[entry.id] = "done"
        order.append(entry)

    for entry in sorted(by_id.values(), key=lambda e: e.id):
        visit(entry, [])
    return order


def domain_of(entry: KBEntry) -> str:
    if entry.kind == "section":
        return slug(entry.name)
    return DOMAIN_BY_KIND[entry.kind]
