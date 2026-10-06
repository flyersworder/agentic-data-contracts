import pytest
from dce.compile.entries import (
    KBEntry,
    compile_order,
    domain_of,
    metric_names,
    slug,
)


def _e(id, kind="calculation", name=None, children=()):
    return KBEntry(id, kind, name or f"Entry {id}", f"text {id}", tuple(children))


def test_children_compile_before_parents_otherwise_id_order():
    entries = [_e(3, children=[2]), _e(1), _e(2), _e(5, children=[1, 4]), _e(4)]
    assert [e.id for e in compile_order(entries)] == [1, 2, 3, 4, 5]


def test_a_cycle_or_a_missing_child_is_refused_by_id():
    with pytest.raises(ValueError, match="cycle"):
        compile_order([_e(1, children=[2]), _e(2, children=[1])])
    with pytest.raises(ValueError, match="9"):
        compile_order([_e(1, children=[9])])
    with pytest.raises(ValueError, match="duplicate"):
        compile_order([_e(1), _e(1)])


def test_an_unknown_kind_is_refused():
    with pytest.raises(ValueError, match="kind"):
        KBEntry(1, "formula", "x", "t")


def test_slug():
    assert slug("Shipped Revenue (EUR)") == "shipped_revenue_eur"
    assert slug("  --  ") == "entry"


def test_colliding_names_all_get_their_id_so_the_choice_is_order_free():
    entries = [
        _e(7, name="Risk Score"),
        _e(2, "predicate", name="risk score"),
        _e(3, name="Other"),
        _e(4, "illustration", name="Risk Score"),
    ]
    assert metric_names(entries) == {7: "risk_score_7", 2: "risk_score_2", 3: "other"}
    assert metric_names(list(reversed(entries))) == metric_names(entries)


def test_domains():
    assert domain_of(_e(1, "calculation")) == "calculations"
    assert domain_of(_e(1, "predicate")) == "definitions"
    assert domain_of(_e(1, "illustration")) == "reference"
    assert (
        domain_of(_e(1, "section", name="5.1 Best Practices")) == "5_1_best_practices"
    )
