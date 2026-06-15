from pathlib import Path

from mje.data import build_items

FIXDIR = Path(__file__).parent / "fixtures" / "spider_mini"


def test_build_items_filters_by_min_joins_and_anonymizes():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=2)
    assert len(items) == 1
    it = items[0]
    assert it.n_joins == 2
    assert all(t.startswith("t") for t in it.endpoint_tables)
    for pair in it.gold_edges:
        for tname, cname in pair:
            assert tname in it.graph.tables
            assert cname in {c.name for c in it.graph.tables[tname]}


def test_min_joins_one_keeps_more():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=1)
    assert len(items) == 2  # the no-join query is still dropped


def test_ambiguity_flag_present():
    items = build_items(FIXDIR / "tables.json", FIXDIR / "dev.json", min_joins=2)
    assert isinstance(items[0].ambiguous, bool)
    assert items[0].ambiguous is False


def test_build_items_bird_query_key():
    fixdir = Path(__file__).parent / "fixtures"
    items = build_items(
        fixdir / "spider_mini" / "tables.json",
        fixdir / "bird_mini" / "dev.json",
        min_joins=2,
        query_key="SQL",
    )
    assert len(items) == 1
    assert items[0].n_joins == 2
