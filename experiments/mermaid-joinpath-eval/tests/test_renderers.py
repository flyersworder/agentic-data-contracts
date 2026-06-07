import json
from pathlib import Path

from mje.anonymize import anonymize
from mje.renderers import RENDERERS, render_mermaid, render_xml
from mje.schema_graph import parse_tables_json

FIX = Path(__file__).parent / "fixtures" / "spider_mini" / "tables.json"


def _anon():
    g = parse_tables_json(json.loads(FIX.read_text())[0])
    ag, _ = anonymize(g)
    return ag


def test_all_renderers_mention_every_table():
    ag = _anon()
    for name, fn in RENDERERS.items():
        out = fn(ag)
        for t in ag.tables:
            assert t in out, f"{name} dropped table {t}"


def test_renderers_encode_every_fk_edge():
    ag = _anon()
    for name, fn in RENDERERS.items():
        out = fn(ag)
        for e in ag.fk_edges:
            assert e.a[1] in out and e.b[1] in out


def test_xml_is_wellformed_enough():
    ag = _anon()
    out = render_xml(ag)
    assert out.count("<table") == len(ag.tables)
    assert out.count("<rel ") == len(ag.fk_edges)


def test_mermaid_has_header():
    ag = _anon()
    out = render_mermaid(ag)
    assert out.strip().startswith("erDiagram")


def test_deterministic_output():
    ag = _anon()
    for fn in RENDERERS.values():
        assert fn(ag) == fn(ag)
