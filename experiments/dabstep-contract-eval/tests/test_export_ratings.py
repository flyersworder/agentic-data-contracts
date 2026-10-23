"""The public copy of the blind ratings, and what it may not carry.

`analysis/export_ratings.py` writes `ratings/` from the private rating folders.
The DABStep side is released whole; the LiveSQLBench side keeps only the
labels, since the raters' notes quote the private gold and the load key
encodes the private task-to-KB mapping.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import export_ratings as ex  # noqa: E402

RATINGS = Path(__file__).resolve().parents[1] / "ratings"


def _write(path: Path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".jsonl":
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    else:
        path.write_text(json.dumps(rows), encoding="utf-8")


def test_the_load_export_keeps_dabstep_items_whole_and_drops_livesqlbench(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    _write(
        src / "key.json",
        {
            "P001": {"bench": "dabstep", "task": "1857"},
            "P002": {"bench": "livesqlbench", "task": "shop_1"},
        },
    )
    dab = {"id": "P001", "rule_matching": True, "steps": 4, "notes": "fee rules"}
    lsb = {"id": "P002", "rule_matching": False, "notes": "KB entry 12"}
    _write(src / "rater-r1-c01.jsonl", [dab, lsb])
    (src / "RUBRIC.md").write_text("rubric", encoding="utf-8")

    ex.export_load(src, dst)

    assert json.loads((dst / "key.json").read_text(encoding="utf-8")) == {
        "P001": {"bench": "dabstep", "task": "1857"}
    }
    rows = (dst / "rater-r1-c01.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(r) for r in rows] == [dab]
    assert (dst / "RUBRIC.md").read_text(encoding="utf-8") == "rubric"


def test_the_determinacy_export_keeps_only_the_labels(tmp_path):
    src, dst = tmp_path / "src", tmp_path / "dst"
    rating = {
        "id": "B01",
        "kb_clarity": "ambiguous",
        "decisive": True,
        "undetermined": ["join path"],
        "notes": "the gold weights Critical 4",
    }
    _write(src / "blind-rater-A.jsonl", [rating])
    _write(src / "blind302-r1-c1.jsonl", [dict(rating, id="C001")])
    _write(src / "blind-key.json", {"B01": "cross_border_1"})
    _write(src / "blind302-key.json", {"C001": "cross_border_1"})
    _write(src / "dossiers" / "index.json", [["cross_border_1", "cross_border", {}]])

    ex.export_determinacy(src, dst)

    label = {"id": "B01", "kb_clarity": "ambiguous", "decisive": True}
    out = (dst / "blind-rater-A.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(r) for r in out] == [label]
    out = (dst / "blind302-r1-c1.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(r) for r in out] == [dict(label, id="C001")]
    assert json.loads((dst / "blind-key.json").read_text(encoding="utf-8")) == {
        "B01": "cross_border_1"
    }
    index = json.loads((dst / "dossiers" / "index.json").read_text(encoding="utf-8"))
    assert index == [["cross_border_1"]]


def test_the_released_load_ratings_rebuild_the_199_rule_matching_tasks():
    import procedural_load as pl
    from contract_text import rule_matching

    first = pl.read_ratings(RATINGS / "procedural-load", "r1")
    second = pl.read_ratings(RATINGS / "procedural-load", "r2")
    assert set(first) == set(second) == {"dabstep"}
    assert len(first["dabstep"]) == len(second["dabstep"]) == 401
    assert len(rule_matching(first["dabstep"], second["dabstep"])) == 199


def test_no_released_determinacy_file_carries_a_note():
    files = list((RATINGS / "lsb-determinacy").rglob("*.jsonl"))
    assert files
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            assert set(json.loads(line)) == {"id", "kb_clarity", "decisive"}
