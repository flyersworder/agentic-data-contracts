"""Write the public copy of the blind ratings to `ratings/`.

The raters' folders stay private because the LiveSQLBench side carries the
benchmark's private material: the load key encodes its task-to-KB mapping and
the determinacy raters' notes quote its gold. What can go public does:

- `ratings/procedural-load/`: the DABStep items whole, key, rater chunks and
  rubric. They define the 199 rule-matching tasks and the held-out set.
- `ratings/lsb-determinacy/`: each LiveSQLBench rating cut to its label
  (`kb_clarity`, `decisive`), with the public keys from anonymous id to task
  and the hard set's task ids. `lsb_kb_clarity.py` runs on it unchanged.
- `ratings/cross-family/<model>/`: the cross-family raters' final outputs
  (prep/cross-family-ratings/PLAN.md) under the same cut: DABStep load items
  whole, LiveSQLBench determinacy labels only, no LiveSQLBench load items.

Inputs: `$PROCEDURAL_LOAD` and `$LSB_AUDIT` (defaults under
~/data/dce-held-results). Rerun after any new rating.

Run:  uv run python analysis/export_ratings.py
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HELD = Path.home() / "data/dce-held-results"
LOAD_SRC = Path(os.environ.get("PROCEDURAL_LOAD", HELD / "procedural-load"))
AUDIT_SRC = Path(os.environ.get("LSB_AUDIT", HELD / "failure-audit"))
CROSS_SRC = Path(os.environ.get("CROSS_FAMILY", HELD / "cross-family"))
OUT = ROOT / "ratings"

LABEL = ("id", "kb_clarity", "decisive")


def _jsonl(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=1) + "\n", encoding="utf-8")


def export_load(src: Path, dst: Path) -> None:
    """The DABStep load ratings whole; every LiveSQLBench item dropped."""
    dst.mkdir(parents=True, exist_ok=True)
    key = json.loads((src / "key.json").read_text(encoding="utf-8"))
    keep = {i: k for i, k in key.items() if k["bench"] == "dabstep"}
    _write_json(dst / "key.json", keep)
    for path in sorted(src.glob("rater-*.jsonl")):
        _write_jsonl(dst / path.name, [r for r in _jsonl(path) if r["id"] in keep])
    shutil.copyfile(src / "RUBRIC.md", dst / "RUBRIC.md")


def export_determinacy(src: Path, dst: Path) -> None:
    """Each LiveSQLBench rating cut to its label, plus the public keys."""
    (dst / "dossiers").mkdir(parents=True, exist_ok=True)
    for pattern in ("blind-rater-*.jsonl", "blind302-*.jsonl"):
        for path in sorted(src.glob(pattern)):
            rows = [{f: r[f] for f in LABEL} for r in _jsonl(path)]
            _write_jsonl(dst / path.name, rows)
    for name in ("blind-key.json", "blind302-key.json"):
        shutil.copyfile(src / name, dst / name)
    index = json.loads((src / "dossiers/index.json").read_text(encoding="utf-8"))
    _write_json(dst / "dossiers/index.json", [[row[0]] for row in index])


def export_cross_family(src: Path, dst: Path, key: dict[str, dict]) -> None:
    """Each new rater's final chunk files (not the per-attempt copies)."""
    for rater in sorted(p for p in src.iterdir() if p.is_dir()):
        out = dst / rater.name
        out.mkdir(parents=True, exist_ok=True)
        for path in sorted(rater.glob("c[0-9][0-9].jsonl")):
            rows = [r for r in _jsonl(path) if key[r["id"]]["bench"] == "dabstep"]
            _write_jsonl(out / path.name, rows)
        for path in sorted(rater.glob("det-*.jsonl")):
            if ".a" in path.stem:
                continue
            _write_jsonl(
                out / path.name, [{f: r[f] for f in LABEL} for r in _jsonl(path)]
            )


def main() -> None:
    export_load(LOAD_SRC, OUT / "procedural-load")
    export_determinacy(AUDIT_SRC, OUT / "lsb-determinacy")
    key = json.loads((LOAD_SRC / "key.json").read_text(encoding="utf-8"))
    export_cross_family(CROSS_SRC, OUT / "cross-family", key)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
