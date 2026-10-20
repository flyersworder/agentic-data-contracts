"""Experiment 6's materials (design: docs/superpowers/specs/
2026-10-20-experiment-6-disclosure-scale-design.md).

The base is experiment 4: its three databases, questions and contracts
(contracts-procedures/livesqlbench/<db>/). The padding of a base is the other
two bases' contracts first, then the 19 other LiveSQLBench databases'
contracts written by dce.author (contracts-scale/padding/livesqlbench/<db>/),
in a permutation seeded per base. Each padding contract is moved into a schema
of its database's name (dce.scale.namespace); a level takes padding in order
until the merged contract reaches its multiple of the base's size.

    UV_FROZEN=1 uv run python prep/scale/build.py contracts
        contracts-scale/merged/<level>/<base>/, first/<level>/<base>/ (the base
        first: the position control), levels.json, sizes.json, and
        merged.sha256. The merged trees are not committed (about 20 MB); the
        job rebuilds them with --out on the pod's disk, and its checksums
        must match the committed merged.sha256.
    UV_FROZEN=1 uv run python prep/scale/build.py data [--proc D] [--lsb D] [--out D]
        the data directory the scale arms run on (default ~/data/scale):
        experiment 4's, with each base's database a working database holding
        every other database's tables in a schema of its name, and
        padding/<db>/<db>_kb.jsonl, the padding databases' KBs. Run on the
        cluster too, with its paths, so no copy of it sits on the volume.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce import scale  # noqa: E402

BASES = ("labor_certification_applications", "reverse_logistics", "solar_panel")
LEVELS: dict[str, float | None] = {"1x": 1, "2x": 2, "4x": 4, "8x": 8, "all": None}
SEED = 2026
BASE_CONTRACTS = ROOT / "contracts-procedures" / "livesqlbench"
PADDING = ROOT / "contracts-scale" / "padding" / "livesqlbench"
OUT = ROOT / "contracts-scale"


def _load(folder: Path) -> tuple[dict, dict]:
    return (
        yaml.safe_load((folder / "contract.yml").read_text(encoding="utf-8")),
        yaml.safe_load((folder / "semantic.yml").read_text(encoding="utf-8")),
    )


def _size(pair: tuple[dict, dict]) -> int:
    return len(scale.yaml_dump(pair[0])) + len(scale.yaml_dump(pair[1]))


def contracts(out: Path = OUT) -> None:
    pool = sorted(p.name for p in PADDING.iterdir() if (p / "semantic.yml").is_file())
    if len(pool) != 19:
        raise SystemExit(f"expected 19 padding contracts in {PADDING}, found {pool}")
    folders = {b: BASE_CONTRACTS / b for b in BASES} | {p: PADDING / p for p in pool}
    namespaced = {db: scale.namespace(*_load(f), db) for db, f in folders.items()}
    sizes = {db: _size(pair) for db, pair in namespaced.items()}
    levels: dict[str, dict[str, list[str]]] = {}
    report: dict[str, dict] = {}
    for base in BASES:
        own = _load(folders[base])
        sizes_here = {**sizes, base: _size(own)}
        near = [b for b in BASES if b != base]
        order = scale.padding_order(base, near, pool, seed=SEED)
        levels[base] = {}
        report[base] = {"order": order, "levels": {}}
        for level, multiple in LEVELS.items():
            padding = scale.level_padding(order, sizes_here, base, multiple)
            levels[base][level] = padding
            pads = [namespaced[p] for p in padding]
            merged = scale.merge(own, pads)
            scale.write(out / "merged" / level / base, *merged)
            scale.write(
                out / "first" / level / base, *scale.merge(own, pads, first=True)
            )
            report[base]["levels"][level] = {
                "padding": len(padding),
                "chars": _size(merged),
                "multiple": round(_size(merged) / sizes_here[base], 2),
            }
            print(f"{base} {level}: {len(padding)} padding, {_size(merged):,} chars")
    out.mkdir(parents=True, exist_ok=True)
    (out / "levels.json").write_text(json.dumps(levels, indent=1) + "\n", "utf-8")
    (out / "sizes.json").write_text(json.dumps(report, indent=1) + "\n", "utf-8")
    sums = sorted(
        f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.relative_to(out)}"
        for sub in ("merged", "first")
        for f in out.glob(f"{sub}/*/*/*.yml")
    )
    (out / "merged.sha256").write_text("\n".join(sums) + "\n", "utf-8")


def data(proc: Path, lsb: Path, out: Path) -> None:
    if out.exists():
        raise SystemExit(f"{out} exists; remove it first")
    shutil.copytree(
        proc, out, ignore=shutil.ignore_patterns("*.working*", "styled", "*.wal")
    )
    pool = sorted(
        p.stem for p in (lsb / "duckdb").glob("*.duckdb") if p.stem not in BASES
    )
    for base in BASES:
        others = {b: proc / "duckdb" / f"{b}.duckdb" for b in BASES if b != base}
        others |= {p: lsb / "duckdb" / f"{p}.duckdb" for p in pool}
        working = out / "duckdb" / f"{base}.duckdb"
        working.unlink()
        scale.working_db(proc / "duckdb" / f"{base}.duckdb", others, working)
        print(f"{base}: working database with {len(others)} schemas")
    for p in pool:
        folder = out / "padding" / p
        folder.mkdir(parents=True)
        shutil.copyfile(
            lsb / "hf-full-v1" / p / f"{p}_kb.jsonl", folder / f"{p}_kb.jsonl"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="what", required=True)
    c = sub.add_parser("contracts")
    c.add_argument("--out", type=Path, default=OUT)
    d = sub.add_parser("data")
    d.add_argument("--proc", type=Path, default=Path.home() / "data" / "procedures")
    d.add_argument("--lsb", type=Path, default=Path.home() / "data" / "livesqlbench")
    d.add_argument("--out", type=Path, default=Path.home() / "data" / "scale")
    args = parser.parse_args()
    if args.what == "contracts":
        contracts(args.out)
    else:
        data(args.proc, args.lsb, args.out)


if __name__ == "__main__":
    main()
