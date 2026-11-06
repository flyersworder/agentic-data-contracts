"""Experiment 7: recovery, assembled from the checkers' files.

Rubric `prep/dataagentbench/RECOVERY-RUBRIC.md`, checker brief
`prep/dataagentbench/CHECKER-BRIEF.md`. Each dataset's checker wrote
`contracts-dataagentbench/recovery-work/<D>.references.json` (committed before
any draw was opened) and then `<D>.scores.json`. This script merges them into
`contracts-dataagentbench/recovery.json` and prints the summary and the offline
predictions of the design:

  (43)  draw A recovers at least half of the data-discoverable items
        (*recovered*, over items x three draws);
  (43c) the no-data control recovers fewer data-discoverable items than draw A
        (per draw), and at most a third of them;
  (43w) draw A recovers fewer definition, world and guidance items than
        data-discoverable ones, by share.

Described items enter no prediction and are reported apart. Beside (43), the
share counting *stated, not built* with *recovered* is printed as
exploratory: the rubric scores a value list no metric reads as stated.

One rule applied in assembly (RECOVERY.md, "Rulings"): a correctable error is
a fault in a contract's SQL or declared structure (a metric, a `from_sql`, a
relationship, a rule). A checker's "correctable error" that lies in prose only
becomes an addition confirmed "no". No score changes.

Every figure printed is computed from recovery.json alone.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections import Counter, defaultdict
from fractions import Fraction
from pathlib import Path

HERE = Path(__file__).resolve().parents[1]
WORK = HERE / "contracts-dataagentbench" / "recovery-work"
OUT = HERE / "contracts-dataagentbench" / "recovery.json"

DRAWS_A = ("draw-a1", "draw-a2", "draw-a3")
VARIANTS = (*DRAWS_A, "draw-b", "no-data")
CATEGORIES = ("recovered", "partial", "stated, not built", "missed")
OTHER = ("definition", "world", "guidance")
STRUCTURAL_MARKS = ("sql", "metrics.", "relationships", "rules.", "rules[", "tables")


def structural(error: dict) -> bool:
    """A correctable error lies in SQL or declared structure, not prose.

    `where` may name several places, separated by ";". A place is prose when it
    names a description; it is structural when it names SQL, a metric, a
    relationship, a rule or a table. One structural place is enough.
    """
    parts = [p.strip() for p in error.get("where", "").split(";")]
    return any(
        "description" not in p and any(m in p for m in STRUCTURAL_MARKS) for p in parts
    )


def apply_error_rule(scores: dict) -> dict:
    """Move prose-only correctable errors to additions confirmed "no"."""
    out = copy.deepcopy(scores)
    for draw in out["draws"].values():
        kept = []
        for e in draw.get("correctable_errors", []):
            if structural(e):
                kept.append(e)
            else:
                draw.setdefault("additions", []).append(
                    {
                        "quote": e.get("cause", ""),
                        "where": e.get("where", "")
                        + " (prose; was a correctable error)",
                        "confirmed": "no",
                    }
                )
        draw["correctable_errors"] = kept
    return out


def _cells(all_scores: list[dict]):
    for s in all_scores:
        for draw, v in s["draws"].items():
            for it in v.get("items", []):
                yield s["dataset"], draw, it["cls"], it["category"]


def counts(all_scores: list[dict]) -> dict:
    """variant -> class -> category -> n; `draw-a` pools the three A draws."""
    c: dict = defaultdict(lambda: defaultdict(Counter))
    for _, draw, cls, cat in _cells(all_scores):
        c[draw][cls][cat] += 1
        if draw in DRAWS_A:
            c["draw-a"][cls][cat] += 1
    return {k: {cl: dict(v) for cl, v in by.items()} for k, by in c.items()}


def _tally(all_scores, draws, classes, cats=("recovered",)) -> tuple[int, int]:
    hit = n = 0
    for _, draw, cls, cat in _cells(all_scores):
        if draw in draws and cls in classes:
            n += 1
            hit += cat in cats
    return hit, n


def predictions(all_scores: list[dict]) -> dict:
    a_rec, a_n = _tally(all_scores, DRAWS_A, ("data-discoverable",))
    a_rs, _ = _tally(
        all_scores, DRAWS_A, ("data-discoverable",), ("recovered", "stated, not built")
    )
    nd_rec, nd_n = _tally(all_scores, ("no-data",), ("data-discoverable",))
    o_rec, o_n = _tally(all_scores, DRAWS_A, OTHER)
    a_share = Fraction(a_rec, a_n) if a_n else Fraction(0)
    per_draw_a = Fraction(a_rec, len(DRAWS_A))
    return {
        "43": {
            "recovered": a_rec,
            "of": a_n,
            "share": float(a_share),
            "recovered_or_stated": a_rs,
            "holds": a_n > 0 and a_share >= Fraction(1, 2),
        },
        "43c": {
            "no_data": nd_rec,
            "of": nd_n,
            "draw_a_per_draw": float(per_draw_a),
            "holds": nd_n > 0
            and nd_rec < per_draw_a
            and Fraction(nd_rec, nd_n) <= Fraction(1, 3),
        },
        "43w": {
            "other": (o_rec, o_n),
            "data": (a_rec, a_n),
            "holds": o_n > 0
            and a_n > 0
            and Fraction(o_rec, o_n) < Fraction(a_rec, a_n),
        },
        "described": {
            "draw-a": _tally(all_scores, DRAWS_A, ("described",)),
            "draw-b": _tally(all_scores, ("draw-b",), ("described",)),
            "no-data": _tally(all_scores, ("no-data",), ("described",)),
        },
    }


def assemble(work: Path = WORK) -> dict:
    datasets = sorted(p.name.split(".")[0] for p in work.glob("*.scores.json"))
    out = {"datasets": {}}
    for d in datasets:
        refs = json.loads((work / f"{d}.references.json").read_text(encoding="utf-8"))
        scores = apply_error_rule(
            json.loads((work / f"{d}.scores.json").read_text(encoding="utf-8"))
        )
        missing = set(VARIANTS) - set(scores["draws"])
        if missing:
            raise ValueError(f"{d}: no scores for {sorted(missing)}")
        out["datasets"][d] = {"references": refs, "draws": scores["draws"]}
    return out


def _as_scores(recovery: dict) -> list[dict]:
    return [
        {"dataset": d, "draws": v["draws"]} for d, v in recovery["datasets"].items()
    ]


def table(recovery: dict) -> str:
    rows = [
        "| item | class | " + " | ".join(VARIANTS) + " |",
        "|---|---|" + "---|" * len(VARIANTS),
    ]
    for v in recovery["datasets"].values():
        ids = [it["id"] for it in v["draws"]["draw-a1"]["items"]]
        for i in ids:
            cats, cls = [], ""
            for draw in VARIANTS:
                it = next(x for x in v["draws"][draw]["items"] if x["id"] == i)
                cls = it["cls"]
                cats.append(it["category"])
            rows.append(f"| {i} | {cls} | " + " | ".join(cats) + " |")
    return "\n".join(rows)


def summary(recovery: dict) -> str:
    s = _as_scores(recovery)
    p = predictions(s)
    c = counts(s)
    lines = [table(recovery), ""]
    lines.append("| variant | class | " + " | ".join(CATEGORIES) + " |")
    lines.append("|---|---|" + "---|" * len(CATEGORIES))
    for var in ("draw-a", *VARIANTS):
        for cls in ("data-discoverable", *OTHER, "described"):
            row = c.get(var, {}).get(cls, {})
            if row:
                lines.append(
                    f"| {var} | {cls} | "
                    + " | ".join(str(row.get(k, 0)) for k in CATEGORIES)
                    + " |"
                )
    p43, p43c, p43w = p["43"], p["43c"], p["43w"]
    lines += [
        "",
        f"(43)  draw A recovers {p43['recovered']} of {p43['of']} data-discoverable "
        f"item-draws ({p43['share']:.0%}): {'holds' if p43['holds'] else 'fails'}. "
        "Exploratory, recovered or stated: "
        f"{p43['recovered_or_stated']} of {p43['of']}.",
        f"(43c) no-data recovers {p43c['no_data']} of {p43c['of']}, draw A "
        f"{p43c['draw_a_per_draw']:.2f} per draw: "
        f"{'holds' if p43c['holds'] else 'fails'}.",
        f"(43w) draw A recovers {p43w['other'][0]} of {p43w['other'][1]} definition, "
        f"world and guidance item-draws against {p43w['data'][0]} of {p43w['data'][1]} "
        f"data-discoverable: {'holds' if p43w['holds'] else 'fails'}.",
        "Described (apart): "
        + ", ".join(f"{k} {v[0]} of {v[1]}" for k, v in p["described"].items()),
    ]
    errs = [
        (d, draw, e)
        for d, v in recovery["datasets"].items()
        for draw, dv in v["draws"].items()
        for e in dv["correctable_errors"]
    ]
    lines.append("")
    lines.append(f"Correctable errors (SQL or structure): {len(errs)}")
    for d, draw, e in errs:
        lines.append(f"- {d} {draw}: {e['where']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Experiment 7: recovery.")
    ap.add_argument("--write", action="store_true", help="write recovery.json")
    args = ap.parse_args(argv)
    rec = assemble()
    if args.write:
        OUT.write_text(
            json.dumps(rec, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    print(summary(json.loads(json.dumps(rec))))


if __name__ == "__main__":
    main()
