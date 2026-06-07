from __future__ import annotations

import argparse
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from scipy.stats import binomtest


def _stratum(n_joins: int) -> str:
    return "2" if n_joins == 2 else "3" if n_joins == 3 else "4+"


def aggregate(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        for st in ("all", _stratum(r["n_joins"])):
            groups[(r["model"], r["rendering"], st)].append(r)
    out = []
    for (model, rendering, st), rs in sorted(groups.items()):
        n = len(rs)
        out.append(
            {
                "model": model,
                "rendering": rendering,
                "stratum": st,
                "n": n,
                "mean_f1": sum(r["f1"] for r in rs) / n,
                "exact_rate": sum(1 for r in rs if r["exact"]) / n,
            }
        )
    return out


def mcnemar_pairs(rows: list[dict], model: str) -> list[dict]:
    by: dict[tuple, bool] = {}
    renderings: set[str] = set()
    items: set[str] = set()
    for r in rows:
        if r["model"] != model:
            continue
        by[(r["item_id"], r["rendering"])] = bool(r["exact"])
        renderings.add(r["rendering"])
        items.add(r["item_id"])

    results = []
    for a, b in combinations(sorted(renderings, reverse=True), 2):
        b01 = b10 = 0
        for it in items:
            if (it, a) in by and (it, b) in by:
                ra, rb = by[(it, a)], by[(it, b)]
                if not ra and rb:
                    b01 += 1
                elif ra and not rb:
                    b10 += 1
        n = b01 + b10
        p = binomtest(b01, n, 0.5).pvalue if n > 0 else 1.0
        results.append(
            {
                "model": model,
                "a": a,
                "b": b,
                "b_a_wrong_b_right": b01,
                "b_a_right_b_wrong": b10,
                "p_value": p,
            }
        )
    return results


def load_rows(path: Path) -> list[dict]:
    return [
        json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()
    ]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results/results.jsonl")
    args = ap.parse_args()
    rows = load_rows(Path(args.results))

    print("\n=== mean F1 / exact-rate by model × rendering × stratum ===")
    for r in aggregate(rows):
        print(
            f"{r['model']:<32} {r['rendering']:<13} {r['stratum']:<4} "
            f"n={r['n']:<4} F1={r['mean_f1']:.3f} exact={r['exact_rate']:.3f}"
        )

    print("\n=== paired McNemar (exact-match) ===")
    for model in sorted({r["model"] for r in rows}):
        for res in mcnemar_pairs(rows, model):
            print(
                f"{model:<32} {res['a']} vs {res['b']}: "
                f"disc {res['b_a_wrong_b_right']}/{res['b_a_right_b_wrong']} "
                f"p={res['p_value']:.4f}"
            )


if __name__ == "__main__":
    main()
