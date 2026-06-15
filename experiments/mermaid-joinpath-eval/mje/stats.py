from __future__ import annotations

import argparse
import json
from collections import defaultdict
from itertools import combinations
from pathlib import Path

from scipy.stats import binomtest


def _stratum(n_joins: int) -> str:
    return "2" if n_joins == 2 else "3" if n_joins == 3 else "4+"


def _is_parse_failure(r: dict) -> bool:
    """No parseable join edge came back (every item has >=2 gold joins, so an
    empty prediction is a parse/refusal failure, not a legitimate answer).

    Counted separately so parse failures don't masquerade as reasoning failures
    and so an asymmetric parse-failure rate across renderings can be detected.
    """
    return len(r.get("pred_edges", [])) == 0


def aggregate(rows: list[dict], exclude_truncated: bool = False) -> list[dict]:
    if exclude_truncated:
        rows = [r for r in rows if not r.get("truncated")]
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        ambig = "ambig" if r.get("ambiguous") else "unambig"
        for st in ("all", _stratum(r["n_joins"]), ambig):
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
                "parse_fail_rate": sum(1 for r in rs if _is_parse_failure(r)) / n,
                "truncated_rate": sum(1 for r in rs if r.get("truncated")) / n,
            }
        )
    return out


def mcnemar_pairs(
    rows: list[dict],
    model: str,
    exclude_truncated: bool = False,
    ambiguity: str = "all",
) -> list[dict]:
    """Paired McNemar (exact-match) across rendering pairs for one model.

    ``ambiguity`` selects the item subset: ``"all"`` (default), ``"unambig"``
    (drop cycle-flagged items, where exact-match is unreliable), or ``"ambig"``.
    ``exclude_truncated`` drops a pair if either side was output-token truncated.

    Discordant counts are reported with keys tied to the actual ``a``/``b``
    rendering names (``a_right_b_wrong`` / ``b_right_a_wrong``) so the direction
    can never be mis-transcribed downstream.
    """
    by: dict[tuple, dict] = {}
    renderings: set[str] = set()
    items: set[str] = set()
    for r in rows:
        if r["model"] != model:
            continue
        if ambiguity == "unambig" and r.get("ambiguous"):
            continue
        if ambiguity == "ambig" and not r.get("ambiguous"):
            continue
        by[(r["item_id"], r["rendering"])] = r
        renderings.add(r["rendering"])
        items.add(r["item_id"])

    results = []
    for a, b in combinations(sorted(renderings, reverse=True), 2):
        a_wins = b_wins = 0
        for it in items:
            ra, rb = by.get((it, a)), by.get((it, b))
            if ra is None or rb is None:
                continue
            if exclude_truncated and (ra.get("truncated") or rb.get("truncated")):
                continue
            ea, eb = bool(ra["exact"]), bool(rb["exact"])
            if ea and not eb:
                a_wins += 1
            elif eb and not ea:
                b_wins += 1
        n = a_wins + b_wins
        p = binomtest(b_wins, n, 0.5).pvalue if n > 0 else 1.0
        results.append(
            {
                "model": model,
                "a": a,
                "b": b,
                "a_right_b_wrong": a_wins,
                "b_right_a_wrong": b_wins,
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
    ap.add_argument(
        "--exclude-truncated",
        action="store_true",
        help="drop output-token-truncated rows (reproduces the 'clean' numbers)",
    )
    args = ap.parse_args()
    rows = load_rows(Path(args.results))

    label = " (clean: truncated excluded)" if args.exclude_truncated else ""
    print(f"\n=== mean F1 / exact / parse-fail / trunc by model × rendering{label} ===")
    print("stratum legend: all | 2,3,4+ (join depth) | ambig,unambig (cycle proxy)")
    for r in aggregate(rows, exclude_truncated=args.exclude_truncated):
        print(
            f"{r['model']:<32} {r['rendering']:<18} {r['stratum']:<8} "
            f"n={r['n']:<4} F1={r['mean_f1']:.3f} exact={r['exact_rate']:.3f} "
            f"parse_fail={r['parse_fail_rate']:.3f} trunc={r['truncated_rate']:.3f}"
        )

    for ambiguity in ("all", "unambig", "ambig"):
        print(
            f"\n=== paired McNemar (exact-match), items={ambiguity}"
            f"{', truncated excluded' if args.exclude_truncated else ''} ==="
        )
        for model in sorted({r["model"] for r in rows}):
            for res in mcnemar_pairs(
                rows,
                model,
                exclude_truncated=args.exclude_truncated,
                ambiguity=ambiguity,
            ):
                print(
                    f"{model:<32} {res['a']} vs {res['b']}: "
                    f"{res['a']}-right/{res['b']}-wrong={res['a_right_b_wrong']} "
                    f"{res['b']}-right/{res['a']}-wrong={res['b_right_a_wrong']} "
                    f"p={res['p_value']:.4f}"
                )


if __name__ == "__main__":
    main()
