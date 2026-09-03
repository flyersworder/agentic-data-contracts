"""Per-arm accuracy split by whether the compiled macro can answer the task.

`analysis/coverage.py` establishes that the macro — a mechanical composition
of the frozen contract — answers 176 of the 332 hard tasks exactly. This
script asks the follow-up: do the agent arms succeed on the SAME tasks?

A `macro` task is one whose answer the contract's own semantics produce
directly. A `derived` task needs those semantics plus a counterfactual or an
optimisation the macro does not encode. If the contract's advantage were only
about supplying the fee formula, it should concentrate on `macro` tasks and
vanish on `derived` ones.

Run:  uv run python analysis/buckets.py [results/*.jsonl ...]
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from coverage import classify  # noqa: E402
from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

ARMS = ["schema_only", "contract_hollow", "manual_prompt", "contract"]
BUCKETS = ["macro", "derived", "unrelated"]


def main(paths: list[str]) -> None:
    cat = classify()
    for path in paths:
        # `dce.stats.load` is the ONE deduplicating reader (glm's file carries
        # three superseded error rows that a later resume re-ran); never parse
        # the file any other way to count outcomes.
        rows = [r for r in load(Path(path)) if r.get("level") == "hard"]
        if not rows:
            continue
        # COMPLETE-CASE ONLY, matching FINDINGS.md: run B lost ~29% of tasks to
        # a 429 truncation. A task an arm never answered (verdict `error` or
        # `hit_limit`, not in ANSWER_VERDICTS) is dropped from EVERY arm, or
        # the arms stop being paired.
        seen: dict[str, set[str]] = defaultdict(set)
        for r in rows:
            if r.get("verdict") in ANSWER_VERDICTS:
                seen[str(r["task_id"])].add(r["arm"])
        complete = {t for t, a in seen.items() if a >= set(ARMS)}
        dropped = len({str(r["task_id"]) for r in rows}) - len(complete)
        if not complete:
            # A probe or smoke file has no paired tasks at all. Say so rather
            # than dying on an empty slice when a `results/*.jsonl` glob picks
            # it up (which the documented invocation does).
            print(f"\nskipping {Path(path).name}: no task has all four arms")
            continue
        rows = [r for r in rows if str(r["task_id"]) in complete]
        model = sorted({r["model"] for r in rows})[0].split("/")[-1]
        tally: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
        for r in rows:
            b = cat.get(str(r["task_id"]))
            if b is None:
                continue
            t = tally[(r["arm"], b)]
            t[1] += 1
            t[0] += r["verdict"] == "correct"
        note = f", {dropped} incomplete task(s) dropped" if dropped else ""
        print(f"\n{model}  ({Path(path).name}, hard tasks, complete cases{note})")
        head = "  ".join(f"{b:>16s}" for b in BUCKETS)
        print(f"{'arm':18s}{head}")
        for arm in ARMS:
            cells = []
            for b in BUCKETS:
                c, n = tally[(arm, b)]
                cells.append(
                    f"{c:3d}/{n:<3d} {100 * c / n:5.1f}%" if n else f"{'-':>16s}"
                )
            print(f"{arm:18s}" + "  ".join(f"{c:>16s}" for c in cells))


if __name__ == "__main__":
    args = sys.argv[1:] or [
        str(ROOT / "results" / "glm-full.jsonl"),
        str(ROOT / "results" / "dsflash-full.jsonl"),
        str(ROOT / "results" / "sol-full.jsonl"),
    ]
    main(args)
