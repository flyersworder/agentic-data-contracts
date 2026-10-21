"""The k=3 panels' decomposition: content against scaffolding, per model.

Reproduces FINDINGS.md, *The k=3 panels: content and scaffolding*. The two
gateway panels (gpt-6-sol and qwen3.8-27b, k=3, all four arms on 401 tasks)
are the paper's main decomposition. Runs A-D are the k=1 breadth.

Per model and scoring view (official strict, and end-to-end, the headline
score; see `dce.stats._as_e2e`):
- accuracy per arm, on all tasks and on the hard ones;
- task-level sign tests over the three repeats, each task scored 0-3, ties
  dropped, two-sided;
- McNemar per repeat, as the panel's plan asked (`docs/paper/README.md`).

The contrasts:
- `contract_hollow` against `schema_only`: the scaffolding alone;
- `contract` against `contract_hollow`: the content;
- `manual_prompt` against `schema_only`: the manual;
- `contract` against `manual_prompt`.

Run:  uv run python analysis/panel_decomposition.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import _as_e2e, _graded, load  # noqa: E402

PANELS = {"gpt-6-sol": "gpt6-panel-r{}", "qwen3.8-27b": "qwen38-panel-r{}"}
REPEATS = (1, 2, 3)
ARMS = ("schema_only", "contract_hollow", "manual_prompt", "contract")
CONTRASTS = (
    ("contract_hollow", "schema_only"),
    ("contract", "contract_hollow"),
    ("manual_prompt", "schema_only"),
    ("contract", "manual_prompt"),
)

Score = dict[str, dict[str, int]]


def scores(repeats: list[list[dict]], level: str | None = None) -> Score:
    """arm -> task -> correct runs over the repeats, optionally one level."""
    out: Score = {}
    for rows in repeats:
        for r in rows:
            if level and r.get("level") != level:
                continue
            out.setdefault(r["arm"], Counter())[str(r["task_id"])] += (
                r["verdict"] == "correct"
            )
    return {arm: dict(c) for arm, c in out.items()}


def _p(up: int, down: int) -> float:
    return binomtest(up, up + down, 0.5).pvalue if up + down else 1.0


def sign(score: Score, a: str, b: str) -> tuple[int, int, float]:
    """Tasks where `a` has more correct runs than `b`, fewer, and the
    two-sided p; a task one arm lacks counts as zero correct there."""
    tasks = set(score.get(a, {})) | set(score.get(b, {}))
    up = sum(score.get(a, {}).get(t, 0) > score.get(b, {}).get(t, 0) for t in tasks)
    down = sum(score.get(a, {}).get(t, 0) < score.get(b, {}).get(t, 0) for t in tasks)
    return up, down, _p(up, down)


def mcnemar(rows: list[dict], a: str, b: str) -> tuple[int, int, float]:
    """One repeat, paired by task: tasks only `a` got right, only `b`, and
    the exact two-sided p."""
    right = {
        (str(r["task_id"]), r["arm"]): r["verdict"] == "correct"
        for r in rows
        if r["arm"] in (a, b)
    }
    tasks = {t for t, _ in right}
    a_only = sum(
        right.get((t, a), False) and not right.get((t, b), False) for t in tasks
    )
    b_only = sum(
        right.get((t, b), False) and not right.get((t, a), False) for t in tasks
    )
    return a_only, b_only, _p(a_only, b_only)


def accuracy(
    score: Score, arm: str, *, repeats: int, tasks: set | None = None
) -> float:
    per = score.get(arm, {})
    tasks = set(per) if tasks is None else tasks
    return sum(per.get(t, 0) for t in tasks) / (repeats * len(tasks))


def _repeats(pattern: str, e2e: bool) -> list[list[dict]]:
    out = []
    for i in REPEATS:
        rows = _graded(load(ROOT / "results" / f"{pattern.format(i)}.jsonl"))
        out.append(_as_e2e(rows) if e2e else rows)
    return out


def main() -> None:
    for model, pattern in PANELS.items():
        for view, e2e in (("official", False), ("end-to-end", True)):
            repeats = _repeats(pattern, e2e)
            print(f"== {model}, {view}")
            for level in (None, "hard"):
                score = scores(repeats, level)
                tasks = set().union(*(score[a] for a in ARMS))
                label = level or "all"
                cells = "  ".join(
                    f"{a} {accuracy(score, a, repeats=3, tasks=tasks):.1%}"
                    for a in ARMS
                )
                print(f"   {label:4s} ({len(tasks)} tasks)  {cells}")
                for a, b in CONTRASTS:
                    up, down, p = sign(score, a, b)
                    gap = accuracy(score, a, repeats=3, tasks=tasks) - accuracy(
                        score, b, repeats=3, tasks=tasks
                    )
                    print(
                        f"      {a} vs {b}: {gap:+.1%}, tasks {up} / {down}, "
                        f"p = {p:.2g}"
                    )
            for a, b in CONTRASTS[:2]:
                per = [mcnemar(rows, a, b) for rows in repeats]
                cells = ", ".join(
                    f"r{i} {x} / {y} p = {p:.2g}" for i, (x, y, p) in zip(REPEATS, per)
                )
                x, y, p = mcnemar_pooled(per)
                print(f"   McNemar {a} vs {b}: {cells}; pooled {x} / {y} p = {p:.2g}")


def mcnemar_pooled(per: list[tuple[int, int, float]]) -> tuple[int, int, float]:
    a_only = sum(x for x, _, _ in per)
    b_only = sum(y for _, y, _ in per)
    return a_only, b_only, _p(a_only, b_only)


if __name__ == "__main__":
    main()
