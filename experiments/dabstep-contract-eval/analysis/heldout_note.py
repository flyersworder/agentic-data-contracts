"""The summing note, measured on rule-matching tasks nobody read.

Exploratory; it answers a reviewer's objection to *The second fact,
measured*. The note `SUM_NOTE` was written after reading transcripts of 34
rule-matching tasks: those that `contract_text` solved and `manual_resolved`
missed in both of experiment 5's first two gpt-6-luna repeats
(*The prose lacked a second fact*). Its registered test, (28), was then run
on all 199 rule-matching tasks, those 34 included. This rebuilds the 34 from
the rows and repeats the comparisons on the other 165, which no transcript
reading touched:
- `manual_summed` against `manual_resolved`, the note's own effect, (28)
  and (28Q);
- `contract_text` against `manual_summed`, (29) and (29Q);
- `contract_text_noted` against `manual_summed`, experiment 5c's (30) and
  (30Q).

Sign tests over tasks, each scored 0-3 over the repeats, two-sided, ties
dropped. End-to-end strict, as in the registered analyses. Prints
aggregates only; the ratings that define the rule-matching tasks are
private (`$PROCEDURAL_LOAD`).

Run:  uv run python analysis/heldout_note.py
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import _as_e2e, _graded, load  # noqa: E402

RUNS = {
    "gpt-6-luna": {
        "exp5": ("exp5-luna-r{}.jsonl", "exp5b-luna-r{}.jsonl"),
        "exp5c": ("exp5c-luna-r{}.jsonl",),
    },
    "qwen3.8-27b": {
        "exp5": ("exp5-qwen-r{}.jsonl",),
        "exp5c": ("exp5c-qwen-r{}.jsonl",),
    },
}
CONTRASTS = (
    ("exp5", "manual_summed", "manual_resolved"),
    ("exp5", "contract_text", "manual_summed"),
    ("exp5c", "contract_text_noted", "manual_summed"),
)


def _right(rows: list[dict], arm: str) -> set[str]:
    return {
        str(r["task_id"]) for r in rows if r["arm"] == arm and r["verdict"] == "correct"
    }


def _attempted(rows: list[dict], arm: str) -> set[str]:
    return {str(r["task_id"]) for r in rows if r["arm"] == arm}


def inspected(r1: list[dict], r2: list[dict], *, among: set[str]) -> set[str]:
    """Tasks in `among` that contract_text solved and manual_resolved missed,
    in both repeats."""
    out = set(among)
    for rows in (r1, r2):
        won = _right(rows, "contract_text")
        lost = _attempted(rows, "manual_resolved") - _right(rows, "manual_resolved")
        out &= won & lost
    return out


def _rows(patterns: tuple[str, ...], i: int) -> list[dict]:
    rows: list[dict] = []
    for pattern in patterns:
        rows += load(ROOT / "results" / pattern.format(i))
    return _as_e2e(_graded(rows))


def _score(patterns: tuple[str, ...]) -> dict[str, Counter]:
    score: dict[str, Counter] = {}
    for i in (1, 2, 3):
        for r in _rows(patterns, i):
            score.setdefault(r["arm"], Counter())[str(r["task_id"])] += (
                r["verdict"] == "correct"
            )
    return score


def main() -> None:
    import procedural_load as pl
    from contract_text import _sign, rule_matching

    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    luna5 = RUNS["gpt-6-luna"]["exp5"]
    rm = rule_matching(first, second) & {
        str(r["task_id"]) for r in _rows(luna5, 1) if r["arm"] == "contract"
    }
    seen = inspected(_rows(luna5, 1), _rows(luna5, 2), among=rm)
    unseen = rm - seen
    print(
        f"== rule-matching {len(rm)} tasks: {len(seen)} inspected, "
        f"{len(unseen)} never read"
    )
    for model, runs in RUNS.items():
        print(f"== {model}")
        scores = {name: _score(patterns) for name, patterns in runs.items()}
        for name, a, b in CONTRASTS:
            score = scores[name]
            for label, among in (("never read", unseen), ("inspected", seen)):
                acc = {
                    x: sum(score[x][t] for t in among) / (3 * len(among))
                    for x in (a, b)
                }
                up, down, p = _sign(score, a, b, among, "two-sided")
                print(
                    f"   {name:5s} {a} vs {b}, {label:10s} ({len(among)}): "
                    f"{acc[a]:.1%} vs {acc[b]:.1%}, {acc[a] - acc[b]:+.1%}, "
                    f"tasks {up} / {down}, p = {p:.2g}"
                )


if __name__ == "__main__":
    main()
