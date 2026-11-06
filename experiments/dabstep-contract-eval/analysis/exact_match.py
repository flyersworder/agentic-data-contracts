"""The form and route effects under exact-match scoring.

Paper v3's small effects (the spec's storyline item 5) rest on the declared
scoring views:
- *official*: DABStep's vendored scorer on the whole final message;
- *end-to-end*: official plus final-paragraph recovery
  (`dce.grade.score_final_paragraph`), which credits an answer that states
  the gold as its last paragraph after its working.

The scorer tolerates format differences, and a round-3 reviewer found that
luna's form effect on the rule-matching tasks, +3.7 points, falls to about
+1.8 when an answer must match the gold exactly. This script reports that
sensitivity for every small effect, beside the declared views. Exact match
is `analysis/leniency.py`'s: strip whitespace and Markdown bold, then compare
the text to the gold.

Two exact variants, so that leniency is not mixed with final-paragraph
recovery (the reviewer's point for Qwen):
- *exact, full message*: officially correct AND the whole answer matches.
  It drops final-paragraph recovery as well as the tolerance.
- *exact, final paragraph*: correct end-to-end AND the text that was graded
  matches: the whole answer where the official verdict credits it, the
  recovered last paragraph where only the end-to-end view does.

The contrasts, each co-run within its experiment, k=3:
- form (5c): the contract's text against prose, both with the two notes;
- route (5): the governed contract against its text, original wording;
- route, notes in both (5d): the same, with the notes in both prompts.

Subsets: the rule-matching tasks (both blind raters), the rest, all 401.
Each line is a task-level two-sided sign test; the rule-matching line adds
the interval clustered over question shapes, the paper's primary clustering.

This is a sensitivity analysis, not a registered test. Prints aggregates
only.

Run:  uv run python analysis/exact_match.py [--model luna|qwen]
"""

from __future__ import annotations

import sys
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.grade import final_paragraph  # noqa: E402
from dce.stats import _as_e2e, _graded, load  # noqa: E402
from leniency import strip  # noqa: E402


def _official(row: dict) -> bool:
    return row.get("verdict") == "correct"


def _end_to_end(row: dict) -> bool:
    return _as_e2e([row])[0].get("verdict") == "correct"


def exact_full(row: dict) -> bool:
    """Officially correct, and the whole answer is the gold."""
    return _official(row) and strip(row.get("answer")) == strip(row.get("gold"))


def exact_paragraph(row: dict) -> bool:
    """Correct end-to-end, and the text that was graded is the gold."""
    if _official(row):
        return exact_full(row)
    if not _end_to_end(row):
        return False
    return strip(final_paragraph(row.get("answer"))) == strip(row.get("gold"))


VIEWS: dict[str, Callable[[dict], bool]] = {
    "official": _official,
    "end-to-end": _end_to_end,
    "exact, full message": exact_full,
    "exact, final paragraph": exact_paragraph,
}

#: name -> (file pattern, arm a, arm b); each contrast reads a against b.
CONTRASTS: dict[str, tuple[str, str, str]] = {
    "form (5c)": ("exp5c-{m}-r{i}.jsonl", "contract_text_noted", "manual_summed"),
    "route (5)": ("exp5-{m}-r{i}.jsonl", "contract", "contract_text"),
    "route, notes in both (5d)": (
        "exp5d-{m}-r{i}.jsonl",
        "contract_noted",
        "contract_text_noted",
    ),
}

REPEATS = (1, 2, 3)


def scores_from(
    repeats: Iterable[list[dict]],
    arms: tuple[str, ...],
    correct: Callable[[dict], bool],
) -> dict[str, Counter[str]]:
    """Per arm, per task: how many repeats `correct` credits."""
    score: dict[str, Counter[str]] = {a: Counter() for a in arms}
    for rows in repeats:
        for r in rows:
            if r["arm"] in score:
                score[r["arm"]][str(r["task_id"])] += bool(correct(r))
    return score


def _acc(score: Counter[str], among: set[str]) -> float:
    return sum(score[t] for t in among) / (len(REPEATS) * len(among))


def main() -> None:
    import argparse

    import procedural_load as pl
    from clustered import contrast, dabstep_clusterings
    from contract_text import _sign, rule_matching

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=("luna", "qwen"), default="luna")
    model = parser.parse_args().model
    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    shapes = {"shape": dabstep_clusterings()["shape"]}

    for name, (pattern, a, b) in CONTRASTS.items():
        repeats = [
            _graded(load(ROOT / "results" / pattern.format(m=model, i=i)))
            for i in REPEATS
        ]
        tasks = {str(r["task_id"]) for r in repeats[0] if r["arm"] == a}
        rm = rule_matching(first, second) & tasks
        subsets = (("rule-matching", rm), ("rest", tasks - rm), ("all", tasks))
        print(f"== {model}: {name}, {a} against {b}")
        for view, correct in VIEWS.items():
            score = scores_from(repeats, (a, b), correct)
            print(f"  {view}")
            for label, among in subsets:
                gap = _acc(score[a], among) - _acc(score[b], among)
                up, down, p = _sign(score, a, b, among, "two-sided")
                print(
                    f"    {label:14s} {len(among):4d} tasks  "
                    f"{_acc(score[a], among):.1%} vs {_acc(score[b], among):.1%}, "
                    f"{100 * gap:+.1f} pts, {up} / {down}, p = {p:.2g}"
                )
            r = contrast(score[a], score[b], rm, runs=len(REPEATS), clusterings=shapes)
            lo, hi = r["clusters"]["shape"]["ci"]
            print(
                f"    rule-matching, by shape ({r['clusters']['shape']['n']} "
                f"clusters): [{100 * lo:+.1f}, {100 * hi:+.1f}]"
            )


if __name__ == "__main__":
    main()
