"""Experiment 5c: with both facts stated as plainly in the contract's text as
in the prose, does the prose still beat it?

Reproduces FINDINGS.md, *The facts stated alike: predictions (30)-(31Q)*.
Experiment 5b found that manual_summed (the manual plus two notes: the
empty-list wildcard, DATA_NOTE, and how overlapping rules combine, SUM_NOTE)
beat contract_text on qwen3.8-27b. The contract carries both facts, but less
plainly: the empty-list clause sits in semantic.yml far from the matching
rule, and total_transaction_fees speaks of "the fee rule that matches" a
transaction. contract_text_noted is contract_text plus the same two notes,
word for word, under the same headings. k=3, the 401 golded DABStep tasks;
manual_summed, contract_text and contract_text_noted run together in each
repeat, on gpt-6-luna and on qwen3.8-27b.

Primary subset and measure as experiment 5: the rule-matching tasks (both
blind raters), end-to-end strict, task-level sign tests over three repeats.

  (30)  gpt-6-luna and (30Q) qwen3.8-27b: contract_text_noted and
        manual_summed tie there: within 5 points and two-sided p >= 0.10.
  (31Q) qwen3.8-27b: contract_text_noted beats contract_text there,
        one-sided p < 0.05.

Also reported: the official verdicts beside end-to-end ones, the rest and all
401, and a drift check -- each rerun arm against its earlier files, by task
and repeat, flagged when the two differ by more than 5 points.

Prints aggregates only.

Run:  uv run python analysis/contract_noted.py [--model luna|qwen]
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from contract_text import _sign, is_tie, rule_matching  # noqa: E402
from dce.stats import _as_e2e, _graded, load  # noqa: E402

FILES = {"luna": "exp5c-luna-r{}.jsonl", "qwen": "exp5c-qwen-r{}.jsonl"}
ARMS = ("manual_summed", "contract_text", "contract_text_noted")
#: Where each rerun arm ran before, for the drift check.
EARLIER = {
    "luna": {
        "manual_summed": "exp5b-luna-r{}.jsonl",
        "contract_text": "exp5-luna-r{}.jsonl",
    },
    "qwen": {
        "manual_summed": "exp5-qwen-r{}.jsonl",
        "contract_text": "exp5-qwen-r{}.jsonl",
    },
}
#: model -> (label of the tie test, label of the notes-repair test or None).
LABELS = {"luna": ("(30)", None), "qwen": ("(30Q)", "(31Q)")}
DRIFT_POINTS = 0.05


def drifted(gap: float) -> bool:
    return abs(gap) > DRIFT_POINTS


def scores(
    results: Path, pattern: str, arms: tuple[str, ...], e2e: bool = True
) -> dict[str, Counter[str]]:
    """Per arm, per task: how many of the three repeats were correct."""
    score: dict[str, Counter[str]] = {a: Counter() for a in arms}
    for i in (1, 2, 3):
        rows = _graded(load(results / pattern.format(i)))
        for r in _as_e2e(rows) if e2e else rows:
            if r["arm"] in score:
                score[r["arm"]][str(r["task_id"])] += r["verdict"] == "correct"
    return score


def _acc(score: Counter[str], among: set[str]) -> float:
    return sum(score[t] for t in among) / (3 * len(among))


def main() -> None:
    import argparse

    import procedural_load as pl

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=sorted(FILES), default="luna")
    model = parser.parse_args().model
    results = ROOT / "results"
    first = pl.read_ratings(pl.RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.RATINGS, "r2")["dabstep"]
    views = {
        "end-to-end": scores(results, FILES[model], ARMS),
        "official": scores(results, FILES[model], ARMS, e2e=False),
    }
    tasks = set(views["end-to-end"]["contract_text_noted"])
    rm = rule_matching(first, second) & tasks
    subsets = (("rule-matching", rm), ("rest", tasks - rm), ("all", tasks))
    for view, score in views.items():
        print(f"== {view}")
        for label, among in subsets:
            print(
                f"{label:14s} {len(among):4d} tasks  "
                + "  ".join(f"{a} {_acc(score[a], among):.1%}" for a in ARMS)
            )
            for a, b in (
                ("contract_text_noted", "manual_summed"),
                ("contract_text_noted", "contract_text"),
                ("contract_text", "manual_summed"),
            ):
                up, down, p = _sign(score, a, b, among, "two-sided")
                print(f"    {a} vs {b}: {up} / {down}, two-sided p = {p:.2g}")

    print("== drift: each rerun arm against its earlier files (end-to-end)")
    score = views["end-to-end"]
    for arm, pattern in EARLIER[model].items():
        before = scores(results, pattern, (arm,))[arm]
        for label, among in subsets:
            gap = _acc(score[arm], among) - _acc(before, among)
            up = sum(score[arm][t] > before[t] for t in among)
            down = sum(score[arm][t] < before[t] for t in among)
            flag = "  DRIFT" if drifted(gap) else ""
            print(
                f"   {arm} {label}: now {_acc(score[arm], among):.1%}, "
                f"before {_acc(before, among):.1%}, {gap:+.1%} ({up} / {down}){flag}"
            )

    tie_label, repair_label = LABELS[model]
    up, down, p = _sign(score, "contract_text_noted", "manual_summed", rm, "two-sided")
    gap = _acc(score["contract_text_noted"], rm) - _acc(score["manual_summed"], rm)
    print(
        f"{tie_label} {'holds' if is_tie(gap, p) else 'fails'}: gap {gap:+.1%}, "
        f"{up} / {down}, two-sided p = {p:.2g}"
    )
    if repair_label:
        up, down, p = _sign(
            score, "contract_text_noted", "contract_text", rm, "greater"
        )
        gap = _acc(score["contract_text_noted"], rm) - _acc(score["contract_text"], rm)
        print(
            f"{repair_label} {'holds' if p < 0.05 else 'fails'}: gap {gap:+.1%}, "
            f"{up} / {down}, one-sided p = {p:.2g}"
        )


if __name__ == "__main__":
    main()
