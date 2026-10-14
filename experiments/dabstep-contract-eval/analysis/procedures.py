"""Experiment 4: a controlled test of knowledge type and of how it is written.

Reproduces FINDINGS.md, *Knowledge type, controlled: predictions (22)-(25)*.
Three LiveSQLBench databases each carry a rule-matching procedure (R) and a
composite (C), both determined by their text (`prep/procedures/`); ten
matched questions per piece per database, 60 in all. Arms schema_only,
manual_prompt (clean KB), manual_styled (the same knowledge written as a
business manual writes it), manual_compiled and contract (the dce.author
contract, checked against the reference implementation), on gpt-6-luna and
gpt-6-sol, k=3, run together in each repeat.

A task scores 0-6 per arm (two models, three repeats); sign tests are over
tasks; permutation tests use 100,000 relabellings, seed 2026.

  (22) Styled prose, R: contract beats manual_styled, one-sided sign test
       p < 0.05.
  (23) manual_prompt minus manual_styled, per task, is larger on R than on
       C: one-sided permutation p < 0.05.
  (24) contract minus manual_styled, per task, is larger on R than on C:
       one-sided permutation p < 0.05.
  (25) Clean prose, R: contract and manual_prompt tie, within 5 points and
       two-sided sign test p >= 0.10.

Manipulation check, reported with them: manual_prompt beats schema_only on
R and on C (the questions need the knowledge). Secondary: each model alone;
manual_compiled; each database.

`--set qwen` applies the same four tests to qwen3.8-27b alone, k=3 (a task
scores 0-3), as (22Q)-(25Q): added after gpt-6-luna and gpt-6-sol were seen.

Run:  uv run python analysis/procedures.py [--set registered|qwen]
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

from scipy.stats import binomtest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dce.stats import load  # noqa: E402

RESULTS = ROOT / "results" / "procedures"
MODEL_SETS = {
    "registered": {
        "gpt-6-luna": "exp4-luna-r{}.jsonl",
        "gpt-6-sol": "exp4-sol-r{}.jsonl",
    },
    "qwen": {"qwen3.8-27b": "exp4-qwen-r{}.jsonl"},
}
LABEL_SUFFIX = {"registered": "", "qwen": "Q"}
ARMS = (
    "schema_only",
    "manual_prompt",
    "manual_styled",
    "manual_compiled",
    "contract",
)
TIE_POINTS = 0.05
PIECE = re.compile(r"_([RC])\d+$")


def piece_of(task_id: str) -> str:
    m = PIECE.search(task_id)
    if not m:
        raise ValueError("not an experiment 4 task id: " + task_id)
    return m.group(1)


def pool(files: list[list[dict]]) -> dict[str, Counter[str]]:
    """arm -> task -> correct runs, over every file given."""
    score: dict[str, Counter[str]] = {}
    for rows in files:
        for r in rows:
            score.setdefault(r["arm"], Counter())[r["task_id"]] += (
                r["verdict"] == "correct"
            )
    return score


def is_tie(gap: float, p: float) -> bool:
    return abs(gap) <= TIE_POINTS and p >= 0.10


def _sign(score, a: str, b: str, tasks, alternative: str):
    up = sum(score[a][t] > score[b][t] for t in tasks)
    down = sum(score[a][t] < score[b][t] for t in tasks)
    n = up + down
    p = binomtest(up, n, 0.5, alternative=alternative).pvalue if n else 1.0
    return up, down, p


def _acc(score, arm: str, tasks, runs: int) -> float:
    return sum(score[arm][t] for t in tasks) / (runs * len(tasks))


def report(score, runs: int, label: str) -> dict:
    tasks = sorted(score["contract"])
    by = {p: [t for t in tasks if piece_of(t) == p] for p in ("R", "C")}
    print(f"== {label}: {len(tasks)} tasks, {runs} runs each")
    for p, ts in by.items():
        print(
            f"   {p} ({len(ts)}): "
            + "  ".join(f"{a} {_acc(score, a, ts, runs):.1%}" for a in ARMS)
        )
        for a, b in (
            ("manual_prompt", "schema_only"),
            ("contract", "manual_prompt"),
            ("contract", "manual_styled"),
            ("manual_prompt", "manual_styled"),
            ("manual_compiled", "manual_prompt"),
        ):
            up, down, pv = _sign(score, a, b, ts, "two-sided")
            print(f"      {a} vs {b}: {up} / {down}, two-sided p = {pv:.2g}")
    return by


def main() -> None:
    import argparse

    from procedural_load import permutation_p

    parser = argparse.ArgumentParser()
    parser.add_argument("--set", choices=sorted(MODEL_SETS), default="registered")
    which = parser.parse_args().set
    models = MODEL_SETS[which]

    def label(n: int) -> str:
        return f"({n}{LABEL_SUFFIX[which]})"

    files = {
        m: [load(RESULTS / pattern.format(i)) for i in (1, 2, 3)]
        for m, pattern in models.items()
    }
    if len(models) > 1:
        for model, rows in files.items():
            report(pool(rows), 3, model)
    score = pool([r for rows in files.values() for r in rows])
    runs = 3 * len(models)
    by = report(score, runs, " and ".join(models))
    r, c = by["R"], by["C"]

    def per_task(a: str, b: str) -> dict[str, float]:
        return {t: (score[a][t] - score[b][t]) / runs for t in r + c}

    def interaction(label: str, a: str, b: str) -> None:
        gains = per_task(a, b)
        p = permutation_p(gains, r, c)
        diff = sum(gains[t] for t in r) / len(r) - sum(gains[t] for t in c) / len(c)
        verdict = "holds" if p < 0.05 else "fails"
        print(
            f"{label} {verdict}: {a} minus {b}, R minus C {diff:+.3f}, "
            f"one-sided permutation p = {p:.2g}"
        )

    up, down, p22 = _sign(score, "contract", "manual_styled", r, "greater")
    verdict = "holds" if p22 < 0.05 else "fails"
    print(f"{label(22)} {verdict}: {up} / {down}, p = {p22:.2g}")
    interaction(label(23), "manual_prompt", "manual_styled")
    interaction(label(24), "contract", "manual_styled")
    up, down, p25 = _sign(score, "contract", "manual_prompt", r, "two-sided")
    gap = _acc(score, "contract", r, runs) - _acc(score, "manual_prompt", r, runs)
    print(
        f"{label(25)} {'holds' if is_tie(gap, p25) else 'fails'}: gap {gap:+.1%}, "
        f"{up} / {down}, two-sided p = {p25:.2g}"
    )
    for db in sorted({t.rsplit("_", 1)[0] for t in r + c}):
        ts = [t for t in r + c if t.rsplit("_", 1)[0] == db]
        for p in ("R", "C"):
            sub = [t for t in ts if piece_of(t) == p]
            print(
                f"   {db} {p}: "
                + "  ".join(f"{a} {_acc(score, a, sub, runs):.0%}" for a in ARMS)
            )


if __name__ == "__main__":
    main()
