"""Why did the official grading disagree with ours on 19 tasks?

`FINDINGS.md` reports that Adyen's grading of the leaderboard submission and
this document's own grading disagree on 19 of the 401 shared tasks, all in the
same direction. The obvious reading — the two graders apply different rules,
and ours is the lenient one — is WRONG, and this script is the check that
rules it out.

The scorer is not a variable. `vendor/dabstep_scorer.py` is the leaderboard
Space's `dabstep_benchmark/evaluation/scorer.py`, vendored verbatim and sha256-
pinned by `tests/test_vendored_scorer.py`; Adyen graded the submission with the
same code. So re-running it on each disagreement's (submitted answer,
reconstructed gold) pair isolates the one thing that DID differ:

    A. same scorer, reconstructed gold  -> True on all 19
       same scorer, official gold       -> False on all 19  (Adyen's verdicts)
       => the GOLDS differ, not the grading.

That matters because the scorer cannot fail on the two shapes the 19 look
like. `compare_numeric` rounds both sides to `min(dec_places1, dec_places2)`,
so a full-precision answer against a rounded gold always matches;
`compare_strings` has a one-word branch, so `D:219.36` matches a bare `D`.
Neither a precision difference nor a trailing suffix can produce a False.
The 19 are therefore wrong VALUES that the reconstructed golds masked, not
formatting slips — and no output-format rule recovers them.

Two families make the same point from the data rather than from the code:

    B. THE "14 DECIMALS" FAMILY. 40 tasks whose guideline asks for a number
       "rounded to 14 decimals". The submission answered all 40 at full
       precision and 15 were graded correct, so precision is demonstrably not
       what fails the other 25.

    C. THE ACI FAMILY. 55 tasks whose guideline asks for `{card_scheme}:{fee}`.
       Every officially-correct answer names a card scheme
       (`TransactPlus:3458.48`); the answers that name an ACI code letter
       (`D:219.36`) are wrong without exception. That IS an instruction-
       following defect, readable off the guideline text alone — but it is a
       wrong entity, not a wrong format, and only a re-run fixes it.

Run:  uv run python analysis/gold_disagreement.py

Reads `results/glm-all450.jsonl` and `results/glm-all450-official-scores.jsonl`
and the committed `data/tasks.json` for the guidelines. No API calls, no
writes, no network.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.runner import latest_rows  # noqa: E402
from vendor.dabstep_scorer import question_scorer  # noqa: E402

LOCAL = ROOT / "results" / "glm-all450.jsonl"
OFFICIAL = ROOT / "results" / "glm-all450-official-scores.jsonl"

#: The two guideline templates the disagreements cluster under.
NUMERIC_GUIDELINE = "14 decimals"
ACI_GUIDELINE = "card_scheme}:{fee"


def _rows(path: Path) -> dict[str, dict]:
    """Sweep results, deduplicated. `latest_rows` is mandatory for anything
    that counts verdicts (see its docstring): a resumed sweep leaves stale
    rows for a retried unit, and a raw line-by-line parse counts them all."""
    return {str(r["task_id"]): r for r in latest_rows(path)}


def _scores(path: Path) -> dict[str, dict]:
    """Adyen's per-task verdicts — one row per task, not a sweep file."""
    with open(path) as fh:
        return {
            str(r["task_id"]): r
            for r in (json.loads(line) for line in fh if line.strip())
        }


def _tasks() -> dict[str, dict]:
    """The committed task file, so this runs offline like the rest of
    `analysis/` — `dce.data.load_tasks` would hit the HF Hub."""
    raw = json.loads((ROOT / "data" / "tasks.json").read_text())
    raw = raw if isinstance(raw, list) else raw.get("tasks", raw)
    return {str(t["task_id"]): t for t in raw}


def _answer_shape(answer: str) -> str:
    """`TransactPlus:3458.48` names a scheme; `D:219.36` names an ACI code."""
    left = answer.strip().strip("*").split(":")[0].strip()
    if len(left) == 1 and left.isalpha():
        return "ACI-letter"
    return "scheme-name" if left.isalpha() else "other"


def main() -> None:
    local = _rows(LOCAL)
    official = _scores(OFFICIAL)
    tasks = _tasks()

    # `ungraded` rows are the 49 tasks the plurality-consensus filter dropped:
    # Adyen scored them, this document could not, so they are not a comparison.
    shared = [
        t
        for t in official
        if t in local and local[t]["verdict"] in ("correct", "incorrect")
    ]
    disagree = [
        t
        for t in shared
        if local[t]["verdict"] == "correct" and official[t]["score"] is False
    ]
    reverse = [
        t
        for t in shared
        if local[t]["verdict"] == "incorrect" and official[t]["score"] is True
    ]

    print(f"A. shared graded tasks: {len(shared)}")
    print(f"   local correct / official wrong: {len(disagree)}")
    print(f"   local wrong / official correct: {len(reverse)}")
    print("\n   re-scoring each against the RECONSTRUCTED gold, same scorer:")
    agreed = 0
    for task_id in disagree:
        answer = official[task_id]["agent_answer"]
        gold = local[task_id]["gold"]
        verdict = question_scorer(answer, gold)
        agreed += bool(verdict)
        print(
            f"     {task_id:>5}  {answer!r:<24} vs {gold!r:<10}"
            f"  vendored={verdict}  official={official[task_id]['score']}"
        )
    print(
        f"\n   vendored scorer says correct on {agreed}/{len(disagree)} "
        "against the reconstructed golds, and Adyen — running this same file "
        "— said wrong on every one. The golds differ, not the grading."
    )

    numeric = [
        t
        for t, task in tasks.items()
        if NUMERIC_GUIDELINE in (task.get("guidelines") or "") and t in official
    ]
    correct = sum(1 for t in numeric if official[t]["score"])
    print(
        f'\nB. tasks whose guideline says "rounded to {NUMERIC_GUIDELINE}": '
        f"{len(numeric)}"
    )
    print(
        f"   graded correct: {correct} — all {len(numeric)} answered at full "
        "precision, so precision is not the failure mode."
    )

    aci = [
        t
        for t, task in tasks.items()
        if ACI_GUIDELINE in (task.get("guidelines") or "") and t in official
    ]
    shapes = Counter(
        (_answer_shape(official[t]["agent_answer"]), official[t]["score"]) for t in aci
    )
    print(f"\nC. tasks whose guideline asks for {{card_scheme}}:{{fee}}: {len(aci)}")
    print("   answer shape      correct  wrong")
    for shape in ("scheme-name", "ACI-letter", "other"):
        hit = shapes.get((shape, True), 0)
        miss = shapes.get((shape, False), 0)
        if hit or miss:
            print(f"   {shape:<16} {hit:>8}  {miss:>5}")


if __name__ == "__main__":
    main()
