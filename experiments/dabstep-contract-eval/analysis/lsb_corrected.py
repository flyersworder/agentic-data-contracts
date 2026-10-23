"""Experiment 3: the missing knowledge, supplied from training tasks' gold.

Reproduces FINDINGS.md, *Completing the knowledge: predictions (13)-(15)*.
Run commit 482da68: four arms (manual_prompt, manual_corrected,
contract_opus, contract_corrected) on gpt-6-luna, k=3, on experiment 2's
three databases. The corrections were written from the gold of the training
half of the tasks (results/livesqlbench/exp3-split.json); the predictions
are tested on the held-out half.

Prints, for held-out and training tasks: correct runs per arm and repeat,
the declared sign tests, and the held-out tasks split by whether their KB
entries (from the private task-to-KB mapping, with dependencies) meet a
corrected entry.

The result files and the corrections are private (the corrected arms'
answers can restate gold-derived knowledge): `$LSB_EXP3`, default
~/data/dce-held-results/exp3, and `$LSB_PRIVATE`/corrected/corrections.
Prints aggregates only.

Run:  uv run python analysis/lsb_corrected.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import knowledge_reuse as kr  # noqa: E402
from dce.stats import load  # noqa: E402
from stronger_author import sign, task_scores  # noqa: E402

RESULTS = Path(os.environ.get("LSB_EXP3", Path.home() / "data/dce-held-results/exp3"))
PRIVATE = Path(os.environ.get("LSB_PRIVATE", Path.home() / "data/livesqlbench"))
ARMS = ("manual_prompt", "manual_corrected", "contract_opus", "contract_corrected")
#: (arm, against, alternative): predictions (13), (14), (15), then the two
#: original arms against each other, as in experiment 2.
TESTS = (
    ("manual_corrected", "manual_prompt", "greater"),
    ("contract_corrected", "contract_opus", "greater"),
    ("contract_corrected", "manual_corrected", "two-sided"),
    ("contract_opus", "manual_prompt", "two-sided"),
)


def corrected_entries(folder: Path) -> dict[str, set[int]]:
    """db -> the KB entry ids some correction names."""
    out: dict[str, set[int]] = {}
    for path in sorted(folder.glob("*.json")):
        for c in json.loads(path.read_text(encoding="utf-8")):
            out.setdefault(path.stem, set()).update(
                int(e) for e in c.get("entries") or []
            )
    return out


def touched(
    uses: dict[str, tuple[str, set[int]]],
    corrected: dict[str, set[int]],
    tasks,
) -> set[str]:
    return {
        t for t in tasks if t in uses and uses[t][1] & corrected.get(uses[t][0], set())
    }


def _block(score, repeats, tasks: set[str], label: str, *, tests: bool) -> None:
    n = 3 * len(tasks)
    print(f"== {label}: {len(tasks)} tasks")
    for a in ARMS:
        per = [
            sum(
                r["arm"] == a and r["verdict"] == "correct" and r["task_id"] in tasks
                for r in rows
            )
            for rows in repeats
        ]
        print(f"  {a:20s} {per}  {sum(per) / n:.1%}")
    if tests:
        for a, b, alt in TESTS:
            better, worse, p = sign(score, a, b, tasks, alt)
            print(f"  sign {a} vs {b}: {better} / {worse}, p={p:.2g} ({alt})")


def main() -> None:
    repeats = [load(RESULTS / f"exp3-luna-r{i}.jsonl") for i in (1, 2, 3)]
    split = json.loads(
        (ROOT / "results/livesqlbench/exp3-split.json").read_text(encoding="utf-8")
    )
    rows = [r for rows in repeats for r in rows]
    print(
        f"{len(rows)} rows, commit {sorted({r['commit_sha'] for r in rows})}, "
        f"${sum(r['usd'] or 0 for r in rows):.2f}"
    )
    score = task_scores(repeats, "primary")
    primary = set(score["contract_opus"])
    held = primary & set(split["held_out"])
    train = primary & set(split["train"])
    _block(score, repeats, held, "held-out primary (the test)", tests=True)
    _block(score, repeats, train, "training primary (upper bound)", tests=True)
    order = task_scores(repeats, "order_only")
    _block(
        order, repeats, set(order["contract_opus"]), "order-only, held out", tests=False
    )
    uses, _ = kr.lsb_inputs()
    hit = touched(uses, corrected_entries(PRIVATE / "corrected" / "corrections"), held)
    for label, tasks in (("meets a corrected entry", hit), ("does not", held - hit)):
        _block(score, repeats, tasks, f"held-out, {label}", tests=False)
        for a, b, alt in TESTS[:2]:
            better, worse, _ = sign(score, a, b, tasks, alt)
            print(f"  {a} vs {b}: {better} / {worse}")


if __name__ == "__main__":
    main()
