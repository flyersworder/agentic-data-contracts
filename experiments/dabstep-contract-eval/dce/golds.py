"""Reconstruct DABStep gold answers from published leaderboard artifacts.

DABStep withholds golds for the 450 test tasks. Where at least two independently
submitted agents were *scored correct* on a task and their answers agree, that
answer is the gold. One correct submission is not enough: a normalized
comparison can pass by luck.

This yields golds only for tasks some agent already solved, so the scorable
subset is easier than the full benchmark. See the spec's "Selection bias"
section — the bias is disclosed, not corrected.
"""

from __future__ import annotations

import re
from collections import defaultdict

_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")


def _norm(value: str) -> str:
    """Loose normalization for *agreement*, not for scoring."""
    text = _BRACKETS.sub("", str(value).strip()).strip().lower()
    try:
        return f"{float(text):.6f}"
    except ValueError:
        return re.sub(r"\s+", " ", text)


def reconstruct(
    submissions: dict[str, dict[str, str]],
    scores: dict[str, dict[str, bool]],
    min_agreement: int = 2,
) -> tuple[dict[str, str], dict[str, str]]:
    """Return (golds, exclusions). exclusions maps task_id -> reason."""
    by_task: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    seen: set[str] = set()

    for agent, answers in submissions.items():
        for task_id, answer in answers.items():
            seen.add(task_id)
            if scores.get(agent, {}).get(task_id):
                by_task[task_id][_norm(answer)].append(answer)

    golds: dict[str, str] = {}
    exclusions: dict[str, str] = {}

    for task_id in sorted(seen):
        groups = by_task.get(task_id, {})
        if not groups:
            exclusions[task_id] = "no_correct_submission"
            continue
        if len(groups) > 1:
            exclusions[task_id] = "conflicting_golds"
            continue
        (raws,) = groups.values()
        if len(raws) < min_agreement:
            exclusions[task_id] = "insufficient_agreement"
            continue
        golds[task_id] = raws[0]

    return golds, exclusions


def check_dev_gate(
    golds: dict[str, str], dev_tasks: list[dict]
) -> tuple[bool, list[str]]:
    """Reconstruction must reproduce every published dev answer exactly.

    A dev task missing from `golds` fails the gate. Treating absence as a pass
    would make the gate vacuous — which is precisely the unverified assumption
    the spec flags.
    """
    mismatches = [
        task["task_id"]
        for task in dev_tasks
        if _norm(golds.get(task["task_id"], "\x00missing")) != _norm(task["answer"])
    ]
    return (not mismatches), mismatches
