"""Reconstruct DABStep gold answers from published leaderboard artifacts.

DABStep withholds golds for the 450 test tasks. Where independently submitted
agents were *scored correct* on a task and a supermajority of them give the same
answer, that answer is the gold. One correct submission is never enough: a
normalized comparison can pass by luck.

Consensus is by **plurality with a support threshold**, not unanimity. The
leaderboard corpus holds 2198 submissions and up to ~1600 correct answers on a
single task, so requiring every correct answer to agree lets one noisy outlier
(a pasted reasoning trace, a trailing period, `0.1232` for `0.123217`) destroy a
task that ~99% of solvers agree on. Measured on the real corpus, unanimity
recovers 37 of 450 tasks; the plurality rule recovers the tasks themselves.

Golds exist only for tasks whose correct answer a supermajority of solvers
renders identically. See the spec's "Selection bias" section — the bias is
disclosed, not corrected.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import NamedTuple

_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")

#: A clear supermajority. Deliberately well below the ~0.95 median share
#: observed in the corpus, so the choice is not tuned to the data.
PLURALITY_THRESHOLD = 0.75


def _norm_atom(value: str) -> str:
    text = _BRACKETS.sub("", str(value).strip()).strip().lower()
    try:
        return f"{float(text):.6f}"
    except ValueError:
        return re.sub(r"\s+", " ", text)


def _norm(value: str) -> str:
    """Loose normalization for *agreement*, not for scoring.

    Comma-separated lists are compared as sets: DABStep's own scorer is
    order-insensitive for lists, so two correct submissions that differ only in
    ordering are the same answer, and treating them as a conflict would be a
    normalizer bug rather than a real disagreement.
    """
    text = _BRACKETS.sub("", str(value).strip()).strip().lower()
    if "," in text:
        parts = [_norm_atom(part) for part in text.split(",")]
        parts = [part for part in parts if part]
        if len(parts) > 1:
            return ", ".join(sorted(parts))
    return _norm_atom(text)


class Reconstruction(NamedTuple):
    """(golds, exclusions, shares) — shares covers accepted golds only."""

    golds: dict[str, str]
    exclusions: dict[str, str]
    shares: dict[str, float]


def reconstruct_with_shares(
    submissions: dict[str, dict[str, str]],
    scores: dict[str, dict[str, bool]],
    min_agreement: int = 2,
    plurality_threshold: float = PLURALITY_THRESHOLD,
) -> Reconstruction:
    """Reconstruct golds, recording each gold's plurality share.

    `exclusions` maps task_id -> one of `no_correct_submission`,
    `insufficient_agreement`, `below_plurality_threshold`.
    """
    by_task: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    seen: set[str] = set()

    for agent, answers in submissions.items():
        for task_id, answer in answers.items():
            seen.add(task_id)
            if scores.get(agent, {}).get(task_id):
                by_task[task_id][_norm(answer)].append(answer)

    golds: dict[str, str] = {}
    exclusions: dict[str, str] = {}
    shares: dict[str, float] = {}

    for task_id in sorted(seen):
        groups = by_task.get(task_id, {})
        if not groups:
            exclusions[task_id] = "no_correct_submission"
            continue
        plurality = max(groups.values(), key=len)
        if len(plurality) < min_agreement:
            exclusions[task_id] = "insufficient_agreement"
            continue
        share = len(plurality) / sum(len(raws) for raws in groups.values())
        if share < plurality_threshold:
            exclusions[task_id] = "below_plurality_threshold"
            continue
        golds[task_id] = plurality[0]
        shares[task_id] = share

    return Reconstruction(golds, exclusions, shares)


def reconstruct(
    submissions: dict[str, dict[str, str]],
    scores: dict[str, dict[str, bool]],
    min_agreement: int = 2,
    plurality_threshold: float = PLURALITY_THRESHOLD,
) -> tuple[dict[str, str], dict[str, str]]:
    """Return (golds, exclusions). exclusions maps task_id -> reason."""
    result = reconstruct_with_shares(
        submissions, scores, min_agreement, plurality_threshold
    )
    return result.golds, result.exclusions


class DevGate(NamedTuple):
    """(ok, mismatches, absent). `ok` speaks only for the checks we could run."""

    ok: bool
    mismatches: list[str]
    absent: list[str]


def check_dev_gate(
    golds: dict[str, str],
    dev_tasks: list[dict],
    corpus_ids: set[str] | None = None,
) -> DevGate:
    """Reconstruction must reproduce every *checkable* published dev answer.

    `dev.jsonl` and `all.jsonl` overlap but are not nested: four of DABStep's
    ten dev tasks appear in no submission file, so no consensus rule can
    reconstruct them. Pass `corpus_ids` — the task ids the submissions actually
    cover — to partition those out as `absent`. They are unverifiable, not
    wrong, and the caller must report them rather than count them as passes: a
    gate blind to 4 of its 10 checks is weaker and has to read that way.

    Without `corpus_ids` nothing is excused: a dev task missing from `golds` is
    a mismatch, so the gate can never pass vacuously.
    """
    known = (
        corpus_ids
        if corpus_ids is not None
        else {str(task["task_id"]) for task in dev_tasks}
    )
    absent = [str(t["task_id"]) for t in dev_tasks if str(t["task_id"]) not in known]
    mismatches = [
        str(task["task_id"])
        for task in dev_tasks
        if str(task["task_id"]) in known
        and _norm(golds.get(str(task["task_id"]), "\x00missing"))
        != _norm(task["answer"])
    ]
    return DevGate(not mismatches, mismatches, absent)
