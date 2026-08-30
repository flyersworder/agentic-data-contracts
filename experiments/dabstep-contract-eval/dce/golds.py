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
from collections import Counter, defaultdict
from typing import NamedTuple

_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")

#: A clear supermajority. Deliberately well below the ~0.95 median share
#: observed in the corpus, so the choice is not tuned to the data. Must be
#: strictly above 0.5: at exactly 0.5 a two-way tie would be "accepted" and
#: the winner decided by dict insertion order, which is not a consensus.
PLURALITY_THRESHOLD = 0.75

#: The dev tasks that DABStep's submission files actually cover. A gate that
#: verified fewer than this checked less than it was designed to and must not
#: be reported as a pass — see `check_dev_gate`.
MIN_DEV_CHECKS = 6


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

    **Do not add a thousands-separator guard here.** `"1,234.56"` deliberately
    shreds to `"1.000000, 234.560000"`. That looks like a bug and is not: in
    this dataset the comma-joined form is overwhelmingly a merchant-id list,
    and `"709,741,454"` — three ids — matches every thousands-separator pattern
    you would reach for, so such a guard silently collapses real lists into the
    single number 709741454. A genuinely thousands-formatted answer still
    normalizes *consistently* across submissions, so agreement is unaffected;
    a shredded id list is not. This exact "fix" was written and reverted once
    already, caught only by `test_list_answers_agree_regardless_of_ordering`.
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

    `plurality_threshold` must exceed 0.5, so that an accepted group is always a
    strict majority and never a tie broken by dict ordering.
    """
    if plurality_threshold <= 0.5:
        raise ValueError(
            f"plurality_threshold must be > 0.5 to exclude ties, got "
            f"{plurality_threshold}"
        )

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
        # The *modal* raw rendering, not an arbitrary member of the group.
        # Agreement was established up to `_norm`, but this string is what Task 4
        # grades model output against with a different normalizer, so a bracketed
        # or trailing-junk variant that happens to sort first would be graded as
        # the gold. Task 1217, for one, has 561 submissions writing the clean
        # form and 115 writing '[POS: 88.49, Ecommerce: 97.68, ]'.
        golds[task_id] = Counter(plurality).most_common(1)[0][0]
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

    `ok` additionally requires that at least one check actually ran. Absence of
    mismatches is not evidence when nothing was compared: an empty `dev_tasks`,
    or a corpus that covers none of them (an empty or failed download), would
    otherwise report a clean pass having verified nothing — the worst possible
    outcome for a go/no-go that gates spending.
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
    checked = len(dev_tasks) - len(absent)
    return DevGate(bool(checked) and not mismatches, mismatches, absent)
