"""Build the DABStep leaderboard submission file from results rows.

A submission is effectively ONE-SHOT per agent name, and the leaderboard
Space grades whatever it is handed: a file quietly missing 12 of the 450
tasks scores as 12 wrong answers, with no error raised anywhere and no way
to take it back. So every check in this module is about REFUSING to write a
file rather than about writing a good one -- the failure this guards against
is silent, public and unrecoverable.

Why an exporter exists at all: the reason to submit is not the ranking (see
`docs/paper-plan.md`). It is that every submission publishes a per-task
verdict from DABStep's own withheld golds, which turns "our reconstructed
golds are probably right" -- the one caveat in FINDINGS a reader cannot
check for themselves -- into a measured agreement rate.

Row selection goes through `latest_rows`, so a retried unit contributes its
final state rather than a stale `error` row's empty answer. Reading the file
line by line instead would submit that empty string and score zero on a task
the sweep actually got right.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dce.runner import latest_rows
from dce.stats import ANSWER_VERDICTS, UNGRADED_VERDICTS

#: Verdicts carrying an answer the model actually produced.
#:
#: `ungraded` is included deliberately: it means no gold existed LOCALLY to
#: score against, which is precisely the case the leaderboard's withheld
#: golds resolve.
#:
#: `scoring_error` too, for a sharper reason. `dce.agent` sets it only when a
#: real answer came back and OUR scorer raised comparing it to the gold --
#: the answer is intact (a test asserts the scorer failure never overwrites
#: it), and whether it is right is the leaderboard's call, not ours.
#: Excluding it would also be a dead end: `--retry` accepts only `error` and
#: `post_run_error`, and `completed_keys` treats `scoring_error` as done, so
#: no resumed sweep would re-run the task and the file could not be built
#: without hand-editing results JSONL.
#:
#: The rest of `dce.stats.HARNESS_VERDICTS` stays out: `hit_limit`, `error`,
#: `post_run_error` and `construction_error` carry an empty answer or an
#: exception message, never a reply. The `answer.strip()` gate below is the
#: backstop if that ever stops being true.
SUBMITTABLE_VERDICTS: frozenset[str] = (
    ANSWER_VERDICTS | UNGRADED_VERDICTS | {"scoring_error"}
)


class SubmissionError(Exception):
    """The rows cannot produce a complete, submittable file."""


#: Provenance fields stamped on every result row. A splice that disagrees on
#: `contract_digest` is two different agents and is refused outright: the
#: contract IS the treatment, so a mid-splice edit means the per-task verdicts
#: the submission buys no longer describe the arm the paper reports.
#:
#: `commit_sha` and `scorer` are reported, not refused. A splice spans two
#: commits BY CONSTRUCTION -- the 49 ungolded tasks can only run once the code
#: admitting them is committed -- so refusing on it would ban the documented
#: flow.
FATAL_PROVENANCE: tuple[str, ...] = ("contract_digest",)
REPORTED_PROVENANCE: tuple[str, ...] = ("commit_sha", "scorer", "golds_hash")


def _select(results: list[Path], *, arm: str, model: str) -> dict[str, dict]:
    """task_id -> the row to submit, across every results file.

    Later files win on a shared key, matching `latest_rows`' within-file
    rule. One function so `provenance` and `build_submission` can never
    disagree about which rows are in play.
    """
    by_task: dict[str, dict] = {}
    for path in results:
        # `latest_rows` -> `_read_rows` returns [] for a path that does not
        # exist, silently. Right for the runner (a fresh `--out`), wrong
        # here: a typo'd override would contribute nothing, an earlier file
        # would already cover all 450, no guard would fire, and stale answers
        # would ship on a one-shot submission.
        if not path.exists():
            raise SubmissionError(
                f"--results {path} does not exist. A missing input reads as "
                "an empty one, so the submission would quietly be built from "
                "whatever the other files hold."
            )
        for row in latest_rows(path):
            if row.get("arm") == arm and row.get("model") == model:
                by_task[row["task_id"]] = row
    return by_task


#: Stands in for a provenance field a row does not carry, so that "some rows
#: predate this field" reads as a DISAGREEMENT rather than as agreement. For a
#: fatal field, absent is the unknown-provenance case the check exists to
#: refuse.
ABSENT = "<absent>"


def _distinct(rows, field: str, *, absent_counts: bool = False) -> list[str]:
    if absent_counts:
        return sorted({str(row.get(field, ABSENT) or ABSENT) for row in rows})
    return sorted({str(row[field]) for row in rows if row.get(field) is not None})


def _assert_one_agent(rows) -> None:
    """Refuse a submission assembled from two different contracts."""
    rows = list(rows)
    for field in FATAL_PROVENANCE:
        values = _distinct(rows, field, absent_counts=True)
        if len(values) > 1:
            raise SubmissionError(
                f"the selected rows disagree on {field}: {', '.join(values)}. "
                "That is two different agents in one submission -- the "
                "per-task verdicts it buys would not describe either arm. "
                "Re-run the whole task set against one contract."
            )


def provenance(results: list[Path], *, arm: str, model: str) -> dict[str, list[str]]:
    """Provenance fields the selected rows DISAGREE on, field -> values.

    Empty when the rows are homogeneous. A divergence here is allowed but
    must be visible: the operator is about to spend a one-shot, unwithdrawable
    submission on it.
    """
    rows = list(_select(results, arm=arm, model=model).values())
    diverged: dict[str, list[str]] = {}
    for field in REPORTED_PROVENANCE:
        values = _distinct(rows, field)
        if len(values) > 1:
            diverged[field] = values
    return diverged


def build_submission(
    results: list[Path], *, arm: str, model: str, tasks: list[dict]
) -> list[dict]:
    """`{"task_id", "agent_answer"}` for every task in `tasks`, in `tasks`
    order, taken from `arm`/`model` rows across `results`.

    Several results files are accepted so the golded tasks from one sweep
    and the ungolded ones from a later run can be spliced. Later files win
    on a shared key, matching `latest_rows`' within-file rule.

    Raises `SubmissionError`, naming the offending task ids, if any task has
    no row, a row that is a harness failure rather than an answer, or a row
    whose answer is blank.
    """
    by_task = _select(results, arm=arm, model=model)
    _assert_one_agent(by_task.values())

    out: list[dict] = []
    missing: list[str] = []
    unusable: list[str] = []
    blank: list[str] = []
    for task in tasks:
        task_id = task["task_id"]
        row = by_task.get(task_id)
        if row is None:
            missing.append(task_id)
            continue
        verdict = row.get("verdict")
        if verdict not in SUBMITTABLE_VERDICTS:
            unusable.append(f"{task_id} ({verdict})")
            continue
        answer = row.get("answer") or ""
        if not answer.strip():
            blank.append(task_id)
            continue
        out.append({"task_id": task_id, "agent_answer": answer})

    problems = []
    if missing:
        problems.append(f"no {arm} row on {model}: {', '.join(missing)}")
    if unusable:
        problems.append(f"harness failure, not an answer: {', '.join(unusable)}")
    if blank:
        problems.append(f"empty answer: {', '.join(blank)}")
    if problems:
        raise SubmissionError(
            f"{len(missing) + len(unusable) + len(blank)} of {len(tasks)} tasks "
            f"cannot be submitted -- "
            + "; ".join(problems)
            + ". Fix them rather than submitting a short file -- the "
            "leaderboard scores a missing task as wrong and a submission "
            "cannot be withdrawn. `error`/`post_run_error` rows re-run under "
            "`dce.runner --retry`; the rest need the task re-run into a fresh "
            "results file, which `--results` will then merge."
        )
    return out


def write_submission(
    out: Path, results: list[Path], *, arm: str, model: str, tasks: list[dict]
) -> int:
    """Write the submission JSONL and return how many tasks it covers.

    The file is built in full BEFORE anything is written, so a refusal
    leaves no partial file on disk -- a half-written submission is the
    artifact most likely to be uploaded by mistake weeks later. An EMPTY
    result is refused for the same reason: it would land as a single blank
    line and read as finished.

    `out` may not be one of `results`; see the comment on the check.
    """
    # Before reading anything. `build_submission` loads every input into
    # memory first, so writing over a results file SUCCEEDS: a paid sweep --
    # verdicts, gold, cost, instrumentation, trace pointers -- is replaced by
    # a two-field answer list, and only `traces/` survives. The README's own
    # recipe puts the sweep at `results/submission.jsonl` and the export at
    # `submission.jsonl`, one path component apart.
    resolved_out = out.resolve()
    for path in results:
        if path.resolve() == resolved_out:
            raise SubmissionError(
                f"--out {out} is also one of the --results file(s). Writing "
                "there would overwrite a results file that cost money to "
                "produce, and the submission carries none of what it holds. "
                "Choose a different --out."
            )

    rows = build_submission(results, arm=arm, model=model, tasks=tasks)
    if not rows:
        raise SubmissionError(
            f"no tasks to submit: --tasks yielded {len(tasks)} task(s). An "
            "empty submission would be written as a single blank line and "
            "look like a finished file. Check the --tasks path."
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(prog="dce.submit")
    parser.add_argument("--results", type=Path, nargs="+", required=True)
    parser.add_argument("--arm", default="contract")
    parser.add_argument("--model", required=True)
    parser.add_argument("--tasks", type=Path, default=Path("data/tasks.json"))
    parser.add_argument("--out", type=Path, default=Path("submission.jsonl"))
    args = parser.parse_args()

    tasks = json.loads(args.tasks.read_text())
    try:
        n = write_submission(
            args.out, args.results, arm=args.arm, model=args.model, tasks=tasks
        )
    except SubmissionError as exc:
        raise SystemExit(str(exc)) from exc
    for field, values in provenance(
        args.results, arm=args.arm, model=args.model
    ).items():
        print(f"note: spliced rows disagree on {field}: {', '.join(values)}")
    print(f"wrote {n} answers to {args.out} ({args.arm} on {args.model})")


if __name__ == "__main__":
    main()
