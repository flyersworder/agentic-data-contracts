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

#: Verdicts carrying an answer the model actually produced. `ungraded` is
#: included deliberately: it means no gold existed LOCALLY to score against,
#: which is precisely the case the leaderboard's withheld golds resolve.
#: Everything else in `dce.stats.HARNESS_VERDICTS` is the harness failing,
#: and its `answer` is an empty string or an exception message.
SUBMITTABLE_VERDICTS: frozenset[str] = ANSWER_VERDICTS | UNGRADED_VERDICTS


class SubmissionError(Exception):
    """The rows cannot produce a complete, submittable file."""


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
    by_task: dict[str, dict] = {}
    for path in results:
        for row in latest_rows(path):
            if row.get("arm") == arm and row.get("model") == model:
                by_task[row["task_id"]] = row

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
            + ". Re-run them (`--retry`) rather than submitting a short file: "
            "the leaderboard scores a missing task as wrong and a submission "
            "cannot be withdrawn."
        )
    return out


def write_submission(
    out: Path, results: list[Path], *, arm: str, model: str, tasks: list[dict]
) -> int:
    """Write the submission JSONL and return how many tasks it covers.

    The file is built in full BEFORE anything is written, so a refusal
    leaves no partial file on disk -- a half-written submission is the
    artifact most likely to be uploaded by mistake weeks later.
    """
    rows = build_submission(results, arm=arm, model=model, tasks=tasks)
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
    print(f"wrote {n} answers to {args.out} ({args.arm} on {args.model})")


if __name__ == "__main__":
    main()
