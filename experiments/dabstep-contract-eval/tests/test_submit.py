"""The leaderboard submission file.

A DABStep submission is effectively one-shot per agent name, and the Space
grades whatever it is given: a file that is quietly short by 12 tasks scores
as 12 wrong answers with no error anywhere. Every test here is about
refusing to write such a file rather than about writing a good one.
"""

import json
from pathlib import Path

import pytest
from dce.submit import SubmissionError, build_submission, write_submission

MODEL = "z-ai/glm-5.3-flash"
TASKS = [
    {"task_id": "1", "level": "easy"},
    {"task_id": "2", "level": "hard"},
    {"task_id": "3", "level": "hard"},
]


def _row(task_id: str, answer: str, *, verdict: str = "correct", arm: str = "contract"):
    return {
        "task_id": task_id,
        "arm": arm,
        "model": MODEL,
        "answer": answer,
        "verdict": verdict,
    }


def _write(path: Path, rows: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    return path


def test_build_submission_emits_task_id_and_agent_answer_for_every_task(tmp_path):
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "yes")],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out == [
        {"task_id": "1", "agent_answer": "138236"},
        {"task_id": "2", "agent_answer": "A"},
        {"task_id": "3", "agent_answer": "yes"},
    ]


def test_build_submission_takes_only_the_requested_arm(tmp_path):
    """Four arms answer every task. Submitting the wrong one, or a mixture,
    is silent and unrecoverable.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [
            _row("1", "contract-answer"),
            _row("1", "schema-answer", arm="schema_only"),
            _row("2", "A"),
            _row("3", "yes"),
        ],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out[0] == {"task_id": "1", "agent_answer": "contract-answer"}


def test_build_submission_takes_only_the_requested_model(tmp_path):
    results = _write(
        tmp_path / "r.jsonl",
        [
            {**_row("1", "other-model"), "model": "openai/gpt-5.6-sol"},
            _row("1", "glm-answer"),
            _row("2", "A"),
            _row("3", "yes"),
        ],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out[0] == {"task_id": "1", "agent_answer": "glm-answer"}


def test_build_submission_uses_the_last_row_for_a_retried_task(tmp_path):
    """A resumed sweep leaves several rows for one key; only the last is that
    unit's true state. This is `latest_rows`' contract, asserted here because
    a submission that ships a stale `error` row's empty answer is a silent
    zero on that task.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [
            _row("1", "", verdict="error"),
            _row("1", "138236"),
            _row("2", "A"),
            _row("3", "yes"),
        ],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out[0] == {"task_id": "1", "agent_answer": "138236"}


def test_build_submission_merges_several_results_files(tmp_path):
    """The splice case: the golded tasks from one sweep, the ungolded ones
    from a later run.
    """
    a = _write(tmp_path / "a.jsonl", [_row("1", "138236"), _row("2", "A")])
    b = _write(tmp_path / "b.jsonl", [_row("3", "yes", verdict="ungraded")])

    out = build_submission([a, b], arm="contract", model=MODEL, tasks=TASKS)

    assert [r["task_id"] for r in out] == ["1", "2", "3"]


def test_build_submission_accepts_an_ungraded_answer(tmp_path):
    """`ungraded` means no gold existed locally — the answer is real, and it
    is exactly what the leaderboard's withheld golds are there to judge.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "yes", verdict="ungraded")],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out[2] == {"task_id": "3", "agent_answer": "yes"}


def test_build_submission_refuses_when_a_task_is_missing(tmp_path):
    results = _write(tmp_path / "r.jsonl", [_row("1", "138236"), _row("2", "A")])

    with pytest.raises(SubmissionError, match="3"):
        build_submission([results], arm="contract", model=MODEL, tasks=TASKS)


def test_build_submission_refuses_a_harness_failure_row(tmp_path):
    """`hit_limit` produced no answer. Shipping it as one submits an empty
    string and scores zero on that task.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "", verdict="hit_limit")],
    )

    with pytest.raises(SubmissionError, match="hit_limit"):
        build_submission([results], arm="contract", model=MODEL, tasks=TASKS)


def test_build_submission_refuses_an_empty_answer(tmp_path):
    """A graded-but-empty answer is a harness artifact far more often than a
    real reply. Rerun the task; do not spend the one-shot submission on it.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "   ")],
    )

    with pytest.raises(SubmissionError, match="empty"):
        build_submission([results], arm="contract", model=MODEL, tasks=TASKS)


def test_write_submission_writes_one_json_object_per_line(tmp_path):
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "yes")],
    )
    out = tmp_path / "submission.jsonl"

    write_submission(out, [results], arm="contract", model=MODEL, tasks=TASKS)

    lines = out.read_text().splitlines()
    assert [json.loads(line) for line in lines] == [
        {"task_id": "1", "agent_answer": "138236"},
        {"task_id": "2", "agent_answer": "A"},
        {"task_id": "3", "agent_answer": "yes"},
    ]


def test_write_submission_leaves_no_file_behind_when_it_refuses(tmp_path):
    """A partial file on disk is the thing most likely to get uploaded by
    mistake later.
    """
    results = _write(tmp_path / "r.jsonl", [_row("1", "138236")])
    out = tmp_path / "submission.jsonl"

    with pytest.raises(SubmissionError):
        write_submission(out, [results], arm="contract", model=MODEL, tasks=TASKS)

    assert not out.exists()


def test_build_submission_accepts_a_scoring_error_answer(tmp_path):
    """`scoring_error` is set only when a real, non-empty answer existed and
    OUR scorer raised on it (see `dce.agent`). The answer is intact, and
    whether it is right is exactly what the leaderboard's withheld golds
    decide — so it is submittable.

    Excluding it would also be a dead end: `--retry` accepts only `error`
    and `post_run_error`, and `completed_keys` treats `scoring_error` as
    done, so no resumed sweep would ever re-run the task. The submission
    would be unbuildable without hand-editing the results file.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [
            _row("1", "138236"),
            _row("2", "A"),
            _row("3", "yes", verdict="scoring_error"),
        ],
    )

    out = build_submission([results], arm="contract", model=MODEL, tasks=TASKS)

    assert out[2] == {"task_id": "3", "agent_answer": "yes"}


def test_write_submission_refuses_to_overwrite_one_of_its_inputs(tmp_path):
    """`build_submission` reads everything into memory first, so writing over
    a results file SUCCEEDS and replaces a paid sweep — verdicts, gold, cost,
    instrumentation, trace pointers — with a two-field answer list. The
    README's own recipe puts the sweep at `results/submission.jsonl` and the
    export at `submission.jsonl`, one path component apart.
    """
    results = _write(
        tmp_path / "r.jsonl",
        [_row("1", "138236"), _row("2", "A"), _row("3", "yes")],
    )

    with pytest.raises(SubmissionError, match="results file"):
        write_submission(results, [results], arm="contract", model=MODEL, tasks=TASKS)

    assert json.loads(results.read_text().splitlines()[0])["arm"] == "contract"


def test_write_submission_refuses_to_write_an_empty_submission(tmp_path):
    """A `--tasks` file that parses to `[]` finds no problems to report, so
    the guards stay quiet and a file containing one blank line lands on disk
    — precisely the artifact most likely to be uploaded weeks later.
    """
    results = _write(tmp_path / "r.jsonl", [_row("1", "138236")])
    out = tmp_path / "submission.jsonl"

    with pytest.raises(SubmissionError, match="no tasks"):
        write_submission(out, [results], arm="contract", model=MODEL, tasks=[])

    assert not out.exists()
