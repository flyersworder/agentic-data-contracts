"""One-shot preparation: download, build the DuckDB, reconstruct golds, gate."""

from __future__ import annotations

import json
from pathlib import Path

from dce.data import CONTEXT_FILES, DATASET, build_duckdb, download_context, load_tasks
from dce.golds import PLURALITY_THRESHOLD, check_dev_gate, reconstruct_with_shares

# Discovered by inspecting the live dataset tree (Task 3, Step 5):
#   data/submissions/v1__<submission_id>__<DD-MM-YYYY>.jsonl  — 2194 files, 5.1 GB
#   data/task_scores/v1__<submission_id>__<DD-MM-YYYY>.jsonl  — 2204 files, 728 MB
# Both hold one JSON object per line, one line per task_id, and both carry
# `agent_answer`. Only task_scores also carries `score`, and its `agent_answer`
# is byte-identical to the submission file's (verified on sampled pairs), so we
# read task_scores alone: it is 8x smaller and cannot mis-pair an answer with a
# score. Per-line schema:
#   {"submission_id": str, "task_id": str, "score": bool, "level": str,
#    "agent_answer": str}
SCORES_DIR = "data/task_scores"


def download_task_scores(dest: Path) -> Path:
    """Download every per-submission score file. Returns the local directory."""
    from huggingface_hub import snapshot_download

    root = snapshot_download(
        DATASET,
        repo_type="dataset",
        allow_patterns=f"{SCORES_DIR}/*",
        local_dir=str(dest),
        max_workers=16,
    )
    return Path(root) / SCORES_DIR


def load_leaderboard_artifacts(dest: Path | None = None) -> tuple[dict, dict]:
    """Return (submissions, scores) keyed by agent name then task id.

    Shapes are {agent: {task_id: answer}} and {agent: {task_id: bool}}. The
    agent key is the leaderboard file stem, which is unique per submission.
    """
    directory = download_task_scores(dest or Path("data") / "hf")
    submissions: dict[str, dict[str, str]] = {}
    scores: dict[str, dict[str, bool]] = {}

    for path in sorted(directory.glob("*.jsonl")):
        agent = path.stem
        answers: dict[str, str] = {}
        verdicts: dict[str, bool] = {}
        with open(path) as fh:
            for line in fh:
                if not line.strip():
                    continue
                row = json.loads(line)
                task_id = str(row["task_id"])
                answers[task_id] = row.get("agent_answer", "")
                verdicts[task_id] = bool(row.get("score"))
        submissions[agent] = answers
        scores[agent] = verdicts

    return submissions, scores


def main() -> None:
    data = Path("data")
    files = download_context(data / "hf")
    build_duckdb({k: files[k] for k in CONTEXT_FILES}, data / "dabstep.duckdb")

    tasks = load_tasks("default")
    (data / "tasks.json").write_text(json.dumps(tasks, indent=2))

    submissions, scores = load_leaderboard_artifacts(data / "hf")
    corpus_ids: set[str] = set()
    for answers in submissions.values():
        corpus_ids |= answers.keys()

    golds, exclusions, shares = reconstruct_with_shares(submissions, scores)
    ok, mismatches, absent = check_dev_gate(golds, load_tasks("dev"), corpus_ids)

    (data / "golds.json").write_text(json.dumps(golds, indent=2))
    (data / "exclusions.json").write_text(json.dumps(exclusions, indent=2))
    (data / "gold_shares.json").write_text(json.dumps(shares, indent=2))

    print(
        f"golds: {len(golds)} / {len(tasks)} tasks "
        f"(plurality threshold {PLURALITY_THRESHOLD})"
    )
    if shares:
        ordered = sorted(shares.values())
        print(
            f"  plurality share of accepted golds: "
            f"min={ordered[0]:.3f} median={ordered[len(ordered) // 2]:.3f} "
            f"max={ordered[-1]:.3f}"
        )
    for reason in sorted(set(exclusions.values())):
        print(f"  excluded {sum(v == reason for v in exclusions.values())}: {reason}")

    # Never print a clean PASS: the gate cannot see the dev tasks that appear in
    # no submission file, and the output has to say so.
    if not ok:
        verdict = f"FAIL — {', '.join(mismatches)}"
    elif absent:
        verdict = (
            f"PASS on {len(load_tasks('dev')) - len(absent)} in-corpus dev tasks "
            f"— WEAKENED: {len(absent)} dev tasks are absent from every "
            f"submission file and could not be checked ({', '.join(absent)})"
        )
    else:
        verdict = "PASS"
    print(f"dev gate: {verdict}")
    if not ok:
        raise SystemExit("dev gate failed; see the spec's fallback before spending")


if __name__ == "__main__":
    main()
