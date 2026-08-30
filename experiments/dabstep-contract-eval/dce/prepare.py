"""One-shot preparation: download, build the DuckDB, reconstruct golds, gate."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import NamedTuple

from dce.data import (
    CONTEXT_FILES,
    DATASET,
    DATASET_REVISION,
    build_duckdb,
    download_context,
    load_tasks,
)
from dce.golds import (
    MIN_DEV_CHECKS,
    PLURALITY_THRESHOLD,
    check_dev_gate,
    reconstruct_with_shares,
)

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


class Corpus(NamedTuple):
    """The leaderboard corpus, plus what it took to be sure we read all of it."""

    submissions: dict[str, dict[str, str]]
    scores: dict[str, dict[str, bool]]
    expected: int
    consumed: int
    manifest_sha256: str
    missing: list[str]
    shadowed: list[str]


def download_task_scores(dest: Path) -> tuple[Path, list[str]]:
    """Download every per-submission score file.

    Returns the local directory and the *authoritative* list of repo-relative
    filenames at the pinned revision. Globbing the directory instead would make
    the corpus a property of the local filesystem rather than of the revision:
    this machine's APFS is case-insensitive and the revision contains five
    case-collision groups, so a glob silently yields 2199 of 2205 files while a
    Linux run yields all 2205. Same pin, different ground truth, no signal.
    """
    from huggingface_hub import list_repo_files, snapshot_download

    root = snapshot_download(
        DATASET,
        repo_type="dataset",
        revision=DATASET_REVISION,
        allow_patterns=f"{SCORES_DIR}/*",
        local_dir=str(dest),
        max_workers=16,
    )
    manifest = sorted(
        name
        for name in list_repo_files(
            DATASET, repo_type="dataset", revision=DATASET_REVISION
        )
        if name.startswith(f"{SCORES_DIR}/") and name.endswith(".jsonl")
    )
    return Path(root) / SCORES_DIR, manifest


def load_leaderboard_corpus(dest: Path | None = None) -> Corpus:
    """Load the corpus by manifest, recording exactly what was consumed.

    Files the manifest names but the filesystem lacks are reported as `missing`;
    files whose local path was already consumed under another manifest name (a
    case collision) are reported as `shadowed`. Both are counted rather than
    silently skipped, and `manifest_sha256` fingerprints the consumed set, so a
    run that saw a different corpus is visible in the gold envelope instead of
    being mistaken for the same ground truth.
    """
    directory, manifest = download_task_scores(dest or Path("data") / "hf")
    submissions: dict[str, dict[str, str]] = {}
    scores: dict[str, dict[str, bool]] = {}
    missing: list[str] = []
    shadowed: list[str] = []
    consumed: list[str] = []
    seen_paths: set[str] = set()

    for name in manifest:
        path = directory / Path(name).name
        if not path.exists():
            missing.append(name)
            continue
        resolved = str(path.resolve()).lower()
        if resolved in seen_paths:
            # Two manifest entries differing only in case share one local file.
            # Reading it twice would double-count that submission's votes.
            shadowed.append(name)
            continue
        seen_paths.add(resolved)

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
        agent = path.stem
        submissions[agent] = answers
        scores[agent] = verdicts
        consumed.append(name)

    digest = hashlib.sha256("\n".join(consumed).encode()).hexdigest()
    return Corpus(
        submissions, scores, len(manifest), len(consumed), digest, missing, shadowed
    )


def load_leaderboard_artifacts(dest: Path | None = None) -> tuple[dict, dict]:
    """Return (submissions, scores) keyed by agent name then task id.

    Shapes are {agent: {task_id: answer}} and {agent: {task_id: bool}}. The
    agent key is the leaderboard file stem, which is unique per submission.
    Thin wrapper over `load_leaderboard_corpus` for callers that only need the
    two mappings.
    """
    corpus = load_leaderboard_corpus(dest)
    return corpus.submissions, corpus.scores


def write_golds(
    path: Path,
    golds: dict[str, str],
    threshold: float = PLURALITY_THRESHOLD,
    revision: str = DATASET_REVISION,
    corpus: Corpus | None = None,
) -> dict:
    """Write the self-describing gold envelope Task 8's runner reads.

    The golds are an *envelope*, not a bare mapping: a gold set reconstructed
    from a live leaderboard is only meaningful next to the revision and
    threshold that produced it. Anything scoring against this file can assert
    it is reading the same ground truth every arm was scored against.

    `manifest_sha256` fingerprints the submission files actually consumed, so
    two runs at the same revision that nonetheless saw different corpora (a
    case-insensitive filesystem, a partial download) are distinguishable rather
    than silently comparable.
    """
    envelope = {
        "revision": revision,
        "threshold": threshold,
        "count": len(golds),
        "submissions_expected": corpus.expected if corpus else None,
        "submissions_consumed": corpus.consumed if corpus else None,
        "manifest_sha256": corpus.manifest_sha256 if corpus else None,
        "golds": golds,
    }
    path.write_text(json.dumps(envelope, indent=2))
    return envelope


def main() -> None:
    data = Path("data")
    files = download_context(data / "hf")
    build_duckdb({k: files[k] for k in CONTEXT_FILES}, data / "dabstep.duckdb")

    tasks = load_tasks("default")
    (data / "tasks.json").write_text(json.dumps(tasks, indent=2))

    corpus = load_leaderboard_corpus(data / "hf")
    corpus_ids: set[str] = set()
    for answers in corpus.submissions.values():
        corpus_ids |= answers.keys()

    golds, exclusions, shares = reconstruct_with_shares(
        corpus.submissions, corpus.scores
    )
    dev_tasks = load_tasks("dev")
    ok, mismatches, absent = check_dev_gate(golds, dev_tasks, corpus_ids)
    checked = len(dev_tasks) - len(absent)

    write_golds(data / "golds.json", golds, corpus=corpus)
    (data / "exclusions.json").write_text(json.dumps(exclusions, indent=2))
    (data / "gold_shares.json").write_text(json.dumps(shares, indent=2))

    print(
        f"corpus: {corpus.consumed} / {corpus.expected} submission files "
        f"(manifest {corpus.manifest_sha256[:12]})"
    )
    if corpus.missing or corpus.shadowed:
        print(
            f"  WARNING: {len(corpus.missing)} missing, "
            f"{len(corpus.shadowed)} shadowed by a case-insensitive filesystem "
            f"— this run saw a different corpus than a case-sensitive one would"
        )
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
        verdict = f"FAIL — {', '.join(mismatches) or 'no checks ran'}"
    elif absent:
        verdict = (
            f"PASS on {checked} in-corpus dev tasks "
            f"— WEAKENED: {len(absent)} dev tasks are absent from every "
            f"submission file and could not be checked ({', '.join(absent)})"
        )
    else:
        verdict = f"PASS on {checked} dev tasks"
    print(f"dev gate: {verdict}")

    # A gate is only evidence if it ran. An empty or failed download would
    # otherwise reconstruct nothing, find nothing to mismatch, and exit 0 —
    # manufacturing confidence for the go/no-go that gates all spending.
    if not golds:
        raise SystemExit("no golds reconstructed; the corpus is empty or unreadable")
    if checked < MIN_DEV_CHECKS:
        raise SystemExit(
            f"dev gate verified only {checked} of the expected {MIN_DEV_CHECKS} "
            f"in-corpus dev tasks; refusing to report a pass"
        )
    if not ok:
        raise SystemExit("dev gate failed; see the spec's fallback before spending")


if __name__ == "__main__":
    main()
