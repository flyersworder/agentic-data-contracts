"""Sweep driver: resumable, budget-capped, one JSONL row per unit of work.

This is the component that spends real money, so its guards matter as much
as its logic:

  * WORKING COPY, ALWAYS. Arms `schema_only` and `manual_prompt` are
    ungoverned — nothing stops either from issuing `DROP TABLE` against
    whatever file it is pointed at (see `dce/arms.py`'s module docstring,
    CALL ORDER). `sweep` makes exactly one working copy per process
    (`make_working_copy`) and hands every arm the *copy*, never the
    pristine file, then runs `check_and_restore` after each task — once
    `run_task` has closed its own arm, which is what makes the check valid.
  * A CORRUPTED DB IS A FINDING, NOT A CRASH. If `check_and_restore` reports
    corruption, that row is stamped `db_corrupted: True` and the sweep
    continues — an ungoverned arm mutating the warehouse is exactly the kind
    of governance gap this experiment exists to surface, so losing the rest
    of a paid sweep over it would be the wrong failure mode.
  * A CONSTRUCTION FAILURE IS A FINDING, NOT A CRASH. `run_task` can raise
    before it ever gets a chance to write a row (e.g. its agent factory
    reading a missing `OPENROUTER_API_KEY`). `sweep` catches that, writes a
    row with a distinct verdict, and moves on rather than aborting a
    partly-paid sweep on one transient failure.
  * THE SPEND CAP IS A RESERVATION, NOT A POSTMORTEM. Before every call,
    `sweep` reserves a realistic per-call estimate (the running mean of
    observed cost, or a small floor before any observation has been made)
    against the remaining budget, and refuses to start a call that would
    push spend past `max_spend`. See `_next_reserve`.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from dce.agent import run_task
from dce.arms import ARMS, check_and_restore, make_working_copy
from dce.data import DATASET_REVISION
from dce.pricing import MODELS

#: Reserved for the very first call of a sweep, before any `usd` has been
#: observed to compute a running mean from. Deliberately conservative (this
#: experiment's models run well under a dollar a task) rather than tight —
#: an over-reservation only delays the start of one call by a beat; an
#: under-reservation lets the cap be overrun.
DEFAULT_RESERVE_USD: float = 0.25

#: A reservation must never be read as "this call is free." Observed `usd`
#: can legitimately be 0.0 (a `hit_limit`/`error` row that made no billable
#: call), and a naive running mean would let a string of those quietly zero
#: out the guard for the very next, possibly expensive, call.
MIN_RESERVE_USD: float = 0.01


def completed_keys(path: Path) -> set[tuple[str, str, str]]:
    """(task_id, arm, model) triples already recorded in `path`.

    Missing file means nothing has run yet, not an error — a sweep's first
    invocation always starts from an empty `out`.
    """
    if not path.exists():
        return set()
    keys = set()
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        keys.add((row["task_id"], row["arm"], row["model"]))
    return keys


def pending(tasks, arms, models, done) -> list[tuple[str, str, str]]:
    """Every (task_id, arm, model) triple in `tasks` x `arms` x `models` not
    already in `done`, in the fixed task/arm/model iteration order — so a
    resumed sweep picks up exactly where an interrupted one left off."""
    return [
        (task["task_id"], arm, model)
        for task in tasks
        for arm in arms
        for model in models
        if (task["task_id"], arm, model) not in done
    ]


def _working_db_path(pristine: Path) -> Path:
    return pristine.with_name(pristine.name + ".working")


def _next_reserve(total_usd: float, n_observed: int, floor: float) -> float:
    """Estimate for the *next* call's cost, to compare against remaining
    budget before making it.

    Once at least one call has been observed, the running mean of what
    calls have actually cost is a far more realistic estimate than a
    hardcoded per-task figure — `per_task_usd` no longer bounds anything
    (the runaway guard is a fixed token budget; see `dce/agent.py`'s
    `TOKEN_BUDGET`), so it is not a trustworthy worst case. Before any
    observation, fall back to `floor`. Either way, never return a figure at
    or below `MIN_RESERVE_USD` — a $0 reservation would let the guard treat
    every call as free.
    """
    if n_observed == 0:
        return max(floor, MIN_RESERVE_USD)
    return max(total_usd / n_observed, MIN_RESERVE_USD)


def sweep(
    tasks,
    arms,
    models,
    golds,
    *,
    out: Path,
    db_path: Path,
    docs,
    max_spend: float,
    golds_hash: str = "",
    run_task_fn=run_task,
    per_task_usd: float = DEFAULT_RESERVE_USD,
) -> float:
    """Run every not-yet-completed (task, arm, model) triple, appending one
    JSON row per unit of work to `out`, until `max_spend` would be exceeded.

    `db_path` is the pristine database; it is never opened directly here or
    handed to `run_task_fn` — see the module docstring. `golds` is the plain
    `task_id -> answer` mapping (callers reading the on-disk envelope must
    unwrap it first; see `_load_golds`).
    """
    working = make_working_copy(db_path, _working_db_path(db_path))
    by_id = {t["task_id"]: t for t in tasks}
    todo = pending(tasks, arms, models, completed_keys(out))
    out.parent.mkdir(parents=True, exist_ok=True)

    spent = 0.0
    n_observed = 0
    with out.open("a") as fh:
        for task_id, arm, model in todo:
            reserve = _next_reserve(spent, n_observed, per_task_usd)
            if spent + reserve > max_spend:
                print(
                    f"stopping: ${spent:.2f} spent + ${reserve:.2f} reserved "
                    f"would exceed ${max_spend:.2f}"
                )
                break

            gold = golds.get(task_id, "")
            try:
                row = run_task_fn(
                    by_id[task_id],
                    arm,
                    model,
                    working,
                    docs,
                    gold,
                    golds_hash=golds_hash,
                    per_task_usd=per_task_usd,
                )
            except Exception as exc:
                # A factory error (e.g. a missing OPENROUTER_API_KEY) would
                # otherwise escape with no row written, aborting a
                # partly-paid sweep on one transient failure. Record it and
                # move on instead.
                row = {
                    "task_id": task_id,
                    "arm": arm,
                    "model": model,
                    "verdict": "construction_error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "usd": 0.0,
                }

            # Valid only now: `run_task_fn` has closed its own arm's
            # connection (see `dce/arms.py`'s module docstring, CALL ORDER).
            integrity = check_and_restore(working, db_path)
            row["db_corrupted"] = integrity.corrupted
            fh.write(json.dumps(row) + "\n")
            fh.flush()

            spent += float(row.get("usd", 0.0))
            n_observed += 1
    return spent


def _load_golds(path: Path) -> tuple[dict[str, str], str]:
    """Read the golds envelope and return (task_id -> answer map, manifest hash).

    `data/golds.json` is an envelope
    (`{"revision", "threshold", "count", "golds", "submissions_expected",
    "submissions_consumed", "manifest_sha256"}`), not a bare mapping — the
    task -> answer map lives under `"golds"`. Reading it as a bare mapping
    would silently iterate the 7 envelope keys instead of the ~406 tasks.

    The `revision` check is what catches a smoke run and a full sweep being
    scored against two different ground-truth snapshots: nothing else in the
    pipeline would notice.
    """
    envelope = json.loads(path.read_text())
    if envelope["revision"] != DATASET_REVISION:
        raise SystemExit(
            f"golds revision {envelope['revision']!r} does not match "
            f"dce.data.DATASET_REVISION {DATASET_REVISION!r}; refusing to "
            "score a sweep against a different dataset snapshot than the "
            "one golds.json was reconstructed from"
        )
    return envelope["golds"], envelope["manifest_sha256"]


def assert_clean_tree(git_status_fn=None) -> None:
    """A scored sweep must be reconstructible from its recorded commit sha.

    With an editable path dependency the library under test IS the working
    tree, so an uncommitted change makes every row's `commit_sha` a lie —
    and nothing in the results would show it.

    `git_status_fn`, if given, replaces the real `git status --porcelain`
    call; injected by tests so this stays offline and deterministic.
    """
    run = git_status_fn or (
        lambda: subprocess.check_output(["git", "status", "--porcelain"], text=True)
    )
    dirty = run().strip()
    if dirty:
        raise SystemExit(
            "refusing to run a scored sweep with a dirty working tree; "
            f"commit or stash first:\n{dirty}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(prog="dce.runner")
    parser.add_argument("--n", type=int, default=0, help="0 = all tasks")
    parser.add_argument("--arms", nargs="+", default=list(ARMS))
    parser.add_argument("--models", nargs="+", default=["z-ai/glm-5.3-flash"])
    parser.add_argument("--max-spend", type=float, required=True)
    parser.add_argument("--out", type=Path, default=Path("results/results.jsonl"))
    parser.add_argument("--db", type=Path, default=Path("data/dabstep.duckdb"))
    parser.add_argument("--golds", type=Path, default=Path("data/golds.json"))
    parser.add_argument("--tasks", type=Path, default=Path("data/tasks.json"))
    args = parser.parse_args()

    for model in args.models:
        if model not in MODELS:
            raise SystemExit(f"unpinned or unknown model: {model}")

    # Before any model call: every result row's commit_sha is the only pin
    # on the library under test, and that pin is only truthful on a clean
    # tree.
    assert_clean_tree()

    golds, golds_hash = _load_golds(args.golds)
    tasks = [t for t in json.loads(args.tasks.read_text()) if t["task_id"] in golds]
    if args.n:
        tasks = tasks[: args.n]

    docs = {
        "manual": Path("data/hf/data/context/manual.md").read_text(),
        "payments_readme": Path("data/hf/data/context/payments-readme.md").read_text(),
    }

    spent = sweep(
        tasks,
        tuple(args.arms),
        tuple(args.models),
        golds,
        out=args.out,
        db_path=args.db,
        docs=docs,
        max_spend=args.max_spend,
        golds_hash=golds_hash,
    )
    print(f"spent ${spent:.2f}")


if __name__ == "__main__":
    main()
