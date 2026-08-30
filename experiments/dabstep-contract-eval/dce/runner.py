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
    Left on disk once the sweep finishes (deliberately — a corrupted-DB row
    is worth being able to inspect post-mortem); the next invocation just
    overwrites it via `make_working_copy` again, so nothing accumulates.
  * A CORRUPTED DB IS A FINDING, NOT A CRASH. If `check_and_restore` reports
    corruption, that row is stamped `db_corrupted: True` and the sweep
    continues — an ungoverned arm mutating the warehouse is exactly the kind
    of governance gap this experiment exists to surface, so losing the rest
    of a paid sweep over it would be the wrong failure mode.
  * A CONSTRUCTION FAILURE IS A FINDING, NOT A CRASH, BUT IT MUST NOT HIDE A
    BUG FOREVER. `run_task` can raise before it ever gets a chance to write
    a row (e.g. its agent factory reading a missing `OPENROUTER_API_KEY`).
    `sweep` catches that, writes a row with `verdict: "construction_error"`,
    and moves on — but `completed_keys` never treats a
    `construction_error` row as done, so a resumed sweep always retries it.
    That is a deliberate trade-off: `except Exception` here also catches an
    ordinary bug (a signature mismatch, say), and always-retry means such a
    bug surfaces on every resume instead of silently completing the sweep
    with a wall of skip-forever rows.
  * THE SPEND CAP BOUNDS THE EXPERIMENT, NOT ONE PROCESS. `sweep` seeds its
    running total from `spent_so_far(out)` — the `usd` already banked by
    every prior invocation against the same `out` file — because resume is
    the headline feature and a cap that resets to $0 on every restart is no
    cap at all under a crash loop or a naive retry wrapper.
  * THE RESERVATION IS A REAL CEILING, NOT A GUESS, AND IT COVERS A WHOLE
    TASK. Before any observation for a given model, `sweep` reserves
    `dce.agent._token_budget_usd(model)` — the true worst case the runaway
    token guard allows, not a hand-picked dollar figure — and once a model
    has been observed, the max (not mean) of what it has actually cost, so
    the reservation is never optimistic. The reservation covers an entire
    task's (arm, model) group at once and is checked before the first call
    in that group, so a truncation always lands on a task boundary: a
    resumed/paired analysis never has to discard a half-finished task.
"""

from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

from dce.agent import _commit_sha, _token_budget_usd, run_task
from dce.arms import ARMS, check_and_restore, make_working_copy
from dce.data import DATASET_REVISION
from dce.frozen import digest
from dce.pricing import MODELS

#: A reservation must never be read as "this call is free." Observed `usd`
#: can legitimately be 0.0 (a `hit_limit`/`error`/`construction_error` row
#: that made no billable call), and neither the seed floor nor the observed
#: max may collapse to that value.
MIN_RESERVE_USD: float = 0.01


@dataclass
class SweepResult:
    """`spent`: total USD banked in `out` after this call, resumed sweeps
    included. `truncated`: True iff this call stopped before completing
    `pending()` because the next task group's reservation would have
    exceeded `max_spend` — `main()` uses this to exit non-zero so a wrapper
    can tell a capped-out run from a completed one."""

    spent: float
    truncated: bool


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def completed_keys(
    path: Path, *, retry_error: bool = False
) -> set[tuple[str, str, str]]:
    """(task_id, arm, model) triples a resumed sweep should skip.

    Verdict-aware, not a blanket "every row already written is done":
    `construction_error` rows are never counted as done (see the module
    docstring) — they cost nothing and made no call, so always retrying
    them is free. `error` rows are counted as done unless `retry_error` is
    set, since an `error` row already cost money and a resume should not
    silently re-pay for it without being asked. `hit_limit` and
    `scoring_error` rows need no special case at all: both are terminal by
    design (a `hit_limit` needs a deliberate higher-budget re-run, not an
    automatic retry at the same budget; a `scoring_error` is fixed by
    re-scoring the stored `answer` offline, never by re-paying for the
    model call), so they simply fall through to "done" like any completed
    row. A row with no `verdict` at all (an older/foreign row shape) is
    also treated as done, matching this function's pre-verdict-aware
    behaviour.
    """
    keys = set()
    for row in _read_rows(path):
        verdict = row.get("verdict")
        if verdict == "construction_error":
            continue
        if verdict == "error" and retry_error:
            continue
        keys.add((row["task_id"], row["arm"], row["model"]))
    return keys


def spent_so_far(path: Path) -> float:
    """Total USD already banked in `path`, across every prior invocation
    that appended to it. Seeds `sweep`'s running total so `max_spend` bounds
    the experiment (every resume put together), not one process's lifetime.

    Tolerant of a missing/null `usd` on a historical row (an older row shape
    from before a field existed) — unlike the strict `row["usd"]` `sweep`
    itself uses for rows it is producing right now; see that function's
    docstring for why the two need different strictness.
    """
    total = 0.0
    for row in _read_rows(path):
        usd = row.get("usd")
        if usd:
            total += usd
    return total


def pending(tasks, arms, models, done) -> list[tuple[str, str, str]]:
    """Every (task_id, arm, model) triple in `tasks` x `arms` x `models` not
    already in `done`, in task-major order: every triple for one task_id is
    contiguous. That ordering is load-bearing, not incidental — `sweep`
    groups consecutive triples by `task_id` to reserve and truncate on task
    boundaries (see `test_pending_groups_consecutive_triples_by_task_id`),
    and a completed task having all of its arms run together is what makes
    a paired per-task comparison possible without dropping a lopsided group.
    """
    return [
        (task["task_id"], arm, model)
        for task in tasks
        for arm in arms
        for model in models
        if (task["task_id"], arm, model) not in done
    ]


def _working_db_path(pristine: Path) -> Path:
    return pristine.with_name(pristine.name + ".working")


def _next_reserve(observed: list[float], floor: float) -> float:
    """Estimate for one more call against a given model, to compare against
    remaining budget before making it.

    Before any observation, `floor` is the real worst case implied by the
    runaway token guard (`dce.agent._token_budget_usd(model)`), not a
    hand-picked figure. Once at least one call against this model has been
    observed, the estimate is the MAX (not the mean) of what it has
    actually cost: a mean guarantees roughly half of all calls exceed the
    reservation by construction, and using the max instead costs nothing
    extra once the ceiling is already known to be much larger (a mean only
    ever pays for itself when the floor would otherwise have been an
    overestimate, which `_token_budget_usd` is not). Either way, never
    returns a figure at or below `MIN_RESERVE_USD` — a $0 reservation would
    let the guard treat the next call as free.
    """
    if not observed:
        return max(floor, MIN_RESERVE_USD)
    return max(max(observed), MIN_RESERVE_USD)


def _seed_observed_by_model(path: Path) -> dict[str, list[float]]:
    """Per-model billable-cost history from `path`, so a resume's very
    first reservation for each model is exactly as informed as if the
    process had never restarted. Only rows with `usd > 0` count — see
    `sweep`'s docstring on why a `construction_error`/`hit_limit`/`error`
    row's `usd: 0.0` must not be allowed to decay the reservation toward
    the floor."""
    by_model: dict[str, list[float]] = {}
    for row in _read_rows(path):
        usd = row.get("usd")
        model = row.get("model")
        if usd and model:
            by_model.setdefault(model, []).append(usd)
    return by_model


def _construction_error_row(
    task: dict, arm: str, model: str, gold: str, golds_hash: str, exc: Exception
) -> dict:
    """A row with the same shape `dce.agent.build_result_row` produces, for
    a task that never got far enough to build one — `run_task`'s agent
    factory raised (e.g. a missing `OPENROUTER_API_KEY`) before any model
    call was made. Everything `sweep` actually knows is filled in (`level`
    matters: `accuracy_by(rows, "level")` needs it on every row, not just
    the ones that ran); only the token/turn fields, which have no
    meaningful value for a call that never happened, are zeroed.
    """
    return {
        "task_id": task["task_id"],
        "level": task.get("level", "unknown"),
        "arm": arm,
        "model": model,
        "answer": "",
        "answer_normalized": "",
        "gold": gold,
        "verdict": "construction_error",
        "input_tokens": 0,
        "output_tokens": 0,
        "cached_tokens": 0,
        "turns": 0,
        "usd": 0.0,
        "tool_calls": [],
        "inspect_rejections": 0,
        "enforcement_blocks": 0,
        "retry_prompts": 0,
        "request_limit": 0,
        "token_cap": 0,
        "contract_digest": digest(),
        "golds_hash": golds_hash,
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
        "error": f"{type(exc).__name__}: {exc}",
    }


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
    golds_hash: str,
    run_task_fn=run_task,
    retry_error: bool = False,
) -> SweepResult:
    """Run every not-yet-completed (task, arm, model) triple, appending one
    JSON row per unit of work to `out`, until the next task's reservation
    would push spend past `max_spend`.

    `db_path` is the pristine database; it is never opened directly here or
    handed to `run_task_fn` — see the module docstring. `golds` is the plain
    `task_id -> answer` mapping (callers reading the on-disk envelope must
    unwrap it first; see `_load_golds`).
    """
    done = completed_keys(out, retry_error=retry_error)
    todo = pending(tasks, arms, models, done)
    by_id = {t["task_id"]: t for t in tasks}
    out.parent.mkdir(parents=True, exist_ok=True)

    spent = spent_so_far(out)
    observed = _seed_observed_by_model(out)

    if not todo:
        # Nothing to do: don't even pay for a working-copy file write.
        return SweepResult(spent=spent, truncated=False)

    working = make_working_copy(db_path, _working_db_path(db_path))
    truncated = False

    with out.open("a") as fh:
        for task_id, group_iter in itertools.groupby(todo, key=lambda t: t[0]):
            group = list(group_iter)
            group_reserve = sum(
                _next_reserve(observed.get(model, []), _token_budget_usd(model))
                for _, _, model in group
            )
            if spent + group_reserve > max_spend:
                print(
                    f"stopping: ${spent:.2f} spent + ${group_reserve:.2f} "
                    f"reserved for task {task_id!r}'s group would exceed "
                    f"${max_spend:.2f}"
                )
                truncated = True
                break

            gold = golds.get(task_id, "")
            for _, arm, model in group:
                try:
                    row = run_task_fn(
                        by_id[task_id],
                        arm,
                        model,
                        working,
                        docs,
                        gold,
                        golds_hash=golds_hash,
                    )
                except Exception as exc:
                    row = _construction_error_row(
                        by_id[task_id], arm, model, gold, golds_hash, exc
                    )

                # Valid only now: `run_task_fn` has closed its own arm's
                # connection (see `dce/arms.py`'s module docstring, CALL
                # ORDER).
                integrity = check_and_restore(working, db_path)
                row["db_corrupted"] = integrity.corrupted
                fh.write(json.dumps(row) + "\n")
                fh.flush()

                # Loud on a missing `usd`: a row that forgot to price itself
                # must stop the sweep, not read as "free and run forever"
                # (measured: `.get("usd", 0.0)` ran 50 tasks under a $2.00
                # cap and reported `spent $0.00`). `usd: None` (an explicit,
                # known "couldn't price this") is handled, not crashed on.
                usd = row["usd"]
                if usd is None:
                    usd = 0.0
                spent += usd
                if usd > 0:
                    observed.setdefault(model, []).append(usd)
    return SweepResult(spent=spent, truncated=truncated)


def _stratified_sample(tasks: list[dict], n: int) -> list[dict]:
    """`n` tasks split across `level`s in population proportion, so a smoke
    run always reads on every stratum instead of however the input happened
    to be ordered.

    Measured: of 406 golded tasks, 336 are hard and 70 are easy; a plain
    `tasks[:6]` reads 6 hard and 0 easy. Grouping by level here (rather than
    relying on `tasks` already being grouped/sorted) means this does not
    depend on `prepare.py`'s output ordering.
    """
    if n <= 0 or n >= len(tasks):
        return tasks
    by_level: dict[str, list[dict]] = {}
    for task in tasks:
        by_level.setdefault(task.get("level", "unknown"), []).append(task)
    total = len(tasks)
    sampled: list[dict] = []
    for group in by_level.values():
        share = len(group) / total
        k = round(n * share)
        sampled.extend(group[:k])
    return sampled


def _load_golds(path: Path) -> tuple[dict[str, str], str]:
    """Read the golds envelope and return (task_id -> answer map, manifest hash).

    `data/golds.json` is an envelope
    (`{"revision", "threshold", "count", "golds", "submissions_expected",
    "submissions_consumed", "manifest_sha256"}`), not a bare mapping — the
    task -> answer map lives under `"golds"`. Reading it as a bare mapping
    would silently iterate its handful of envelope keys instead of ~406
    tasks; checked explicitly here (raising `SystemExit`, not letting a
    bare mapping fail later with `KeyError('revision')`) so that mistake is
    loud and immediate instead of a confusing crash deep in the sweep.

    The `revision` check is what catches a smoke run and a full sweep being
    scored against two different ground-truth snapshots: nothing else in the
    pipeline would notice.
    """
    envelope = json.loads(path.read_text())
    if (
        not isinstance(envelope, dict)
        or "golds" not in envelope
        or "revision" not in envelope
    ):
        raise SystemExit(
            f"{path} does not look like a golds envelope (expected top-level "
            '"revision" and "golds" keys) — passed the bare task->answer '
            "mapping instead of the envelope it lives under?"
        )
    if envelope["revision"] != DATASET_REVISION:
        raise SystemExit(
            f"golds revision {envelope['revision']!r} does not match "
            f"dce.data.DATASET_REVISION {DATASET_REVISION!r}; refusing to "
            "score a sweep against a different dataset snapshot than the "
            "one golds.json was reconstructed from"
        )
    return envelope["golds"], envelope["manifest_sha256"]


def _find_repo_root(cwd: Path | None = None) -> Path:
    return Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"], text=True, cwd=cwd
        ).strip()
    )


def _porcelain_path(line: str) -> str:
    # Format: "XY <path>" or, for a rename, "XY old -> new".
    path = line[3:]
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip('"')


def assert_clean_tree(
    *,
    out: Path | None = None,
    repo_root: Path | None = None,
    git_status_fn=None,
) -> None:
    """A scored sweep must be reconstructible from its recorded commit sha.

    With an editable path dependency the library under test IS the working
    tree, so an uncommitted change makes every row's `commit_sha` a lie —
    and nothing in the results would show it.

    Two deliberate exceptions to an otherwise strict check:

      * The check runs against `repo_root` (found via `git rev-parse
        --show-toplevel`, not inherited from `os.getcwd()`) so its verdict
        does not depend on which directory the sweep happened to be invoked
        from.
      * `out` — the sweep's own results file and its parent directory — is
        excluded from the dirty check. `results/` is deliberately committed
        (the tamper-evidence claim depends on it being readable in git
        history), so its being freshly written between commits is expected
        and would otherwise make every resumed sweep refuse to start.
        Everything else must still be clean.

    `git_status_fn`, if given, replaces the real `git status --porcelain`
    call; injected by tests so this stays offline and deterministic.
    """
    root = repo_root
    if git_status_fn is not None:
        run = git_status_fn
    else:
        root = root or _find_repo_root()

        def run() -> str:
            return subprocess.check_output(
                ["git", "-C", str(root), "status", "--porcelain"], text=True
            )

    lines = [line for line in run().splitlines() if line.strip()]

    if out is not None:
        root = root or _find_repo_root()
        try:
            out_rel = out.resolve().relative_to(root.resolve()).as_posix()
        except ValueError:
            out_rel = None
        if out_rel is not None:
            out_parent = str(Path(out_rel).parent.as_posix())

            def _is_output_path(line: str) -> bool:
                path = _porcelain_path(line)
                return path == out_rel or path.startswith(out_parent + "/")

            lines = [line for line in lines if not _is_output_path(line)]

    dirty = "\n".join(lines).strip()
    if dirty:
        raise SystemExit(
            "refusing to run a scored sweep with a dirty working tree; "
            f"commit or stash first:\n{dirty}"
        )


def _exit_code_for(result: SweepResult) -> int:
    return 2 if result.truncated else 0


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
    parser.add_argument(
        "--retry",
        choices=["error"],
        default=None,
        help="also retry rows with this verdict on resume (an 'error' row "
        "already cost money; 'construction_error' rows are always retried "
        "for free)",
    )
    args = parser.parse_args()

    for arm in args.arms:
        if arm not in ARMS:
            raise SystemExit(f"unknown arm: {arm!r}; expected one of {ARMS}")
    for model in args.models:
        if model not in MODELS:
            raise SystemExit(f"unpinned or unknown model: {model}")

    # Before any model call: every result row's commit_sha is the only pin
    # on the library under test, and that pin is only truthful on a clean
    # tree. `out` (this sweep's own results file) is exempt — see
    # `assert_clean_tree`'s docstring.
    assert_clean_tree(out=args.out)

    worst = max(_token_budget_usd(m) for m in args.models)
    admits = int(args.max_spend // worst) if worst > 0 else 0
    print(
        f"reserve ceiling ${worst:.2f}/task ({args.models}); "
        f"${args.max_spend:.2f} cap admits up to {admits} worst-case task(s) "
        "before the first observation tightens it"
    )

    golds, golds_hash = _load_golds(args.golds)
    tasks = [t for t in json.loads(args.tasks.read_text()) if t["task_id"] in golds]
    if args.n:
        tasks = _stratified_sample(tasks, args.n)

    docs = {
        "manual": Path("data/hf/data/context/manual.md").read_text(),
        "payments_readme": Path("data/hf/data/context/payments-readme.md").read_text(),
    }

    result = sweep(
        tasks,
        tuple(args.arms),
        tuple(args.models),
        golds,
        out=args.out,
        db_path=args.db,
        docs=docs,
        max_spend=args.max_spend,
        golds_hash=golds_hash,
        retry_error=args.retry == "error",
    )
    print(f"spent ${result.spent:.2f}")
    if result.truncated:
        print(
            f"stopped at the ${args.max_spend:.2f} cap; re-run to continue "
            "(resume is automatic)",
            file=sys.stderr,
        )
    raise SystemExit(_exit_code_for(result))


if __name__ == "__main__":
    main()
