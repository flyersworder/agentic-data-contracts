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
  * A CONSTRUCTION FAILURE IS A FINDING, NOT A CRASH — AND IT IS THE *ONLY*
    THING THAT CAN STILL ESCAPE `run_task` UNPRICED. `run_task` raises
    `dce.agent.AgentConstructionError` when its agent factory fails (e.g. a
    missing `OPENROUTER_API_KEY`), before any billable call was made; `sweep`
    catches exactly that type and nothing else, writing a row with
    `verdict: "construction_error"`. Anything else `run_task_fn` raises is a
    real bug and is left to propagate and stop the sweep loudly — `run_task`
    itself guards its whole post-call tail so a bug there returns a priced
    row instead of raising (see `dce/agent.py`'s `_priced_fallback_row`);
    only agent construction is still allowed to raise, and only because
    nothing billable has happened yet when it does. Retries are bounded, not
    infinite: `completed_keys` stops retrying a unit after
    `MAX_CONSTRUCTION_ATTEMPTS` `construction_error` rows and treats it as
    terminal — "cheap twice, then loud" — and `gave_up_keys` lets `main()`
    warn about exactly which units that happened to. As a backstop against
    all of the above regressing at once, `_construction_error_row` prices
    itself at `_token_budget_usd(model)`, not `$0.00`: an unknown-cost
    failure consumes budget instead of reading as free, deliberately, even
    though a genuine construction failure never actually spent anything.
  * THE SPEND CAP BOUNDS THE EXPERIMENT, NOT ONE PROCESS. `sweep` seeds its
    running total from `spent_so_far(out)` — the `usd` already banked by
    every prior invocation against the same `out` file — because resume is
    the headline feature and a cap that resets to $0 on every restart is no
    cap at all under a crash loop or a naive retry wrapper. A row's `usd` is
    read and validated BEFORE it is written to `out`: an un-priced row must
    never land on disk looking like a free, permanently-done unit.
  * THE RESERVATION IS A REAL CEILING, NOT A GUESS, AND IT COVERS A WHOLE
    TASK. Before any observation for a given model, `sweep` reserves
    `dce.agent._token_budget_usd(model)` — the true worst case the runaway
    token guard allows, not a hand-picked dollar figure. Once a model HAS
    been observed, the reservation is the larger of that ceiling and the max
    (not mean) of what it has actually cost so far — never just the
    observed max alone, which would let one cheap early call collapse the
    reservation for every later, possibly-worst-case call (measured: an 82%
    budget overrun this way). The reservation covers an entire task's (arm,
    model) group at once and is checked before the first call in that
    group, so a truncation always lands on a task boundary: a
    resumed/paired analysis never has to discard a half-finished task.
  * A RETRIED UNIT ACCUMULATES ROWS; SCORING MUST DEDUPE. Every attempt at a
    (task_id, arm, model) key appends a new row to `out` rather than
    replacing the old one — `spent_so_far`/`completed_keys`/the reserve
    estimator all deliberately want every row (cumulative real spend,
    accurate retry counts), but a *scorer* wants each unit counted exactly
    once, by its most recent outcome. `latest_rows(out)` returns exactly
    that (last row per key wins); every downstream reader that scores this
    file — Task 9's accuracy/McNemar calculations included — MUST use it
    rather than iterating `out` directly, or a stale `construction_error`
    row sitting earlier in the file gets silently counted as a wrong answer
    on top of the real, later outcome.
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

from dce.agent import AgentConstructionError, _commit_sha, _token_budget_usd, run_task
from dce.arms import ARMS, check_and_restore, make_working_copy
from dce.data import DATASET_REVISION
from dce.frozen import digest
from dce.pricing import MODELS

#: A reservation must never be read as "this call is free." Observed `usd`
#: can legitimately be 0.0 (a `hit_limit`/`error` row that made no billable
#: call), and neither the seed floor nor the observed max may collapse to
#: that value.
MIN_RESERVE_USD: float = 0.01

#: "Cheap twice, then loud": a unit that fails agent construction this many
#: times in a row is treated as terminal rather than retried forever —
#: `dce.agent.AgentConstructionError` is usually a persistent problem (a
#: missing env var, say), not a transient one, so hammering it every resume
#: would never succeed and would only ever hide the same bug.
MAX_CONSTRUCTION_ATTEMPTS: int = 2


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


def _construction_error_state(
    path: Path,
) -> tuple[dict[tuple[str, str, str], str | None], dict[tuple[str, str, str], int]]:
    """For every (task_id, arm, model) key seen in `path`: its most recently
    written verdict, and how many `construction_error` rows it has
    accumulated in total. Shared by `completed_keys` (resume/skip
    decisions) and `gave_up_keys` (visibility into units that exhausted
    their retry budget) so the two can never disagree about what "the
    state of this key" means.
    """
    last_verdict: dict[tuple[str, str, str], str | None] = {}
    error_counts: dict[tuple[str, str, str], int] = {}
    for row in _read_rows(path):
        key = (row["task_id"], row["arm"], row["model"])
        verdict = row.get("verdict")
        last_verdict[key] = verdict
        if verdict == "construction_error":
            error_counts[key] = error_counts.get(key, 0) + 1
    return last_verdict, error_counts


def completed_keys(
    path: Path, *, retry_error: bool = False
) -> set[tuple[str, str, str]]:
    """(task_id, arm, model) triples a resumed sweep should skip.

    Verdict-aware, not a blanket "every row already written is done", and
    keyed off each unit's MOST RECENT row (a retried unit accumulates
    several; see `latest_rows`), not merely "any row exists":

      * `construction_error` — retried (not done) until
        `MAX_CONSTRUCTION_ATTEMPTS` such rows have piled up for the same
        key, at which point it is terminal: "cheap twice, then loud" rather
        than free-forever (see `gave_up_keys`). Bounding this matters
        because `run_task`'s `except Exception` around agent construction
        also catches an ordinary bug (a signature mismatch, say); unbounded
        free retries would hide such a bug behind a wall of skip-forever
        rows forever, instead of surfacing it loudly after two tries.
      * `error` — done unless `retry_error` is set, since an `error` row
        already cost money and a resume should not silently re-pay for it
        without being asked.
      * `hit_limit` / `scoring_error` — need no special case: both are
        terminal by design (a `hit_limit` needs a deliberate higher-budget
        re-run, not an automatic retry at the same budget; a
        `scoring_error` is fixed by re-scoring the stored `answer` offline,
        never by re-paying for the model call), so they simply fall through
        to "done" like any completed row.
      * No verdict at all (an older/foreign row shape) — also treated as
        done, matching this function's pre-verdict-aware behaviour.
    """
    last_verdict, error_counts = _construction_error_state(path)
    done: set[tuple[str, str, str]] = set()
    for key, verdict in last_verdict.items():
        if verdict == "construction_error":
            if error_counts[key] >= MAX_CONSTRUCTION_ATTEMPTS:
                done.add(key)  # retries exhausted: terminal, stop retrying
            continue
        if verdict == "error" and retry_error:
            continue
        done.add(key)
    return done


def gave_up_keys(path: Path) -> set[tuple[str, str, str]]:
    """Keys whose unit hit `MAX_CONSTRUCTION_ATTEMPTS` `construction_error`
    rows and will therefore never be attempted again by `completed_keys`.
    Exposed so `main()` can print a loud warning about exactly which units
    that happened to, rather than the sweep quietly going silent on them
    forever — "cheap twice, then loud" needs the "loud" half to be visible
    somewhere other than a grep through `out`.
    """
    last_verdict, error_counts = _construction_error_state(path)
    return {
        key
        for key, verdict in last_verdict.items()
        if verdict == "construction_error"
        and error_counts[key] >= MAX_CONSTRUCTION_ATTEMPTS
    }


def latest_rows(path: Path) -> list[dict]:
    """One row per (task_id, arm, model) key: the LAST row written for that
    key wins.

    A retried unit accumulates multiple rows across resumes — a
    `construction_error` that hasn't yet hit `MAX_CONSTRUCTION_ATTEMPTS`, a
    `--retry error` re-run, or simply a resumed sweep picking a unit back
    up — and only the most recent one reflects its true current state.
    Measured: three failing resumes then a success left four rows for one
    key; a scorer iterating raw rows counts the three stale
    `construction_error` rows as wrong answers on top of the real success
    (that unit would score 1/4 = 25% despite succeeding). EVERY READER OF
    THIS FILE FOR SCORING — Task 9's accuracy/McNemar calculations
    included — MUST call this instead of iterating `out` directly; see the
    module docstring.
    """
    last: dict[tuple[str, str, str], dict] = {}
    for row in _read_rows(path):
        last[(row["task_id"], row["arm"], row["model"])] = row
    return list(last.values())


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
    observed, the estimate is the LARGER of that same `floor` and the MAX
    (not the mean) of what it has actually cost — `floor` must stay in the
    comparison even after an observation exists: an early cheap call (a
    short, easy task) would otherwise collapse the reservation toward
    whatever it happened to cost, discarding the real per-call ceiling for
    every later, possibly-worst-case task (measured: an 82% budget overrun
    doing exactly that with only physically-possible costs). A mean is
    rejected for the same reason `floor` must not be discarded: it
    guarantees roughly half of all calls exceed the reservation by
    construction, and using the max instead costs nothing extra once the
    ceiling is already known to be much larger. Either way, never returns a
    figure at or below `MIN_RESERVE_USD` — a $0 reservation would let the
    guard treat the next call as free.
    """
    if not observed:
        return max(floor, MIN_RESERVE_USD)
    return max(max(observed), floor, MIN_RESERVE_USD)


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
    task: dict,
    arm: str,
    model: str,
    gold: str,
    golds_hash: str,
    exc: AgentConstructionError,
) -> dict:
    """A row with the same shape `dce.agent.build_result_row` produces, for
    a task that never got far enough to build one — `run_task` raised
    `dce.agent.AgentConstructionError` (e.g. a missing `OPENROUTER_API_KEY`)
    before any model call was made. Everything `sweep` actually knows is
    filled in (`level` matters: `accuracy_by(rows, "level")` needs it on
    every row, not just the ones that ran); only the token/turn fields,
    which have no meaningful value for a call that never happened, are
    zeroed.

    `usd` is priced at `_token_budget_usd(model)` — the pessimistic
    per-task ceiling — not `$0.00`, even though a genuine
    `AgentConstructionError` really did cost nothing. This is a deliberate
    backstop, not an accounting truth: it is the third of three layers
    against the same failure class (see the module docstring), there so
    that even if the other two — a non-throwing `run_task` tail, and
    bounded retries — ever regress together, an unknown-cost failure still
    consumes budget instead of reading as free and running unbounded.
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
        "usd": _token_budget_usd(model),
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
                except AgentConstructionError as exc:
                    # The ONLY exception `run_task_fn` is expected to raise
                    # (see the module docstring and `AgentConstructionError`
                    # itself) — anything else is a real bug and is left to
                    # propagate, stopping the sweep loudly rather than
                    # silently treating a possibly-billable failure as a
                    # free construction error.
                    row = _construction_error_row(
                        by_id[task_id], arm, model, gold, golds_hash, exc
                    )

                # Valid only now: `run_task_fn` has closed its own arm's
                # connection (see `dce/arms.py`'s module docstring, CALL
                # ORDER).
                integrity = check_and_restore(working, db_path)
                row["db_corrupted"] = integrity.corrupted

                # Read and validate the price BEFORE writing: a row must
                # never land on disk un-priced-but-looking-done — that
                # would make a resumed sweep treat it as both free and
                # permanently finished. A row missing `usd` entirely raises
                # `KeyError` here, before `fh.write`, so nothing partial
                # ever hits the file (measured without this ordering: an
                # un-priced row landed on disk as `verdict: "correct"` and
                # was thereafter free and done forever). `usd: None` (an
                # explicit, known "couldn't price this") is handled, not
                # crashed on.
                usd = row["usd"]
                if usd is None:
                    usd = 0.0

                fh.write(json.dumps(row) + "\n")
                fh.flush()

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
      * `out` itself — the sweep's own results file, exactly, NOT its
        parent directory — is excluded from the dirty check. `results/` is
        deliberately committed (the tamper-evidence claim depends on it
        being readable in git history), so `out` being freshly written
        between commits is expected and would otherwise make every resumed
        sweep refuse to start. Exempting the whole directory instead of
        just `out` would let a change to a DIFFERENT, PREVIOUS run's
        results file — an edit or deletion — pass silently; only the exact
        file this invocation writes to is exempt. Everything else,
        including every other file already sitting in `results/`, must
        still be clean.

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
            lines = [line for line in lines if _porcelain_path(line) != out_rel]

    dirty = "\n".join(lines).strip()
    if dirty:
        raise SystemExit(
            "refusing to run a scored sweep with a dirty working tree; "
            f"commit or stash first:\n{dirty}"
        )


def _exit_code_for(result: SweepResult) -> int:
    return 2 if result.truncated else 0


def _worst_case_task_group_usd(arms, models) -> float:
    """The true worst-case reservation `sweep` checks before starting one
    task's group — every arm x every model's `_token_budget_usd` ceiling,
    summed — not one call's ceiling alone. Printing the per-call figure
    instead overstates how many tasks a cap admits by a factor of
    `len(arms)`: measured, printing $0.18/task (one model's ceiling)
    against a real per-group cost of $0.55 (three arms) claimed 27
    admissible tasks where the truth was 9; with two models the same
    mistake printed $7.32 against an actual $22.51.
    """
    return len(arms) * sum(_token_budget_usd(m) for m in models)


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
        "already cost money; 'construction_error' rows are retried "
        f"automatically, up to {MAX_CONSTRUCTION_ATTEMPTS} attempts, "
        "regardless of this flag)",
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

    worst_group = _worst_case_task_group_usd(args.arms, args.models)
    admits = int(args.max_spend // worst_group) if worst_group > 0 else 0
    print(
        f"reserve ceiling ${worst_group:.2f}/task-group "
        f"({len(args.arms)} arms x {args.models}); ${args.max_spend:.2f} cap "
        f"admits up to {admits} worst-case task-group(s) before the first "
        "observation tightens it"
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

    gave_up = gave_up_keys(args.out)
    if gave_up:
        print(
            f"WARNING: {len(gave_up)} unit(s) gave up after "
            f"{MAX_CONSTRUCTION_ATTEMPTS} failed agent-construction attempts "
            "and will NOT be retried automatically (fix the underlying "
            "problem, e.g. a missing OPENROUTER_API_KEY, then re-run): "
            + ", ".join(f"{t}/{a}/{m}" for t, a, m in sorted(gave_up)),
            file=sys.stderr,
        )

    raise SystemExit(_exit_code_for(result))


if __name__ == "__main__":
    main()
