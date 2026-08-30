"""Sweep driver: resumable, budget-capped, one JSONL row per unit of work.

This is the component that spends real money, so its guards matter as much
as its logic. THE CENTRAL INVARIANT, stated once, that every rule below
exists to uphold:

    ONCE A PRICED ROW EXISTS, IT REACHES DISK BEFORE ANYTHING ELSE MAY
    THROW.

`run_task_fn` returning (or `_construction_error_row` building a
substitute) is the moment a row becomes "priced" — real spend, or a
deliberate pessimistic guard charge, is now a fact that must be recorded.
Everything between that moment and `fh.write` — `check_and_restore`,
`json.dumps`, updating `spent` — is now wrapped so that a failure THERE
still lets the row land on disk (with the failure noted on it) before any
exception propagates. This was gotten wrong twice already, in the same
component, at two different frames: first by treating `run_task_fn`'s
whole outcome as unpriced on ANY exception (see `AgentConstructionError`'s
narrowing below), and then — after that was fixed — by discovering that
the loop calling it had exactly the same problem one frame later
(`check_and_restore` itself raising, or `json.dumps` raising, discarded an
already-priced row the same way). The invariant above is the fix stated
once instead of patched frame by frame.

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
  * A CORRUPTED DB IS A FINDING, NOT A CRASH — AND A FAILED CHECK IS A
    DIFFERENT FINDING, NOT A LOST ROW. `check_and_restore` itself can raise
    (a real `PermissionError` from `_sha256`, a full disk on the repair
    `copyfile`) — it is not an exotic caller: corruption is the EXPECTED
    outcome for the ungoverned arms this experiment studies, so its
    `unlink` + multi-MB `copyfile` path runs often, and its triggers
    (unreadable pristine file, full disk) are persistent, which is
    precisely when a resuming wrapper hits it again and again. Per the
    invariant above, a `check_and_restore` failure still gets the row
    written — `db_corrupted: None` (unknown, not `False`) plus an
    `integrity_error` note — with `spent` updated, and ONLY THEN
    re-raises, so the sweep still stops loudly on a persistent environment
    problem without losing the accounting for the call that already
    happened.
  * A CLOSE FAILURE INVALIDATES THE CHECK THAT FOLLOWS IT, AND MUST SAY SO.
    `run_task`'s `setup.close()` runs inside its own `try/except`, so a
    close failure there no longer replaces a good row — but it must not be
    SILENT either: if the connection did not actually close, this sweep's
    subsequent `check_and_restore` call is not a valid check (see
    `dce/arms.py`'s CALL ORDER — a check against a live connection can
    report a repair that does not survive that connection's later
    checkpoint-on-close, and a leaked connection's own effects can then be
    misattributed to the arm under test on a LATER task too). `run_task`
    stamps `close_error` onto the row when this happens; `sweep` treats
    such a row's integrity result as untrustworthy (`db_corrupted: None`)
    regardless of what `check_and_restore` actually reported, rather than
    trusting a check it cannot vouch for.
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
    nothing billable has happened yet when it does. Retries are bounded on
    TWO levels, not one:
      - Per-key: `completed_keys` stops retrying a unit after
        `MAX_CONSTRUCTION_ATTEMPTS` TRAILING `construction_error` rows
        (consecutive at the END of that key's history, not a lifetime
        total — a key that once failed, then succeeded, then failed again
        must not be given up on after that one fresh failure) and treats
        it as terminal — "cheap twice, then loud" — and `gave_up_keys` lets
        `main()` warn about exactly which units that happened to.
      - Sweep-wide: `CIRCUIT_BREAKER_THRESHOLD` CONSECUTIVE
        `construction_error` outcomes, across however many DIFFERENT keys,
        stops the whole sweep immediately. A missing `OPENROUTER_API_KEY`
        fails every unit identically; without this, per-key bounding alone
        would still write up to `len(tasks) x len(arms) x len(models) x
        MAX_CONSTRUCTION_ATTEMPTS` phantom-priced rows before the SPEND cap
        (not the circuit breaker) finally noticed — 2,436 rows and ~$446 of
        guard-ledger charges at 406 tasks x 3 arms, all before
        `gave_up_keys` is what an operator happens to check.
  * SEPARATE REAL SPEND FROM GUARD SPEND. Pricing a `construction_error`
    pessimistically is right for the CAP (an unknown-cost failure must not
    read as free) but wrong for the LEDGER (a genuine construction failure
    spent nothing real). Every row therefore carries two fields: `usd`
    (what was actually billed — 0.0 for a genuine `construction_error`) and
    `usd_guard` (what the spend cap counts — the pessimistic ceiling for a
    `construction_error`, identical to `usd` for every other verdict, since
    a real call really did happen). `spent_so_far` sums `usd_guard` — the
    cap's own ledger; `real_spent_so_far` sums `usd` — what to actually
    report as spent. `main()` prints the real figure and treats the guard
    figure as bookkeeping, not an accounting claim.
  * THE SPEND CAP BOUNDS THE EXPERIMENT, NOT ONE PROCESS. `sweep` seeds its
    running total from `spent_so_far(out)` — the `usd_guard` already banked
    by every prior invocation against the same `out` file — because resume
    is the headline feature and a cap that resets to $0 on every restart is
    no cap at all under a crash loop or a naive retry wrapper. A row's
    `usd`/`usd_guard` are read and VALIDATED (a real, finite, non-negative
    number, or `None` treated as 0.0 — never a string, a bool, a negative
    figure, or NaN) BEFORE the row is written to `out`: an invalid or
    un-priced value must never land on disk looking like a free,
    permanently-done unit that then bricks every future `spent_so_far`
    read with no recovery but hand-editing JSONL.
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

Two gaps recorded here as deliberately unfixed, not silently tolerated —
see `dce/agent.py`'s module docstring for the full detail on both:

  * `KeyboardInterrupt` during a live model call escapes everything in
    both this module and `run_task` (measured: 5 interrupts, ~$6.00 real
    spend, 0 rows). Accepted: it is user-initiated, and a sweep the
    operator is actively killing does not need its own bookkeeping to
    survive the kill.
  * `TOKEN_BUDGET` has roughly 25% slack against the true per-request
    ceiling, because `total_tokens_limit` is only checked BETWEEN requests.
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
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
#: TRAILING times (consecutive at the end of its own history, not a
#: lifetime total — see `_construction_error_state`) is treated as terminal
#: rather than retried forever. `dce.agent.AgentConstructionError` is
#: usually a persistent problem (a missing env var, say), not a transient
#: one, so hammering it every resume would never succeed and would only
#: ever hide the same bug.
MAX_CONSTRUCTION_ATTEMPTS: int = 2

#: A sweep-wide safety valve distinct from the per-key cap above: this many
#: CONSECUTIVE `construction_error` outcomes, across however many different
#: keys, stops the whole sweep immediately rather than grinding through
#: every remaining unit at the same guaranteed-to-fail cost. Deliberately
#: larger than `MAX_CONSTRUCTION_ATTEMPTS` (which bounds retries of ONE
#: key across resumes) — this bounds a systemic failure WITHIN one
#: invocation, before it can write thousands of phantom-priced rows.
CIRCUIT_BREAKER_THRESHOLD: int = 5


@dataclass
class SweepResult:
    """`spent`: total `usd_guard` banked in `out` after this call — the
    cap's own ledger, resumed sweeps included. `real_spent`: total `usd`
    actually billed — what to report as spent. `truncated`: True iff this
    call stopped before completing `pending()` because the next task
    group's reservation would have exceeded `max_spend`. `circuit_broken`:
    True iff this call stopped because `CIRCUIT_BREAKER_THRESHOLD`
    consecutive construction failures looked like a systemic problem, not
    per-task bad luck. `main()` uses `truncated`/`circuit_broken` to exit
    non-zero so a wrapper can tell a capped-out or broken-circuit run from
    a completed one.
    """

    spent: float
    real_spent: float
    truncated: bool
    circuit_broken: bool = False


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _construction_error_state(
    path: Path,
) -> tuple[dict[tuple[str, str, str], str | None], dict[tuple[str, str, str], int]]:
    """For every (task_id, arm, model) key seen in `path`: its most recently
    written verdict, and how many TRAILING `construction_error` rows it has
    accumulated — consecutive at the end of its history, not a lifetime
    total. A row with any other verdict resets that key's trailing count to
    0 even though it may later be overwritten by a fresh
    `construction_error`; a file reading `construction_error, correct,
    construction_error` for the same key therefore has a trailing count of
    1, not 2 — the intervening success means this is one fresh failure, not
    the second half of a persistent one, and must not be given up on after
    only one recent attempt.

    Shared by `completed_keys` (resume/skip decisions) and `gave_up_keys`
    (visibility into units that exhausted their retry budget) so the two
    can never disagree about what "the state of this key" means.
    """
    last_verdict: dict[tuple[str, str, str], str | None] = {}
    trailing_counts: dict[tuple[str, str, str], int] = {}
    for row in _read_rows(path):
        key = (row["task_id"], row["arm"], row["model"])
        verdict = row.get("verdict")
        last_verdict[key] = verdict
        if verdict == "construction_error":
            trailing_counts[key] = trailing_counts.get(key, 0) + 1
        else:
            trailing_counts[key] = 0
    return last_verdict, trailing_counts


def completed_keys(
    path: Path, *, retry_error: bool = False
) -> set[tuple[str, str, str]]:
    """(task_id, arm, model) triples a resumed sweep should skip.

    Verdict-aware, not a blanket "every row already written is done", and
    keyed off each unit's MOST RECENT row (a retried unit accumulates
    several; see `latest_rows`), not merely "any row exists":

      * `construction_error` — retried (not done) until
        `MAX_CONSTRUCTION_ATTEMPTS` TRAILING such rows have piled up for
        the same key (see `_construction_error_state`), at which point it
        is terminal: "cheap twice, then loud" rather than free-forever
        (see `gave_up_keys`). Bounding this matters because `run_task`'s
        `except Exception` around agent construction also catches an
        ordinary bug (a signature mismatch, say); unbounded free retries
        would hide such a bug behind a wall of skip-forever rows forever,
        instead of surfacing it loudly after two tries.
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
    last_verdict, trailing_counts = _construction_error_state(path)
    done: set[tuple[str, str, str]] = set()
    for key, verdict in last_verdict.items():
        if verdict == "construction_error":
            if trailing_counts[key] >= MAX_CONSTRUCTION_ATTEMPTS:
                done.add(key)  # retries exhausted: terminal, stop retrying
            continue
        if verdict == "error" and retry_error:
            continue
        done.add(key)
    return done


def gave_up_keys(path: Path) -> set[tuple[str, str, str]]:
    """Keys whose unit hit `MAX_CONSTRUCTION_ATTEMPTS` TRAILING
    `construction_error` rows and will therefore never be attempted again
    by `completed_keys`. Exposed so `main()` can print a loud warning about
    exactly which units that happened to, rather than the sweep quietly
    going silent on them forever — "cheap twice, then loud" needs the
    "loud" half to be visible somewhere other than a grep through `out`.
    """
    last_verdict, trailing_counts = _construction_error_state(path)
    return {
        key
        for key, verdict in last_verdict.items()
        if verdict == "construction_error"
        and trailing_counts[key] >= MAX_CONSTRUCTION_ATTEMPTS
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
    """Total `usd_guard` already banked in `path`, across every prior
    invocation that appended to it — the spend CAP's own ledger. Seeds
    `sweep`'s running total so `max_spend` bounds the experiment (every
    resume put together), not one process's lifetime.

    Falls back to a row's `usd` when `usd_guard` is absent (an older row
    shape from before the two were split — see the module docstring's
    SEPARATE REAL SPEND FROM GUARD SPEND section) and is tolerant of a
    missing/null value entirely — unlike the strict validation `sweep`
    itself applies to rows it is producing right now; see that function's
    docstring for why the two need different strictness.
    """
    total = 0.0
    for row in _read_rows(path):
        usd = row.get("usd_guard", row.get("usd"))
        if usd:
            total += usd
    return total


def real_spent_so_far(path: Path) -> float:
    """Total `usd` — real, billed dollars — already banked in `path`. This
    is what `main()` reports as "spent"; `spent_so_far`'s `usd_guard` total
    is the cap's bookkeeping figure, not an accounting claim (a unit with
    two pessimistically-priced construction errors and one real $0.01
    success has `spent_so_far` well above $0.01 but `real_spent_so_far`
    exactly $0.01).
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
    """Per-model GUARD-cost history from `path` (`usd_guard`, falling back
    to `usd` for an older row shape), so a resume's very first reservation
    for each model is exactly as informed as if the process had never
    restarted. Only rows with a positive value count — a `hit_limit`/`error`
    row's `usd_guard: 0.0` must not be allowed to decay the reservation
    toward the floor. Reads `usd_guard`, not `usd`, deliberately: a
    `construction_error` row's pessimistic guard charge IS a valid signal
    for what this model's worst case looks like, even though its `usd` is
    genuinely 0.0 — see the module docstring's SEPARATE REAL SPEND FROM
    GUARD SPEND section.
    """
    by_model: dict[str, list[float]] = {}
    for row in _read_rows(path):
        usd_guard = row.get("usd_guard", row.get("usd"))
        model = row.get("model")
        if usd_guard and model:
            by_model.setdefault(model, []).append(usd_guard)
    return by_model


def _validated_usd(value, field_name: str) -> float:
    """`usd`/`usd_guard` must be a real, finite, non-negative number before
    a row is allowed to reach disk. `None` is the one explicit "couldn't
    price this" signal and is treated as `0.0`; anything else invalid — a
    string, a `bool` (a `bool` IS an `int` in Python; explicitly excluded
    so `True` cannot silently price a row at $1.00), a negative figure, NaN
    — raises loudly here rather than landing on disk. Measured without
    this: a stray `"1.20"` string wrote fine, then `spent +=` raised on
    every subsequent read of the file — bricking `spent_so_far` for that
    results file permanently, with no recovery but hand-editing JSONL; six
    negative rows produced `spent = -600.0`.
    """
    if value is None:
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number or None, got {value!r}")
    value = float(value)
    if math.isnan(value):
        raise ValueError(f"{field_name} is NaN")
    if value < 0:
        raise ValueError(f"{field_name} is negative: {value!r}")
    return value


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

    `usd` — the REAL figure — is `0.0`: a genuine `AgentConstructionError`
    really did cost nothing. `usd_guard` — what the spend cap counts — is
    `_token_budget_usd(model)`, the pessimistic per-task ceiling, not
    `$0.00`. This split is deliberate: pricing the CAP pessimistically is a
    backstop (the third of three layers against the same failure class, see
    the module docstring) so that even if a non-throwing `run_task` tail
    and bounded retries ever regress together, an unknown-cost failure
    still consumes cap budget instead of reading as free — but pricing the
    LEDGER (`usd`) the same way would misrepresent a $0 failure as real
    spend, which is exactly the "spent is no longer an accounting figure"
    bug this split fixes.
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
        "usd_guard": _token_budget_usd(model),
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
    would push spend past `max_spend`, or `CIRCUIT_BREAKER_THRESHOLD`
    consecutive construction failures signal a systemic problem.

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
    real_spent = real_spent_so_far(out)
    observed = _seed_observed_by_model(out)

    if not todo:
        # Nothing to do: don't even pay for a working-copy file write.
        return SweepResult(spent=spent, real_spent=real_spent, truncated=False)

    working = make_working_copy(db_path, _working_db_path(db_path))
    truncated = False
    circuit_broken = False
    consecutive_construction_errors = 0

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

                # INVARIANT (see module docstring): `row` now exists and is
                # priced. Everything below must let it reach disk before
                # anything else may throw.

                # `run_task` stamps `close_error` when its OWN `setup.close()`
                # failed — meaning the connection may still be open, so
                # `check_and_restore` below is not a valid check regardless
                # of what it reports (see `dce/arms.py`'s CALL ORDER). Valid
                # otherwise: `run_task_fn` has closed its own arm's
                # connection.
                integrity_untrustworthy = row.get("close_error") is not None
                integrity_exc: Exception | None = None
                try:
                    integrity = check_and_restore(working, db_path)
                    row["db_corrupted"] = (
                        None if integrity_untrustworthy else integrity.corrupted
                    )
                except Exception as exc:
                    row["db_corrupted"] = None
                    integrity_exc = exc
                if integrity_untrustworthy:
                    note = (
                        "setup.close() failed for this task "
                        f"({row['close_error']}); the connection may still "
                        "be open, so check_and_restore's result cannot be "
                        "trusted"
                    )
                    row["integrity_error"] = (
                        f"{note}; check_and_restore also raised "
                        f"{type(integrity_exc).__name__}: {integrity_exc}"
                        if integrity_exc is not None
                        else note
                    )
                elif integrity_exc is not None:
                    row["integrity_error"] = (
                        f"{type(integrity_exc).__name__}: {integrity_exc}"
                    )

                # Validate the price BEFORE writing: a row must never land
                # on disk un-priced-or-invalid-but-looking-done — that
                # would make a resumed sweep treat it as both free (or
                # corruptly mispriced) and permanently finished. A row
                # missing `usd` entirely raises `KeyError` here, before
                # `fh.write`, so nothing partial ever hits the file.
                usd = _validated_usd(row["usd"], "usd")
                usd_guard = _validated_usd(row.get("usd_guard", usd), "usd_guard")

                # A field that turned out not to be JSON-serializable must
                # not lose the row — `default=repr` guarantees SOMETHING
                # legible lands on disk instead of the row vanishing.
                try:
                    line = json.dumps(row)
                except Exception:
                    line = json.dumps(row, default=repr)
                fh.write(line + "\n")
                fh.flush()

                spent += usd_guard
                real_spent += usd
                if usd_guard > 0:
                    observed.setdefault(model, []).append(usd_guard)

                if row.get("verdict") == "construction_error":
                    consecutive_construction_errors += 1
                else:
                    consecutive_construction_errors = 0
                if consecutive_construction_errors >= CIRCUIT_BREAKER_THRESHOLD:
                    circuit_broken = True

                # Only now, after the row is safely on disk and every
                # counter above is updated, does a `check_and_restore`
                # failure get to stop the sweep — loudly, as any other
                # persistent environment problem should, but without
                # losing the row or the spend it represents.
                if integrity_exc is not None:
                    raise integrity_exc

                if circuit_broken:
                    print(
                        f"stopping: {CIRCUIT_BREAKER_THRESHOLD} consecutive "
                        "construction errors across different units — "
                        "likely a systemic problem (missing "
                        "OPENROUTER_API_KEY? a bad model id?), not "
                        "per-task bad luck"
                    )
                    break
            if circuit_broken:
                break
    return SweepResult(
        spent=spent,
        real_spent=real_spent,
        truncated=truncated,
        circuit_broken=circuit_broken,
    )


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
    if result.circuit_broken:
        return 3
    if result.truncated:
        return 2
    return 0


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
        f"automatically, up to {MAX_CONSTRUCTION_ATTEMPTS} trailing "
        "attempts, regardless of this flag)",
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
    # Real dollars, not the guard ledger — see the module docstring's
    # SEPARATE REAL SPEND FROM GUARD SPEND section.
    print(f"spent ${result.real_spent:.2f} (guard ledger ${result.spent:.2f})")
    if result.circuit_broken:
        print(
            f"stopped: {CIRCUIT_BREAKER_THRESHOLD} consecutive construction "
            "errors across different units — this looks systemic (a "
            "missing OPENROUTER_API_KEY, say), not per-task bad luck; fix "
            "the underlying problem before re-running",
            file=sys.stderr,
        )
    elif result.truncated:
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
