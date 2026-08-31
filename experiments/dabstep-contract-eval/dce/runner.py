"""Sweep driver: resumable, budget-capped, one JSONL row per unit of work.

This is the component that spends real money, so its guards matter as much
as its logic. THE CENTRAL INVARIANT, stated once, that every rule below
exists to uphold:

    ONCE A PRICED ROW EXISTS, IT REACHES DISK BEFORE ANYTHING ELSE MAY
    THROW.

`run_task_fn` returning (or `_construction_error_row` building a
substitute) is the moment a row becomes "priced" — real spend, or a
deliberate pessimistic guard charge, is now a fact that must be recorded.

STRUCTURAL, NOT A SERIES OF GUARDED STATEMENTS: there is exactly ONE site
in `sweep`'s loop body that writes a row (`_safe_json_dumps` + `fh.write`),
and every step between "`row` exists" and that site — the integrity check,
pricing validation — COLLECTS its failure onto the row (an `integrity_error`
or `pricing_error` note, a normalized/pessimistic value swapped in) instead
of raising through it. A collected exception, if any, is held in
`pending_exc` and only gets to propagate AFTER the write, the flush, and
the `spent`/`real_spent` update. This was gotten wrong THREE times already
in this one component, at three different frames, each reproducing the
identical signature (a resumed sweep burning real money while
`spent_so_far`/rows-on-disk stayed at zero) via a different mechanism:

  1. `run_task_fn`'s whole outcome was treated as unpriced on ANY
     exception (fixed by `AgentConstructionError`'s narrowing, below, plus
     `run_task`'s own guarded post-call tail in `dce/agent.py`).
  2. The loop calling it had the identical problem one frame later:
     `check_and_restore` itself raising, or `json.dumps` raising,
     discarded an already-priced row the same way (fixed by writing
     before re-raising, and by `_safe_json_dumps`'s fallback layers).
  3. The FIX for (2) still left two statements between the two guarded
     calls unguarded: `_validated_usd(row["usd"], ...)` and
     `_validated_usd(row["usd_guard"], ...)` raised and lost the row on a
     `Decimal`, a NaN, an infinite value, or a missing key — the identical
     bug, one frame further in, dressed as "validate before writing"
     rather than "guard this specific call". Fixed by making pricing
     validation collect-and-normalize (see `_priced_or_pessimistic`)
     instead of raise-and-lose, using the SAME policy `check_and_restore`
     failures already got, rather than the opposite one.

The regression pin for this is
`test_sweep_lands_the_row_no_matter_what_fails_between_return_and_write`, a
parametrized test that injects a failure at every known point in this
sequence (a bad `usd`, a bad `usd_guard`, a non-`str` dict key, a circular
reference, `check_and_restore` raising) and asserts the row always lands —
so a fifth frame introduced later has to explicitly break a test, not slip
past silently the way the first three did.

A FULL DISK IS HANDLED ON BOTH SIDES, not documented away: `fh.write`/
`fh.flush` failing dumps the serialized row to stderr before re-raising
(so a priced row is recoverable by hand from the log), and `_read_rows`
forgives an unparseable FINAL line — a torn tail is what a killed or
out-of-space append leaves behind, and raising on it used to make the
entire accumulated results file unreadable to every reader at once,
turning a one-row loss into a dead paid sweep. A corrupt line anywhere
but the last still raises. The READ side's forgiveness has a WRITE side
counterpart, `_repair_torn_tail`, called once before `sweep`'s append loop
opens `out`: `out.open("a")` appends onto whatever bytes are already
there, torn tail included, which MERGES the next row onto it instead of
starting a fresh line — silently swallowing that row (it reads back as
torn again, so the unit is re-attempted and re-paid forever) and then, on
the very next resume, pushing the still-corrupt merged line off the tail
entirely, which bricks every reader for the whole file instead of just
one row. `_repair_torn_tail` truncates a genuinely torn tail before the
first append (matching what the read side already forgives) rather than
appending onto it.

ONE EDGE OF "REACHES DISK" THAT IS DOCUMENTED, NOT CLOSED: `flush()` is
not `fsync()`: it hands the bytes to the OS's page cache, which
survives THIS PROCESS dying (a crash, a `raise`, `SIGKILL`) but not the
MACHINE dying before the OS itself flushes that cache to disk. "Reaches
disk" in this module means "survives process death", not "survives power
loss".

EXIT CODE TAXONOMY (`main()`, via `_exit_code_for` plus one path it does
NOT cover):

  * `0` — completed cleanly.
  * `1` — an UNCAUGHT exception propagated out of `main()` entirely (e.g.
    a `check_and_restore` failure with no working `except` above it, or
    genuinely any other bug) — Python's own default for an uncaught
    exception, not something `_exit_code_for` assigns. No final `spent`
    line is printed in this case: the traceback is the last thing on
    stderr, and the row(s) already written are the only record of what
    happened up to that point.
  * `2` — `SweepResult.truncated`: the spend cap would have been exceeded.
  * `3` — `SweepResult.circuit_broken`: too many consecutive construction
    failures across different units.
  * `4` — `SweepResult.connection_leaked`: a DuckDB connection failed to
    close; the working copy's integrity substrate can no longer be
    trusted for this invocation.

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
  * A LEAKED CONNECTION STOPS THE SWEEP — IT DOES NOT JUST FLAG ONE ROW.
    `run_task`'s `setup.close()` runs inside its own `try/except`, so a
    close failure there no longer replaces a good row — but it must not be
    SILENT either: if the connection did not actually close, this sweep's
    `check_and_restore` call is not a valid check (see `dce/arms.py`'s CALL
    ORDER — a check against a live connection can report a repair that
    does not survive that connection's later checkpoint-on-close). Worse,
    a live leaked connection can keep mutating the SAME working copy
    underneath whatever runs next, so EVERY later task's check in this
    sweep would be equally untrustworthy — and each of those later rows
    would carry a `db_corrupted` value that looks just as authoritative as
    a real one. The experiment's headline governance finding is "did an
    ungoverned arm mutate the warehouse", so a stream of unreliable
    `db_corrupted` values is worse than no sweep at all. `run_task` stamps
    `close_error` onto the row when this happens; `sweep` writes that row
    (`db_corrupted: None`, the invariant above still holds) and then STOPS
    THE WHOLE SWEEP — a distinct outcome (`SweepResult.connection_leaked`,
    a distinct exit code) from a budget truncation or a circuit break.
    Resume already works, so restarting in a fresh process (a fresh
    working copy) loses nothing but the one task in flight; stopping
    loudly on a rare failure is far cheaper than silently producing rows
    nobody can trust.
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
    `usd`/`usd_guard` are VALIDATED (a real, finite, non-negative number,
    or `None` treated as 0.0 — never a string, a `Decimal`, a bool, a
    negative figure, NaN, or `float("inf")`) before it contributes to
    either total — but an invalid value is NORMALIZED to the pessimistic
    per-model ceiling with a `pricing_error` note (see
    `_priced_or_pessimistic`), not raised and discarded: the row still
    reaches disk (the central invariant above) either way, so an invalid
    or un-priced value can no longer land on disk looking like a free,
    permanently-done unit, and it no longer costs the row itself to say
    so.
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

CONCURRENCY (`--workers N`, default 1). `sweep` runs N TASK GROUPS at once
on N threads. The unit of dispatch is the whole group (every arm x model for
one task), never an individual unit, which is what preserves two properties
the serial loop got for free: a truncation still lands on a task boundary (so
a paired analysis never has a half-finished task), and one task's arms still
share one copy of the database.

Four pieces of state became shared, and each is handled explicitly rather
than hoped about:

  * THE RESULTS FILE. Still exactly ONE write site, now inside
    `_SweepLedger.record`, which holds the lock across write + flush +
    spend update. An unsynchronized `write`/`flush` interleaves partial
    lines, and a torn line in the MIDDLE of the file (not the tail) is the
    one corruption `_read_rows` deliberately refuses to forgive — it would
    brick every reader of the whole paid sweep at once.
  * THE SPEND LEDGER. Serially, `spent` alone was a sufficient basis for the
    cap check because nothing was ever in flight. With N groups running,
    N-1 of them have spent money no row has banked yet, so the reservation
    is now HELD from dispatch to completion (`_SweepLedger.reserved`) and
    the check is `spent + reserved + this group`. This double-counts a
    running group; the error is always toward stopping early, which is the
    only direction a spend guard may be wrong in.
  * THE WORKING COPY — the one that is not merely a data race. Each worker
    gets its OWN copy (`<db>.working`, `<db>.working-1`, ...). One shared
    copy would put `check_and_restore`'s whole-file repair `copyfile` in the
    middle of another worker's live query, and would destroy the
    attribution of `db_corrupted` to the arm that caused it — and
    `db_corrupted` IS this experiment's headline governance finding, so a
    shared copy would be wrong, not just flaky.
  * THE STOP CONDITIONS. A worker cannot `break` its peers, so budget
    truncation, the circuit breaker, a leaked connection, and a real bug all
    set a `threading.Event` that every worker checks before pulling more
    work. Groups already in flight run to completion and their rows land:
    stopping must never cost a row that has already been paid for. An
    exception that must stop the sweep is parked on the ledger and re-raised
    by `sweep` after the pool drains, preserving the serial ordering (write,
    flush, spend update, THEN propagate).

The circuit breaker's "consecutive" now means consecutive in COMPLETION
order, which under concurrency is not dispatch order. That is still the
right reading: it asks whether the last N things that finished were all
construction failures, which is exactly the systemic-failure signal (a
missing `OPENROUTER_API_KEY` fails every unit identically) it exists to
catch.

At `workers=1` every behaviour above — including the order rows are written
in — is identical to the serial sweep this replaced.

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
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path

from dce.agent import (
    REASONING_EFFORT,
    WORST_CASE_TOKEN_BUDGET_USD,
    AgentConstructionError,
    _commit_sha,
    _spec_field,
    _token_budget_usd,
    run_task,
)
from dce.arms import ARMS, check_and_restore, make_working_copy
from dce.data import DATASET_REVISION
from dce.frozen import digest
from dce.golds import PLURALITY_THRESHOLD, golds_sha256
from dce.grade import active_scorer
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
#:
#: CROSS-VALIDATED WITH `CIRCUIT_BREAKER_THRESHOLD` BELOW, NOT INDEPENDENT
#: OF IT: within one `sweep` invocation, no key is ever attempted twice, so
#: every one of `CIRCUIT_BREAKER_THRESHOLD`'s failing keys has a TRAILING
#: count of exactly 1 the first time a systemic failure trips the circuit
#: breaker. This value must stay > 1 for that to matter — at exactly 1, a
#: key's very first failure would ALSO satisfy this cap, so `gave_up_keys`
#: would report the same keys the circuit breaker just caught, instead of
#: the circuit breaker being the sole, first alarm for a systemic problem.
#: Pinned by `test_circuit_breaker_fires_before_any_key_is_given_up`; a
#: future edit to either constant that breaks this ordering fails that
#: test, not silently.
MAX_CONSTRUCTION_ATTEMPTS: int = 2

#: A sweep-wide safety valve distinct from the per-key cap above: this many
#: CONSECUTIVE `construction_error` outcomes, across however many different
#: keys, stops the whole sweep immediately rather than grinding through
#: every remaining unit at the same guaranteed-to-fail cost. Deliberately
#: larger than `MAX_CONSTRUCTION_ATTEMPTS` (which bounds retries of ONE
#: key across resumes) — this bounds a systemic failure WITHIN one
#: invocation, before it can write thousands of phantom-priced rows. See
#: `MAX_CONSTRUCTION_ATTEMPTS`'s own comment for the precise relationship
#: the two constants are required to keep.
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
    per-task bad luck. `connection_leaked`: True iff this call stopped
    because `run_task`'s `setup.close()` failed for some task — the
    working copy's integrity substrate can no longer be trusted for any
    later task, so the sweep stops rather than keep producing `db_corrupted`
    values nobody can rely on; a fresh process (resume) gets a fresh
    working copy. `main()` uses these three flags to exit non-zero so a
    wrapper can tell a capped-out, broken-circuit, or leaked-connection run
    from a completed one.
    """

    spent: float
    real_spent: float
    truncated: bool
    circuit_broken: bool = False
    connection_leaked: bool = False


def _summable(value) -> float:
    """`value` as a float when it is a real, finite number; `0.0` for
    anything else (`None`, a string, a `bool`, NaN, infinity).

    The spend readers below run over rows written by every past version of
    this code, and over a torn or hand-edited file, so a single bad value
    must cost that row's contribution rather than the whole read. `bool` is
    excluded deliberately, for the same reason `_validated_usd` excludes
    it: `True` would otherwise silently price a row at $1.00.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    value = float(value)
    return value if math.isfinite(value) else 0.0


def _read_rows(path: Path) -> list[dict]:
    """Every JSON row in `path`, with ONE forgiveness: an unparseable FINAL
    line is skipped (loudly, on stderr) instead of raising.

    A torn last line is the one legitimate artifact of a killed or ENOSPC
    write — `fh.write` handing over fewer bytes than it was given, or the
    process dying between the write and the flush — and it costs exactly
    the one row it truncated. Raising on it instead made the whole
    accumulated results file unreadable to SIX readers at once
    (`spent_so_far`, `real_spent_so_far`, `completed_keys`, `latest_rows`,
    and, through `latest_rows`, `dce.stats.load`/`report`), so a full disk
    stopped costing one row and started costing the entire paid sweep:
    resume could not tell what was already done, and analysis could not
    read any of it, until a human hand-truncated the file.

    A corrupt line ANYWHERE ELSE still raises. Only the tail can be torn by
    an interrupted append; a bad line in the middle means something rewrote
    the file, which is not a truncation and must not be silently skipped.
    """
    if not path.exists():
        return []
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    rows: list[dict] = []
    for index, line in enumerate(lines):
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            if index != len(lines) - 1:
                raise
            print(
                f"WARNING: {path} ends in an unparseable line "
                f"({len(line)} chars) — a torn tail from an interrupted or "
                "out-of-space write. Skipping it; every earlier row was read "
                "normally, and the lost row's unit is simply re-attempted on "
                "the next resume.",
                file=sys.stderr,
            )
    return rows


def _repair_torn_tail(path: Path) -> None:
    """The write-side counterpart to `_read_rows`'s forgiveness: make `path`
    safe to append to.

    `_read_rows` already forgives an unparseable FINAL line (a torn tail
    left by a killed or out-of-space write). But `sweep` used to open `out`
    with `out.open("a")` and append straight onto that torn line, which
    MERGES the next row's bytes onto the torn ones instead of starting a
    fresh line. Two failures follow from that one merge, both reproduced:

      * The merged line is now the file's new last line, and it is STILL
        unparseable (torn prefix + a whole new JSON row, no separator) —
        so it reads right back as torn again. The row `sweep` just wrote,
        and paid for, is swallowed: `_read_rows` forgives the merged line
        exactly as it forgave the original one, `completed_keys` sees no
        progress, and the unit is silently re-attempted (and re-paid) on
        every future resume.
      * The SECOND resume appends again, pushing that still-corrupt merged
        line off the tail — it is no longer the LAST line, so `_read_rows`
        now RAISES on it ("a corrupt line anywhere but the last still
        raises"), bricking every reader (`spent_so_far`, `completed_keys`,
        `dce.stats.report`, ...) for the whole file, not just the one row.

    This function runs once, before `sweep`'s append loop opens `out`, and
    repairs the file to match what `_read_rows` already treats it as:

      * If the file's last line, read in isolation, parses as valid JSON,
        it is simply missing a trailing newline (e.g. a hand-edited file,
        or one written before this repair existed) — nothing is torn or
        lost, so only a newline is appended.
      * If it does not parse, it IS the torn tail. It is truncated off
        entirely (not left in place) — matching `_read_rows` forgiving it
        on the read side, so the next row `sweep` appends starts the file's
        new last line instead of merging onto a dead one, and the file
        stays fully parseable across any number of resumes.

    Truncating (rather than only inserting a separating newline) is the
    right call here specifically because a torn line, once anything is
    appended after it, permanently stops being "the last line" — the one
    exemption `_read_rows` grants. Leaving the torn bytes in place while
    still appending would just move today's data loss (one swallowed row)
    into tomorrow's brick (every reader raising) one resume later; removing
    them is what makes the exemption's premise ("only the tail is ever
    torn") stay true after this repair.
    """
    if not path.exists():
        return
    data = path.read_bytes()
    if not data or data.endswith(b"\n"):
        return
    last_newline = data.rfind(b"\n")
    last_line = data[last_newline + 1 :]
    try:
        json.loads(last_line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        print(
            f"WARNING: {path} ends in an unparseable line with no trailing "
            f"newline ({len(last_line)} bytes) — a torn tail from a killed "
            "or out-of-space write. Truncating it before appending, so the "
            "row this resume is about to write lands on its own line "
            "instead of merging onto the torn one; the torn line's own "
            "unit is simply re-attempted, exactly as the read side already "
            "treats it.",
            file=sys.stderr,
        )
        path.write_bytes(data[: last_newline + 1])
    else:
        # The last line is valid JSON; it is just missing its trailing
        # newline. Nothing was lost — restore only the separator.
        path.write_bytes(data + b"\n")


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
    path: Path, *, retry_verdicts: tuple[str, ...] = ()
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
      * `error` / `post_run_error` — done unless named in
        `retry_verdicts` (`--retry`), since both already cost money and a
        resume should not silently re-pay for them without being asked.
        `post_run_error` is OUR bug (bookkeeping after a completed model
        call), so re-running it is sometimes exactly right once the bug is
        fixed — hence its presence among the choices, not because it is
        cheap.
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
        if verdict in retry_verdicts:
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
    SEPARATE REAL SPEND FROM GUARD SPEND section). A missing, null,
    non-numeric or non-finite value contributes `0.0` rather than raising:
    this reader runs over rows written by every past version of this code,
    so it must degrade rather than brick every future resume — unlike the
    strict validation `sweep` applies to rows it is producing right now;
    see that function's docstring for why the two need different
    strictness. (This docstring previously claimed that tolerance while a
    string `usd` still raised `TypeError` on the `+=`; `_summable` is what
    makes the claim true.)
    """
    total = 0.0
    for row in _read_rows(path):
        total += _summable(row.get("usd_guard", row.get("usd")))
    return total


def real_spent_so_far(path: Path) -> float:
    """Total `usd` — real, billed dollars — already banked in `path`. This
    is what `main()` reports as "spent"; `spent_so_far`'s `usd_guard` total
    is the cap's bookkeeping figure, not an accounting claim (a unit with
    two pessimistically-priced construction errors and one real $0.01
    success has `spent_so_far` well above $0.01 but `real_spent_so_far`
    exactly $0.01).

    Non-numeric and non-finite values contribute `0.0` rather than raising,
    for the same reason — see `spent_so_far` and `_summable`.
    """
    total = 0.0
    for row in _read_rows(path):
        total += _summable(row.get("usd"))
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


def _working_db_path(pristine: Path, worker: int = 0) -> Path:
    """Worker 0 keeps the historical `.working` name so a single-worker
    resume reuses the file a pre-concurrency sweep left behind instead of
    orphaning it; every other worker gets its own suffixed copy. See
    `_worker` for why the copies cannot be shared.
    """
    if worker == 0:
        return pristine.with_name(pristine.name + ".working")
    return pristine.with_name(f"{pristine.name}.working-{worker}")


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
        usd_guard = _summable(row.get("usd_guard", row.get("usd")))
        model = row.get("model")
        if usd_guard and isinstance(model, str):
            by_model.setdefault(model, []).append(usd_guard)
    return by_model


def _validated_usd(value, field_name: str) -> float:
    """`usd`/`usd_guard` must be a real, FINITE, non-negative number before
    a row is allowed to reach disk. `None` is the one explicit "couldn't
    price this" signal and is treated as `0.0`; anything else invalid — a
    string, a `bool` (a `bool` IS an `int` in Python; explicitly excluded
    so `True` cannot silently price a row at $1.00), a negative figure, NaN
    OR `float("inf")` — raises. `math.isfinite` (not a bare `isnan` check)
    is what catches infinity: `Infinity` is not valid JSON per RFC 8259 (a
    non-Python consumer chokes on it), and an accepted `inf` would make
    `spent_so_far` permanently `inf`, truncating every future resume to
    zero pending work — precisely the bricking outcome this validator
    exists to prevent, just via a different value than the ones already
    covered. Callers of this function COLLECT its exception and normalize
    rather than propagate it — see `_priced_or_pessimistic` — so raising
    here is the mechanism for "this needs normalizing", not "lose the
    row"; the row-losing failure mode this docstring used to describe
    (six negative rows producing `spent = -600.0`, a stray `"1.20"` string
    bricking every future read) is what motivated validating in the first
    place, not what happens now when validation fails.
    """
    if value is None:
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be a number or None, got {value!r}")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{field_name} is not finite: {value!r}")
    if value < 0:
        raise ValueError(f"{field_name} is negative: {value!r}")
    return value


def _pessimistic_usd(model: str) -> float:
    """`_token_budget_usd(model)`, falling back to the worst case across
    every pinned model (`dce.agent.WORST_CASE_TOKEN_BUDGET_USD`) if even
    that fails — an unrecognized `model` string. This function is the
    fallback of last resort for a pricing failure and must never itself
    raise.
    """
    try:
        return _token_budget_usd(model)
    except Exception:
        return WORST_CASE_TOKEN_BUDGET_USD


def _priced_or_pessimistic(
    row: dict, field: str, model: str
) -> tuple[float, str | None]:
    """Extract and validate `row[field]` (`usd`/`usd_guard`); on ANY
    failure — a missing key, wrong type, negative, NaN, infinite — this
    NORMALIZES to the pessimistic ceiling and returns an error note
    instead of raising. There is no version of "the row already exists
    and is on its way to the one write site" where discarding it over a
    pricing bug is the right response — the same policy `check_and_restore`
    failures already get (collect, write, re-raise after), not the
    opposite one a bare `row[field]` access used to apply here.
    """
    try:
        raw = row[field]
    except KeyError:
        return _pessimistic_usd(model), f"{field} missing"
    try:
        return _validated_usd(raw, field), None
    except Exception as exc:
        return (
            _pessimistic_usd(model),
            f"{field}={raw!r}: {type(exc).__name__}: {exc}",
        )


def _safe_json_dumps(row: dict) -> str:
    """Serialize `row` for the one write site in `sweep` — this function
    must never raise, because the row it is given already exists and must
    reach disk regardless of what it contains.

    Three layers, each covering what the last one cannot:

      1. Plain `json.dumps(row)`.
      2. `default=repr` — covers a VALUE `json` doesn't natively know how
         to serialize (an exception object, a custom class instance, ...).
         Does NOT cover a non-`str`-coercible dict KEY (`json` raises
         `TypeError` about the key before `default` is ever consulted —
         `default` only ever runs on values) or a circular reference
         (`json` detects the cycle and raises `ValueError` internally,
         again before `default` gets a say).
      3. A minimal envelope built from `str(row)` — Python's `str`/`repr`
         on a `dict` has its own built-in cycle guard (renders `{...}` for
         a self-reference rather than recursing or raising), and places no
         constraint on key types at all, so this covers both gaps layer 2
         leaves open.

    THE ENVELOPE MUST CARRY THE PRICE, not merely enough to find the line.
    Layer 3 landing a row that reads `$0.00` is the SAME failure this
    module's central invariant exists to prevent, one layer deeper: a
    resumed sweep really spent the money, `spent_so_far` summed `0.0`, and
    `--max-spend` stopped binding at all (reproduced: 20 resumes, $24.00
    real, 20 rows on disk, `spent_so_far` $0.00). So `usd` and `usd_guard`
    are carried, normalized through `_priced_or_pessimistic` exactly as the
    write site itself normalizes them — by the time `sweep` calls this,
    both are already validated finite floats, and a pessimistic ceiling is
    the right answer for any other caller's malformed row.

    `level` and `db_corrupted` are carried for the same reason one layer
    out: without `level`, `dce.stats.accuracy_by(rows, "level")` buckets
    the row as `"unknown"` and the stratum tables silently misreport;
    without `db_corrupted`, the experiment's HEADLINE governance finding
    is misattributed (a `True` reads as absent, i.e. "check never ran").

    The four identifying fields are `str()`-coerced and ALWAYS present,
    never filtered on `isinstance`. A non-`str` `task_id` used to make the
    envelope omit the key entirely, after which `latest_rows` and
    `completed_keys` — which index `row["task_id"]` — raised `KeyError` on
    the whole file, taking down every future resume AND `dce.stats.report`.
    A wrong-typed identifier is a nuisance; a missing one is a brick.
    """
    try:
        return json.dumps(row)
    except Exception:
        pass
    try:
        return json.dumps(row, default=repr)
    except Exception:
        pass
    envelope: dict = {}
    try:
        envelope["unserializable_row"] = str(row)
    except Exception:
        envelope["unserializable_row"] = "<could not stringify row>"
    for key in ("task_id", "arm", "model", "verdict"):
        try:
            envelope[key] = str(row.get(key))
        except Exception:
            envelope[key] = "unknown"
    try:
        envelope["level"] = str(row.get("level", "unknown"))
    except Exception:
        envelope["level"] = "unknown"
    corrupted = row.get("db_corrupted")
    envelope["db_corrupted"] = corrupted if isinstance(corrupted, bool) else None
    model = row.get("model")
    for key in ("usd", "usd_guard"):
        try:
            value, _ = _priced_or_pessimistic(
                row, key, model if isinstance(model, str) else ""
            )
        except Exception:
            value = WORST_CASE_TOKEN_BUDGET_USD
        envelope[key] = value
    try:
        return json.dumps(envelope)
    except Exception:
        return json.dumps({"unserializable_row": "<could not stringify row>"})


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
        # Always False: no model call ever happened on this path, so there was
        # nothing for the forcing turn to force. Present so every row in a
        # results file has the same keys.
        "forced_answer": False,
        # No model call happened, so there is no transcript to point at.
        "trace_path": None,
        "provider_tag": _spec_field(model, "provider_tag"),
        "quantization": _spec_field(model, "quantization"),
        "reasoning_effort": REASONING_EFFORT,
        "reasoning_tokens": 0,
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
        # The scorer in force for this process — see
        # `dce.agent.build_result_row`. Nothing was scored here (no call
        # was ever made), but the column stays uniform across row shapes.
        "scorer": active_scorer(),
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
        "error": f"{type(exc).__name__}: {exc}",
    }


class _SweepLedger:
    """Every piece of state `sweep`'s workers share, behind ONE lock.

    Concurrency does not relax the module's central invariant — a priced row
    still reaches disk before anything else may throw — it only moves the
    enforcement point. `record` IS the one write site, and it holds the lock
    across the write, the flush, and the spend/observation update, so no two
    workers can interleave a partial line or lose an update to a read-modify-
    write race on `spent`.

    THE RESERVATION IS NOW HELD, NOT JUST COMPUTED. Serially, `sweep` reserved
    one task group, ran it, banked the cost, and reserved the next: `spent`
    alone was a sufficient basis for the check because nothing was ever in
    flight. With `workers` groups running at once, `workers - 1` of them have
    spent real money that no row has banked yet, so a guard reading `spent`
    alone would admit `workers` groups against a budget for one. `reserved`
    holds each in-flight group's ceiling from dispatch (`try_reserve`) to
    completion (`release`), and the check is against `spent + reserved`.

    That deliberately DOUBLE-COUNTS a group while it runs: its rows bank into
    `spent` as they land while its reservation is still held. The error is
    always in the conservative direction — the sweep stops slightly early,
    never slightly over — which is the only direction a spend guard is allowed
    to be wrong in.

    STOPPING IS A FLAG, NOT A `break`. The serial loop broke out of two nested
    `for`s. A worker cannot break its peers, so every stop condition (budget,
    circuit breaker, leaked connection, a real bug) sets `stop`, which every
    worker checks before pulling more work. Groups already in flight run to
    completion and their rows land — stopping the sweep must never cost a row
    that has already been paid for.
    """

    def __init__(
        self,
        fh,
        *,
        spent: float,
        real_spent: float,
        observed: dict[str, list[float]],
        max_spend: float,
    ) -> None:
        self._lock = threading.Lock()
        self._fh = fh
        self.spent = spent
        self.real_spent = real_spent
        self.observed = observed
        self.max_spend = max_spend
        self.reserved = 0.0
        self.consecutive_construction_errors = 0
        self.truncated = False
        self.circuit_broken = False
        self.connection_leaked = False
        self.pending_exc: BaseException | None = None
        self.stop = threading.Event()

    def try_reserve(self, group, task_id: str) -> float | None:
        """Reserve a whole task group's ceiling, or refuse it and stop the
        sweep. Returns the amount held (to hand back to `release`), or `None`
        if this group must not run.

        The reservation covers the entire group and is taken before the
        group's first call, so a truncation always lands on a task boundary
        and a paired analysis never has to discard a half-finished task —
        the same property the serial loop had, for the same reason.
        """
        with self._lock:
            if self.stop.is_set():
                return None
            amount = sum(
                _next_reserve(self.observed.get(model, []), _token_budget_usd(model))
                for _, _, model in group
            )
            if self.spent + self.reserved + amount > self.max_spend:
                print(
                    f"stopping: ${self.spent:.2f} spent + "
                    f"${self.reserved:.2f} reserved in flight + "
                    f"${amount:.2f} for task {task_id!r}'s group would "
                    f"exceed ${self.max_spend:.2f}"
                )
                self.truncated = True
                self.stop.set()
                return None
            self.reserved += amount
            return amount

    def release(self, amount: float) -> None:
        with self._lock:
            self.reserved -= amount

    def record(self, row: dict, model: str, usd: float, usd_guard: float) -> None:
        """THE ONE WRITE SITE, plus the accounting that must not be separable
        from it. See `_safe_json_dumps` for why serialization cannot raise.
        """
        payload = _safe_json_dumps(row)
        with self._lock:
            try:
                self._fh.write(payload + "\n")
                self._fh.flush()
            except Exception:
                # The OS refused the bytes (a full disk, most likely).
                # Nothing here can make them land, but the row is already
                # priced, so it must not vanish silently: dump it to stderr
                # — where a redirected log or a scrollback can still recover
                # it by hand — and then let the failure stop the sweep
                # loudly.
                print(
                    "FATAL: could not write a priced result row (a full "
                    "disk?). The row follows verbatim on the next line so "
                    "it can be recovered by hand; the sweep stops now "
                    "rather than continuing to spend money it cannot "
                    "record.",
                    file=sys.stderr,
                )
                print(payload, file=sys.stderr)
                raise

            self.spent += usd_guard
            self.real_spent += usd
            if usd_guard > 0:
                self.observed.setdefault(model, []).append(usd_guard)

            # Counted in COMPLETION order, which under concurrency is not
            # dispatch order. That is the right reading anyway: the breaker
            # asks "are the last N things that finished all construction
            # failures", which is exactly the systemic-failure signal
            # (a missing OPENROUTER_API_KEY fails every unit identically)
            # it exists to catch.
            if row.get("verdict") == "construction_error":
                self.consecutive_construction_errors += 1
            else:
                self.consecutive_construction_errors = 0
            if self.consecutive_construction_errors >= CIRCUIT_BREAKER_THRESHOLD:
                self.circuit_broken = True
                self.stop.set()

    def fail(self, exc: BaseException) -> None:
        """Hold the FIRST exception that must stop the sweep, and stop it.

        Serially this was a bare `raise` from inside the loop. A worker
        thread's `raise` reaches nobody, so the exception is parked here and
        re-raised by `sweep` once the pool has drained — after every in-flight
        row has landed, which is the same ordering the serial code had (write,
        flush, spend update, *then* propagate).
        """
        with self._lock:
            if self.pending_exc is None:
                self.pending_exc = exc
            self.stop.set()


def _run_group(
    group,
    working: Path,
    ledger: _SweepLedger,
    *,
    by_id: dict,
    golds: dict,
    docs,
    golds_hash: str,
    run_task_fn,
    trace_dir: Path | None,
    db_path: Path,
) -> None:
    """Run every (arm, model) unit of ONE task against ONE worker's working
    copy. Lifted verbatim out of `sweep`'s inner loop; the only changes are
    that shared state goes through `ledger` and that a stop condition sets a
    flag instead of `break`ing a loop the caller no longer owns.
    """
    for task_id, arm, model in group:
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
                trace_dir=trace_dir,
            )
        except AgentConstructionError as exc:
            # The ONLY exception `run_task_fn` is expected to raise (see the
            # module docstring and `AgentConstructionError` itself) —
            # anything else is a real bug and stops the sweep loudly rather
            # than silently treating a possibly-billable failure as a free
            # construction error.
            row = _construction_error_row(
                by_id[task_id], arm, model, gold, golds_hash, exc
            )

        # INVARIANT (see module docstring): `row` now exists and is priced.
        # Every failure between here and `ledger.record` is COLLECTED onto
        # `row` — never raised through — so the row always reaches the one
        # write site. `pending_exc`, if set, only gets to stop the sweep
        # AFTER the write, the flush, and the spend update.
        pending_exc: Exception | None = None

        # `run_task` stamps `close_error` when its OWN `setup.close()`
        # failed — meaning the connection may still be open. That does not
        # just invalidate THIS task's `check_and_restore` call: a still-open
        # connection can keep mutating this worker's working copy underneath
        # whatever runs next on it, so every later check by this worker
        # would be equally untrustworthy, each looking just as authoritative
        # as a real one. The experiment's headline governance finding is
        # "did an ungoverned arm mutate the warehouse" — a stream of
        # unreliable `db_corrupted` values is worse than no sweep at all, so
        # the whole sweep stops rather than running `check_and_restore`
        # again against unknown state.
        row_leaked = row.get("close_error") is not None
        if row_leaked:
            row["db_corrupted"] = None
            row["integrity_error"] = (
                "DuckDB connection leaked while closing this "
                f"task's arm ({row['close_error']}); the working "
                "copy's integrity substrate can no longer be "
                "trusted for this or any later task in this sweep"
            )
        else:
            try:
                integrity = check_and_restore(working, db_path)
                row["db_corrupted"] = integrity.corrupted
            except Exception as exc:
                row["db_corrupted"] = None
                row["integrity_error"] = f"{type(exc).__name__}: {exc}"
                pending_exc = exc

        # Pricing is NORMALIZED, never raised through — the same
        # collect-then-carry-on policy `integrity_error` gets just above.
        usd, usd_error = _priced_or_pessimistic(row, "usd", model)
        usd_guard, usd_guard_error = _priced_or_pessimistic(row, "usd_guard", model)
        row["usd"] = usd
        row["usd_guard"] = usd_guard
        pricing_errors = [e for e in (usd_error, usd_guard_error) if e]
        if pricing_errors:
            row["pricing_error"] = "; ".join(pricing_errors)
            if pending_exc is None:
                pending_exc = ValueError(row["pricing_error"])

        ledger.record(row, model, usd, usd_guard)

        # Only now, after the row is safely on disk and every counter is
        # updated, do the stop conditions get to act.
        if row_leaked:
            ledger.connection_leaked = True
            ledger.stop.set()
            print(
                "stopping: a DuckDB connection leaked while closing "
                f"task {task_id!r}'s arm; the working copy's "
                "integrity substrate can no longer be trusted for "
                "any later task in this sweep. Resume in a fresh "
                "process to continue — only this task's own "
                "check_and_restore result is affected."
            )
            return

        if pending_exc is not None:
            ledger.fail(pending_exc)
            return

        if ledger.circuit_broken:
            print(
                f"stopping: {CIRCUIT_BREAKER_THRESHOLD} consecutive "
                "construction errors across different units — "
                "likely a systemic problem (missing "
                "OPENROUTER_API_KEY? a bad model id?), not "
                "per-task bad luck"
            )
            return


def _worker(
    index: int,
    work_q,
    ledger: _SweepLedger,
    *,
    db_path: Path,
    **group_kwargs,
) -> None:
    """One worker thread: pull task groups until the queue is empty or the
    sweep stops.

    THE WORKING COPY IS PER WORKER, AND THAT IS NOT AN OPTIMIZATION.
    `check_and_restore` repairs a corrupted copy by copying the whole
    pristine file back over it. Sharing one copy across workers would land
    that `copyfile` in the middle of another worker's live query — and, far
    worse for the experiment, `db_corrupted` would stop attributing
    corruption to the arm that caused it, since any worker's ungoverned arm
    could have been the one that mutated the shared file. `db_corrupted` IS
    the headline governance finding, so a shared copy would not be merely
    flaky; it would be wrong.

    Created lazily, on this worker's first group, so a `--workers 8` run over
    two remaining units does not copy the database eight times.
    """
    working: Path | None = None
    try:
        while not ledger.stop.is_set():
            try:
                task_id, group = work_q.get_nowait()
            except queue.Empty:
                return
            amount = ledger.try_reserve(group, task_id)
            if amount is None:
                return
            try:
                if working is None:
                    working = make_working_copy(
                        db_path, _working_db_path(db_path, index)
                    )
                _run_group(group, working, ledger, db_path=db_path, **group_kwargs)
            finally:
                ledger.release(amount)
    except BaseException as exc:  # noqa: BLE001 - re-raised by `sweep`
        # A worker's `raise` reaches nobody. Park it and stop the sweep; the
        # main thread re-raises it after the pool drains, so a real bug still
        # stops the sweep loudly rather than silently losing one worker.
        ledger.fail(exc)


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
    retry_verdicts: tuple[str, ...] = (),
    trace_dir: Path | None = None,
    workers: int = 1,
) -> SweepResult:
    """Run every not-yet-completed (task, arm, model) triple, appending one
    JSON row per unit of work to `out`, until the next task's reservation
    would push spend past `max_spend`, or `CIRCUIT_BREAKER_THRESHOLD`
    consecutive construction failures signal a systemic problem.

    `db_path` is the pristine database; it is never opened directly here or
    handed to `run_task_fn` — see the module docstring. `golds` is the plain
    `task_id -> answer` mapping (callers reading the on-disk envelope must
    unwrap it first; see `_load_golds`).

    `workers` task groups run concurrently, each on its OWN working copy of
    the database. The unit of dispatch is the whole task group (every arm x
    model for one task), never an individual unit: that is what keeps a
    truncation on a task boundary and keeps one task's arms on one copy of
    the database. At `workers=1` the behaviour — including row order — is
    identical to the serial sweep this replaced. Threads, not processes: the
    work is network I/O inside `run_sync`, and one shared file handle plus
    one shared spend ledger is a far smaller surface to get right than the
    same state split across processes.
    """
    if workers < 1:
        raise ValueError(f"workers must be at least 1, got {workers}")

    done = completed_keys(out, retry_verdicts=retry_verdicts)
    todo = pending(tasks, arms, models, done)
    by_id = {t["task_id"]: t for t in tasks}
    out.parent.mkdir(parents=True, exist_ok=True)

    spent = spent_so_far(out)
    real_spent = real_spent_so_far(out)
    observed = _seed_observed_by_model(out)

    if not todo:
        # Nothing to do: don't even pay for a working-copy file write.
        return SweepResult(spent=spent, real_spent=real_spent, truncated=False)

    # Write-side counterpart to `_read_rows`'s forgiveness — see
    # `_repair_torn_tail`'s docstring: appending directly onto a torn tail
    # merges the next row onto it instead of starting a fresh line, which
    # first swallows that row silently and then bricks the whole file on
    # the very next resume.
    _repair_torn_tail(out)

    # Pre-filled before any worker starts, so `queue.Empty` unambiguously
    # means "all work dispatched" rather than "nothing ready yet".
    work_q: queue.Queue = queue.Queue()
    for task_id, group_iter in itertools.groupby(todo, key=lambda t: t[0]):
        work_q.put((task_id, list(group_iter)))

    with out.open("a") as fh:
        ledger = _SweepLedger(
            fh,
            spent=spent,
            real_spent=real_spent,
            observed=observed,
            max_spend=max_spend,
        )
        group_kwargs = dict(
            by_id=by_id,
            golds=golds,
            docs=docs,
            golds_hash=golds_hash,
            run_task_fn=run_task_fn,
            trace_dir=trace_dir,
        )
        threads = [
            threading.Thread(
                target=_worker,
                args=(i, work_q, ledger),
                kwargs={"db_path": db_path, **group_kwargs},
                name=f"dce-sweep-{i}",
            )
            for i in range(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    if ledger.pending_exc is not None:
        raise ledger.pending_exc
    return SweepResult(
        spent=ledger.spent,
        real_spent=ledger.real_spent,
        truncated=ledger.truncated,
        circuit_broken=ledger.circuit_broken,
        connection_leaked=ledger.connection_leaked,
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
    """Read the golds envelope and return (task_id -> answer map, gold hash).

    `data/golds.json` is an envelope
    (`{"revision", "threshold", "count", "golds", "submissions_expected",
    "submissions_consumed", "manifest_sha256"}`), not a bare mapping — the
    task -> answer map lives under `"golds"`. Reading it as a bare mapping
    would silently iterate its handful of envelope keys instead of ~406
    tasks; checked explicitly here (raising `SystemExit`, not letting a
    bare mapping fail later with `KeyError('revision')`) so that mistake is
    loud and immediate instead of a confusing crash deep in the sweep.

    The `revision` check is what catches a smoke run and a full sweep being
    scored against two different ground-truth snapshots on the DATASET
    axis. The `threshold` check is its counterpart on the RECONSTRUCTION
    axis: Ruling 8 requires re-running `dce.prepare` at 0.60 / 0.75 / 0.90
    to publish the sensitivity table, and `data/golds.json` is gitignored,
    so an in-place overwrite at another threshold would otherwise be
    invisible to the sweep, to git, and to the results file alike.

    `golds_hash` — stamped into EVERY result row — is
    `dce.golds.golds_sha256`, a fingerprint of the gold mapping itself. It
    used to be `manifest_sha256`, which fingerprints the SUBMISSION CORPUS:
    identical across two gold sets that differ in every answer, because
    they were reconstructed from the same corpus. The stored value is
    verified against a recomputation here rather than trusted, so a
    hand-edited envelope (hash kept, answers changed) is caught too; an
    envelope written before this field existed is simply hashed on the fly.
    `manifest_sha256` stays in the envelope — it still records which corpus
    was consumed, which is a different and also-necessary fact.
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
    if envelope.get("threshold") != PLURALITY_THRESHOLD:
        raise SystemExit(
            f"golds threshold {envelope.get('threshold')!r} does not match "
            f"dce.golds.PLURALITY_THRESHOLD {PLURALITY_THRESHOLD!r}; this "
            "golds.json was reconstructed under a different consensus rule "
            "(a sensitivity run, most likely — see Ruling 8). Re-run "
            "`python -m dce.prepare` to restore the primary gold set before "
            "scoring anything against it"
        )
    computed = golds_sha256(envelope["golds"])
    stored = envelope.get("golds_sha256")
    if stored is not None and stored != computed:
        raise SystemExit(
            f"golds.json's stored golds_sha256 {stored!r} does not match the "
            f"hash of the golds it contains ({computed!r}) — the file has "
            "been edited since it was written; refusing to score against it"
        )
    return envelope["golds"], computed


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
    if result.connection_leaked:
        return 4
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
        "--traces",
        type=Path,
        default=None,
        help=(
            "Directory for full per-run transcripts (default: "
            "traces/<out-stem>/). Pass --no-traces to switch them off. A "
            "result row records THAT a run failed; only a trace records why."
        ),
    )
    parser.add_argument(
        "--no-traces",
        action="store_true",
        help="Do not write transcripts. Saves disk, and gives up diagnosis.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "task groups to run concurrently, each on its own working copy "
            "of the database (default 1 = the serial sweep). Runs are pure "
            "network I/O, so this is close to linear in wall-clock; but "
            "every model call is pinned to ONE provider endpoint with "
            "allow_fallbacks:false, so a rate limit there surfaces as an "
            "error rather than a re-route. Raise it gradually and watch "
            "the error rate."
        ),
    )
    parser.add_argument(
        "--retry",
        choices=["error", "post_run_error"],
        default=None,
        help="also retry rows with this verdict on resume (an 'error' or "
        "'post_run_error' row already cost money; 'construction_error' rows "
        "are retried "
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

    if args.workers < 1:
        raise SystemExit(f"--workers must be at least 1, got {args.workers}")

    worst_group = _worst_case_task_group_usd(args.arms, args.models)
    admits = int(args.max_spend // worst_group) if worst_group > 0 else 0
    print(
        f"reserve ceiling ${worst_group:.2f}/task-group "
        f"({len(args.arms)} arms x {args.models}); ${args.max_spend:.2f} cap "
        f"admits up to {admits} worst-case task-group(s) before the first "
        "observation tightens it"
    )
    if args.workers > 1 and admits < args.workers:
        # Reservations are HELD while a group is in flight (see
        # `_SweepLedger`), so the cap bounds concurrency as well as spend: a
        # budget admitting fewer worst-case groups than there are workers
        # runs serially at first no matter what `--workers` says, until
        # observations tighten the estimate. Said out loud, because the
        # symptom otherwise looks like the pool being broken.
        print(
            f"note: ${args.max_spend:.2f} admits only {admits} worst-case "
            f"group(s), so fewer than {args.workers} workers will be busy "
            "until the first observations tighten the reserve"
        )

    golds, golds_hash = _load_golds(args.golds)
    tasks = [t for t in json.loads(args.tasks.read_text()) if t["task_id"] in golds]
    if args.n:
        tasks = _stratified_sample(tasks, args.n)

    docs = {
        "manual": Path("data/hf/data/context/manual.md").read_text(),
        "payments_readme": Path("data/hf/data/context/payments-readme.md").read_text(),
    }

    # Default the trace directory off the results filename, so two sweeps
    # writing different results files cannot interleave their transcripts.
    trace_dir = (
        None if args.no_traces else (args.traces or Path("traces") / args.out.stem)
    )

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
        retry_verdicts=(args.retry,) if args.retry else (),
        trace_dir=trace_dir,
        workers=args.workers,
    )
    # Real dollars, not the guard ledger — see the module docstring's
    # SEPARATE REAL SPEND FROM GUARD SPEND section.
    print(f"spent ${result.real_spent:.2f} (guard ledger ${result.spent:.2f})")
    if result.connection_leaked:
        print(
            "stopped: a DuckDB connection leaked while closing an arm's "
            "session, and the working copy's integrity substrate can no "
            "longer be trusted for any later task — a stream of "
            "unreliable db_corrupted values would be worse than no sweep "
            "at all. Resume in a fresh process to continue: only the "
            "current task's own result is affected.",
            file=sys.stderr,
        )
    elif result.circuit_broken:
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
