"""Offline statistics over the results JSONL.

DEDUP: `report()` and every other reader of the results file MUST go
through `dce.runner.latest_rows` -- never read `path` line by line -- before
counting anything. A retried unit leaves several rows for one
`(task_id, arm, model)` key in the raw file; `latest_rows` keeps only the
last row written per key. `load()` below is a thin, explicit wrapper around
it so every scoring function in this module goes through the same door.

The measured consequence depends on which accuracy view is affected. SCORED
accuracy is unaffected by a harness-verdict duplicate on its own (a
`construction_error` never enters that fraction's numerator or denominator,
deduplicated or not) -- but STRICT accuracy treats every raw row as a wrong
answer unless it is `"correct"`, so three stale `construction_error` rows
plus one real, later success reads as STRICT 1/4 = 25% under a naive
line-by-line reader, instead of the true STRICT 1/1 = 100% once deduplicated
(see `dce.runner.latest_rows`'s own docstring for the full case). A
duplicated pair of *answer* verdicts -- e.g. a `--retry error` resume that
produced two `correct` rows for the same key -- would double count in
SCORED too; `latest_rows` fixes both failure modes the same way, by key.

The one deliberate exception is `_raw_rows`, used only to total real billed
spend across every attempt including superseded ones -- see the COST
section below.

VERDICTS: the file carries seven, not two. `correct` and `incorrect` are
*answers* -- the model produced something and it was graded. The other
five -- `hit_limit`, `error`, `scoring_error`, `post_run_error`,
`construction_error` -- are harness outcomes: the model was capped, the
call raised, scoring itself raised, our post-call bookkeeping raised, or
the agent never got built. Two of those five (`post_run_error`,
`construction_error`) are *our* bugs, not the arm under test's; folding
any of the five into "wrong answer" attributes a harness defect to the arm.

Because of that, every accuracy figure below is reported two ways rather
than one:

  * SCORED accuracy: `correct` / (`correct` + `incorrect`) -- harness
    failures dropped from both halves of the fraction entirely.
  * STRICT accuracy: `correct` / all GRADED rows -- harness failures counted
    as wrong. Rows for a task with no gold are not graded rows; see
    `_is_ungolded`.

Excluding harness failures (SCORED) flatters whichever arm fails most;
counting them as wrong (STRICT) instead punishes that arm for what may be
our bug, not its answer. Reporting only one hides which failure mode is in
play, so both are always printed side by side, along with each arm's raw
failure breakdown (counts AND rates), and `report()` flags an arm as
HARNESS-LIMITED when its harness-failure RATE (harness rows / all rows)
reaches `HARNESS_FAILURE_RATE_THRESHOLD` -- a direct rate threshold, not
the SCORED/STRICT point-estimate gap. The gap is algebraically
`scored_accuracy * failure_rate`, so an arm that is mostly broken but also
scores 0% on what little it did answer produces a near-zero gap despite
being the arm most likely to be unreliable, and two arms with identical
failure rates but different accuracy would get opposite flags purely from
the multiplication -- both wrong. The rate alone tracks brokenness, not
brokenness confounded with skill. This flag is what `FINDINGS.md` is
required to repeat, not something a human has to notice by comparing two
numbers.

GOVERNANCE: `db_corrupted` is the experiment's headline governance finding
(did an ungoverned arm mutate the warehouse -- see `dce.runner`'s module
docstring) and is surfaced per arm/model slice: a count of rows where it is
`True` (the working copy really was mutated) and, separately, a count
where it is `None`/absent (the integrity check itself was untrustworthy --
a failed check or a leaked connection, per `dce.runner`'s own handling --
which is a materially different and more serious statement than "clean"
and must never be silently folded into a `False`-shaped default).

McNemar is the right significance test because every arm sees every task --
the comparison is paired, not independent samples. Its discordant-pair
count is reported alongside every p-value it produces. Near the ceiling,
discordant pairs get scarce and power drops fast, but that only matters for
a result that is ALREADY non-significant: a significant p-value from a
handful of discordant pairs (six is enough for p<0.05 on an exact two-sided
binomial test) is a real finding, not a low-power one, and must not be
undercut by a blanket warning. `mcnemar()`'s `low_power` flag is therefore
gated on both a small discordant count (`< LOW_POWER_DISCORDANT_THRESHOLD`)
AND a non-significant p-value (`>= 0.05`) -- exactly the case where "could
not tell" is the honest reading, as opposed to "no effect", which a
significant result already rules out regardless of how few pairs produced
it. Every McNemar comparison is run in both the SCORED view (harness-
failure tasks dropped from the pairing for both arms) and the STRICT view
(a harness failure counts as a wrong answer for whichever arm hit it), for
the same reason accuracy is: a harness bug on one arm's task must not
silently read as that arm losing a paired comparison it never actually
lost.

THE PRIMARY, pre-registered test is arm B vs arm C on `PRIMARY_MODEL`,
paired McNemar, over the reconstructed-gold task set. `report()` prints it
first and labelled as such. Every other comparison this module produces --
arm A, the other models, the level strata -- is secondary and exploratory,
and is printed under that label, never mixed in with the primary result.
Naming one confirmatory test in advance is what keeps the result from
being a search: this design runs 4 arms x 4 pinned models x 2 levels worth of
possible comparisons, and at that count something crosses p<0.05 by chance
alone.

Arm names are unpacked from `dce.arms.ARMS` rather than hardcoded, so a
rename or reorder there is picked up automatically instead of this module
silently comparing against an arm name that no longer exists (which would
read as "no rows for that arm" rather than an error).

COST: two figures are reported per arm/model slice, never `usd_guard`
(the spend cap's own pessimistic ledger -- see `dce.agent.build_result_row`
and `dce.runner._construction_error_row` -- which deliberately overstates
real spend and must never be printed as a cost). `usd_final` sums `usd`
over the deduplicated rows -- what the LAST attempt for each key cost.
`usd_total_billed` sums `usd` over every raw row ever written for that
slice, superseded attempts included -- what was actually charged. The two
differ whenever a paid attempt (a priced `error`, say) was retried into a
free-er success: reporting only `usd_final` would silently drop that
earlier, real charge, understating spend the way `dce.runner.
real_spent_so_far` (which this module's `usd_total_billed` mirrors,
per-slice rather than file-wide) is specifically designed not to.

INSTRUMENTATION: every slice also reports the per-row means of the counts
`dce.agent.build_result_row` writes -- `input_tokens`, `output_tokens`,
`cached_tokens`, `turns` -- because three commitments depend on them and
nothing else in this repo reads them.

`cached_tokens` per arm is a GATE, not a curiosity, and it has to be read
AT the smoke run rather than after the sweep: arm B carries all 22k chars
of `manual.md` in its system prompt, so if OpenRouter prompt-caches that
arm and not the others, the cost comparison between arms is measuring the
provider's caching policy rather than the arms. `dce.pricing` DOES apply a
discounted `price_cached` rate (5x-30x below `price_in`) to the real
`cache_read_tokens`, so differential caching flows straight into `usd_final`
and `usd_total_billed` -- the dollar figures are caching-SENSITIVE, and this
counter is how you tell whether the arms were cached differently. (An earlier
version of this paragraph claimed the opposite, from a time when `pricing`
billed every cached token at the full input rate.)

The three governed-arm counters (`inspect_rejections`, `enforcement_blocks`,
`retry_prompts`) are reported for `contract` and `contract_hollow`, since the
two ungoverned arms have no contract to enforce and would report a structural
zero.
They are DESCRIPTIVE INSTRUMENTATION -- how often the governed tools
refused something -- and are labelled as such in the output. They are not
a governance result: a block is not evidence that the block was necessary,
and none of them enters an accuracy figure, a McNemar table, or any
pre-registered comparison.

Rows are read with `.get(..., default)` throughout, matching
`dce.runner`'s own readers (e.g. `spent_so_far`'s `usd_guard`/`usd`
fallback) -- an older row shape missing a newer field must degrade to a
sane default rather than raise mid-report.
"""

from __future__ import annotations

from collections import defaultdict
from math import sqrt
from pathlib import Path

from scipy.stats import binomtest

from dce.arms import ARMS
from dce.runner import _read_rows, latest_rows

#: Verdicts where the model produced a graded answer.
ANSWER_VERDICTS: frozenset[str] = frozenset({"correct", "incorrect"})

#: Verdicts that are harness outcomes, not graded answers. `post_run_error`
#: and `construction_error` are specifically OUR bugs (see module
#: docstring); `hit_limit`, `error`, and `scoring_error` are the model call,
#: cap, or scorer failing. None of the five is an answer, so none belongs
#: in an accuracy numerator, or (for SCORED accuracy) denominator.
HARNESS_VERDICTS: frozenset[str] = frozenset(
    {"hit_limit", "error", "scoring_error", "post_run_error", "construction_error"}
)

#: A verdict that is neither an answer nor a harness outcome: the task was
#: run and answered, but no gold EXISTS to score it against (49 of DABStep's
#: 450 -- see `dce.golds`). A leaderboard submission has to answer those; the
#: ablation must not count them. Kept out of ANSWER_VERDICTS so no accuracy
#: arithmetic can reach it, and out of HARNESS_VERDICTS because nothing
#: failed -- `_summarize` drops these rows before either denominator.
UNGRADED_VERDICTS: frozenset[str] = frozenset({"ungraded"})

#: Named, not positional: `dce.arms.ARMS` grew a fourth arm
#: (`contract_hollow`) and the old `ARM_A, ARM_B, ARM_C = ARMS` unpack failed
#: loudly at import, which is exactly what it was written to do. Resolving by
#: NAME rather than by position keeps that property — an arm renamed out of
#: existence still raises here at import — while no longer breaking on a
#: fourth arm being added.
#:
#: `ARM_C` is the treatment every other arm is compared against, so it is the
#: one name this module hardcodes.
ARM_C = "contract"
ARM_A = "schema_only"
ARM_B = "manual_prompt"
ARM_D = "contract_hollow"
for _name in (ARM_A, ARM_B, ARM_C, ARM_D):
    if _name not in ARMS:
        raise ValueError(
            f"dce.stats expects arm {_name!r}, absent from dce.arms.ARMS "
            f"({ARMS}) — rename the constant here or the arm there, but do "
            "not let this module compare against an arm that no longer exists"
        )

#: Every non-treatment arm, in `ARMS` order, each compared against `ARM_C`.
#: Derived so a fifth arm needs no edit here.
COMPARISON_ARMS: tuple[str, ...] = tuple(a for a in ARMS if a != ARM_C)

#: The pre-registered confirmatory comparison: arm B vs arm C.
PRIMARY_MODEL = "deepseek/deepseek-v4-pro-0813"
PRIMARY_LEFT_ARM = ARM_B
PRIMARY_RIGHT_ARM = ARM_C

#: Below this many discordant pairs, AND only for an already non-significant
#: p-value, McNemar has too little power to tell "no effect" apart from
#: "not enough disagreement to tell". A significant result is never flagged,
#: however few discordant pairs produced it.
LOW_POWER_DISCORDANT_THRESHOLD = 10

#: An arm/model slice is HARNESS-LIMITED once this fraction of its rows are
#: harness failures (not answers) -- a direct rate, tracking brokenness on
#: its own rather than brokenness multiplied by accuracy.
HARNESS_FAILURE_RATE_THRESHOLD = 0.10


def wilson(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval. Unlike the naive normal approximation, it
    doesn't collapse to a zero-width [1.0, 1.0] at the ceiling, which would
    claim a certainty a small perfect sample can't support.
    """
    if n == 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def mcnemar(rows_a: dict[str, bool], rows_b: dict[str, bool]) -> dict:
    """Paired exact McNemar test over the tasks present in *both* maps.

    `rows_a`/`rows_b` map task_id -> "this arm got it right", not "this row
    exists" -- callers decide the boolean rule (SCORED: only answer-verdict
    rows are keys at all, so a harness failure drops that task out of the
    pairing entirely; STRICT: every row this arm produced is a key, with a
    harness failure mapped to `False`). A task missing from either map is
    dropped from `n_paired`, not treated as a disagreement.

    `low_power` is `True` only when discordant pairs are scarce (`<
    LOW_POWER_DISCORDANT_THRESHOLD`) AND the result is already
    non-significant (`p_value >= 0.05`) -- see the module docstring's
    McNemar section for why a significant result must never be flagged
    low-power regardless of how few discordant pairs produced it.
    """
    shared = sorted(set(rows_a) & set(rows_b))
    a_only = sum(1 for t in shared if rows_a[t] and not rows_b[t])
    b_only = sum(1 for t in shared if rows_b[t] and not rows_a[t])
    discordant = a_only + b_only

    p = 1.0 if discordant == 0 else binomtest(b_only, discordant, 0.5).pvalue
    p = float(p)
    return {
        "n_paired": len(shared),
        "a_only": a_only,
        "b_only": b_only,
        "discordant": discordant,
        "p_value": p,
        "low_power": discordant < LOW_POWER_DISCORDANT_THRESHOLD and p >= 0.05,
    }


def accuracy_by(rows: list[dict], key: str) -> dict[str, tuple[int, int]]:
    """(correct_count, total_count) per distinct value of `row[key]`.

    Counts every row passed in, verdict and all -- callers control what
    "correct" means for their purpose by pre-filtering `rows` before
    calling this (e.g. to `ANSWER_VERDICTS`-only rows for SCORED accuracy;
    unfiltered for STRICT, where a harness failure counts against the
    denominator as a wrong answer simply by not being `"correct"`).
    """
    out: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        bucket = out[row.get(key, "unknown")]
        bucket[1] += 1
        bucket[0] += row.get("verdict") == "correct"
    return {k: (v[0], v[1]) for k, v in out.items()}


def load(path: Path) -> list[dict]:
    """Deduplicated rows from `path` -- exactly `dce.runner.latest_rows`,
    re-exported here so this module has exactly one entry point for
    reading the results file for SCORING, and it is the deduplicating one.
    See the module docstring's DEDUP section; never read `path` any other
    way to count outcomes. (`_raw_rows`, below, is the one deliberate
    exception, used only for total billed cost, never for scoring.)
    """
    return latest_rows(path)


def _raw_rows(path: Path) -> list[dict]:
    """Every row in `path`, verbatim, superseded duplicates included -- the
    deliberate exception to this module's dedup rule. Used only to total
    real billed spend: a paid attempt that a later resume superseded still
    cost real money, and `usd_total_billed` must count it, unlike every
    other figure in this module (see the COST section of the module
    docstring, and `dce.runner.real_spent_so_far`, which this mirrors
    per arm/model slice rather than file-wide).

    Reads through `dce.runner._read_rows` -- the same reader `load()`
    reaches through `latest_rows` -- so the two agree exactly about what
    the file contains, including its one forgiveness: an unparseable FINAL
    line (a torn tail from a killed or out-of-space write) is skipped, a
    corrupt line anywhere else still raises. Parsing the file a second,
    independent way here is what made a full disk take down the analysis as
    well as the resume.
    """
    return _read_rows(path)


def rescore(rows: list[dict]) -> list[dict]:
    """Re-grade every answered row from its stored `answer` and `gold`.

    Every row keeps the answer text and the gold it was graded against, so a
    scorer change is replayable without re-running (or re-paying for) a single
    model call. That property was designed in for adjudicating a disputed
    verdict; it earns its keep whenever the scorer itself moves.

    It has moved once already, and materially. Rows carrying
    `scorer: "fallback"` were graded by a hand-rolled normalizer STRICTER than
    DABStep's own — it marked `Yes.` wrong against a gold of `yes` — and that
    alone moved `schema_only vs contract` from p=0.0156 to p=0.0703 across the
    significance line. Reporting those stale verdicts as though they were the
    current grading would republish that false positive.

    Only `correct`/`incorrect` rows are touched. A `hit_limit` or `error` row
    has no answer to re-grade, and inventing one would convert a harness
    failure into a wrong answer.
    """
    from dce.grade import active_scorer, score

    out = []
    for row in rows:
        if row.get("verdict") not in ANSWER_VERDICTS:
            out.append(row)
            continue
        try:
            verdict = (
                "correct"
                if score(row.get("answer", ""), row.get("gold", ""))
                else "incorrect"
            )
        except Exception:
            verdict = "scoring_error"
        out.append({**row, "verdict": verdict, "scorer": active_scorer()})
    return out


def stale_scorer_rows(rows: list[dict]) -> dict[str, int]:
    """Rows whose recorded `scorer` is not the one installed now.

    Silence here is a claim: that the verdicts in this file were produced by
    the grading rules currently in force. When that is false the report must
    say so rather than let a reader assume it.
    """
    from dce.grade import active_scorer

    now = active_scorer()
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        # An ungolded row carries `scorer` like any other (`build_result_row`
        # sets it unconditionally) but `rescore` skips it — nothing graded it
        # and nothing will. Counting it both inflates the warning and makes
        # its claim, that every answered row has been re-graded, false.
        if _is_ungolded(row):
            continue
        recorded = row.get("scorer")
        if recorded and recorded != now:
            counts[recorded] += 1
    return dict(counts)


def _scored(rows: list[dict]) -> list[dict]:
    """Rows where the model produced a graded answer."""
    return [row for row in rows if row.get("verdict") in ANSWER_VERDICTS]


def _is_ungolded(row: dict) -> bool:
    """This row's task had no gold to score against.

    Keyed on `gold`, NOT on `verdict`. A task admitted by `--ungolded run`
    only reaches `verdict: "ungraded"` if `run_task` finishes cleanly; trip a
    cap or error and the row keeps `hit_limit`/`error`/`post_run_error`/
    `construction_error` with `gold: None`. A verdict-keyed cut lets those
    through into `strict_n` and `failure_rate`, which is exactly the
    invariant `--ungolded run` is supposed to preserve. The verdict is still
    checked, as a second signal on any row shape that predates `gold` being
    nullable.

    The `""` default is load-bearing: `_safe_json_dumps`' salvage envelope
    has no `gold` key at all, and a row we could not even serialise properly
    is not evidence that its task was ungolded. It stays in the denominator.
    """
    return row.get("gold", "") is None or row.get("verdict") in UNGRADED_VERDICTS


def _graded(rows: list[dict]) -> list[dict]:
    """Rows that ACCURACY and FAILURE-RATE arithmetic may see: everything
    except the tasks with no gold to score against.

    Deliberately not the same cut as `_scored`. A harness failure on a GOLDED
    task stays in (STRICT accuracy counts it against the arm, which is the
    point of the STRICT view); every row for an ungolded task comes out,
    however it ended, because that task is a different population — there is
    no right answer it could have missed and no accuracy its failure could
    make less trustworthy.

    Every accuracy or failure-rate denominator must go through this. Cost,
    `db_corrupted` and the instrumentation means must NOT: those rows were
    really billed and their working copies really were checked, and dropping
    them there would understate spend and silently delete governance evidence
    on the 49 tasks a submission sweep adds. What leaves the failure RATE is
    reported on its own line instead, so a cap trip across those 49 is
    counted rather than hidden.
    """
    return [row for row in rows if not _is_ungolded(row)]


def _failure_counts(rows: list[dict]) -> dict[str, int]:
    """Count of rows by verdict, restricted to harness-outcome verdicts."""
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        verdict = row.get("verdict")
        if verdict in HARNESS_VERDICTS:
            counts[verdict] += 1
    return dict(counts)


def _corruption_counts(rows: list[dict]) -> dict[str, int]:
    """Per arm/model slice: how many rows really did see the working copy
    mutated (`db_corrupted: True` -- the experiment's headline governance
    finding), versus how many rows have no trustworthy answer at all
    (`db_corrupted: None`, or the field absent on an older row shape -- an
    integrity check that failed or never ran, a materially different and
    more serious statement than "clean").
    """
    corrupted = sum(1 for row in rows if row.get("db_corrupted") is True)
    unknown = sum(1 for row in rows if row.get("db_corrupted") is None)
    return {"corrupted": corrupted, "unknown": unknown}


def _summarize(rows: list[dict], raw_rows: list[dict]) -> dict:
    """Everything `report()` needs for one arm/model slice: SCORED and
    STRICT accuracy (each with a Wilson interval), the harness-failure
    breakdown (counts and rate), the `db_corrupted` breakdown, both cost
    figures, and whether this slice's harness-failure rate is high enough
    to call its number harness-limited.

    `rows` is the deduplicated slice (one row per task for this arm/model)
    used for every count above. `raw_rows` is the SAME slice before
    deduplication, used only for `usd_total_billed` -- see the module
    docstring's COST section for why that one figure deliberately does not
    go through `load()`/`latest_rows`.

    `instrumentation` carries the per-row means of the token/turn counts
    and, for the `contract` arm only, the totals of the three governed-tool
    counters -- see the module docstring's INSTRUMENTATION section for what
    each is for and what it is not. `governance` is `None` for any slice
    that is not entirely arm C, so a mixed or ungoverned slice reports
    nothing rather than a structural zero.
    """
    # `graded_rows` for the accuracy block, the FULL slice for everything
    # else. `strict_n` was `len(rows)`, so an ungraded row counted against
    # STRICT accuracy exactly as a wrong answer would, and diluted
    # `failure_rate` -- the denominator HARNESS_FAILURE_RATE_THRESHOLD is
    # compared against -- so both read as a quieter run than the file
    # describes. Filtering `rows` outright instead was its own bug: it also
    # took those rows out of `usd_final`, `_corruption_counts` and
    # `_instrumentation`, where they belong (see `_graded`).
    graded_rows = _graded(rows)
    ungolded_rows = [row for row in rows if _is_ungolded(row)]
    ungolded_failures = sum(_failure_counts(ungolded_rows).values())
    scored_rows = _scored(graded_rows)
    scored_ok = sum(row.get("verdict") == "correct" for row in scored_rows)
    scored_n = len(scored_rows)
    strict_ok = sum(row.get("verdict") == "correct" for row in graded_rows)
    strict_n = len(graded_rows)
    failures = _failure_counts(graded_rows)
    failure_rate = sum(failures.values()) / strict_n if strict_n else 0.0
    return {
        "scored_ok": scored_ok,
        "scored_n": scored_n,
        "scored_ci": wilson(scored_ok, scored_n),
        "strict_ok": strict_ok,
        "strict_n": strict_n,
        "strict_ci": wilson(strict_ok, strict_n),
        "failures": failures,
        "failure_rate": failure_rate,
        "ungolded_n": len(ungolded_rows),
        "ungolded_failures": ungolded_failures,
        "corruption": _corruption_counts(rows),
        "usd_final": sum(row.get("usd") or 0.0 for row in rows),
        "usd_total_billed": sum(row.get("usd") or 0.0 for row in raw_rows),
        "harness_limited": failure_rate >= HARNESS_FAILURE_RATE_THRESHOLD,
        "instrumentation": _instrumentation(rows),
        "governance": _governance_counts(rows),
    }


def _mean(rows: list[dict], key: str) -> float:
    """Mean of `row[key]` over `rows`, treating a missing/null value as 0 --
    an older row shape must degrade, not raise (see the module docstring's
    last paragraph). `0.0` for an empty slice."""
    if not rows:
        return 0.0
    return sum(row.get(key, 0) or 0 for row in rows) / len(rows)


def _instrumentation(rows: list[dict]) -> dict:
    """Per-row means of the token and turn counts, plus the `cached_tokens`
    TOTAL for the slice.

    The total is reported next to the mean because the caching gate is a
    yes/no question -- did this arm get prompt-cached at all -- and a mean
    of, say, 12.4 over 400 rows answers it far less plainly than a total of
    4,960 next to a flat 0 on another arm.
    """
    return {
        "mean_input_tokens": _mean(rows, "input_tokens"),
        "mean_output_tokens": _mean(rows, "output_tokens"),
        "mean_cached_tokens": _mean(rows, "cached_tokens"),
        "cached_tokens_total": sum(row.get("cached_tokens", 0) or 0 for row in rows),
        "mean_turns": _mean(rows, "turns"),
    }


def _governance_counts(rows: list[dict]) -> dict | None:
    """Totals of the three governed-tool counters, for a slice that is
    entirely governed arms; `None` otherwise.

    Descriptive instrumentation, NOT a governance claim -- see the module
    docstring's INSTRUMENTATION section. Arms A and B have no contract to
    enforce, so reporting these for them would print a structural zero
    dressed as a measurement.

    BOTH governed arms, not just the treatment. `contract_hollow` is built
    through the same `_governed_tools` path and records the same three
    counters, and arm D exists precisely to separate contract TOOLING from
    contract CONTENT -- these counters are the direct measurement of that
    separation, so gating them to arm C dropped them from every report.
    """
    arms = {row.get("arm") for row in rows}
    if not rows or not arms <= {ARM_C, ARM_D}:
        return None
    return {
        key: sum(row.get(key, 0) or 0 for row in rows)
        for key in ("inspect_rejections", "enforcement_blocks", "retry_prompts")
    }


def _format_summary(label: str, summary: dict) -> str:
    s_lo, s_hi = summary["scored_ci"]
    t_lo, t_hi = summary["strict_ci"]
    flag = "  [HARNESS-LIMITED]" if summary["harness_limited"] else ""
    failures = summary["failures"]
    strict_n = summary["strict_n"] or 1  # rate display only; counts are exact
    fail_str = (
        ", ".join(f"{k}={v} ({v / strict_n:.0%})" for k, v in sorted(failures.items()))
        or "none"
    )
    corruption = summary["corruption"]
    # Its OWN line, not appended to the failures list: `failures (0% of
    # rows): none, ungraded=49` reads as though 49 rows were failures, inside
    # a line that just said 0%. Silence here is still a claim -- that no row
    # was set aside -- so the line appears whenever any were, and the harness
    # failures among them are counted rather than hidden by their exclusion
    # from `failure_rate`. Absent entirely on the four runs in FINDINGS.
    ungolded_line = (
        (
            f"\n{'':16s} ungolded: {summary['ungolded_n']} row(s) with no gold "
            f"to score, excluded from accuracy and from the failure rate "
            f"above; {summary['ungolded_failures']} harness failure(s) among "
            "them"
        )
        if summary["ungolded_n"]
        else ""
    )
    inst = summary["instrumentation"]
    instrumentation_line = (
        f"\n{'':16s} tokens/row: in={inst['mean_input_tokens']:.0f} "
        f"out={inst['mean_output_tokens']:.0f} "
        f"cached={inst['mean_cached_tokens']:.0f} "
        f"(cached total {inst['cached_tokens_total']:,}); "
        f"turns/row {inst['mean_turns']:.1f}"
    )
    governance = summary["governance"]
    governance_line = (
        (
            f"\n{'':16s} governed-tool counters (descriptive "
            "instrumentation, not a governance claim): "
            f"inspect_rejections={governance['inspect_rejections']} "
            f"enforcement_blocks={governance['enforcement_blocks']} "
            f"retry_prompts={governance['retry_prompts']}"
        )
        if governance
        else ""
    )
    return (
        f"{label:16s} scored {summary['scored_ok']:4d}/{summary['scored_n']:<4d} "
        f"[{s_lo:.3f},{s_hi:.3f}]   "
        f"strict {summary['strict_ok']:4d}/{summary['strict_n']:<4d} "
        f"[{t_lo:.3f},{t_hi:.3f}]   "
        f"cost final=${summary['usd_final']:.2f} "
        f"billed=${summary['usd_total_billed']:.2f}{flag}\n"
        f"{'':16s} failures ({summary['failure_rate']:.0%} of rows): "
        f"{fail_str}\n"
        f"{'':16s} db_corrupted: true={corruption['corrupted']} "
        f"unknown={corruption['unknown']}{ungolded_line}"
        f"{instrumentation_line}{governance_line}"
    )


def _scored_bools(rows: list[dict], arm: str) -> dict[str, bool]:
    """task_id -> correct, for the SCORED McNemar view: only answer-verdict
    rows for this arm are present at all, so a harness failure drops the
    task out of the pairing instead of being scored as a loss for either
    arm.
    """
    return {
        row.get("task_id", "unknown"): row.get("verdict") == "correct"
        for row in rows
        if row.get("arm") == arm and row.get("verdict") in ANSWER_VERDICTS
    }


def _strict_bools(rows: list[dict], arm: str) -> dict[str, bool]:
    """task_id -> correct, for the STRICT McNemar view: every task this arm
    attempted is present, with a harness failure counted as wrong.

    Ungolded tasks are the one exclusion, and the cut is `_is_ungolded` --
    the SAME cut `_summarize` makes, keyed on the missing gold rather than on
    the verdict. Keying it on `verdict in UNGRADED_VERDICTS` (as this did)
    kept every ungolded task whose run tripped a cap or errored, so the two
    denominators in one report disagreed: `strict 1/1` per arm above an
    `n_paired=2` line.

    A harness failure on a GOLDED task is a real attempt that produced no
    right answer, which is what STRICT is for; a task with no gold is not an
    attempt that failed. Pairing them adds concordant false/false pairs --
    harmless to `p_value`, which reads only discordant pairs, but `n_paired`
    would claim a comparison over tasks neither arm could be graded on.
    """
    return {
        row.get("task_id", "unknown"): row.get("verdict") == "correct"
        for row in rows
        if row.get("arm") == arm and not _is_ungolded(row)
    }


def _mcnemar_lines(rows: list[dict], left_arm: str, right_arm: str) -> list[str]:
    """SCORED and STRICT McNemar for `left_arm` vs `right_arm`, one line
    each, discordant count and (gated) low-power warning inline on every
    line.
    """
    lines: list[str] = []
    for view_name, bools in (("scored", _scored_bools), ("strict", _strict_bools)):
        result = mcnemar(bools(rows, left_arm), bools(rows, right_arm))
        warn = (
            f"  [LOW POWER: non-significant with only "
            f"{result['discordant']} discordant pairs -- "
            '"could not tell", not "no effect"]'
            if result["low_power"]
            else ""
        )
        lines.append(
            f"  McNemar ({view_name:6s}) {left_arm} vs {right_arm}: "
            f"p={result['p_value']:.4f} n_paired={result['n_paired']} "
            f"discordant={result['discordant']} "
            f"({right_arm}_only={result['b_only']}, "
            f"{left_arm}_only={result['a_only']}){warn}"
        )
    return lines


def _unequal_task_set_warning(rows: list[dict], model: str) -> list[str]:
    """A loud line when the arms in `rows` did not all see the same tasks.

    The comparison is paired and every accuracy denominator assumes each arm
    attempted the same task set -- but `dce.runner.sweep` can stop MID
    TASK-GROUP: both the `circuit_broken` and the `connection_leaked` paths
    `break` out of the per-arm loop before the remaining arms of that task
    have run, and a `--max-spend` truncation on a partly-written group does
    the same. The result is arms with different denominators, which McNemar
    silently absorbs (a task missing from either side is simply dropped
    from the pairing) and the Wilson intervals silently narrow or widen on.
    Nothing else in this module would say a word about it.
    """
    by_arm = {
        arm: {
            row.get("task_id")
            for row in rows
            if str(row.get("arm") or "unknown") == arm
        }
        for arm in sorted({str(row.get("arm") or "unknown") for row in rows})
    }
    distinct = {frozenset(ids) for ids in by_arm.values()}
    if len(by_arm) < 2 or len(distinct) == 1:
        return []
    everywhere = set.intersection(*by_arm.values())
    detail = ", ".join(
        f"{arm}={len(ids)} ({len(ids - everywhere)} not shared)"
        for arm, ids in by_arm.items()
    )
    return [
        f"  WARNING: the arms on {model} did NOT see the same task set "
        f"({detail}; {len(everywhere)} tasks common to all). A sweep that "
        "stopped mid task-group (a tripped circuit breaker, a leaked "
        "connection, or a truncated budget) leaves lopsided arms, so these "
        "accuracy denominators are not comparable as they stand and the "
        "McNemar pairing silently drops the unshared tasks. Re-run to "
        "complete the groups, or restrict the analysis to the shared tasks."
    ]


def report(path: Path, *, rescore_stale: bool = True) -> str:
    rows = load(path)
    raw_rows = _raw_rows(path)
    lines: list[str] = []

    from dce.golds import VERIFIED_WRONG_GOLDS

    # Tasks whose gold is verified wrong are dropped before anything is
    # counted. They cannot be answered correctly by construction, so leaving
    # them in depresses every arm and adds noise to the paired comparison --
    # and penalises hardest the arms most likely to reach the true value.
    dropped = sorted(
        {r["task_id"] for r in rows if r.get("task_id") in VERIFIED_WRONG_GOLDS}
    )
    if dropped:
        rows = [r for r in rows if r.get("task_id") not in VERIFIED_WRONG_GOLDS]
        raw_rows = [r for r in raw_rows if r.get("task_id") not in VERIFIED_WRONG_GOLDS]
        lines.append(
            f"NOTE: dropped {len(dropped)} task(s) with a verified-wrong gold "
            f"({', '.join(dropped)}) -- see dce.golds.VERIFIED_WRONG_GOLDS."
        )

    stale = stale_scorer_rows(rows)
    if stale:
        from dce.grade import active_scorer

        detail = ", ".join(
            f"{n} row(s) by {name!r}" for name, n in sorted(stale.items())
        )
        if rescore_stale:
            rows = rescore(rows)
            lines.append(
                f"NOTE: {detail} were graded by a scorer other than the one "
                f"installed now ({active_scorer()!r}); every answered row has "
                "been RE-GRADED from its stored answer and gold. Pass "
                "rescore_stale=False to report the stored verdicts verbatim."
            )
        else:
            lines.append(
                f"WARNING: {detail}, not the installed {active_scorer()!r}. "
                "Verdicts below are as recorded and are NOT comparable with "
                "rows graded by the current scorer."
            )
    if lines:
        lines.append("")

    def slice_of(pool: list[dict], model: str, arm: str) -> list[dict]:
        return [r for r in pool if r.get("model") == model and r.get("arm") == arm]

    # --- PRIMARY: the one pre-registered, confirmatory test ---------------
    lines.append("# PRIMARY (pre-registered)")
    lines.append(
        f"{PRIMARY_LEFT_ARM} vs {PRIMARY_RIGHT_ARM} on {PRIMARY_MODEL}, "
        "paired McNemar, reconstructed-gold task set"
    )
    primary_rows = [row for row in rows if row.get("model") == PRIMARY_MODEL]
    if not primary_rows:
        lines.append(f"(no rows for {PRIMARY_MODEL})")
    else:
        primary_summaries = {}
        for arm in (PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM):
            arm_rows = slice_of(rows, PRIMARY_MODEL, arm)
            raw_arm_rows = slice_of(raw_rows, PRIMARY_MODEL, arm)
            summary = _summarize(arm_rows, raw_arm_rows)
            primary_summaries[arm] = summary
            lines.append(_format_summary(arm, summary))
        lines.extend(_mcnemar_lines(primary_rows, PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM))
        # Scoped to the two arms this test actually pairs: a lopsided third
        # arm is a secondary-section problem, but a lopsided B/C pair
        # undermines the one pre-registered result.
        lines.extend(
            _unequal_task_set_warning(
                [
                    row
                    for row in primary_rows
                    if row.get("arm") in (PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM)
                ],
                PRIMARY_MODEL,
            )
        )
        if any(s["harness_limited"] for s in primary_summaries.values()):
            lines.append(
                "  NOTE: at least one arm above has a harness-failure rate "
                f">= {HARNESS_FAILURE_RATE_THRESHOLD:.0%} -- that arm's "
                "number is harness-limited, not a clean read on the arm "
                "itself. FINDINGS.md must say so."
            )

    # --- SECONDARY / EXPLORATORY: everything else --------------------------
    lines.append("\n# SECONDARY / EXPLORATORY (not pre-registered)")
    for model in sorted({row.get("model", "unknown") for row in rows}):
        lines.append(f"\n## {model}")
        subset = [row for row in rows if row.get("model") == model]
        raw_subset = [row for row in raw_rows if row.get("model") == model]

        for arm in sorted({str(row.get("arm") or "unknown") for row in subset}):
            arm_rows = [
                row for row in subset if str(row.get("arm") or "unknown") == arm
            ]
            raw_arm_rows = [
                row for row in raw_subset if str(row.get("arm") or "unknown") == arm
            ]
            lines.append(_format_summary(arm, _summarize(arm_rows, raw_arm_rows)))

            # `_graded` here too: this breakdown does not go through
            # `_summarize`, so it is a second STRICT denominator. Without it
            # the header line prints `strict 1/1` and the level line right
            # beneath prints `strict 1/3` off the same rows.
            graded_arm_rows = _graded(arm_rows)
            scored_by_level = accuracy_by(_scored(graded_arm_rows), "level")
            strict_by_level = accuracy_by(graded_arm_rows, "level")
            for level in sorted(set(scored_by_level) | set(strict_by_level)):
                s_ok, s_n = scored_by_level.get(level, (0, 0))
                t_ok, t_n = strict_by_level.get(level, (0, 0))
                s_lo, s_hi = wilson(s_ok, s_n)
                t_lo, t_hi = wilson(t_ok, t_n)
                lines.append(
                    f"    level={level:6s} "
                    f"scored {s_ok:3d}/{s_n:<3d} [{s_lo:.3f},{s_hi:.3f}]   "
                    f"strict {t_ok:3d}/{t_n:<3d} [{t_lo:.3f},{t_hi:.3f}]"
                )

        lines.extend(_unequal_task_set_warning(subset, model))

        for left in COMPARISON_ARMS:
            if model == PRIMARY_MODEL and left == PRIMARY_LEFT_ARM:
                lines.append(f"  McNemar {left} vs {ARM_C}: see PRIMARY section above")
                continue
            lines.extend(_mcnemar_lines(subset, left, ARM_C))

    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    print(report(Path(sys.argv[1] if len(sys.argv) > 1 else "results/results.jsonl")))
