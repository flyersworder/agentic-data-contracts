"""Offline statistics over the results JSONL.

DEDUP: `report()` and every other reader of the results file MUST go
through `dce.runner.latest_rows` -- never read `path` line by line -- before
counting anything. A retried unit leaves several rows for one
`(task_id, arm, model)` key in the raw file; `latest_rows` keeps only the
last row written per key. Skipping that step counts a unit's stale
failures on top of its real, later outcome -- measured: a unit that
succeeded after three failed resumes would score 1/4 = 25% instead of
1/1 = 100%. See `dce.runner.latest_rows`'s own docstring for the full case.
`load()` below is a thin, explicit wrapper around it so every function in
this module goes through the same door.

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
  * STRICT accuracy: `correct` / all rows -- harness failures counted as
    wrong.

Excluding harness failures (SCORED) flatters whichever arm fails most;
counting them as wrong (STRICT) instead punishes that arm for what may be
our bug, not its answer. Reporting only one hides which failure mode is in
play, so both are always printed side by side, along with each arm's raw
failure breakdown, and `report()` flags an arm as HARNESS-LIMITED when the
two diverge by `MATERIAL_DIVERGENCE_PP` or more (or when there are no
scored rows at all) -- that flag is what `FINDINGS.md` is required to
repeat, not something a human has to notice by comparing two numbers.

McNemar is the right significance test because every arm sees every task --
the comparison is paired, not independent samples. Its discordant-pair
count is reported alongside every p-value it produces, because near the
ceiling discordant pairs get scarce and a non-significant result then
means "this design could not tell", not "the arms are equal"; a count
below `LOW_POWER_DISCORDANT_THRESHOLD` gets an explicit low-power warning.
Every McNemar comparison is run in both the SCORED view (harness-failure
tasks dropped from the pairing for both arms) and the STRICT view (a
harness failure counts as a wrong answer for whichever arm hit it), for
the same reason accuracy is: a harness bug on one arm's task must not
silently read as that arm losing a paired comparison it never actually
lost.

THE PRIMARY, pre-registered test is `manual_prompt` (arm B) vs `contract`
(arm C) on `PRIMARY_MODEL`, paired McNemar, over the reconstructed-gold
task set. `report()` prints it first and labelled as such. Every other
comparison this module produces -- arm A, the other models, the level
strata -- is secondary and exploratory, and is printed under that label,
never mixed in with the primary result. Naming one confirmatory test in
advance is what keeps the result from being a search: this design runs
3 arms x 3 models x 2 levels worth of possible comparisons, and at that
count something crosses p<0.05 by chance alone.

Cost is reported from `usd` -- real, billed spend -- never `usd_guard`,
which is the spend cap's own pessimistic ledger (see
`dce.agent.build_result_row` and `dce.runner._construction_error_row`) and
deliberately overstates real spend.
"""

from __future__ import annotations

from collections import defaultdict
from math import sqrt
from pathlib import Path

from scipy.stats import binomtest

from dce.runner import latest_rows

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

#: The pre-registered confirmatory comparison. Arm names match
#: `dce.arms.ARMS` ("schema_only" = A, "manual_prompt" = B, "contract" = C).
PRIMARY_MODEL = "deepseek/deepseek-v4-pro-0813"
PRIMARY_LEFT_ARM = "manual_prompt"
PRIMARY_RIGHT_ARM = "contract"

#: Below this many discordant pairs, McNemar has too little power to tell
#: "no effect" apart from "not enough disagreement to tell".
LOW_POWER_DISCORDANT_THRESHOLD = 10

#: A SCORED-vs-STRICT gap this large (a fraction, e.g. 0.05 = 5 points)
#: for one arm/model means harness failures are moving the number enough
#: that it must be read as harness-limited, not as a clean accuracy result.
MATERIAL_DIVERGENCE_PP = 0.05


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
    """
    shared = sorted(set(rows_a) & set(rows_b))
    a_only = sum(1 for t in shared if rows_a[t] and not rows_b[t])
    b_only = sum(1 for t in shared if rows_b[t] and not rows_a[t])
    discordant = a_only + b_only

    p = 1.0 if discordant == 0 else binomtest(b_only, discordant, 0.5).pvalue
    return {
        "n_paired": len(shared),
        "a_only": a_only,
        "b_only": b_only,
        "discordant": discordant,
        "p_value": float(p),
        "low_power": discordant < LOW_POWER_DISCORDANT_THRESHOLD,
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
        bucket[0] += row["verdict"] == "correct"
    return {k: (v[0], v[1]) for k, v in out.items()}


def load(path: Path) -> list[dict]:
    """Deduplicated rows from `path` -- exactly `dce.runner.latest_rows`,
    re-exported here so this module has exactly one entry point for
    reading the results file, and it is the deduplicating one. See the
    module docstring's DEDUP section; never read `path` any other way for
    scoring.
    """
    return latest_rows(path)


def _scored(rows: list[dict]) -> list[dict]:
    """Rows where the model produced a graded answer."""
    return [row for row in rows if row["verdict"] in ANSWER_VERDICTS]


def _failure_counts(rows: list[dict]) -> dict[str, int]:
    """Count of rows by verdict, restricted to harness-outcome verdicts."""
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if row["verdict"] in HARNESS_VERDICTS:
            counts[row["verdict"]] += 1
    return dict(counts)


def _summarize(rows: list[dict]) -> dict:
    """Everything `report()` needs for one arm/model slice: SCORED and
    STRICT accuracy (each with a Wilson interval), the harness-failure
    breakdown, real spend, and whether this slice's SCORED/STRICT gap is
    large enough to call its number harness-limited.
    """
    scored_rows = _scored(rows)
    scored_ok = sum(row["verdict"] == "correct" for row in scored_rows)
    scored_n = len(scored_rows)
    strict_ok = sum(row["verdict"] == "correct" for row in rows)
    strict_n = len(rows)
    scored_pt = scored_ok / scored_n if scored_n else 0.0
    strict_pt = strict_ok / strict_n if strict_n else 0.0
    # No scored rows at all is harness-limited by definition, regardless of
    # what the (0.0 vs 0.0) point-estimate gap happens to compute to.
    harness_limited = (
        scored_n == 0 or abs(scored_pt - strict_pt) >= MATERIAL_DIVERGENCE_PP
    )
    return {
        "scored_ok": scored_ok,
        "scored_n": scored_n,
        "scored_ci": wilson(scored_ok, scored_n),
        "strict_ok": strict_ok,
        "strict_n": strict_n,
        "strict_ci": wilson(strict_ok, strict_n),
        "failures": _failure_counts(rows),
        "usd": sum(row["usd"] for row in rows),
        "harness_limited": harness_limited,
    }


def _format_summary(label: str, summary: dict) -> str:
    s_lo, s_hi = summary["scored_ci"]
    t_lo, t_hi = summary["strict_ci"]
    flag = "  [HARNESS-LIMITED]" if summary["harness_limited"] else ""
    failures = summary["failures"]
    fail_str = ", ".join(f"{k}={v}" for k, v in sorted(failures.items())) or "none"
    return (
        f"{label:16s} scored {summary['scored_ok']:4d}/{summary['scored_n']:<4d} "
        f"[{s_lo:.3f},{s_hi:.3f}]   "
        f"strict {summary['strict_ok']:4d}/{summary['strict_n']:<4d} "
        f"[{t_lo:.3f},{t_hi:.3f}]   "
        f"${summary['usd']:.2f}{flag}\n"
        f"{'':16s} failures: {fail_str}"
    )


def _scored_bools(rows: list[dict], arm: str) -> dict[str, bool]:
    """task_id -> correct, for the SCORED McNemar view: only answer-verdict
    rows for this arm are present at all, so a harness failure drops the
    task out of the pairing instead of being scored as a loss for either
    arm.
    """
    return {
        row["task_id"]: row["verdict"] == "correct"
        for row in rows
        if row["arm"] == arm and row["verdict"] in ANSWER_VERDICTS
    }


def _strict_bools(rows: list[dict], arm: str) -> dict[str, bool]:
    """task_id -> correct, for the STRICT McNemar view: every task this arm
    attempted is present, with a harness failure counted as wrong.
    """
    return {
        row["task_id"]: row["verdict"] == "correct" for row in rows if row["arm"] == arm
    }


def _mcnemar_lines(rows: list[dict], left_arm: str, right_arm: str) -> list[str]:
    """SCORED and STRICT McNemar for `left_arm` vs `right_arm`, one line
    each, discordant count and low-power warning inline on every line.
    """
    lines: list[str] = []
    for view_name, bools in (("scored", _scored_bools), ("strict", _strict_bools)):
        result = mcnemar(bools(rows, left_arm), bools(rows, right_arm))
        warn = (
            "  [LOW POWER: too few discordant pairs to tell]"
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


def report(path: Path) -> str:
    rows = load(path)
    lines: list[str] = []

    # --- PRIMARY: the one pre-registered, confirmatory test ---------------
    lines.append("# PRIMARY (pre-registered)")
    lines.append(
        f"{PRIMARY_LEFT_ARM} vs {PRIMARY_RIGHT_ARM} on {PRIMARY_MODEL}, "
        "paired McNemar, reconstructed-gold task set"
    )
    primary_rows = [row for row in rows if row["model"] == PRIMARY_MODEL]
    if not primary_rows:
        lines.append(f"(no rows for {PRIMARY_MODEL})")
    else:
        primary_summaries = {}
        for arm in (PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM):
            arm_rows = [row for row in primary_rows if row["arm"] == arm]
            summary = _summarize(arm_rows)
            primary_summaries[arm] = summary
            lines.append(_format_summary(arm, summary))
        lines.extend(_mcnemar_lines(primary_rows, PRIMARY_LEFT_ARM, PRIMARY_RIGHT_ARM))
        if any(s["harness_limited"] for s in primary_summaries.values()):
            lines.append(
                "  NOTE: SCORED and STRICT accuracy diverge by "
                f">= {MATERIAL_DIVERGENCE_PP:.0%} (or one arm has no scored "
                "rows) for at least one arm above -- that arm's number is "
                "harness-limited, not a clean read on the arm itself. "
                "FINDINGS.md must say so."
            )

    # --- SECONDARY / EXPLORATORY: everything else --------------------------
    lines.append("\n# SECONDARY / EXPLORATORY (not pre-registered)")
    for model in sorted({row["model"] for row in rows}):
        lines.append(f"\n## {model}")
        subset = [row for row in rows if row["model"] == model]

        for arm in sorted({row["arm"] for row in subset}):
            arm_rows = [row for row in subset if row["arm"] == arm]
            lines.append(_format_summary(arm, _summarize(arm_rows)))
            by_level = accuracy_by(_scored(arm_rows), "level")
            for level, (ok, n) in sorted(by_level.items()):
                lo, hi = wilson(ok, n)
                lines.append(
                    f"    level={level:6s} scored {ok:3d}/{n:<3d} [{lo:.3f},{hi:.3f}]"
                )

        for left in ("schema_only", "manual_prompt"):
            if model == PRIMARY_MODEL and left == PRIMARY_LEFT_ARM:
                lines.append(f"  McNemar {left} vs contract: see PRIMARY section above")
                continue
            lines.extend(_mcnemar_lines(subset, left, "contract"))

    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    print(report(Path(sys.argv[1] if len(sys.argv) > 1 else "results/results.jsonl")))
