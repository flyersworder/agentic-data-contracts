"""Experiment 5d: with both facts stated alike, do the governed tools still
add to the same contract pasted as text?

Reproduces FINDINGS.md, *Tools with the facts stated alike: predictions
(41)-(42Q)*. Experiment 5 compared `contract` with `contract_text`, the same
contract behind the governed tools or pasted in the prompt: a tie on luna's
rule-matching tasks and +10.1 points for the tools on Qwen. But both arms
carried the contract's original wording, which states the summing of
overlapping rules less plainly than `SUM_NOTE` ("the fee rule that matches"),
and Qwen read it as one rule per transaction in about half its runs.
Experiment 5c added the two notes to the text arm only. `contract_noted` adds
them, word for word and under the same headings, to the governed arm, so
`contract_noted` and `contract_text_noted` state the same facts alike and
differ only in the contract's route. k=3, the 401 golded DABStep tasks;
`contract`, `contract_text_noted` and `contract_noted` run together in each
repeat, on gpt-6-luna and on qwen3.8-27b.

Primary subset and measure as experiments 5 and 5c: the rule-matching tasks
(both blind raters), end-to-end strict, task-level sign tests over three
repeats.

  (41)  gpt-6-luna and (41Q) qwen3.8-27b: contract_noted and
        contract_text_noted tie there: within 5 points and two-sided
        p >= 0.10.
  (42Q) qwen3.8-27b: contract_noted beats contract there, one-sided
        p < 0.05: the notes repair the governed arm as they repaired the
        text.
  (42)  gpt-6-luna: contract_noted and contract tie there.

Fixed before the run, how the outcomes read. (41Q) holding means Qwen's
+10.1 for the tools in experiment 5 was the original wording, not the
route. (41Q) failing with the governed arm ahead means the lookup adds to
the facts on Qwen even when they are stated alike: a delivery effect,
model-dependent if (41) holds. Failing with the text ahead means the route
costs Qwen accuracy once the facts are plain.

Also reported: the official verdicts beside end-to-end ones, the rest and
all 401, intervals clustered over question shapes and over fee families
(the paper's primary uncertainty, added before any row was read), and a
drift check -- each rerun arm against its earlier files, by
task and repeat, flagged when the two differ by more than 5 points.

Prints aggregates only.

Run:  uv run python analysis/governed_noted.py [--model luna|qwen]
"""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from contract_noted import _acc, drifted, scores  # noqa: E402
from contract_text import _sign, is_tie, rule_matching  # noqa: E402

FILES = {"luna": "exp5d-luna-r{}.jsonl", "qwen": "exp5d-qwen-r{}.jsonl"}
ARMS = ("contract", "contract_text_noted", "contract_noted")
#: Where each rerun arm ran before, for the drift check.
EARLIER = {
    "luna": {
        "contract": "exp5-luna-r{}.jsonl",
        "contract_text_noted": "exp5c-luna-r{}.jsonl",
    },
    "qwen": {
        "contract": "exp5-qwen-r{}.jsonl",
        "contract_text_noted": "exp5c-qwen-r{}.jsonl",
    },
}
#: model -> (label of the route test, label of the notes test).
LABELS = {"luna": ("(41)", "(42)"), "qwen": ("(41Q)", "(42Q)")}

Verdict = tuple[bool, float, int, int, float]


def route_tie(score: dict[str, Counter[str]], tasks: set[str]) -> Verdict:
    """(41)/(41Q): the same facts, looked up or pasted, tie."""
    a, b = "contract_noted", "contract_text_noted"
    up, down, p = _sign(score, a, b, tasks, "two-sided")
    gap = _acc(score[a], tasks) - _acc(score[b], tasks)
    return is_tie(gap, p), gap, up, down, p


def notes_test(model: str, score: dict[str, Counter[str]], tasks: set[str]) -> Verdict:
    """(42Q): the notes lift Qwen's governed arm, one-sided; (42): on luna,
    where the original wording was not misread, they tie."""
    gap = _acc(score["contract_noted"], tasks) - _acc(score["contract"], tasks)
    if model == "qwen":
        up, down, p = _sign(score, "contract_noted", "contract", tasks, "greater")
        return p < 0.05, gap, up, down, p
    up, down, p = _sign(score, "contract_noted", "contract", tasks, "two-sided")
    return is_tie(gap, p), gap, up, down, p


#: The two registered comparisons, as (a, b).
PAIRS = (("contract_noted", "contract_text_noted"), ("contract_noted", "contract"))


def clustered_contrasts(
    score: dict[str, Counter[str]],
    subsets: tuple[tuple[str, set[str]], ...],
    clusterings: dict[str, dict[str, str]],
) -> list[tuple[str, dict]]:
    """Each registered comparison on each subset, with the task interval and
    one interval and sign test per clustering (`clustered.contrast`)."""
    from clustered import contrast

    return [
        (
            f"{label}: {a} vs {b}",
            contrast(score[a], score[b], among, runs=3, clusterings=clusterings),
        )
        for label, among in subsets
        for a, b in PAIRS
    ]


def main() -> None:
    import argparse

    import procedural_load as pl

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=sorted(FILES), default="luna")
    model = parser.parse_args().model
    results = ROOT / "results"
    first = pl.read_ratings(pl.PUBLIC_RATINGS, "r1")["dabstep"]
    second = pl.read_ratings(pl.PUBLIC_RATINGS, "r2")["dabstep"]
    views = {
        "end-to-end": scores(results, FILES[model], ARMS),
        "official": scores(results, FILES[model], ARMS, e2e=False),
    }
    tasks = set(views["end-to-end"]["contract_noted"])
    rm = rule_matching(first, second) & tasks
    subsets = (("rule-matching", rm), ("rest", tasks - rm), ("all", tasks))
    for view, score in views.items():
        print(f"== {view}")
        for label, among in subsets:
            print(
                f"{label:14s} {len(among):4d} tasks  "
                + "  ".join(f"{a} {_acc(score[a], among):.1%}" for a in ARMS)
            )
            for a, b in (
                ("contract_noted", "contract_text_noted"),
                ("contract_noted", "contract"),
                ("contract", "contract_text_noted"),
            ):
                up, down, p = _sign(score, a, b, among, "two-sided")
                print(f"    {a} vs {b}: {up} / {down}, two-sided p = {p:.2g}")

    print("== drift: each rerun arm against its earlier files (end-to-end)")
    score = views["end-to-end"]
    for arm, pattern in EARLIER[model].items():
        before = scores(results, pattern, (arm,))[arm]
        for label, among in subsets:
            gap = _acc(score[arm], among) - _acc(before, among)
            up = sum(score[arm][t] > before[t] for t in among)
            down = sum(score[arm][t] < before[t] for t in among)
            flag = "  DRIFT" if drifted(gap) else ""
            print(
                f"   {arm} {label}: now {_acc(score[arm], among):.1%}, "
                f"before {_acc(before, among):.1%}, {gap:+.1%} ({up} / {down}){flag}"
            )

    from clustered import dabstep_clusterings, report

    print("== clustered (end-to-end)")
    for label, r in clustered_contrasts(score, subsets, dabstep_clusterings()):
        report(label, r)

    route_label, notes_label = LABELS[model]
    for label, (holds, gap, up, down, p) in (
        (route_label, route_tie(score, rm)),
        (notes_label, notes_test(model, score, rm)),
    ):
        print(
            f"{label} {'holds' if holds else 'fails'}: gap {gap:+.1%}, "
            f"{up} / {down}, p = {p:.2g}"
        )


if __name__ == "__main__":
    main()
