"""How many of an arm's errors can be found WITHOUT the golds?

The two systems above this experiment on DABStep's validated leaderboard both
build their artifact against the benchmark's ground-truth answers. One half of
NVIDIA's method does not: their offline *group-consistency* check reads only
the agent's own answers across similar questions and flags the ones solved by
a conflicting method. Nothing about a contract frozen before any question was
read rules that out — so this script asks what it is worth here, and reports
the answer as a yield rather than as an accuracy.

THE RULE, AND WHY IT NEVER TOUCHES A GOLD

    1. Group tasks by question SHAPE, reusing `analysis/templates.py`'s
       normaliser (entities and numbers replaced, so sibling questions from
       one template collapse together).
    2. Within a group, reduce each answer to a token-class SIGNATURE:
       `D:219.36` -> `A:N`, `TransactPlus:3458.48` -> `W:N`, `42.9` -> `N`.
       Precision and Markdown bold deliberately do not survive — this asks
       what KIND of thing was answered, not how it was written.
    3. Flag every row whose signature is a strict minority of its group.

Steps 1-3 read the question text and the arm's own answers. The gold is used
only afterwards, to score the flags, and `precision` below is therefore a
measurement of the rule rather than an input to it.

WHAT THE OUTPUT MEANS

    errors     rows the (reconstructed) golds call incorrect
    base       share of the arm's rows that are errors — what blind guessing
               would score, and the number `precision` has to beat
    flags      rows the rule flags, knowing no golds
    precision  share of flags that really are errors
    lift       precision / base — 1.0x is a rule worth nothing
    recall     share of the arm's errors the rule catches

A rule that flagged everything would have precision == base and recall 1.0;
one that flagged nothing would have no precision at all. Both numbers are
needed, and neither is a score for the arm.

Run:  uv run python analysis/group_consistency.py [results/*.jsonl ...]

No API calls. Reads `data/tasks.json`, `data/dabstep.duckdb` (read-only, for
the merchant and MCC vocabularies the normaliser needs) and the results files.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dce.stats import ANSWER_VERDICTS, load  # noqa: E402

ARMS = ["contract", "contract_hollow", "manual_prompt", "schema_only"]

#: Groups smaller than this have no majority worth being a minority of.
MIN_GROUP = 3

_BOLD = re.compile(r"^\*+|\*+$")
# The inner group must END on a digit, or a thousands separator would swallow
# the comma that separates two list items ("156, 159" is a list, not a number).
_NUMBER = re.compile(r"[-+]?\d(?:[\d,]*\d)?(?:\.\d+)?%?")
_LETTER = re.compile(r"\b[A-Za-z]\b")
_WORD = re.compile(r"[A-Za-z][A-Za-z_'-]*")
#: Sentinels, because the class marks are themselves letters and would be
#: rewritten by the next pass.
_MARKS = {"\x01": "N", "\x02": "A", "\x03": "W"}


def signature(answer: str | None) -> str:
    """Reduce an answer to its token classes: number, single letter, word.

    `D:219.36` and `TransactPlus:3458.48` differ (`A:N` against `W:N`) because
    they answer different kinds of thing. `42.9` and `42.89798400000003` do
    not, because they do not.
    """
    text = _BOLD.sub("", (answer or "").strip()).strip()
    if not text:
        return ""
    text = _NUMBER.sub("\x01", text)
    text = _LETTER.sub("\x02", text)
    text = _WORD.sub("\x03", text)
    for mark, name in _MARKS.items():
        text = text.replace(mark, name)
    return re.sub(r"\s+", " ", text).strip()


def flag_minorities(signatures: list[str], min_group: int = MIN_GROUP) -> list[int]:
    """Indices whose signature is a strict minority of the group.

    A group with no unique modal signature flags nothing: two answers
    disagreeing two-all identifies no odd one out, and guessing which side is
    the majority is exactly the kind of tuning this check is meant to avoid.
    """
    if len(signatures) < min_group:
        return []
    counts = Counter(signatures)
    top = max(counts.values())
    if sum(1 for count in counts.values() if count == top) > 1:
        return []
    return [i for i, sig in enumerate(signatures) if counts[sig] < top]


def question_shapes() -> dict[str, str]:
    """task_id -> normalised question, via `analysis/templates.py`."""
    spec = importlib.util.spec_from_file_location(
        "templates", Path(__file__).parent / "templates.py"
    )
    assert spec and spec.loader
    templates = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(templates)

    con = duckdb.connect()
    con.execute(f"ATTACH '{ROOT / 'data' / 'dabstep.duckdb'}' AS src (READ_ONLY)")
    merchants = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT merchant FROM src.merchant_data"
        ).fetchall()
    ]
    descriptions = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT description FROM src.merchant_category_codes"
        ).fetchall()
    ]
    con.close()

    tasks = json.loads((ROOT / "data" / "tasks.json").read_text())
    tasks = tasks if isinstance(tasks, list) else tasks.get("tasks", tasks)
    return {
        str(t["task_id"]): templates.normalise(t["question"], merchants, descriptions)
        for t in tasks
    }


def against_official(shapes: dict[str, str]) -> None:
    """The same rule on the one file that has Adyen's own verdicts.

    Every other block scores the rule against reconstructed golds, which the
    leaderboard submission showed to be wrong on 19 tasks. This block scores it
    against the official ones — and against those 19 in particular, since a
    check that spots errors without golds ought to spot the errors the golds
    themselves got wrong.
    """
    local = {
        str(r["task_id"]): r
        for r in (
            json.loads(line)
            for line in (ROOT / "results" / "glm-all450.jsonl").read_text().splitlines()
            if line.strip()
        )
    }
    official = {
        str(r["task_id"]): r
        for r in (
            json.loads(line)
            for line in (ROOT / "results" / "glm-all450-official-scores.jsonl")
            .read_text()
            .splitlines()
            if line.strip()
        )
    }

    groups: dict[str, list[str]] = defaultdict(list)
    for task_id in local:
        groups[shapes.get(task_id, "")].append(task_id)

    considered, flagged = [], []
    for members in groups.values():
        if len(members) < MIN_GROUP:
            continue
        considered.extend(members)
        sigs = [signature(local[m].get("answer")) for m in members]
        flagged.extend(members[i] for i in flag_minorities(sigs))

    print("\nglm-all450 (contract arm), scored both ways")
    graders = {
        "reconstructed golds": lambda t: local[t]["verdict"] == "incorrect",
        "official golds": lambda t: official[t]["score"] is False,
    }
    for name, wrong in graders.items():
        errors = sum(1 for t in considered if wrong(t))
        hits = sum(1 for t in flagged if wrong(t))
        base = errors / len(considered)
        precision = hits / len(flagged) if flagged else 0.0
        print(
            f"  {name:<21} base {base:6.1%} (n={len(considered)})  "
            f"flags {len(flagged):>3}  precision {precision:6.1%}  "
            f"lift {(precision / base if base else 0):.2f}x  "
            f"recall {(hits / errors if errors else 0):5.1%}"
        )

    disputed = {
        t
        for t in official
        if t in local
        and local[t]["verdict"] == "correct"
        and official[t]["score"] is False
    }
    false_alarms = [t for t in flagged if local[t]["verdict"] == "correct"]
    print(
        f"  of the {len(disputed)} tasks the official grading overturned, "
        f"the rule flags {len(disputed & set(flagged))}"
    )
    print(
        f"  of its {len(false_alarms)} flags on locally-correct rows, "
        f"{sum(1 for t in false_alarms if official[t]['score'] is False)} "
        "are officially wrong"
    )


def main(paths: list[str], shapes: dict[str, str]) -> None:

    for path in paths:
        rows = [r for r in load(Path(path)) if r.get("verdict") in ANSWER_VERDICTS]
        if not rows:
            continue
        print(f"\n{path}")
        print(
            f"  {'arm':<17}{'n':>5}{'errors':>8}{'base':>8}"
            f"{'flags':>7}{'precision':>11}{'lift':>7}{'recall':>8}"
        )
        for arm in ARMS:
            groups: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                if row["arm"] == arm:
                    groups[shapes.get(str(row["task_id"]), "")].append(row)

            considered, flagged = [], []
            for members in groups.values():
                if len(members) < MIN_GROUP:
                    continue
                considered.extend(members)
                sigs = [signature(m.get("answer")) for m in members]
                flagged.extend(members[i] for i in flag_minorities(sigs))

            if not considered:
                continue
            errors = sum(1 for r in considered if r["verdict"] == "incorrect")
            hits = sum(1 for r in flagged if r["verdict"] == "incorrect")
            base = errors / len(considered)
            precision = hits / len(flagged) if flagged else 0.0
            print(
                f"  {arm:<17}{len(considered):>5}{errors:>8}{base:>7.1%}"
                f"{len(flagged):>7}{precision:>10.1%}"
                f"{(precision / base if base else 0):>6.1f}x"
                f"{(hits / errors if errors else 0):>8.1%}"
            )


if __name__ == "__main__":
    args = sys.argv[1:] or [
        str(p) for p in sorted((ROOT / "results").glob("*-full.jsonl"))
    ]
    shapes = question_shapes()
    main(args, shapes)
    against_official(shapes)
