"""The contract with the two facts in place (experiment 5e) is derived, not
authored, so its difference from the frozen contract must be exactly the two
edits the design names and nothing else.

E1 inserts `DATA_NOTE` after the fees domain's sentence stating the manual's
null rule; E2 replaces the first sentence of `total_transaction_fees`'
description with `SUM_NOTE`. Both by text substitution on the raw files, so
the comments, the order and every other line stay byte-identical: the pasted
arm must differ from `contract_text` in the two edits alone.
"""

import difflib
import re
from pathlib import Path

import pytest
import yaml
from dce import plain
from dce.benchmarks.dabstep import DATA_NOTE, SUM_NOTE

from agentic_data_contracts import DataContract

ROOT = Path(__file__).parent.parent
REAL = ROOT / "contract"
PLAIN = ROOT / "contract_plain"
FILES = ("contract.yml", "semantic.yml")


def _raw(d: Path, name: str) -> str:
    return (d / name).read_text(encoding="utf-8")


def _flat(text: str) -> str:
    return " ".join(text.split())


def test_generation_is_idempotent(tmp_path: Path):
    first = plain.build(REAL, tmp_path / "a")
    second = plain.build(REAL, tmp_path / "b")
    for name in FILES:
        assert _raw(first, name) == _raw(second, name)


def test_committed_artifact_matches_the_generator(tmp_path: Path):
    fresh = plain.build(REAL, tmp_path / "fresh")
    for name in FILES:
        assert _raw(fresh, name) == _raw(PLAIN, name), (
            f"{name} is stale -- re-run `python -m dce.plain`"
        )


def test_e1_inserts_the_data_note_after_the_null_rule_and_nothing_else():
    """With whitespace folded, the derived contract.yml is the original with
    the note inserted after "... for every ACI." and no other change."""
    before, after = (
        _flat(_raw(REAL, "contract.yml")),
        _flat(_raw(PLAIN, "contract.yml")),
    )
    anchor = "a null `aci` means the rule applies for every ACI."
    assert before.count(anchor) == 1
    assert after == before.replace(anchor, f"{anchor} {_flat(DATA_NOTE)}")


def test_e2_replaces_the_first_sentence_with_the_sum_note_and_nothing_else():
    before, after = (
        _flat(_raw(REAL, "semantic.yml")),
        _flat(_raw(PLAIN, "semantic.yml")),
    )
    old = (
        "Total fees over a set of transactions: the per-transaction fee summed "
        "after each transaction has been joined to the fee rule that matches it."
    )
    assert before.count(old) == 1
    assert after == before.replace(old, _flat(SUM_NOTE))


def test_the_raw_diff_is_one_hunk_per_file_and_no_comment_moves():
    """Line by line, each file changes in one place only, and no changed
    line is a comment: the edits sit inside the two block scalars."""
    for name in FILES:
        diff = list(
            difflib.unified_diff(
                _raw(REAL, name).splitlines(),
                _raw(PLAIN, name).splitlines(),
                n=0,
                lineterm="",
            )
        )
        hunks = [line for line in diff if line.startswith("@@")]
        assert len(hunks) == 1, name
        changed = [
            line[1:]
            for line in diff
            if line[:1] in "+-" and not line.startswith(("+++", "---"))
        ]
        assert changed and not any(c.lstrip().startswith("#") for c in changed)


def _paths(value, prefix=()):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _paths(v, (*prefix, k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from _paths(v, (*prefix, i))
    else:
        yield prefix, value


def test_the_parsed_contracts_differ_in_exactly_two_fields():
    differ = []
    for name in FILES:
        real = dict(_paths(yaml.safe_load(_raw(REAL, name))))
        derived = dict(_paths(yaml.safe_load(_raw(PLAIN, name))))
        assert real.keys() == derived.keys(), name
        differ += [(name, p) for p in real if real[p] != derived[p]]
    assert len(differ) == 2
    (c_file, c_path), (s_file, s_path) = differ
    assert c_file == "contract.yml" and c_path[-1] == "description"
    assert s_file == "semantic.yml" and s_path[-1] == "description"


def test_the_parsed_texts_read_as_the_notes_in_place():
    contract = yaml.safe_load(_raw(PLAIN, "contract.yml"))
    fees = next(d for d in contract["semantic"]["domains"] if d["name"] == "fees")
    assert (
        f"a null `aci` means the rule applies for every ACI. {DATA_NOTE} "
        "A fee rule therefore matches"
    ) in fees["description"]
    semantic = yaml.safe_load(_raw(PLAIN, "semantic.yml"))
    total = next(
        m for m in semantic["metrics"] if m["name"] == "total_transaction_fees"
    )
    assert total["description"].startswith(f"{SUM_NOTE} Use the same matching")


def test_the_sql_is_unchanged():
    real = yaml.safe_load(_raw(REAL, "semantic.yml"))
    derived = yaml.safe_load(_raw(PLAIN, "semantic.yml"))
    assert [m["sql_expression"] for m in derived["metrics"]] == [
        m["sql_expression"] for m in real["metrics"]
    ]


def test_the_name_is_kept():
    """A suffix would show in the governed prompt."""
    real = DataContract.from_yaml(REAL / "contract.yml")
    derived = DataContract.from_yaml(PLAIN / "contract.yml")
    assert derived.name == real.name


def test_the_digest_is_its_own():
    from dce.frozen import digest

    assert plain.plain_digest() != digest()
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", plain.plain_digest())


def test_a_missing_or_repeated_anchor_is_refused():
    with pytest.raises(ValueError, match="once"):
        plain.splice("no anchor here\n", "every ACI.", "x", keep=True)
    with pytest.raises(ValueError, match="once"):
        plain.splice("  every ACI. every\n  ACI.\n", "every ACI.", "x", keep=True)


def test_splice_matches_across_folded_line_breaks_and_rewraps_at_the_indent():
    raw = "  d: >\n    one two\n    three four. five six\n    seven.\n"
    out = plain.splice(raw, "two three four.", "NEW TEXT.", keep=False)
    assert out == "  d: >\n    one\n    NEW TEXT.\n    five six\n    seven.\n"
    assert yaml.safe_load(out)["d"] == "one NEW TEXT. five six seven.\n"
    ins = plain.splice(raw, "two three four.", "ADDED.", keep=True)
    assert yaml.safe_load(ins)["d"] == "one two three four. ADDED. five six seven.\n"


def test_wrapping_never_breaks_a_hyphenated_word():
    """A folded line break reads as a space: "list-\\ntyped" would become
    "list- typed"."""
    for name in FILES:
        for line in _raw(PLAIN, name).splitlines():
            assert not re.search(r"\w-$", line), line
