import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import contract_text as ct  # noqa: E402


def test_rule_matching_needs_both_raters():
    first = {"1": {"rule_matching": True}, "2": {"rule_matching": True}}
    second = {"1": {"rule_matching": True}, "2": {"rule_matching": False}}
    assert ct.rule_matching(first, second) == {"1"}


def test_tie_needs_both_a_small_gap_and_no_significant_sign_test():
    assert ct.is_tie(gap=0.04, p=0.5)
    assert not ct.is_tie(gap=0.06, p=0.5)
    assert not ct.is_tie(gap=0.01, p=0.05)


def test_each_model_reads_its_own_repeats():
    assert ct.FILES["luna"] == "exp5-luna-r{}.jsonl"
    assert ct.FILES["qwen"] == "exp5-qwen-r{}.jsonl"


def test_qwen_predicts_the_tools_add_to_the_text():
    # (26) replicates (20); (27) replaces (21)'s tie with a one-sided lead.
    assert ct.PREDICTIONS["luna"] == ("(20)", "(21)", "tie")
    assert ct.PREDICTIONS["qwen"] == ("(26)", "(27)", "greater")
