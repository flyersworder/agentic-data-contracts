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
