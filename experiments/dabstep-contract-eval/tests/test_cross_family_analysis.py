"""The pure parts of `analysis/cross_family.py` (prep/cross-family-ratings/PLAN.md).

The robustness rule is the plan's section D: a result that was significant in
the paper is robust on an alternative subset when the estimate keeps its sign
and stays significant; a result that was not significant gets no claim.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import cross_family as cf  # noqa: E402


def test_a_significant_result_that_keeps_sign_and_significance_is_robust():
    assert cf.verdict(3.7, (0.6, 7.9), 4.1, (0.2, 8.0)) == "robust"


def test_losing_significance_is_not_robust():
    assert cf.verdict(3.7, (0.6, 7.9), 3.0, (-0.5, 6.4)) == "not robust"


def test_flipping_sign_is_not_robust_even_when_significant():
    assert cf.verdict(3.7, (0.6, 7.9), -2.0, (-4.0, -0.1)) == "not robust"


def test_a_negative_significant_result_is_robust_when_it_stays_negative():
    assert cf.verdict(-16.0, (-24.0, -8.0), -10.0, (-18.0, -2.0)) == "robust"


def test_a_result_not_significant_in_the_paper_gets_no_claim():
    assert cf.verdict(5.5, (-0.4, 10.4), 9.0, (2.0, 15.0)) == "no claim"


def test_a_p_value_can_stand_in_for_an_interval():
    assert cf.verdict(46.0, None, 40.0, None, ref_p=1e-9, alt_p=1e-6) == "robust"
    assert cf.verdict(46.0, None, 40.0, None, ref_p=1e-9, alt_p=0.2) == "not robust"


def test_new_load_ratings_are_keyed_by_benchmark_and_task():
    key = {
        "P001": {"bench": "dabstep", "task": "1857"},
        "P002": {"bench": "livesqlbench", "task": "shop_1"},
    }
    rows = [{"id": "P001", "rule_matching": True}, {"id": "P002", "load": 1}]
    out = cf.by_task(rows, key)
    assert out == {
        "dabstep": {"1857": {"id": "P001", "rule_matching": True}},
        "livesqlbench": {"shop_1": {"id": "P002", "load": 1}},
    }


def test_new_determinacy_ratings_are_keyed_by_task():
    rows = [{"id": "C001", "kb_clarity": "clear"}]
    assert cf.det_by_task(rows, {"C001": "shop_1"}) == {
        "shop_1": {"id": "C001", "kb_clarity": "clear"}
    }


def test_cohens_kappa_is_one_for_identical_labels_and_zero_for_chance():
    assert cf.kappa([True, False, True], [True, False, True]) == 1.0
    assert cf.kappa([True, True, False, False], [True, False, True, False]) == 0.0
