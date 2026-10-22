import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import clustered as cl  # noqa: E402


def test_a_task_difference_is_the_gap_in_correct_runs_over_the_repeats():
    a = {"1": 3, "2": 0, "3": 2}
    b = {"1": 0, "2": 0}
    d = cl.diffs(a, b, ["1", "2", "3"], runs=3)
    assert d == {"1": 1.0, "2": 0.0, "3": 2 / 3}


def test_a_constant_difference_has_a_degenerate_interval_at_that_value():
    d = {str(i): 0.25 for i in range(20)}
    assert cl.task_interval(d) == pytest.approx((0.25, 0.25))
    cluster_of = {str(i): str(i % 4) for i in range(20)}
    assert cl.cluster_interval(d, cluster_of) == pytest.approx((0.25, 0.25))


def test_the_bootstrap_is_reproducible_under_the_fixed_seed():
    d = {str(i): (i % 3) / 3 for i in range(30)}
    assert cl.task_interval(d) == cl.task_interval(d)


def test_clustering_widens_the_interval_when_tasks_move_together():
    # Ten clusters of ten identical tasks: half gain a full point, half
    # lose nothing. Over tasks the interval is tight; over clusters it is
    # as wide as ten draws allow.
    d, cluster_of = {}, {}
    for c in range(10):
        for i in range(10):
            t = f"{c}-{i}"
            d[t] = 1.0 if c < 5 else 0.0
            cluster_of[t] = str(c)
    lo_t, hi_t = cl.task_interval(d)
    lo_c, hi_c = cl.cluster_interval(d, cluster_of)
    assert lo_c < lo_t and hi_c > hi_t
    assert lo_t <= 0.5 <= hi_t


def test_a_task_without_a_cluster_is_its_own_cluster():
    d = {"a": 1.0, "b": 1.0, "c": 0.0}
    up, down, _ = cl.cluster_sign(d, {"a": "f", "b": "f"})
    assert (up, down) == (1, 0)
    assert cl.n_clusters(d, {"a": "f", "b": "f"}) == 2


def test_the_cluster_sign_test_counts_clusters_by_their_summed_difference():
    d = {"a": 1.0, "b": -1 / 3, "c": -1 / 3, "d": 0.0, "e": 1 / 3, "f": -1 / 3}
    cluster_of = {"a": "x", "b": "x", "c": "y", "d": "y", "e": "z", "f": "z"}
    # x sums to +1/3, y to -1/3, z to 0 (a tie, dropped).
    up, down, p = cl.cluster_sign(d, cluster_of)
    assert (up, down) == (1, 1)
    assert p == 1.0


def test_the_cluster_sign_test_is_one_sided_when_asked():
    d = {str(i): 1.0 for i in range(4)}
    cluster_of = {str(i): str(i) for i in range(4)}
    assert cl.cluster_sign(d, cluster_of, "greater")[2] == pytest.approx(1 / 16)
    assert cl.cluster_sign(d, cluster_of)[2] == pytest.approx(1 / 8)


def test_clopper_pearson_matches_known_values():
    lo, hi = cl.clopper_pearson(0, 10)
    assert lo == 0.0
    assert hi == pytest.approx(1 - 0.025 ** (1 / 10))
    lo, hi = cl.clopper_pearson(10, 10)
    assert hi == 1.0
    assert lo == pytest.approx(0.025 ** (1 / 10))
    # 3 of 540: the experiment 6 count the reviewer bounded at about 1.6%.
    assert cl.clopper_pearson(3, 540)[1] == pytest.approx(0.0162, abs=5e-4)


def test_clopper_pearson_accepts_an_effective_count():
    lo, hi = cl.clopper_pearson(1.5, 270.0)
    assert 0 < lo < 1.5 / 270 < hi < 1


def test_the_rule_of_three_bounds_a_zero_count():
    assert cl.rule_of_three(22138) == pytest.approx(1.355e-4, rel=1e-3)
    assert cl.rule_of_three(300) == 0.01


def test_a_log_odds_gain_is_the_change_in_logit():
    assert cl.logit_gain(0.5, 0.5) == 0.0
    assert cl.logit_gain(0.39, 0.60) == pytest.approx(
        math.log(0.6 / 0.4) - math.log(0.39 / 0.61)
    )
    # Equal points, unequal log-odds: a gain from a low floor counts for more.
    assert cl.logit_gain(0.06, 0.14) > cl.logit_gain(0.39, 0.47)


def test_a_log_odds_gain_survives_a_rate_at_zero_or_one():
    assert math.isfinite(cl.logit_gain(0.0, 1.0))


def test_a_ratio_interval_resamples_numerator_and_denominator_together():
    num = {str(i): 1.0 for i in range(10)}
    den = {str(i): 2.0 for i in range(10)}
    lo, hi, dropped = cl.ratio_interval(num, den)
    assert (lo, hi, dropped) == pytest.approx((0.5, 0.5, 0))


def test_a_ratio_resample_without_a_positive_denominator_is_dropped():
    num = {"a": 1.0, "b": 0.0}
    den = {"a": 1.0, "b": 0.0}
    lo, hi, dropped = cl.ratio_interval(num, den, resamples=2000)
    assert (lo, hi) == (1.0, 1.0)
    assert 0 < dropped < 2000


def test_the_design_effect_is_one_without_clustering_and_grows_with_it():
    values = {str(i): float(i % 2) for i in range(40)}
    singletons = {t: t for t in values}
    assert cl.design_effect(values, singletons) == pytest.approx(1.0, rel=0.05)
    together = {t: str(int(t) % 2) for t in values}
    assert cl.design_effect(values, together) > 5


def test_a_contrast_reports_every_clustering_it_is_given():
    a = {str(i): 3 for i in range(12)}
    b = {str(i): 0 for i in range(12)}
    out = cl.contrast(a, b, list(a), runs=3, clusterings={"x": {t: "one" for t in a}})
    assert out["effect"] == 1.0
    assert out["tasks"] == (12, 0)
    assert out["clusters"]["x"]["n"] == 1
    assert out["clusters"]["x"]["sign"][:2] == (1, 0)


def test_a_knowledge_piece_is_the_database_and_the_question_kind():
    assert cl.knowledge_piece("solar_panel_R07") == "solar_panel:R"
    assert (
        cl.knowledge_piece("labor_certification_applications_C10")
        == "labor_certification_applications:C"
    )


def test_a_fee_task_names_fees():
    assert cl.names_fees("What is the average fee for account type R?")
    assert cl.names_fees("Which Fee IDs apply?")
    assert not cl.names_fees("What is the most expensive MCC for 10 euros?")
