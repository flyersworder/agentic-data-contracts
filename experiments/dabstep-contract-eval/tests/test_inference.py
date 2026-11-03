"""The registered shape-level engine (experiment 5e's design, *Inference*):
the task-weighted mean difference, a sign-flip test on per-shape sums,
intervals by inverting that test, the shape bootstrap beside, verdicts
against a 5-point bound, and Holm."""

import numpy as np
import pytest
from dce import inference as inf


def _diffs(values: dict[str, float]) -> dict[str, float]:
    return dict(values)


def _one_task_per_shape(sums: list[float]) -> tuple[dict, dict]:
    d = {f"t{i}": v for i, v in enumerate(sums)}
    return d, {t: f"s{i}" for i, t in enumerate(d)}


def test_task_differences_are_mean_runs_and_a_missing_task_counts_zero():
    a = {"1": 3, "2": 1}
    b = {"1": 1, "3": 2}
    d = inf.task_differences(a, b, ["1", "2", "3"], runs=3)
    assert d == pytest.approx({"1": 2 / 3, "2": 1 / 3, "3": -2 / 3})


def test_the_estimate_is_task_weighted_whatever_the_shapes():
    d = {"a": 1.0, "b": 1.0, "c": 0.0, "d": -1 / 3}
    shapes = {"a": "x", "b": "x", "c": "y", "d": "z"}
    c = inf.contrast(d, shapes)
    assert c.estimate == pytest.approx((1 + 1 + 0 - 1 / 3) / 4)
    assert c.n == 4 and c.shapes == 3


def test_shape_sums_put_a_task_without_a_shape_in_a_cluster_of_its_own():
    sums, sizes = inf.shape_sums({"a": 1.0, "b": 0.5, "c": 0.25}, {"a": "x"})
    assert sorted(sizes.tolist()) == [1, 1, 1]
    assert sorted(sums.tolist()) == [0.25, 0.5, 1.0]


def test_two_shapes_are_enumerated_exactly():
    d, shapes = _one_task_per_shape([3.0, 1.0])
    p, exact = inf.flip_p(*inf.shape_sums(d, shapes), n=2)
    assert exact and p == pytest.approx(0.5)


def test_seven_shapes_all_one_way_give_the_minimum_p_of_the_design():
    """'a contrast with seven or fewer non-zero shapes cannot reach Holm
    significance (minimum p about 0.016)': 2 / 2**7."""
    d, shapes = _one_task_per_shape([1.0] * 7 + [0.0] * 5)
    c = inf.contrast(d, shapes)
    assert c.exact and c.nonzero == 7
    assert c.p == pytest.approx(2 / 2**7)


def test_zero_shapes_do_not_change_the_p_value():
    d1, s1 = _one_task_per_shape([1.0, 2.0, -0.5, 1.5])
    d2, s2 = _one_task_per_shape([1.0, 2.0, -0.5, 1.5, 0.0, 0.0, 0.0])
    assert inf.contrast(d1, s1).p == pytest.approx(inf.contrast(d2, s2).p)


def test_many_shapes_are_sampled_with_a_fixed_seed():
    d, shapes = _one_task_per_shape([1.0] * 20)
    c = inf.contrast(d, shapes)
    assert not c.exact
    # Exactly 2 / 2**20; 100,000 flips see it as (1 + hits) / (1 + flips).
    assert c.p < 1e-4
    assert c.p >= 1 / (1 + inf.FLIPS)
    assert inf.contrast(d, shapes).p == c.p


def test_a_one_sided_p_reads_the_named_tail():
    d, shapes = _one_task_per_shape([1.0, 2.0, 0.5, 1.5, 1.0])
    sums, _ = inf.shape_sums(d, shapes)
    less, _ = inf.flip_p(sums, n=5, side="less")
    greater, _ = inf.flip_p(sums, n=5, side="greater")
    assert greater == pytest.approx(1 / 32)
    assert less == pytest.approx(1.0)


def test_the_interval_inverts_the_test():
    rng = np.random.default_rng(1)
    d = {f"t{i}": float(rng.integers(-3, 4)) / 3 for i in range(60)}
    shapes = {t: f"s{i % 12}" for i, t in enumerate(d)}
    c = inf.contrast(d, shapes)
    lo95, hi95 = c.ci95
    lo90, hi90 = c.ci90
    assert lo95 <= lo90 <= c.estimate <= hi90 <= hi95
    sums, sizes = inf.shape_sums(d, shapes)
    n = len(d)
    eps = 1e-3
    # Just inside the bound the shifted test does not reject; just outside it does.
    for delta, inside in ((hi95 - eps, True), (hi95 + eps, False)):
        p, _ = inf.flip_p(sums - delta * sizes, n=n)
        assert (p >= 0.05) == inside
    for delta, inside in ((lo95 + eps, True), (lo95 - eps, False)):
        p, _ = inf.flip_p(sums - delta * sizes, n=n)
        assert (p >= 0.05) == inside
    assert (c.p < 0.05) == (not lo95 <= 0 <= hi95)


def test_a_symmetric_set_gets_a_symmetric_interval():
    d, shapes = _one_task_per_shape([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])
    c = inf.contrast(d, shapes)
    lo, hi = c.ci95
    assert lo + hi == pytest.approx(2 * c.estimate, abs=1e-6)


def test_too_few_shapes_leave_the_interval_at_the_range_of_the_differences():
    """Four shapes cannot reject anything at 5%: 2 / 16 > 0.05."""
    d, shapes = _one_task_per_shape([0.2, 0.4, 0.6, 1.0])
    c = inf.contrast(d, shapes)
    assert c.ci95 == pytest.approx((0.2, 1.0))


def test_the_shape_bootstrap_is_reported_and_seeded():
    rng = np.random.default_rng(2)
    d = {f"t{i}": float(rng.integers(-3, 4)) / 3 for i in range(80)}
    shapes = {t: f"s{i % 20}" for i, t in enumerate(d)}
    c = inf.contrast(d, shapes)
    assert c.boot95[0] <= c.boot90[0] <= c.boot90[1] <= c.boot95[1]
    assert inf.contrast(d, shapes).boot95 == c.boot95
    one = inf.contrast({"a": 0.5, "b": 0.5}, {"a": "x", "b": "x"})
    assert one.boot95 == pytest.approx((0.5, 0.5))


def test_task_level_sign_counts_and_test():
    d = {"a": 1.0, "b": 1 / 3, "c": 0.0, "d": -1 / 3}
    c = inf.contrast(d, {t: t for t in d})
    assert (c.up, c.down) == (2, 1)
    assert c.sign_p == pytest.approx(1.0)


def test_non_superiority_and_non_inferiority_read_the_90_percent_interval():
    c = inf.Contrast(
        n=199,
        shapes=25,
        nonzero=20,
        estimate=-0.01,
        p=0.6,
        exact=False,
        ci90=(-0.04, 0.049),
        ci95=(-0.05, 0.06),
        boot90=(0.0, 0.0),
        boot95=(0.0, 0.0),
        up=10,
        down=12,
        sign_p=0.8,
    )
    assert inf.non_superior(c)
    assert inf.non_inferior(c)
    wide = inf.Contrast(**{**c.__dict__, "ci90": (-0.051, 0.05)})
    assert not inf.non_superior(wide)
    assert not inf.non_inferior(wide)


def test_a_tie_reads_equivalent_inconclusive_or_different():
    base = dict(
        n=199,
        shapes=25,
        nonzero=20,
        estimate=0.0,
        exact=False,
        ci95=(0.0, 0.0),
        boot90=(0.0, 0.0),
        boot95=(0.0, 0.0),
        up=1,
        down=1,
        sign_p=1.0,
    )
    eq = inf.Contrast(p=0.5, ci90=(-0.03, 0.04), **base)
    wide = inf.Contrast(p=0.5, ci90=(-0.03, 0.07), **base)
    diff = inf.Contrast(p=0.01, ci90=(0.01, 0.04), **base)
    assert inf.reading(eq) == "equivalent"
    assert inf.reading(wide) == "inconclusive"
    assert inf.reading(diff) == "different"


def test_the_one_sided_p_at_a_bound_agrees_with_the_90_percent_interval():
    rng = np.random.default_rng(3)
    d = {f"t{i}": float(rng.integers(-1, 2)) / 3 for i in range(120)}
    shapes = {t: f"s{i % 24}" for i, t in enumerate(d)}
    c = inf.contrast(d, shapes)
    p_sup = inf.bound_p(d, shapes, inf.BOUND, "less")
    p_inf = inf.bound_p(d, shapes, -inf.BOUND, "greater")
    assert (p_sup < 0.05) == inf.non_superior(c)
    assert (p_inf < 0.05) == inf.non_inferior(c)


def test_holm_steps_down_and_stays_monotone():
    adj = inf.holm({"a": 0.01, "b": 0.04, "c": 0.03, "d": 0.005})
    assert adj == pytest.approx({"a": 0.03, "b": 0.06, "c": 0.06, "d": 0.02})
    assert inf.holm({"x": 0.7, "y": 0.6}) == pytest.approx({"x": 1.0, "y": 1.0})
