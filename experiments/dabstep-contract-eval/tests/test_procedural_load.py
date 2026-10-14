import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import procedural_load as pl  # noqa: E402


def test_load_is_the_mean_of_the_two_raters():
    assert pl.task_load({"load": 3}, {"load": 2}) == 2.5


def test_high_load_starts_at_two():
    assert pl.is_high(2.0)
    assert not pl.is_high(1.5)


def test_quadratic_weighted_kappa():
    assert pl.weighted_kappa([0, 1, 2, 3], [0, 1, 2, 3]) == pytest.approx(1.0)
    # One-step disagreements cost less than a three-step one.
    near = pl.weighted_kappa([0, 1, 2, 3], [1, 1, 2, 3])
    far = pl.weighted_kappa([0, 1, 2, 3], [3, 1, 2, 3])
    assert far < near < 1.0


def test_gain_is_contract_minus_prose_over_runs():
    score = {"contract": {"t": 5}, "prose": {"t": 2}}
    assert pl.gain(score, "contract", "prose", "t", runs=6) == pytest.approx(0.5)


def test_permutation_p_is_small_when_high_tasks_gain_more():
    gains = {f"h{i}": 1.0 for i in range(8)} | {f"l{i}": 0.0 for i in range(8)}
    hi = [t for t in gains if t[0] == "h"]
    lo = [t for t in gains if t[0] == "l"]
    assert pl.permutation_p(gains, hi, lo, n=2000) < 0.01
    assert pl.permutation_p(gains, lo, hi, n=2000) > 0.99


def test_permutation_p_counts_the_observed_labelling():
    gains = {"a": 0.0, "b": 0.0}
    assert pl.permutation_p(gains, ["a"], ["b"], n=9) == 1.0


def test_split_by_load_keeps_only_tasks_rated_by_both():
    first = {"t1": {"load": 3}, "t2": {"load": 0}, "t3": {"load": 2}}
    second = {"t1": {"load": 2}, "t2": {"load": 1}}
    loads = pl.loads(first, second)
    assert loads == {"t1": 2.5, "t2": 0.5}
    hi, lo = pl.split(loads, {"t1", "t2", "t9"})
    assert (hi, lo) == (["t1"], ["t2"])


def test_ratings_are_read_through_the_key(tmp_path):
    (tmp_path / "key.json").write_text(
        '{"P001": {"bench": "dabstep", "task": "1712"},'
        ' "P002": {"bench": "livesqlbench", "task": "shop_1"}}',
        encoding="utf-8",
    )
    (tmp_path / "rater-r1-c1.jsonl").write_text(
        '{"id": "P001", "load": 3}\n{"id": "P002", "load": 1}\n', encoding="utf-8"
    )
    out = pl.read_ratings(tmp_path, "r1")
    assert out["dabstep"] == {"1712": {"id": "P001", "load": 3}}
    assert out["livesqlbench"] == {"shop_1": {"id": "P002", "load": 1}}
