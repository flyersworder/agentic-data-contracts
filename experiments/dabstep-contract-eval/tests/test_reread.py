"""`analysis/reread.py`: the committed contrasts re-read with the registered
engine, and corruption stratified by model."""

import sys
from collections import Counter
from pathlib import Path

import pytest
from scipy.stats import fisher_exact

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import reread as rr  # noqa: E402


def _row(label: str, model: str):
    return next(r for r in rr.ROWS if r.label == label and r.model == model)


def test_the_registered_ties_on_dabsteps_shapes_are_all_read():
    ties = {r.tie: (r.model, r.a, r.b) for r in rr.ROWS if r.tie}
    assert ties == {
        "(21)": ("luna", "contract", "contract_text"),
        "(29)": ("luna", "contract_text", "manual_summed"),
        "(29Q)": ("qwen", "contract_text", "manual_summed"),
        "(30)": ("luna", "contract_text_noted", "manual_summed"),
        "(30Q)": ("qwen", "contract_text_noted", "manual_summed"),
        "(41)": ("luna", "contract_noted", "contract_text_noted"),
        "(41Q)": ("qwen", "contract_noted", "contract_text_noted"),
        "(42)": ("luna", "contract_noted", "contract"),
    }


def test_each_contrast_reads_the_files_it_was_run_in():
    assert _row("route (5)", "qwen").files == ("exp5-qwen-r{}.jsonl",)
    assert _row("text vs prose (5b)", "luna").files == (
        "exp5-luna-r{}.jsonl",
        "exp5b-luna-r{}.jsonl",
    )
    assert _row("form (5c)", "luna").files == ("exp5c-luna-r{}.jsonl",)
    assert _row("route, notes both (5d)", "qwen").files == ("exp5d-qwen-r{}.jsonl",)
    assert _row("notes lift (5d)", "luna").a == "contract_noted"
    assert _row("notes lift (5d)", "luna").b == "contract"


def test_the_held_out_rows_are_the_second_notes_lift_on_both_task_sets():
    held = [r for r in rr.ROWS if r.among in ("unseen", "unseen shapes")]
    assert {(r.model, r.among) for r in held} == {
        ("luna", "unseen"),
        ("luna", "unseen shapes"),
        ("qwen", "unseen"),
        ("qwen", "unseen shapes"),
    }
    assert all((r.a, r.b) == ("manual_summed", "manual_resolved") for r in held)


def test_unseen_shapes_keep_only_shapes_without_an_inspected_task():
    shapes = {"1": "x", "2": "x", "3": "y", "4": "y", "5": "z"}
    assert rr.unseen_shapes({"1"}, {"1", "2", "3", "4", "5"}, shapes) == {
        "3",
        "4",
        "5",
    }


def test_one_stratum_exact_conditional_p_is_fishers():
    table = [(0, 100, 7, 120)]
    expected = fisher_exact([[0, 100], [7, 113]])[1]
    assert rr.exact_conditional(table) == pytest.approx(expected)


def test_the_exact_conditional_p_convolves_the_strata():
    """Brute force over two small strata: the sum of the first cells, given
    every margin."""
    from itertools import product

    from scipy.stats import hypergeom

    strata = [(0, 5, 3, 6), (1, 4, 2, 4)]
    dists = []
    for ex, nx, ey, ny in strata:
        m = ex + ey
        dist = hypergeom(nx + ny, m, nx)
        lo, hi = max(0, m - ny), min(m, nx)
        dists.append({k: dist.pmf(k) for k in range(lo, hi + 1)})
    total: Counter = Counter()
    for (k1, p1), (k2, p2) in product(dists[0].items(), dists[1].items()):
        total[k1 + k2] += p1 * p2
    observed = total[sum(s[0] for s in strata)]
    expected = sum(p for p in total.values() if p <= observed * (1 + 1e-7))
    assert rr.exact_conditional(strata) == pytest.approx(expected)


def test_the_mantel_haenszel_odds_ratio():
    strata = [(1, 10, 2, 10), (3, 20, 1, 20)]
    assert rr.mantel_haenszel(strata) == pytest.approx(1.825 / 1.325)
    assert rr.mantel_haenszel([(0, 10, 3, 10)]) == 0.0


def test_a_contrast_line_carries_the_reading_of_a_tie():
    from dce.inference import Contrast

    c = Contrast(
        n=199,
        shapes=25,
        nonzero=8,
        estimate=-0.018,
        p=0.82,
        exact=True,
        ci90=(-0.091, 0.056),
        ci95=(-0.106, 0.067),
        boot90=(0.0, 0.0),
        boot95=(0.0, 0.0),
        up=16,
        down=23,
        sign_p=0.34,
    )
    line = rr.line(_row("route, notes both (5d)", "luna"), c)
    assert line.startswith("(41)")
    assert "-1.8" in line and "[-9.1, +5.6]" in line and "[-10.6, +6.7]" in line
    assert "16/23" in line and "inconclusive" in line
    assert rr.line(_row("notes lift (5d)", "qwen"), c).startswith("(42Q)")
