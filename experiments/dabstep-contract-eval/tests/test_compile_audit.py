from dce.benchmarks import livesqlbench
from dce.compile.audit import render
from dce.compile.run import compile_database

from tests.test_compile_run import shop_driver


def test_the_audit_counts_and_samples_reproducibly(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    bench_dir = tmp_path / "livesqlbench"
    compile_database(inputs, bench_dir / "shop", shop_driver([]), spend_cap=10.0)
    text = render(bench_dir, n=2, seed=7)
    assert "| shop | 6 | 2 | 2 | 2 | 4 | 0 | 2 | 100% | 0 | $1.50 |" in text
    assert text == render(bench_dir, n=2, seed=7)  # the sample is seeded
    assert text.count("Verdict: agrees / disagrees / unclear") == 2
    assert render(bench_dir, n=0, seed=7).count("Verdict:") == 4  # all metrics
