"""LiveSQLBench's corrected arms: the same knowledge, completed, two ways.

`manual_corrected` is `manual_prompt` reading a corrected KB, and
`contract_corrected` is a governed arm reading a corrected contract. Both
live under `LSB_DATA/corrected/` -- outside the repository, because the
corrections are derived from the gold -- as `kb/<db>_kb.jsonl` and
`contracts/<db>/`.
"""

import json
import shutil

import pytest
from dce.benchmarks.livesqlbench import LiveSQLBench

CORRECTED = ("manual_corrected", "contract_corrected")


def _setup(lsb_data, tmp_path):
    from dce.benchmarks import livesqlbench as lsb
    from dce.compile.run import compile_database

    from tests.test_compile_run import shop_driver

    inputs = lsb.compile_inputs(lsb_data, "shop")
    base = tmp_path / "contract"
    compile_database(inputs, base / "shop", shop_driver([]), spend_cap=10.0)
    corrected = lsb_data / "corrected"
    shutil.copytree(base / "shop", corrected / "contracts" / "shop")
    path = corrected / "contracts" / "shop" / "contract.yml"
    text = path.read_text(encoding="utf-8")
    path.write_text(text.replace("livesqlbench-shop", "shop-corrected"), "utf-8")
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    rows = [json.loads(x) for x in kb.read_text(encoding="utf-8").splitlines() if x]
    rows[0]["definition"] += " Clarification: a missing value counts as zero."
    (corrected / "kb").mkdir(parents=True)
    (corrected / "kb" / "shop_kb.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    bench = LiveSQLBench.from_data(lsb_data, contracts=base)
    return bench


def _task(bench, task_id="shop_1"):
    return next(t for t in bench.tasks() if t.task_id == task_id)


def test_they_are_extra_arms_and_only_the_contract_is_governed():
    for arm in CORRECTED:
        assert arm in LiveSQLBench.extra_arms and arm not in LiveSQLBench.arms
    assert "contract_corrected" in LiveSQLBench.governed_arms
    assert "manual_corrected" not in LiveSQLBench.governed_arms


def test_manual_corrected_is_the_manual_with_the_corrected_kb(lsb_data, tmp_path):
    bench = _setup(lsb_data, tmp_path)
    task = _task(bench)
    db = lsb_data / "duckdb" / "shop.duckdb"
    manual = bench.build_arm("manual_prompt", task, db)
    fixed = bench.build_arm("manual_corrected", task, db)
    try:
        assert "a missing value counts as zero" in fixed.system_prompt
        assert "a missing value counts as zero" not in manual.system_prompt
        assert [t.name for t in fixed.tools] == [t.name for t in manual.tools]
    finally:
        manual.close()
        fixed.close()


def test_contract_corrected_reads_the_private_contract(lsb_data, tmp_path):
    bench = _setup(lsb_data, tmp_path)
    task = _task(bench)
    arm = bench.build_arm(
        "contract_corrected", task, lsb_data / "duckdb" / "shop.duckdb"
    )
    try:
        assert "shop-corrected" in arm.system_prompt
    finally:
        arm.close()


def test_the_digests_record_the_corrected_files(lsb_data, tmp_path):
    bench = _setup(lsb_data, tmp_path)
    task = _task(bench)
    digests = {a: bench.arm_digest(a, task) for a in ("manual_prompt", *CORRECTED)}
    assert len(set(digests.values())) == 3


def test_check_arms_needs_the_corrected_files(lsb_data, tmp_path):
    bench = _setup(lsb_data, tmp_path)
    tasks = bench.tasks()
    bench.check_arms(CORRECTED, tasks)
    (lsb_data / "corrected" / "kb" / "shop_kb.jsonl").unlink()
    with pytest.raises(SystemExit, match="manual_corrected"):
        bench.check_arms(("manual_corrected",), tasks)
    shutil.rmtree(lsb_data / "corrected" / "contracts")
    with pytest.raises(SystemExit, match="contract_corrected"):
        bench.check_arms(("contract_corrected",), tasks)
