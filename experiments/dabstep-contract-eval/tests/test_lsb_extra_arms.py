"""LiveSQLBench's extra contract arms and the database filter.

`contract_opus` and `contract_authored` are `contract` with another contract
for the task's database: the claudeopus5.5 compile with declared joins, and
the contract a whole-database author session wrote. Each reads its own
directory; nothing else about the arm changes. `--lsb-dbs` restricts a sweep
to named databases.
"""

import argparse
import shutil

import pytest
from dce.benchmarks import livesqlbench as livesqlbench_module
from dce.benchmarks.livesqlbench import LiveSQLBench

EXTRA = ("contract_opus", "contract_authored")


def _compiled(lsb_data, tmp_path):
    from dce.compile.run import compile_database

    from tests.test_compile_run import shop_driver

    roots = {}
    inputs = livesqlbench_module.compile_inputs(lsb_data, "shop")
    for arm in ("contract", *EXTRA):
        root = tmp_path / arm
        compile_database(inputs, root / "shop", shop_driver([]), spend_cap=10.0)
        roots[arm] = root
    # Make the three contracts distinguishable.
    for arm in EXTRA:
        path = roots[arm] / "shop" / "contract.yml"
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace("livesqlbench-shop", f"shop-{arm}"), "utf-8")
    bench = LiveSQLBench.from_data(
        lsb_data,
        contracts=roots["contract"],
        extra_contracts={a: roots[a] for a in EXTRA},
    )
    return bench, roots


def _task(bench, task_id):
    return next(t for t in bench.tasks() if t.task_id == task_id)


def test_they_are_extra_governed_arms():
    for arm in EXTRA:
        assert arm in LiveSQLBench.extra_arms
        assert arm not in LiveSQLBench.arms
        assert arm in LiveSQLBench.governed_arms


def test_the_default_directories():
    root = livesqlbench_module.DEFAULT_CONTRACTS.parent.parent
    assert livesqlbench_module.EXTRA_CONTRACTS == {
        "contract_opus": root / "contracts-opus-joins" / "livesqlbench",
        "contract_authored": root / "contracts-authored" / "livesqlbench",
    }


@pytest.mark.parametrize("arm", EXTRA)
def test_each_is_contract_with_its_own_contract(lsb_data, tmp_path, arm):
    from dce.frozen import load_contract_at

    bench, roots = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_4")
    db = lsb_data / "duckdb" / "shop.duckdb"
    base = bench.build_arm("contract", task, db)
    extra = bench.build_arm(arm, task, db)
    try:
        own = load_contract_at(roots[arm] / "shop" / "contract.yml")
        assert own.name == f"shop-{arm}"
        assert extra.system_prompt == base.system_prompt.replace(
            "livesqlbench-shop", f"shop-{arm}"
        )
        assert [t.name for t in extra.tools] == [t.name for t in base.tools]
    finally:
        base.close()
        extra.close()


def test_the_digests_differ_by_contract(lsb_data, tmp_path):
    bench, _ = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_1")
    digests = {a: bench.arm_digest(a, task) for a in ("contract", *EXTRA)}
    assert len(set(digests.values())) == 3


def test_check_arms_needs_each_extra_contract(lsb_data, tmp_path):
    bench, roots = _compiled(lsb_data, tmp_path)
    tasks = bench.tasks()
    bench.check_arms(("contract", *EXTRA), tasks)
    shutil.rmtree(roots["contract_authored"] / "shop")
    bench.check_arms(("contract", "contract_opus"), tasks)
    with pytest.raises(SystemExit, match="contract_authored"):
        bench.check_arms(("contract_authored",), tasks)


def test_lsb_dbs_restricts_the_tasks(lsb_data):
    every = LiveSQLBench.from_data(lsb_data).tasks()
    assert {t.meta["db"] for t in every} == {"shop"}
    assert LiveSQLBench.from_data(lsb_data, dbs=("shop",)).tasks() == every
    with pytest.raises(SystemExit, match="nope"):
        LiveSQLBench.from_data(lsb_data, dbs=("nope",))


def test_the_cli_passes_lsb_dbs(lsb_data):
    parser = argparse.ArgumentParser()
    LiveSQLBench.add_arguments(parser)
    args = parser.parse_args(["--lsb-data", str(lsb_data), "--lsb-dbs", "shop"])
    assert args.lsb_dbs == ["shop"]
    assert LiveSQLBench.from_args(args).tasks()
