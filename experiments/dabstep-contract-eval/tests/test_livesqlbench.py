import json
import shutil
from pathlib import Path

import dce.benchmarks.livesqlbench as livesqlbench_module
import duckdb
import pytest
from dce.benchmark import Benchmark, Grade, benchmark_class
from dce.benchmarks.livesqlbench import (
    ANSWER_INSTRUCTION,
    BASE_PROMPT,
    SCORER,
    LiveSQLBench,
    _sha256,
    render_column_meanings,
    render_manual,
)
from dce.benchmarks.lsb_grade import gold_digest

FIXTURE = Path(__file__).parent / "fixtures" / "lsb"

#: Shipped revenue per customer, descending: shop_1's gold, shop_2's reversed.
REVENUE_DESC = (
    "```sql\nSELECT c.name, SUM(o.amount) FROM customers c "
    "JOIN orders o ON o.custid = c.custid WHERE o.status = 'shipped' "
    "GROUP BY c.name ORDER BY 2 DESC\n```"
)


@pytest.fixture
def data(tmp_path: Path) -> Path:
    """The fixture laid out as `LSB_DATA`, with its DuckDB file built."""
    root = tmp_path / "lsb"
    shutil.copytree(FIXTURE, root)
    (root / "duckdb").mkdir()
    con = duckdb.connect(str(root / "duckdb" / "shop.duckdb"))
    con.execute((FIXTURE / "shop.sql").read_text(encoding="utf-8"))
    con.close()
    return root


@pytest.fixture
def bench(data: Path) -> LiveSQLBench:
    """The fixture with the shop contract compiled (by a scripted model), so
    every arm, the compiled ones included, can be built."""
    from dce.compile.run import compile_database

    from tests.test_compile_run import shop_driver

    contracts = data.parent / "contracts"
    inputs = livesqlbench_module.compile_inputs(data, "shop")
    compile_database(inputs, contracts / "shop", shop_driver([]), spend_cap=10.0)
    return LiveSQLBench.from_data(data, contracts=contracts)


def _task(bench, task_id):
    return next(t for t in bench.tasks() if t.task_id == task_id)


def test_registered_and_satisfies_the_protocol(bench):
    assert benchmark_class("livesqlbench") is LiveSQLBench
    assert isinstance(bench, Benchmark)


def test_class_attributes(bench):
    assert LiveSQLBench.arms == (
        "schema_only",
        "manual_prompt",
        "manual_compiled",
        "contract",
    )
    assert LiveSQLBench.extra_arms == (
        "contract_opus",
        "contract_authored",
        "contract_gap",
        "manual_corrected",
        "contract_corrected",
        "manual_styled",
        "manual_gap",
        "manual_full",
        "manual_noted",
        "contract_text",
        *livesqlbench_module.SCALE_ARMS,
    )
    assert LiveSQLBench.governed_arms == frozenset(
        {
            "contract",
            "contract_opus",
            "contract_authored",
            "contract_gap",
            "contract_corrected",
            *livesqlbench_module.SCALE_GOVERNED,
        }
    )
    assert LiveSQLBench.roles == {
        "baseline": "schema_only",
        "manual": "manual_prompt",
        "manual_plus": "manual_compiled",
        "contract": "contract",
    }
    assert LiveSQLBench.primary is None
    assert LiveSQLBench.group_label == "db"
    assert LiveSQLBench.subsets == ("primary", "order_only")
    assert LiveSQLBench.excluded_tasks == frozenset()


def test_loads_the_frozen_query_tasks_only(bench):
    tasks = bench.tasks()
    assert [t.task_id for t in tasks] == [
        "shop_1",
        "shop_2",
        "shop_3",
        "shop_4",
        "shop_5",
    ]
    assert [t.subset for t in tasks] == [
        "primary",
        "order_only",
        "primary",
        "primary",
        "primary",
    ]
    assert {t.group for t in tasks} == {"shop"}


def test_the_prompt_is_the_query_and_the_fixed_instruction(bench):
    task = _task(bench, "shop_3")
    assert task.prompt == "How many gold customers are there?\n\n" + ANSWER_INSTRUCTION
    assert "```sql" in ANSWER_INSTRUCTION


def test_row_fields(bench):
    assert bench.row_fields(_task(bench, "shop_3")) == {
        "db": "shop",
        "high_level": True,
        "order": False,
    }


def test_pristine_db_is_the_tasks_database(bench, data):
    assert bench.pristine_db(_task(bench, "shop_1")) == data / "duckdb" / "shop.duckdb"


def test_both_arms_carry_schema_and_column_meanings_and_only_the_manual_carries_the_kb(
    bench, data
):
    task = _task(bench, "shop_1")
    db = data / "duckdb" / "shop.duckdb"
    schema_only = bench.build_arm("schema_only", task, db)
    manual = bench.build_arm("manual_prompt", task, db)
    schema = (data / "hf-full-v1" / "shop" / "shop_schema.txt").read_text(
        encoding="utf-8"
    )
    for setup in (schema_only, manual):
        assert setup.system_prompt.startswith(BASE_PROMPT)
        assert schema.strip() in setup.system_prompt
        assert (
            "- orders.amount: Order value; null when not yet priced."
            in setup.system_prompt
        )
        assert [t.name for t in setup.tools] == [
            "list_tables",
            "describe_table",
            "execute_sql",
        ]
    assert "Shipped Revenue" not in schema_only.system_prompt
    assert (
        manual.system_prompt
        == schema_only.system_prompt
        + "\n\n## Knowledge base\n\n"
        + render_manual(
            [
                json.loads(line)
                for line in (data / "hf-full-v1" / "shop" / "shop_kb.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
        )
    )


def test_the_arms_tools_run_with_postgres_semantics(bench, data):
    task = _task(bench, "shop_4")
    setup = bench.build_arm("schema_only", task, data / "duckdb" / "shop.duckdb")
    execute = next(t for t in setup.tools if t.name == "execute_sql")
    assert execute.function("SELECT 7 / 2 AS x").splitlines() == ["x", "3"]


def test_an_unknown_arm_raises(bench, data):
    with pytest.raises(ValueError, match="unknown arm"):
        bench.build_arm(
            "contract_hollow", _task(bench, "shop_1"), data / "duckdb" / "shop.duckdb"
        )


def test_rendering_of_meanings_and_manual():
    text = render_column_meanings(
        {
            "d|t|a": "Plain.",
            "d|t|j": {"column_meaning": "JSON col.", "fields_meaning": {"f": "F."}},
        }
    )
    assert text == '- t.a: Plain.\n- t.j: JSON col. Fields: {"f": "F."}'
    manual = render_manual(
        [
            {"id": 2, "knowledge": "B", "description": "d2", "definition": "f2"},
            {"id": 1, "knowledge": "A", "description": "d1", "definition": "f1"},
        ]
    )
    assert manual == "### A\n\nd1\n\nf1\n\n### B\n\nd2\n\nf2"


def test_every_fixture_tasks_own_gold_sql_grades_correct(bench):
    gold_sql = json.loads((FIXTURE / "gold_sql.json").read_text(encoding="utf-8"))
    for task in bench.tasks():
        answer = f"```sql\n{gold_sql[task.task_id]}\n```"
        assert bench.grade(task, answer) == Grade("correct"), task.task_id


def test_an_order_only_task_is_graded_without_order(bench):
    # shop_2's gold is ascending; a descending answer is still right.
    answer = REVENUE_DESC
    assert bench.grade(_task(bench, "shop_2"), answer) == Grade("correct")
    # The same answer to shop_1 (primary, ordered, gold descending) is right ...
    assert bench.grade(_task(bench, "shop_1"), answer) == Grade("correct")
    # ... and an ascending one is not.
    asc = answer.replace("DESC", "ASC")
    assert bench.grade(_task(bench, "shop_1"), asc) == Grade("incorrect")


def test_gold_ref_golds_hash_normalize_and_scorer(bench, data):
    gold = json.loads((data / "gold_results_full_v1.json").read_text(encoding="utf-8"))
    task = _task(bench, "shop_3")
    assert bench.gold_ref(task) == gold_digest(gold["shop_3"]["rows"])
    assert len(bench.golds_hash()) == 64
    assert bench.normalize("text\n```sql\nSELECT 1\n```") == "SELECT 1"
    assert bench.normalize("no sql") == ""
    assert LiveSQLBench.scorer() == SCORER
    assert LiveSQLBench.rescore({"answer": "x", "gold": "y"}) is None
    assert LiveSQLBench.e2e_correct({"answer": "x"}) is False


def test_arm_digest_covers_every_file_the_arms_prompt_is_built_from(bench, data):
    folder = data / "hf-full-v1" / "shop"
    task = _task(bench, "shop_1")
    before = {arm: bench.arm_digest(arm, task) for arm in bench.arms}
    assert before["schema_only"] != before["manual_prompt"]

    kb = folder / "shop_kb.jsonl"
    kb.write_text(kb.read_text() + "\n")
    _sha256.cache_clear()
    assert bench.arm_digest("schema_only", task) == before["schema_only"]
    assert bench.arm_digest("manual_prompt", task) != before["manual_prompt"]

    for name in ("shop_schema.txt", "shop_column_meaning_base.json"):
        changed = {arm: bench.arm_digest(arm, task) for arm in bench.arms}
        path = folder / name
        path.write_text(path.read_text() + "\n")
        _sha256.cache_clear()
        for arm in bench.arms:
            assert bench.arm_digest(arm, task) != changed[arm], (name, arm)


def test_a_missing_database_file_stops_loading_by_name(data):
    (data / "duckdb" / "shop.duckdb").unlink()
    with pytest.raises(SystemExit, match="shop"):
        LiveSQLBench.from_data(data)


def test_a_task_without_a_gold_stops_loading(data):
    gold_path = data / "gold_results_full_v1.json"
    gold = json.loads(gold_path.read_text(encoding="utf-8"))
    del gold["shop_3"]
    gold_path.write_text(json.dumps(gold), encoding="utf-8")
    with pytest.raises(SystemExit, match="shop_3"):
        LiveSQLBench.from_data(data)


def test_builds_from_its_command_line_flag(data):
    import argparse

    parser = argparse.ArgumentParser()
    LiveSQLBench.add_arguments(parser)
    args = parser.parse_args(["--lsb-data", str(data)])
    assert [t.task_id for t in LiveSQLBench.from_args(args).tasks()][0] == "shop_1"


def test_the_contracts_root_has_a_flag(data, tmp_path):
    """Experiment 4 runs the same arms on its own data and contracts."""
    import argparse

    from dce.benchmarks.livesqlbench import DEFAULT_CONTRACTS

    parser = argparse.ArgumentParser()
    LiveSQLBench.add_arguments(parser)
    args = parser.parse_args(["--lsb-data", str(data)])
    assert LiveSQLBench.from_args(args)._contracts == DEFAULT_CONTRACTS
    args = parser.parse_args(
        ["--lsb-data", str(data), "--lsb-contracts", str(tmp_path / "c")]
    )
    bench = LiveSQLBench.from_args(args)
    assert bench._contract_dir("shop") == tmp_path / "c" / "shop"


def test_no_gold_reaches_a_row(bench, data, tmp_path):
    """A full run_task row, through a fake model, carries the gold digest and
    never a gold value."""
    from dce.agent import run_task

    class Fake:
        def run_sync(self, *a, usage=None, **k):
            class R:
                output = REVENUE_DESC

            return R()

    working = tmp_path / "w.duckdb"
    shutil.copyfile(data / "duckdb" / "shop.duckdb", working)
    row = run_task(
        _task(bench, "shop_1"),
        "schema_only",
        "z-ai/glm-5.3-flash",
        bench,
        working,
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["benchmark"] == "livesqlbench" and row["subset"] == "primary"
    assert len(row["gold"]) == 64
    assert "Bob" not in json.dumps({k: v for k, v in row.items() if k != "answer"})


def test_an_agent_cannot_read_files_beside_the_database(bench, data):
    """The gold results sit one directory above the database, and DuckDB's
    file functions would otherwise read them from inside an agent's query."""
    task = _task(bench, "shop_1")
    gold = data / "gold_results_full_v1.json"
    for arm in LiveSQLBench.arms:
        setup = bench.build_arm(arm, task, data / "duckdb" / "shop.duckdb")
        try:
            if setup.adapter is not None:
                # A governed arm's tools need a run context; they all query
                # through this adapter's connection, so it is what is checked.
                def execute(sql, adapter=setup.adapter):
                    try:
                        return str(adapter.execute_limited(sql, 50).rows)
                    except Exception as exc:
                        return f"ERROR: {exc}"

                count = ["[(5,)]"]
            else:
                tool = next(t for t in setup.tools if t.name == "execute_sql")
                execute = tool.function
                count = ["n", "5"]
            # An error message echoes the query, so check for what the file
            # holds (a task id, a gold value) or, for glob, a file name.
            for sql, leak in (
                (f"SELECT content FROM read_text('{gold}')", "Bob"),
                (f"SELECT * FROM read_json_auto('{gold}')", "shop_1"),
                (f"SELECT * FROM glob('{data}/*')", "gold_results"),
            ):
                out = execute(sql)
                assert leak not in out, (arm, sql, out)
            # ... while the database itself still answers.
            assert execute("SELECT count(*) AS n FROM orders").splitlines() == count
        finally:
            setup.close()


def _compiled(lsb_data, tmp_path):
    """The shop fixture compiled with a scripted model, as Task 8's test does."""
    from dce.compile.run import compile_database

    from tests.test_compile_run import shop_driver

    contracts = tmp_path / "contracts"
    inputs = livesqlbench_module.compile_inputs(lsb_data, "shop")
    compile_database(inputs, contracts / "shop", shop_driver([]), spend_cap=10.0)
    return LiveSQLBench.from_data(lsb_data, contracts=contracts), contracts


def test_manual_compiled_is_the_manual_then_the_compiled_definitions(
    lsb_data, tmp_path
):
    from dce.compile.emit import render_compiled

    bench, contracts = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_1")
    db = lsb_data / "duckdb" / "shop.duckdb"
    manual = bench.build_arm("manual_prompt", task, db)
    compiled = bench.build_arm("manual_compiled", task, db)
    assert compiled.system_prompt == (
        manual.system_prompt
        + "\n\n## Compiled definitions\n\n"
        + render_compiled(contracts / "shop")
    )
    assert "SUM(CASE WHEN status = 'shipped' THEN amount END)" in compiled.system_prompt
    assert [t.name for t in compiled.tools] == [t.name for t in manual.tools]


def test_the_contract_arm_is_context_procedure_contract_over_governed_tools(
    lsb_data, tmp_path
):
    from dce.frozen import load_contract_at
    from dce.tools import CONTRACT_PROCEDURE

    bench, contracts = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_4")
    db = lsb_data / "duckdb" / "shop.duckdb"
    schema_only = bench.build_arm("schema_only", task, db)
    setup = bench.build_arm("contract", task, db)
    try:
        contract = load_contract_at(contracts / "shop" / "contract.yml")
        assert setup.system_prompt == (
            f"{schema_only.system_prompt}\n\n{CONTRACT_PROCEDURE}\n\n"
            f"{contract.to_system_prompt()}"
        )
        assert "## Knowledge base" not in setup.system_prompt
        names = [t.name for t in setup.tools]
        assert "lookup_metric" in names and "run_query" in names
        # PG_COMPAT reaches the governed connection too.
        result = setup.adapter.execute_limited("SELECT 7 / 2 AS x", 5)
        assert [tuple(r) for r in result.rows] == [(3,)]
    finally:
        setup.close()


def test_compiled_arms_digest_the_contract(lsb_data, tmp_path):
    bench, _ = _compiled(lsb_data, tmp_path)
    task = _task(bench, "shop_1")
    digests = {arm: bench.arm_digest(arm, task) for arm in LiveSQLBench.arms}
    assert len(set(digests.values())) == 4


def test_check_arms_names_databases_without_a_compiled_contract(lsb_data, tmp_path):
    bench = LiveSQLBench.from_data(lsb_data, contracts=tmp_path / "none")
    tasks = bench.tasks()
    bench.check_arms(("schema_only", "manual_prompt"), tasks)  # nothing compiled needed
    with pytest.raises(SystemExit, match=r"\['shop'\]"):
        bench.check_arms(("contract",), tasks)
    broken = tmp_path / "broken" / "shop"
    broken.mkdir(parents=True)
    (broken / "contract.yml").write_text("name: [", encoding="utf-8")
    bench = LiveSQLBench.from_data(lsb_data, contracts=tmp_path / "broken")
    with pytest.raises(SystemExit, match="does not load"):
        bench.check_arms(("manual_compiled",), tasks)


def test_check_arms_loads_the_semantic_source_too(lsb_data, tmp_path):
    bench, contracts = _compiled(lsb_data, tmp_path)
    tasks = bench.tasks()
    (contracts / "shop" / "semantic.yml").unlink()
    with pytest.raises(SystemExit, match="shop"):
        bench.check_arms(("contract",), tasks)
    with pytest.raises(SystemExit, match="shop"):
        bench.check_arms(("manual_compiled",), tasks)
