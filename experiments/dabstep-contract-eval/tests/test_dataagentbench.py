"""Experiment 7's adapter: DataAgentBench's phase-1 queries, five arms, DAB's
system prompt adapted to the harness's tools, and DAB's validators."""

import hashlib
import json
from pathlib import Path

import pytest
from dce.benchmark import BENCHMARK_NAMES, Benchmark, benchmark_class
from dce.benchmarks import dab_data, dataagentbench
from dce.benchmarks.dataagentbench import (
    GOVERNED_EXAMPLES,
    GOVERNED_TOOLS,
    PLAIN_EXAMPLES,
    PLAIN_TOOLS,
    SESSION_NOTE,
    DataAgentBench,
    system_prompt,
)
from dce.frozen import load_contract_at
from dce.tools import CONTRACT_PROCEDURE

from tests.test_dab_data import make_dataset

HINTS = "\n  Hint one: a synthetic hint, padded with whitespace.\n\n"

VALIDATE = """def validate(llm_output: str):
    if "1980s" in llm_output:
        return True, "Ground truth '1980s' found."
    return False, "Ground truth '1980s' not found."
"""

CONTRACT = """version: '1.0'
name: toy_{draw}
semantic:
  source:
    type: yaml
    path: ./semantic.yml
  allowed_tables:
  - schema: books_database
    tables:
    - books_info
  - schema: review_database
    tables:
    - review
  forbidden_operations:
  - DELETE
  - DROP
"""

SEMANTIC = """metrics:
- name: review_count_{draw}
  description: Number of reviews.
  sql_expression: COUNT(*)
  source_model: review_database.review
"""


@pytest.fixture
def data(tmp_path: Path) -> Path:
    root = tmp_path / "dab"
    folder = make_dataset(root)
    (folder / "db_description_withhint.txt").write_text(HINTS, encoding="utf-8")
    for n, query in ((1, "Which decade?"), (2, {"query": "How many reviews?"})):
        q = folder / f"query{n}"
        q.mkdir()
        (q / "query.json").write_text(json.dumps(query), encoding="utf-8")
        (q / "validate.py").write_text(VALIDATE, encoding="utf-8")
        (q / "ground_truth.csv").write_text("1980s\n", encoding="utf-8")
    dab_data.build_dataset(root / "src", "toy", root / "duckdb" / "toy.duckdb")
    return root


def write_contracts(root: Path) -> Path:
    for draw in ("draw-a1", "draw-b"):
        folder = root / draw / "toy"
        folder.mkdir(parents=True)
        tag = draw.replace("-", "_")
        (folder / "contract.yml").write_text(
            CONTRACT.format(draw=tag), encoding="utf-8"
        )
        (folder / "semantic.yml").write_text(
            SEMANTIC.format(draw=tag), encoding="utf-8"
        )
    return root


@pytest.fixture
def bench(data: Path, tmp_path: Path) -> DataAgentBench:
    contracts = write_contracts(tmp_path / "contracts")
    return DataAgentBench.from_data(data, contracts=contracts, datasets=("toy",))


def _task(bench, task_id):
    return next(t for t in bench.tasks() if t.task_id == task_id)


def _build(bench, arm, task_id="toy/1"):
    task = _task(bench, task_id)
    return bench.build_arm(arm, task, bench.pristine_db(task))


def test_registered_and_satisfies_the_protocol(bench):
    assert "dataagentbench" in BENCHMARK_NAMES
    assert benchmark_class("dataagentbench") is DataAgentBench
    assert isinstance(bench, Benchmark)


def test_class_attributes():
    assert DataAgentBench.arms == (
        "dab_desc",
        "dab_hints",
        "dab_contract",
        "dab_contract_text",
        "dab_hints_contract",
    )
    assert DataAgentBench.governed_arms == frozenset(
        {"dab_contract", "dab_hints_contract"}
    )
    assert DataAgentBench.roles == {
        "baseline": "dab_desc",
        "manual": "dab_hints",
        "contract": "dab_contract",
    }
    assert DataAgentBench.scorer() == "dab-validate-py/1"


def test_tasks_mirror_dabs_user_message(bench, data):
    description = (
        (data / "src" / "query_toy" / "db_description.txt")
        .read_text(encoding="utf-8")
        .strip()
    )
    tasks = bench.tasks()
    assert [t.task_id for t in tasks] == ["toy/1", "toy/2"]
    assert {t.group for t in tasks} == {"toy"}
    assert tasks[0].prompt == (
        f"QUERY:\nWhich decade?\n\nDATABASE DESCRIPTION:\n{description}"
        f"\n\n{SESSION_NOTE}"
    )
    assert tasks[1].prompt.startswith("QUERY:\nHow many reviews?\n\n")
    assert bench.row_fields(tasks[1]) == {"dataset": "toy", "query": 2}


def test_hints_are_appended_as_dabs_run_agent_does(bench):
    task = _task(bench, "toy/1")
    plain = _build(bench, "dab_desc")
    hinted = _build(bench, "dab_hints")
    try:
        assert plain.user_prompt is None  # the task's own message
        assert hinted.user_prompt == task.prompt + "\n\n" + HINTS.strip()
        assert hinted.system_prompt == plain.system_prompt == system_prompt(False)
        assert [t.name for t in hinted.tools] == [t.name for t in plain.tools]
    finally:
        plain.close()
        hinted.close()


def test_plain_arms_list_every_schema(bench):
    setup = _build(bench, "dab_desc")
    tools = {t.name: t for t in setup.tools}
    listed = tools["list_tables"].function().splitlines()
    assert listed == [
        "books_database.books_info",
        "review_database.Order",
        "review_database.review",
        "trade_database.AAPL",
        "trade_database.BRK-B",
    ]
    out = tools["execute_sql"].function(
        "SELECT count(*) AS n FROM review_database.review"
    )
    assert out.splitlines() == ["n", "3"]


def test_prompt_blocks_name_each_arms_actual_tools(bench):
    plain = _build(bench, "dab_desc")
    governed = _build(bench, "dab_contract")
    try:
        for block, setup in ((PLAIN_TOOLS, plain), (GOVERNED_TOOLS, governed)):
            named = [
                line[2:].split(":")[0]
                for line in block.splitlines()
                if line.startswith("- ")
            ]
            assert sorted(named) == sorted(t.name for t in setup.tools)
        for examples, setup in ((PLAIN_EXAMPLES, plain), (GOVERNED_EXAMPLES, governed)):
            tools = {t.name for t in setup.tools}
            for line in examples.splitlines():
                if line.startswith("{"):
                    assert json.loads(line)["tool"] in tools
    finally:
        plain.close()
        governed.close()


def test_system_prompt_drops_python_mongo_and_return_answer():
    for governed in (False, True):
        text = system_prompt(governed)
        for gone in ("execute_python", "Mongo", "return_answer", "query_db", "list_db"):
            assert gone not in text
        assert text.startswith("You are a data analysis agent.")


def test_contract_arms_are_governed_with_their_draws(bench, tmp_path):
    task = _task(bench, "toy/1")
    hinted = _build(bench, "dab_hints")
    a1 = load_contract_at(tmp_path / "contracts" / "draw-a1" / "toy" / "contract.yml")
    b = load_contract_at(tmp_path / "contracts" / "draw-b" / "toy" / "contract.yml")
    contract = _build(bench, "dab_contract")
    both = _build(bench, "dab_hints_contract")
    try:
        assert contract.session is not None and both.session is not None
        assert "run_query" in {t.name for t in contract.tools}
        assert contract.system_prompt == (
            f"{system_prompt(True)}\n\n{CONTRACT_PROCEDURE}\n\n{a1.to_system_prompt()}"
        )
        assert contract.user_prompt is None
        assert both.system_prompt == (
            f"{system_prompt(True)}\n\n{CONTRACT_PROCEDURE}\n\n{b.to_system_prompt()}"
        )
        assert both.user_prompt == hinted.user_prompt != task.prompt
    finally:
        for setup in (hinted, contract, both):
            setup.close()


def test_contract_text_pastes_draw_a1_over_plain_tools(bench, tmp_path):
    plain = _build(bench, "dab_desc")
    text = _build(bench, "dab_contract_text")
    folder = tmp_path / "contracts" / "draw-a1" / "toy"
    assert text.session is None
    assert [t.name for t in text.tools] == [t.name for t in plain.tools]
    assert text.system_prompt.startswith(
        system_prompt(False) + "\n\n## Data contract\n\n"
    )
    for name in ("contract.yml", "semantic.yml"):
        body = (folder / name).read_text(encoding="utf-8").strip()
        assert f"### {name}\n\n```yaml\n{body}\n```" in text.system_prompt
    assert text.user_prompt is None


def test_check_arms_stops_on_a_missing_contract(data, tmp_path):
    bench = DataAgentBench.from_data(
        data, contracts=tmp_path / "none", datasets=("toy",)
    )
    tasks = bench.tasks()
    bench.check_arms(("dab_desc", "dab_hints"), tasks)  # no contract needed
    for arm in ("dab_contract", "dab_contract_text", "dab_hints_contract"):
        with pytest.raises(SystemExit, match=f"{arm}.*toy"):
            bench.check_arms((arm,), tasks)


def test_check_arms_stops_on_a_contract_that_does_not_load(data, tmp_path):
    contracts = write_contracts(tmp_path / "contracts")
    (contracts / "draw-b" / "toy" / "semantic.yml").write_text(
        "metrics: [", encoding="utf-8"
    )
    bench = DataAgentBench.from_data(data, contracts=contracts, datasets=("toy",))
    bench.check_arms(("dab_contract",), bench.tasks())
    with pytest.raises(SystemExit, match="dab_hints_contract"):
        bench.check_arms(("dab_hints_contract",), bench.tasks())


def test_grade_runs_the_querys_validator(bench):
    task = _task(bench, "toy/1")
    assert bench.grade(task, "The 1980s.").verdict == "correct"
    assert bench.grade(task, "The 1990s.").verdict == "incorrect"
    long = bench.grade(task, "1980s" + "." * 300)
    assert (long.verdict, long.fields) == ("correct", {"overlong": True})
    assert bench.normalize(" x ") == " x "


def test_rows_never_carry_the_gold(bench):
    task = _task(bench, "toy/1")
    assert bench.gold_ref(task) == hashlib.sha256(b"1980s").hexdigest()
    assert len(bench.golds_hash()) == 64
    assert DataAgentBench.rescore({"answer": "1980s"}) is None


def test_arm_digests_differ(bench):
    task = _task(bench, "toy/1")
    digests = {arm: bench.arm_digest(arm, task) for arm in DataAgentBench.arms}
    assert len(set(digests.values())) == len(DataAgentBench.arms)


def test_from_data_refuses_a_validator_outside_the_allowed_imports(data):
    path = data / "src" / "query_toy" / "query2" / "validate.py"
    path.write_text("import os\n" + VALIDATE, encoding="utf-8")
    with pytest.raises(SystemExit, match="toy/2"):
        DataAgentBench.from_data(data, datasets=("toy",))


def test_from_data_refuses_a_missing_database(data):
    (data / "duckdb" / "toy.duckdb").unlink()
    with pytest.raises(SystemExit, match="toy"):
        DataAgentBench.from_data(data, datasets=("toy",))


def test_run_task_sends_the_arms_message_and_records_overlong(bench):
    from dce.agent import run_task

    task = _task(bench, "toy/1")
    sent = []

    class Fake:
        def run_sync(self, prompt, *a, usage=None, **k):
            sent.append(prompt)
            usage.input_tokens, usage.output_tokens = 100, 10

            class Result:
                output = "1980s" + "." * 300

            return Result()

    rows = [
        run_task(
            task,
            arm,
            "z-ai/glm-5.3-flash",
            bench,
            bench.pristine_db(task),
            agent_factory=lambda **_: Fake(),
        )
        for arm in ("dab_desc", "dab_hints")
    ]
    assert sent[0] == task.prompt
    assert sent[1] == task.prompt + "\n\n" + HINTS.strip()
    for row in rows:
        assert row["verdict"] == "correct"
        assert row["overlong"] is True
        assert (row["dataset"], row["query"], row["group"]) == ("toy", 1, "toy")
        assert row["gold"] == hashlib.sha256(b"1980s").hexdigest()
        assert row["scorer"] == "dab-validate-py/1"


def test_phase_one_counts():
    assert sum(dataagentbench.dab_fetch.QUERY_COUNTS.values()) == 40
    assert len(dataagentbench.dab_fetch.DATASETS) == 9
