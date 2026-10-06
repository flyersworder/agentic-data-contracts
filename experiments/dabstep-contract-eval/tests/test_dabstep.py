import json
from pathlib import Path

import duckdb
import pytest
from dce.benchmark import Benchmark, Grade, Task, benchmark_class
from dce.benchmarks.dabstep import ARMS, EXTRA_ARMS, GOVERNED_ARMS, DABStep, arm_digest
from dce.data import DATASET_REVISION
from dce.frozen import digest, hollow_digest, uninterpreted_digest
from dce.golds import PLURALITY_THRESHOLD, VERIFIED_WRONG_GOLDS

DOCS = {"manual": "FEE RULE ALPHA: match on card_scheme.", "payments_readme": "cols"}
RECORD = {
    "task_id": "7",
    "question": "What is X?",
    "guidelines": "A number.",
    "level": "hard",
}


def _bench(golds=None, tmp: Path | None = None) -> DABStep:
    return DABStep(
        db=(tmp or Path(".")) / "d.duckdb",
        golds={"7": "0.12"} if golds is None else golds,
        golds_hash="h",
        docs=DOCS,
    )


def test_the_registry_returns_dabstep():
    assert benchmark_class("dabstep") is DABStep


def test_dabstep_satisfies_the_protocol():
    assert isinstance(_bench(), Benchmark)


def test_class_attributes_the_analysis_reads():
    assert DABStep.name == "dabstep"
    assert DABStep.arms == ARMS and DABStep.extra_arms == EXTRA_ARMS
    assert DABStep.governed_arms == GOVERNED_ARMS
    assert DABStep.roles == {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }
    assert DABStep.primary == (
        "deepseek/deepseek-v4-pro-0813",
        "manual_prompt",
        "contract",
    )
    assert DABStep.group_label == "level"
    assert DABStep.excluded_tasks == frozenset(VERIFIED_WRONG_GOLDS)


def test_task_prompt_is_the_question_and_guidelines_exactly():
    task = DABStep.task(RECORD)
    assert task == Task(
        "7", "What is X?\n\nAnswer guidelines: A number.", "hard", RECORD
    )
    no_guidelines = DABStep.task({"task_id": "8", "question": "Q"})
    assert no_guidelines.prompt == "Q\n\nAnswer guidelines: "
    assert no_guidelines.group == "unknown"


def test_grade_uses_the_dabstep_scorer_and_ungraded_without_gold():
    task = DABStep.task(RECORD)
    assert _bench().grade(task, "0.12") == Grade("correct")
    assert _bench().grade(task, "0.13") == Grade("incorrect")
    assert _bench(golds={}).grade(task, "0.12") == Grade("ungraded")


def test_row_provenance_matches_the_old_row_fields():
    bench, task = _bench(), DABStep.task(RECORD)
    assert bench.gold_ref(task) == "0.12"
    assert _bench(golds={}).gold_ref(task) is None
    assert bench.normalize(" [0.12] ") == "0.12"
    assert bench.row_fields(task) == {"level": "hard"}
    assert bench.golds_hash() == "h"


def test_each_governed_arm_is_stamped_with_the_contract_it_loads():
    task = DABStep.task(RECORD)
    bench = _bench()
    assert bench.arm_digest("contract", task) == arm_digest("contract") == digest()
    assert arm_digest("contract_hollow") == hollow_digest()
    assert arm_digest("contract_uninterpreted") == uninterpreted_digest()
    assert arm_digest("manual_resolved") == digest()
    assert arm_digest("schema_only") == digest()
    # Three artifacts, three stamps: if two collapsed, a row could not say
    # which contract its arm read.
    assert len({digest(), hollow_digest(), uninterpreted_digest()}) == 3


def test_build_arm_matches_the_module_function(tmp_path: Path):
    from dce.benchmarks.dabstep import build_arm

    db = tmp_path / "d.duckdb"
    duckdb.connect(str(db)).close()
    bench = _bench(tmp=tmp_path)
    via_benchmark = bench.build_arm("manual_prompt", DABStep.task(RECORD), db)
    direct = build_arm("manual_prompt", db, DOCS)
    assert via_benchmark.system_prompt == direct.system_prompt
    assert bench.pristine_db(DABStep.task(RECORD)) == db


def test_rescore_and_e2e_work_from_a_stored_row():
    row = {"answer": "The answer is:\n\n0.12", "gold": "0.12", "verdict": "incorrect"}
    assert DABStep.rescore({"answer": "0.12", "gold": "0.12"}) == "correct"
    assert DABStep.rescore({"answer": "0.13", "gold": "0.12"}) == "incorrect"
    assert DABStep.e2e_correct(row) is True
    assert DABStep.e2e_correct({**row, "answer": "0.13"}) is False


def test_from_files_selects_golded_tasks_and_reads_the_docs(tmp_path: Path):
    from dce.golds import golds_sha256

    golds = {"1": "a"}
    (tmp_path / "golds.json").write_text(
        json.dumps(
            {
                "revision": DATASET_REVISION,
                "threshold": PLURALITY_THRESHOLD,
                "golds": golds,
                "golds_sha256": golds_sha256(golds),
            }
        ),
        encoding="utf-8",
    )
    records = [
        {"task_id": "1", "question": "q1", "guidelines": "g", "level": "easy"},
        {"task_id": "2", "question": "q2", "guidelines": "g", "level": "hard"},
    ]
    (tmp_path / "tasks.json").write_text(json.dumps(records), encoding="utf-8")
    ctx = tmp_path / "ctx"
    ctx.mkdir()
    (ctx / "manual.md").write_text("M", encoding="utf-8")
    (ctx / "payments-readme.md").write_text("R", encoding="utf-8")

    bench = DABStep.from_files(
        db=tmp_path / "d.duckdb",
        golds_path=tmp_path / "golds.json",
        tasks_path=tmp_path / "tasks.json",
        context_dir=ctx,
    )
    assert [t.task_id for t in bench.tasks()] == ["1"]
    assert bench.golds_hash() == golds_sha256(golds)
    run_all = DABStep.from_files(
        db=tmp_path / "d.duckdb",
        golds_path=tmp_path / "golds.json",
        tasks_path=tmp_path / "tasks.json",
        context_dir=ctx,
        ungolded="run",
    )
    assert [t.task_id for t in run_all.tasks()] == ["1", "2"]


def test_unknown_arm_raises(tmp_path: Path):
    with pytest.raises(ValueError, match="unknown arm"):
        _bench().build_arm("nope", DABStep.task(RECORD), tmp_path / "d.duckdb")
