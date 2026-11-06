"""Experiment 7's author: `dce.author` on a DataAgentBench dataset.

The prompt is 4b's with mechanical changes only -- the description as free
text in place of numbered KB entries, no coverage map, several schemas -- and
its content guidance byte for byte. Three variants: draw A (no hints), draw B
(the hints appended as the adapter's `dab_hints` arm appends them) and the
no-data control (no run_sql, no sample rows). A schema of many same-shape
tables is allowed with `tables: ["*"]`, listed programmatically when the
contract is written.

Every hints file here is synthetic.
"""

import gzip
import hashlib
import json
from pathlib import Path

import pytest
import yaml
from dce import author
from dce.benchmark import Task
from dce.benchmarks import dab_author, dab_data, dab_schema, dataagentbench
from dce.compile.session import DriverResult

from tests.test_dab_data import make_dataset

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.01}
HINTS = "\n  Synthetic note: nothing here comes from DataAgentBench.\n\n"

CONTRACT = """
version: "1.0"
name: toy-authored
semantic:
  source: {type: yaml, path: ./semantic.yml}
  allowed_tables:
    - schema: books_database
      tables: [books_info]
    - schema: review_database
      tables: [review, Order]
    - schema: trade_database
      tables: ["*"]
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT, CREATE, ALTER,
    MERGE, GRANT, REVOKE, COPY]
  domains:
    - name: reviews
      summary: Ratings
      description: Ratings run from 1 to 5; some are missing.
  rules: []
"""

SEMANTIC = """
metrics:
  - name: rated_reviews
    description: Reviews with a rating.
    sql_expression: "COUNT(rating)"
    source_model: review_database.review
  - name: priced_days
    description: Trading days with an adjusted close, for AAPL.
    sql_expression: 'COUNT("Adj Close")'
    source_model: trade_database.AAPL
  - name: reviews_per_book
    description: Reviews over books, across two databases.
    sql_expression: "COUNT(*)"
    source_model: books_database.books_info
    from_sql: "books_database.books_info b CROSS JOIN review_database.review r"
    grain: one row per book and review
"""

#: The rendered prompt templates of experiment 4b (LiveSQLBench), pinned:
#: the DataAgentBench mode must leave them as they were.
PINNED = {
    "SYSTEM_PROMPT": "2685ed8299abd131d1cb5247716d41b0127ba4f4356690f7d077ac227fb609af",
    "USER_PROMPT": "da3896e712f5854a44bd8dbc1c5bb65625e1c93baa112f6d382c5e8edd5fe240",
    "DATA_BULLET": "14478bf6ae136bd509d4140ef7a06aa9336127f49c17ffdd67da0c55518de770",
}


@pytest.fixture
def data(tmp_path: Path) -> Path:
    root = tmp_path / "dab"
    folder = make_dataset(root)
    (folder / "db_description_withhint.txt").write_text(HINTS, encoding="utf-8")
    dab_data.build_dataset(root / "src", "toy", root / "duckdb" / "toy.duckdb")
    return root


def _folder(data: Path) -> Path:
    return data / "src" / "query_toy"


def scripted(*calls, tools=frozenset({"run_sql", "submit_contract"})):
    replies = []

    def driver(*, system_prompt, user_prompt, tools: list, **kwargs):
        by_name = {t.__name__: t for t in tools}
        assert set(by_name) == expected
        driver.system_prompt, driver.user_prompt = system_prompt, user_prompt
        driver.kwargs = kwargs
        for name, call in calls:
            replies.append(by_name[name](**call))
        return DriverResult(messages=[{"scripted": True}], usage=dict(USAGE))

    expected = set(tools)
    driver.replies = replies
    return driver


def _submit(**overrides):
    kwargs = {"contract_yml": CONTRACT, "semantic_yml": SEMANTIC}
    kwargs.update(overrides)
    return ("submit_contract", kwargs)


# -- the prompt ---------------------------------------------------------------


def test_the_livesqlbench_prompts_are_unchanged():
    for name, digest in PINNED.items():
        text = getattr(author, name)
        assert hashlib.sha256(text.encode("utf-8")).hexdigest() == digest, name


def test_the_dab_system_prompt_is_4bs_with_the_mechanical_changes_only(data):
    inputs = dab_author.author_inputs(data, "toy")
    dab = author.system_prompt(inputs)
    expected = author.SYSTEM_PROMPT
    for old, new in author.DAB_PROMPT_CHANGES:
        assert author.SYSTEM_PROMPT.count(old) == 1, old
        expected = expected.replace(old, new)
    assert author.DAB_SYSTEM_PROMPT == expected
    assert "kb_coverage" not in dab and "column meanings" not in dab
    assert "main.<table>" not in dab and "schema main" not in dab
    assert "<schema>.<table>" in dab and 'tables: ["*"]' in dab
    assert "DuckDB's own semantics apply." in dab


def test_the_content_guidance_is_byte_identical(data, lsb_data):
    from dce.benchmarks import livesqlbench

    lsb = author.system_prompt(livesqlbench.compile_inputs(lsb_data, "shop"))
    dab = author.system_prompt(dab_author.author_inputs(data, "toy"))
    start = "Write the contract a careful engineer would:\n"
    guidance = lsb[lsb.index(start) : lsb.index("- Write DuckDB SQL.")]
    assert author.DATA_BULLET in guidance
    assert guidance in dab
    # and the example of the form, from its heading to the end
    form = lsb[lsb.index("## The form, by example") :]
    assert dab.endswith(form)


def test_the_no_data_prompt_drops_the_data_bullet_only(data):
    inputs = dab_author.author_inputs(data, "toy", no_data=True)
    with_data = author.system_prompt(dab_author.author_inputs(data, "toy"))
    assert author.system_prompt(inputs, no_data=True) == with_data.replace(
        author.DATA_BULLET, ""
    )


def test_the_user_prompt_is_the_description_and_the_schema(data):
    inputs = dab_author.author_inputs(data, "toy")
    text = author.user_prompt(inputs)
    described = (_folder(data) / "db_description.txt").read_text(encoding="utf-8")
    assert described.strip() + "\n\n" + dataagentbench.SESSION_NOTE in text
    assert dab_schema.schema_text(data / "duckdb" / "toy.duckdb").strip() in text
    assert "-- first 3 rows:" in text
    assert "Column meanings" not in text and "entries)" not in text
    assert "Synthetic note" not in text  # draw A has no hints


def test_draw_b_appends_the_hints_as_the_adapters_hint_arm_does(data):
    inputs = dab_author.author_inputs(data, "toy", hints=True)
    folder = _folder(data)
    plain = dataagentbench.dataset_description(folder)
    assert inputs.description == plain + "\n\n" + HINTS.strip()
    assert inputs.description == dataagentbench.with_hints(plain, folder)
    assert inputs.description in author.user_prompt(inputs)
    names = {p.name for p in inputs.sources}
    assert "db_description_withhint.txt" in names


def test_the_no_data_user_prompt_has_the_schema_without_sample_rows(data):
    inputs = dab_author.author_inputs(data, "toy", no_data=True)
    text = author.user_prompt(inputs, no_data=True)
    assert "CREATE TABLE books_database.books_info (" in text
    assert "rows:" not in text
    assert inputs.description is not None and inputs.description in text


def test_the_inputs_name_every_schema_and_no_question(data):
    inputs = dab_author.author_inputs(data, "toy")
    assert inputs.schemas == ("books_database", "review_database", "trade_database")
    assert inputs.entries == ()
    assert {p.name for p in inputs.sources} == {"db_description.txt", "toy.duckdb"}


# -- the session ----------------------------------------------------------------


def test_a_contract_over_several_schemas_is_accepted_and_written(data, tmp_path):
    inputs = dab_author.author_inputs(data, "toy")
    driver = scripted(
        ("run_sql", {"sql": 'SELECT COUNT(*) FROM trade_database."BRK-B"'}),
        _submit(),
    )
    out = tmp_path / "toy"
    status, spent = author.author_database(inputs, out, driver, spend_cap=1.0)
    assert (status, spent) == ("done", 0.01)
    assert driver.replies[0].startswith("count_star()") and driver.replies[1] == (
        "Accepted."
    )
    text = (out / "contract.yml").read_text(encoding="utf-8")
    assert text.startswith(
        "# Authored by dce.author -- do not edit; re-author instead.\n"
        "# benchmark: dataagentbench, database: toy, author: claudeopus5.5\n"
    )
    lines = [
        json.loads(x)
        for x in gzip.decompress((out / author.LOG).read_bytes()).splitlines()
    ]
    assert lines[0]["benchmark"] == "dataagentbench" and lines[0]["no_data"] is False
    assert lines[1]["accepted"]


def test_a_wildcard_is_listed_table_by_table_when_written(data, tmp_path):
    from agentic_data_contracts import DataContract

    out = tmp_path / "toy"
    inputs = dab_author.author_inputs(data, "toy")
    author.author_database(inputs, out, scripted(_submit()), spend_cap=1.0)
    contract = yaml.safe_load(
        (out / "contract.yml").read_text(encoding="utf-8").split("\n", 2)[2]
    )
    blocks = {b["schema"]: b["tables"] for b in contract["semantic"]["allowed_tables"]}
    assert blocks["trade_database"] == ["AAPL", "BRK-B"]
    loaded = DataContract.from_yaml(out / "contract.yml")
    assert not loaded.has_wildcard_tables()
    assert "trade_database.BRK-B" in loaded.allowed_table_names()


def test_the_submission_needs_no_coverage_map(data, tmp_path):
    seen = {}

    def driver(*, tools, **_):
        submit = {t.__name__: t for t in tools}["submit_contract"]
        seen["params"] = list(submit.__annotations__)
        seen["reply"] = submit(contract_yml=CONTRACT, semantic_yml=SEMANTIC)
        return DriverResult(messages=[], usage=dict(USAGE))

    inputs = dab_author.author_inputs(data, "toy")
    author.author_database(inputs, tmp_path / "toy", driver, spend_cap=1.0)
    assert "kb_coverage" not in seen["params"]
    assert seen["reply"] == "Accepted."


@pytest.mark.parametrize(
    ("override", "expect"),
    [
        (
            {"contract_yml": CONTRACT.replace("[review, Order]", "[review]")},
            "review_database.Order",
        ),
        (
            {"contract_yml": CONTRACT.replace('tables: ["*"]', "tables: [AAPL]")},
            "trade_database.BRK-B",
        ),
        (
            {"contract_yml": CONTRACT.replace("trade_database", "nowhere")},
            "nowhere",
        ),
        (
            {"semantic_yml": SEMANTIC.replace("COUNT(rating)", "COUNT(ratingx)")},
            "rated_reviews",
        ),
        (
            {
                "semantic_yml": SEMANTIC.replace(
                    "books_database.books_info b", "main.books_info b"
                )
            },
            "reviews_per_book",
        ),
        ({"contract_yml": CONTRACT.replace("DELETE, ", "")}, "DELETE"),
    ],
)
def test_a_bad_submission_is_refused_with_the_reason(data, tmp_path, override, expect):
    inputs = dab_author.author_inputs(data, "toy")
    driver = scripted(_submit(**override), _submit())
    status, _ = author.author_database(inputs, tmp_path / "toy", driver, spend_cap=1.0)
    assert driver.replies[0].startswith("ERROR:") and expect in driver.replies[0]
    assert driver.replies[1] == "Accepted." and status == "done"


def test_many_missing_tables_are_summarised(tmp_path):
    import duckdb
    from dce.compile.db import CompileDB

    path = tmp_path / "many.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA trade")
    for i in range(30):
        con.execute(f"CREATE TABLE trade.t{i:02d} (x BIGINT)")
    con.close()
    db = CompileDB(path)
    try:
        problems = author.check_submission(
            CONTRACT.replace("trade_database", "trade").replace(
                'tables: ["*"]', "tables: [t00]"
            ),
            "metrics: []\n",
            None,
            inputs=_bare_inputs(path),
            db=db,
            schemas={"trade": [f"t{i:02d}" for i in range(30)]},
        )
    finally:
        db.close()
    missing = [p for p in problems if "allowed_tables must include" in p]
    assert len(missing) == 11  # ten by name, then the count of the rest
    assert "19 more tables of schema trade" in missing[-1]


def _bare_inputs(path: Path):
    from dce.compile.inputs import CompileInputs

    return CompileInputs(
        benchmark="dataagentbench",
        db_name="many",
        entries=(),
        ddl="",
        column_meanings="",
        dialect_note="",
        db_path=path,
        init_sql=(),
        sources=(),
        out_subdir="many",
        description="",
        schemas=("trade",),
    )


def test_run_sql_reads_every_schema_and_writes_nothing(data, tmp_path):
    inputs = dab_author.author_inputs(data, "toy")
    driver = scripted(
        ("run_sql", {"sql": "SELECT COUNT(*) FROM books_database.books_info"}),
        ("run_sql", {"sql": "CREATE TABLE books_database.x (y INT)"}),
        ("run_sql", {"sql": "SELECT * FROM read_text('/etc/hostname')"}),
    )
    author.author_database(inputs, tmp_path / "toy", driver, spend_cap=1.0)
    assert not driver.replies[0].startswith("ERROR:")
    assert driver.replies[1].startswith("ERROR:")
    assert driver.replies[2].startswith("ERROR:")


def test_the_no_data_control_has_no_query_tool(data, tmp_path):
    inputs = dab_author.author_inputs(data, "toy", no_data=True)
    driver = scripted(_submit(), tools={"submit_contract"})
    out = tmp_path / "toy"
    status, _ = author.author_database(inputs, out, driver, spend_cap=1.0, no_data=True)
    assert status == "done"
    assert "run_sql" not in driver.system_prompt
    assert "rows:" not in driver.user_prompt
    header = json.loads(
        gzip.decompress((out / author.LOG).read_bytes()).splitlines()[0]
    )
    assert header["no_data"] is True


def test_no_data_inputs_with_sample_rows_are_refused(data, tmp_path):
    inputs = dab_author.author_inputs(data, "toy")  # built with sample rows
    with pytest.raises(AssertionError, match="sample rows"):
        author.user_prompt(inputs, no_data=True)


# -- the CLI ----------------------------------------------------------------------


def test_the_variants_cover_the_adapters_draws():
    assert list(dab_author.VARIANTS) == [
        "draw-a1",
        "draw-a2",
        "draw-a3",
        "draw-b",
        "no-data",
    ]
    assert set(dataagentbench.CONTRACT_DRAWS.values()) <= set(dab_author.VARIANTS)
    assert dab_author.VARIANTS["draw-b"] == {"hints": True, "no_data": False}
    assert dab_author.VARIANTS["no-data"] == {"hints": False, "no_data": True}
    assert dab_author.VARIANTS["draw-a1"] == {"hints": False, "no_data": False}


def _fake_claude(calls: list):
    """Stands in for `claude_driver`: submits the toy contract at once."""

    def fake(*, system_prompt, user_prompt, tools, **kwargs):
        calls.append({"user": user_prompt, "tools": {t.__name__ for t in tools}})
        calls[-1].update(kwargs)
        submit = {t.__name__: t for t in tools}["submit_contract"]
        assert submit(contract_yml=CONTRACT, semantic_yml=SEMANTIC) == "Accepted."
        return DriverResult(messages=[{"fake": True}], usage=dict(USAGE))

    return fake


def _run(data: Path, out: Path, *extra: str) -> None:
    dab_author.main(["--dab-data", str(data), "--out", str(out), *extra])


def test_the_cli_runs_every_variant_end_to_end(data, tmp_path, monkeypatch, capsys):
    from dce.compile import run as run_module
    from dce.frozen import load_contract_at
    from dce.tools import governed_arm

    calls: list = []
    monkeypatch.setattr(run_module, "claude_driver", _fake_claude(calls))
    monkeypatch.setattr(dab_author.dab_fetch, "DATASETS", ("toy",))
    out = tmp_path / "contracts-dataagentbench"
    _run(data, out, "--max-spend", "1")
    assert len(calls) == 5
    assert all(c["timeout"] == author.AUTHOR_TIMEOUT_S for c in calls)
    assert all(c["request_limit"] == author.AUTHOR_REQUEST_LIMIT for c in calls)
    hinted = [c for c in calls if "Synthetic note" in c["user"]]
    assert len(hinted) == 1
    no_query = [c for c in calls if c["tools"] == {"submit_contract"}]
    assert len(no_query) == 1 and "rows:" not in no_query[0]["user"]
    for variant in dab_author.VARIANTS:
        folder = out / variant / "toy"
        assert (folder / "contract.yml").is_file(), variant
        assert (folder / "semantic.yml").is_file()
        assert (folder / author.LOG).is_file()
    assert "toy: done" in capsys.readouterr().out

    # The adapter's governed arms load what the CLI wrote.
    bench = dataagentbench.DataAgentBench(data=data, records=[], contracts=out)
    task = Task(task_id="toy/1", prompt="", group="toy", meta={"dataset": "toy"})
    bench.check_arms(("dab_contract", "dab_hints_contract"), [task])
    setup = governed_arm(
        dataagentbench.system_prompt(True),
        data / "duckdb" / "toy.duckdb",
        load_contract_at(out / "draw-a1" / "toy" / "contract.yml"),
    )
    try:
        assert "trade_database.BRK-B" in setup.system_prompt
    finally:
        setup.close()

    # A second run skips every finished session and spends nothing.
    _run(data, out, "--max-spend", "1")
    assert len(calls) == 5
    assert capsys.readouterr().out.count("skipped") == 5


def test_the_cli_selects_variants_and_datasets_and_respects_the_cap(
    data, tmp_path, monkeypatch, capsys
):
    from dce.compile import run as run_module

    calls: list = []
    monkeypatch.setattr(run_module, "claude_driver", _fake_claude(calls))
    out = tmp_path / "out"
    _run(data, out, "--variant", "draw-b", "--dataset", "toy", "--max-spend", "1")
    assert [p.parent.parent.name for p in out.glob("*/toy/contract.yml")] == ["draw-b"]
    # the spend already logged under --out counts toward the cap
    with pytest.raises(SystemExit) as stop:
        _run(
            data, out, "--variant", "draw-a1", "--dataset", "toy", "--max-spend", "0.01"
        )
    assert stop.value.code == 2
    assert "capped" in capsys.readouterr().out
    assert len(calls) == 1


def test_the_cli_needs_a_spend_cap(data, tmp_path):
    with pytest.raises(SystemExit):
        _run(data, tmp_path / "out")
