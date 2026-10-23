"""The cross-family rating harness: what a valid rating is, how the rubric is
moved into the sandbox, and what the query server refuses.

See `prep/cross-family-ratings/PLAN.md`.
"""

import importlib.util
import sys
from pathlib import Path

import duckdb
import pytest

HERE = Path(__file__).resolve().parents[1] / "prep" / "cross-family-ratings"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, HERE / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look their module up here
    spec.loader.exec_module(module)
    return module


rate = _load("rate")
qserver = _load("qserver")

LOAD_ROW = {
    "id": "P001",
    "needed": ["fee formula"],
    "steps": 2,
    "rule_matching": True,
    "multi_grain": False,
    "branching": False,
    "direct": True,
    "load": 2,
    "notes": "one sentence",
}
DET_ROW = {
    "id": "C001",
    "kb_clarity": "ambiguous",
    "undetermined": ["join path"],
    "decisive": True,
    "question_states_it": False,
    "mapping_ok": True,
    "notes": "one sentence",
}


def test_a_complete_valid_chunk_has_no_errors():
    assert rate.validate("load", [LOAD_ROW], ["P001"]) == []
    assert rate.validate("determinacy", [DET_ROW], ["C001"]) == []


def test_missing_duplicate_and_unexpected_ids_are_each_reported():
    rows = [LOAD_ROW, LOAD_ROW, dict(LOAD_ROW, id="P009")]
    errors = rate.validate("load", rows, ["P001", "P002"])
    assert any("P002" in e and "missing" in e for e in errors)
    assert any("P001" in e and "duplicate" in e for e in errors)
    assert any("P009" in e and "unexpected" in e for e in errors)


@pytest.mark.parametrize(
    "field, value",
    [
        ("load", 4),
        ("load", "2"),
        ("rule_matching", "yes"),
        ("steps", -1),
        ("needed", "fee formula"),
    ],
)
def test_a_load_field_outside_the_rubric_is_an_error(field, value):
    errors = rate.validate("load", [dict(LOAD_ROW, **{field: value})], ["P001"])
    assert any(field in e for e in errors)


def test_a_bool_is_not_accepted_as_a_load_level():
    # bool is a subclass of int in Python; True must not pass as level 1.
    errors = rate.validate("load", [dict(LOAD_ROW, load=True)], ["P001"])
    assert any("load" in e for e in errors)


@pytest.mark.parametrize(
    "field, value",
    [("kb_clarity", "unclear"), ("decisive", "true"), ("undetermined", None)],
)
def test_a_determinacy_field_outside_the_rubric_is_an_error(field, value):
    errors = rate.validate("determinacy", [dict(DET_ROW, **{field: value})], ["C001"])
    assert any(field in e for e in errors)


def test_a_missing_field_is_an_error():
    row = {k: v for k, v in DET_ROW.items() if k != "decisive"}
    assert any("decisive" in e for e in rate.validate("determinacy", [row], ["C001"]))


def test_jsonl_parsing_skips_blank_lines_and_code_fences():
    text = '```json\n{"id": "P001"}\n\n{"id": "P002"}\n```\n'
    assert rate.parse_jsonl(text) == [{"id": "P001"}, {"id": "P002"}]


def test_an_unparseable_line_is_kept_as_an_error_marker():
    rows = rate.parse_jsonl('{"id": "P001"}\nnot json\n')
    assert rows[0] == {"id": "P001"}
    assert "_unparsed" in rows[1]
    assert rate.validate("load", rows, ["P001"])


def test_packet_ids_are_the_task_headings_in_order():
    text = (
        "# Batch c01: tasks P001-P002 (2 tasks)\n\n"
        "## P001 (manual task)\n\nQ\n\n## P002 (entry task)\n"
    )
    assert rate.packet_ids(text) == ["P001", "P002"]


BLIND_RUBRIC = """# Blind KB-clarity rubric (LiveSQLBench)

PRIVATE MATERIAL: the dossiers contain gold SQL and gold results. Write only
your own verdict file named below; never into a git repository; no web.
Read ONLY the files in `dossiers-blind/` and this rubric. Do not open any
other file or directory under `failure-audit/` (other verdicts, keys or
dossiers): this pass must not know how any agent did.

Each `dossiers-blind/Bnn.md` holds a task's question.

The question for every task: **does the question plus the KB determine what
the gold computes?** Go through the gold SQL clause by clause.

You may check the data read-only, from
`~/projects/agentic-data-contracts/experiments/dabstep-contract-eval`:
  UV_FROZEN=1 uv run python -c "con=connect('<db>'); print(con.sql('<sql>'))"
(e.g. to see whether a NULL treatment changes the result at all).

Output: one JSON line per task, all 55, to the file named in your instructions:
"""


def test_the_moved_rubric_names_the_sandbox_and_keeps_the_criteria():
    moved = rate.adapt_blind_rubric(BLIND_RUBRIC)
    assert "clause by clause" in moved
    assert "./q <db>" in moved
    assert "~/projects" not in moved and "uv run" not in moved
    assert "failure-audit" not in moved and "dossiers-blind/" not in moved
    assert "out.jsonl" in moved


def test_the_moved_rubric_fails_loudly_if_the_original_wording_changed():
    with pytest.raises(ValueError):
        rate.adapt_blind_rubric(BLIND_RUBRIC.replace("You may check the data", "X"))


@pytest.fixture
def lsb_dir(tmp_path):
    con = duckdb.connect(str(tmp_path / "shop.duckdb"))
    con.execute("CREATE TABLE t AS SELECT range AS x FROM range(500)")
    con.close()
    (tmp_path / "secret.csv").write_text("a\n1\n", encoding="utf-8")
    return tmp_path


def test_the_query_server_answers_a_read_only_query(lsb_dir):
    out = qserver.handle(lsb_dir, "shop", "SELECT count(*) FROM t")
    assert "500" in out


def test_the_query_server_caps_the_rows_it_returns(lsb_dir):
    out = qserver.handle(lsb_dir, "shop", "SELECT x FROM t ORDER BY x")
    assert f"first {qserver.MAX_ROWS} of 500 rows" in out
    assert str(qserver.MAX_ROWS + 5) not in out.splitlines()[-1]


@pytest.mark.parametrize("db", ["../shop", "shop/../shop", "", "nope", "/etc/passwd"])
def test_the_query_server_refuses_any_name_that_is_not_a_database(lsb_dir, db):
    assert qserver.handle(lsb_dir, db, "SELECT 1").startswith("error: unknown database")


def test_the_query_server_cannot_read_other_files(lsb_dir):
    out = qserver.handle(
        lsb_dir, "shop", f"SELECT * FROM read_csv('{lsb_dir}/secret.csv')"
    )
    assert out.startswith("error:")


def test_the_query_server_cannot_write(lsb_dir):
    out = qserver.handle(lsb_dir, "shop", "CREATE TABLE u (x INT)")
    assert out.startswith("error:")


def test_each_session_gets_a_fresh_copilot_home_with_only_the_login(tmp_path):
    login = tmp_path / "login" / "config.json"
    login.parent.mkdir()
    login.write_text("{}", encoding="utf-8")
    (login.parent / "session-state").mkdir()
    home = rate.fresh_copilot_home(tmp_path / "homes" / "s1", login)
    assert sorted(p.name for p in home.iterdir()) == ["config.json"]
    (home / "session-state").mkdir()
    again = rate.fresh_copilot_home(tmp_path / "homes" / "s1", login)
    assert sorted(p.name for p in again.iterdir()) == ["config.json"]
