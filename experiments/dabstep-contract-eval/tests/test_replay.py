"""The pure parts of `analysis/replay.py`.

`render` turns a DuckDB result set back into the kind of string an agent would
have reported, and every number the script prints depends on it agreeing with
the agent's own formatting conventions. Imported by path because `analysis/`
is a directory of scripts, not a package.
"""

import gzip
import importlib.util
import json
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "replay", Path(__file__).parent.parent / "analysis" / "replay.py"
)
assert _SPEC and _SPEC.loader
replay = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(replay)

render = replay.render
cells = replay.cells
sql_statements = replay.sql_statements


def test_cells_are_the_individual_values():
    # A reported answer is almost always a PROJECTION of the result, not the
    # whole of it: a query returning ("GlobalCard", 0.329) backs the answer
    # "GlobalCard". Membership is the test; equality is not.
    assert cells([("GlobalCard", 0.329)]) == ["GlobalCard", "0.329"]
    assert cells([(1,), (2,)]) == ["1", "2"]
    assert cells([]) == []


def test_cells_drop_nulls_rather_than_naming_them():
    assert cells([("A", None)]) == ["A"]


def test_empty_result_renders_empty():
    assert render([]) == ""


def test_single_scalar():
    assert render([(5,)]) == "5"
    assert render([(42.9,)]) == "42.9"


def test_single_column_many_rows_is_a_list():
    assert render([(1,), (2,), (3,)]) == "1, 2, 3"


def test_single_row_many_columns_is_a_list():
    assert render([("TransactPlus", 3458.48)]) == "TransactPlus, 3458.48"


def test_grid_is_flattened_row_major():
    assert render([("A", 1), ("B", 2)]) == "A, 1, B, 2"


def test_nulls_become_empty_cells_not_the_string_none():
    # `None` would score against a gold as the literal word, which is worse
    # than an empty cell: it can accidentally match a "Not Applicable".
    assert render([(None,)]) == ""
    assert render([("A", None)]) == "A, "


def test_sql_statements_are_ordered_and_only_from_sql_tools(tmp_path):
    # A small synthetic transcript, since the real ones are release assets and
    # not in the repo. It covers both argument encodings and every skip rule.
    def call(tool, args):
        return {"part_kind": "tool-call", "tool_name": tool, "args": args}

    messages = [
        {"parts": [{"part_kind": "user-prompt", "content": "q"}]},
        {"parts": [call("run_query", {"sql": "SELECT 1"})]},
        {"parts": [call("describe_table", {"sql": "SELECT 'not answering'"})]},
        {"parts": [call("execute_sql", json.dumps({"sql": "SELECT 2"}))]},
        {"parts": [call("run_query", "{not json")]},
        {"parts": [call("run_query", {"sql": "   "})]},
        {"parts": [call("run_query", {"sql": "SELECT 3"})]},
    ]
    trace = tmp_path / "t__contract__m.json.gz"
    with gzip.open(trace, "wt", encoding="utf-8") as fh:
        json.dump(messages, fh)

    assert sql_statements(trace) == ["SELECT 1", "SELECT 2", "SELECT 3"]
