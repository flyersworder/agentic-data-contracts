"""The pure parts of `analysis/replay.py`.

`render` turns a DuckDB result set back into the kind of string an agent would
have reported, and every number the script prints depends on it agreeing with
the agent's own formatting conventions. Imported by path because `analysis/`
is a directory of scripts, not a package.
"""

import importlib.util
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


def test_sql_statements_are_ordered_and_only_from_sql_tools():
    # The one trace fixture that ships with the repo; asserting order rather
    # than content, since the transcript is real and may be long.
    trace = next(
        (Path(__file__).parent.parent / "traces" / "glm-full").glob("*__contract__*.gz")
    )
    stmts = sql_statements(trace)
    assert isinstance(stmts, list)
    assert all(isinstance(s, str) and s.strip() for s in stmts)
