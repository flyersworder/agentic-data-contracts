"""Row-encoding tests for run_query / preview_table (issue #44)."""

import json
from datetime import date
from decimal import Decimal

from agentic_data_contracts.tools.factory import _render_rows

COLUMNS = ["region", "units", "note"]
ROWS = [
    ("EMEA", 412, None),
    ("APAC", 87, "re-run\tpending\nline2"),
    ("AMER", 0, ""),
]


class _DriverRow:
    """A row that is iterable and indexable but is neither list nor tuple.

    Stands in for a third-party adapter returning its driver's row type.
    ``dict(zip(...))`` accepts this; ``json.dumps`` does not.
    """

    def __init__(self, *values: object) -> None:
        self._values = list(values)

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, index: int) -> object:
        return self._values[index]


def test_compact_renders_positional_arrays() -> None:
    assert _render_rows(COLUMNS, ROWS, "compact") == [
        ["EMEA", 412, None],
        ["APAC", 87, "re-run\tpending\nline2"],
        ["AMER", 0, ""],
    ]


def test_records_renders_one_dict_per_row() -> None:
    assert _render_rows(COLUMNS, ROWS, "records")[0] == {
        "region": "EMEA",
        "units": 412,
        "note": None,
    }


def test_empty_rows_render_empty_in_both_modes() -> None:
    assert _render_rows(COLUMNS, [], "compact") == []
    assert _render_rows(COLUMNS, [], "records") == []


def test_null_stays_distinct_from_empty_string() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[0][2] is None
    assert rendered[2][2] == ""


def test_tab_and_newline_survive_serialization() -> None:
    rendered = json.loads(
        json.dumps(_render_rows(COLUMNS, ROWS, "compact"), default=str)
    )
    assert rendered[1][2] == "re-run\tpending\nline2"


def test_decimal_and_date_coerce_identically_in_both_modes() -> None:
    columns = ["salary", "hired"]
    rows = [(Decimal("100000.00"), date(2025, 1, 31))]
    compact = json.loads(
        json.dumps(_render_rows(columns, rows, "compact"), default=str)
    )
    records = json.loads(
        json.dumps(_render_rows(columns, rows, "records"), default=str)
    )
    assert compact[0] == ["100000.00", "2025-01-31"]
    assert records[0] == {"salary": "100000.00", "hired": "2025-01-31"}


def test_non_tuple_row_serializes_as_array_not_string() -> None:
    # Without the list(row) coercion json.dumps routes _DriverRow through
    # default=str and emits "<_DriverRow object at 0x...>" instead of an array.
    rendered = json.loads(
        json.dumps(
            _render_rows(COLUMNS, [_DriverRow("EMEA", 412, None)], "compact"),
            default=str,
        )
    )
    assert rendered == [["EMEA", 412, None]]


def test_row_format_is_exported_from_package_root() -> None:
    import agentic_data_contracts

    assert "RowFormat" in agentic_data_contracts.__all__
