import pytest

from agentic_data_contracts.validation._scalar import _scalar_value


def test_returns_the_value_for_a_scalar_shaped_result():
    assert _scalar_value(["total"], [(42,)], "metric") == (42.0, None)


def test_reports_each_unusable_condition_distinctly():
    assert _scalar_value(["total"], [], "metric") == (None, "metric returned no rows")
    assert _scalar_value(["total"], [(None,)], "metric") == (
        None,
        "metric returned NULL",
    )
    value, reason = _scalar_value(["total"], [(float("nan"),)], "metric")
    assert value is None
    assert reason is not None
    assert "non-finite" in reason


def test_raises_when_not_scalar_shaped():
    with pytest.raises(ValueError, match="exactly one column"):
        _scalar_value(["a", "b"], [(1, 2)], "metric")
    with pytest.raises(ValueError, match="at most one row"):
        _scalar_value(["a"], [(1,), (2,)], "metric")
