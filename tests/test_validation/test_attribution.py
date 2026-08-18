import math

import pytest

from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition
from agentic_data_contracts.validation.attribution import (
    INTERACTION_KEY,
    AttributionResult,
    attribute_change,
    check_attribution,
)

# The #67 worked example. parent = volume * rate.
BEFORE = {"volume": 10_000.0, "rate": 0.35}
AFTER = {"volume": 15_000.0, "rate": 0.45}


def _metric(
    convention: str | None,
    *,
    convention_operand: str | None = None,
    operator: str = "product",
    operands: list[str] | None = None,
) -> MetricDefinition:
    return MetricDefinition(
        name="activations",
        description="",
        sql_expression="",
        decompositions=[
            Decomposition(
                operator=operator,
                operands=operands if operands is not None else ["volume", "rate"],
                convention=convention,
                convention_operand=convention_operand,
            )
        ],
    )


class TestWorkedExample:
    """Reproduces every row of the #67 convention table."""

    def test_explicit_leaves_the_residual_unattributed(self) -> None:
        r = attribute_change(_metric("explicit"), before=BEFORE, after=AFTER)
        assert abs(r.delta_parent - 3250.0) < 1e-6
        assert abs(r.contributions["volume"] - 1750.0) < 1e-6
        assert abs(r.contributions["rate"] - 1000.0) < 1e-6
        assert abs(r.interaction - 500.0) < 1e-6

    def test_split_evenly_divides_the_residual(self) -> None:
        r = attribute_change(_metric("split_evenly"), before=BEFORE, after=AFTER)
        assert abs(r.contributions["volume"] - 2000.0) < 1e-6
        assert abs(r.contributions["rate"] - 1250.0) < 1e-6
        # The raw residual is still reported so the placement stays auditable.
        assert abs(r.interaction - 500.0) < 1e-6

    def test_fold_into_rate_is_laspeyres(self) -> None:
        r = attribute_change(
            _metric("fold_into", convention_operand="rate"),
            before=BEFORE,
            after=AFTER,
        )
        assert abs(r.contributions["volume"] - 1750.0) < 1e-6
        assert abs(r.contributions["rate"] - 1500.0) < 1e-6

    def test_fold_into_volume_is_paasche(self) -> None:
        r = attribute_change(
            _metric("fold_into", convention_operand="volume"),
            before=BEFORE,
            after=AFTER,
        )
        assert abs(r.contributions["volume"] - 2250.0) < 1e-6
        assert abs(r.contributions["rate"] - 1000.0) < 1e-6

    def test_every_convention_sums_to_delta(self) -> None:
        # The #67 finding: all four are arithmetically correct. What differs is
        # the split, which is exactly why it must be declared.
        for metric in (
            _metric("explicit"),
            _metric("split_evenly"),
            _metric("fold_into", convention_operand="rate"),
            _metric("fold_into", convention_operand="volume"),
        ):
            r = attribute_change(metric, before=BEFORE, after=AFTER)
            total = math.fsum(r.contributions.values())
            if r.convention == "explicit":
                total += r.interaction
            assert abs(total - r.delta_parent) < 1e-6

    def test_shares_are_relative_to_delta(self) -> None:
        r = attribute_change(_metric("split_evenly"), before=BEFORE, after=AFTER)
        assert r.shares is not None
        assert abs(r.shares["volume"] - 2000.0 / 3250.0) < 1e-9

    def test_shares_is_none_when_delta_is_zero(self) -> None:
        # None, not {} -- "undefined" must be distinguishable from "all zero".
        r = attribute_change(
            _metric("split_evenly"),
            before={"volume": 100.0, "rate": 0.5},
            after={"volume": 50.0, "rate": 1.0},
        )
        assert abs(r.delta_parent) < 1e-9
        assert r.shares is None


class TestOtherOperators:
    def test_ratio_residual_is_the_mix_effect(self) -> None:
        r = attribute_change(
            _metric("explicit", operands=["num", "den"], operator="ratio"),
            before={"num": 100.0, "den": 10.0},
            after={"num": 120.0, "den": 12.0},
        )
        assert abs(r.delta_parent) < 1e-9
        assert abs(r.contributions["num"] - 2.0) < 1e-9
        assert abs(r.contributions["den"] - (100.0 / 12.0 - 10.0)) < 1e-9
        assert (
            abs(r.interaction - (0.0 - r.contributions["num"] - r.contributions["den"]))
            < 1e-9
        )

    def test_sum_is_linear_and_needs_no_convention(self) -> None:
        r = attribute_change(
            _metric(None, operator="sum", operands=["a", "b"]),
            before={"a": 1.0, "b": 2.0},
            after={"a": 4.0, "b": 6.0},
        )
        assert abs(r.delta_parent - 7.0) < 1e-9
        assert abs(r.contributions["a"] - 3.0) < 1e-9
        assert abs(r.contributions["b"] - 4.0) < 1e-9
        assert abs(r.interaction) < 1e-9

    def test_difference_negates_the_subtrahend(self) -> None:
        r = attribute_change(
            _metric(None, operator="difference", operands=["a", "b"]),
            before={"a": 10.0, "b": 3.0},
            after={"a": 12.0, "b": 8.0},
        )
        assert abs(r.delta_parent - (-3.0)) < 1e-9
        assert abs(r.contributions["a"] - 2.0) < 1e-9
        assert abs(r.contributions["b"] - (-5.0)) < 1e-9


class TestPreconditions:
    def test_no_decomposition_raises(self) -> None:
        metric = MetricDefinition(name="m", description="", sql_expression="")
        with pytest.raises(ValueError, match="declares no decompositions"):
            attribute_change(metric, before={}, after={})

    def test_index_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            attribute_change(
                _metric("explicit"), before=BEFORE, after=AFTER, decomposition=3
            )

    def test_key_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="do not match the declared operands"):
            attribute_change(_metric("explicit"), before={"volume": 1.0}, after=AFTER)

    def test_cross_term_operator_without_convention_raises(self) -> None:
        # The kernel only works on a governed metric. That is the point.
        with pytest.raises(ValueError, match="declares no attribution convention"):
            attribute_change(_metric(None), before=BEFORE, after=AFTER)

    def test_zero_ratio_denominator_raises(self) -> None:
        with pytest.raises(ValueError, match="denominator"):
            attribute_change(
                _metric("explicit", operands=["num", "den"], operator="ratio"),
                before={"num": 1.0, "den": 0.0},
                after={"num": 1.0, "den": 2.0},
            )

    def test_unknown_convention_raises(self) -> None:
        # A hand-built Decomposition can carry any string; attribute_change
        # must not assume it came through the load-time validator.
        with pytest.raises(ValueError, match="unknown attribution convention"):
            attribute_change(_metric("nonsense"), before=BEFORE, after=AFTER)

    def test_fold_into_without_convention_operand_raises(self) -> None:
        with pytest.raises(ValueError, match="convention_operand"):
            attribute_change(_metric("fold_into"), before=BEFORE, after=AFTER)

    def test_fold_into_convention_operand_not_in_operands_raises(self) -> None:
        with pytest.raises(ValueError, match="convention_operand"):
            attribute_change(
                _metric("fold_into", convention_operand="bogus"),
                before=BEFORE,
                after=AFTER,
            )


def test_result_is_frozen() -> None:
    # Matches the AttributeError + type-ignore-comment style TestResultType
    # uses in test_reconciliation.py.
    r = attribute_change(_metric("explicit"), before=BEFORE, after=AFTER)
    assert isinstance(r, AttributionResult)
    with pytest.raises(AttributeError):
        r.delta_parent = 0.0  # type: ignore


def test_interaction_key_is_exported() -> None:
    assert INTERACTION_KEY == "interaction"


class TestCheckAttribution:
    def test_matching_breakdown_passes(self) -> None:
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2000.0, "rate": 1250.0},
        )
        assert r.matches is True
        assert r.sums_to_delta is True
        assert r.reason is None

    def test_wrong_convention_is_caught_even_though_it_sums(self) -> None:
        # This is the #67 failure exactly: correct arithmetic, wrong split.
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1500.0},
        )
        assert r.sums_to_delta is True
        assert r.matches is False
        assert r.reason is not None
        assert r.deviations is not None
        assert abs(r.deviations["volume"] - (-250.0)) < 1e-6

    def test_explicit_requires_the_interaction_key(self) -> None:
        r = check_attribution(
            _metric("explicit"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1000.0, INTERACTION_KEY: 500.0},
        )
        assert r.matches is True

    def test_explicit_sums_to_delta_only_with_the_interaction_key_correct(
        self,
    ) -> None:
        # explicit is the only convention where sums_to_delta depends on
        # INTERACTION_KEY being summed in -- the other conventions fold the
        # residual into a factor before check_attribution ever sees it.
        exact = check_attribution(
            _metric("explicit"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1000.0, INTERACTION_KEY: 500.0},
        )
        assert exact.matches is True
        assert exact.sums_to_delta is True

        wrong_interaction = check_attribution(
            _metric("explicit"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1000.0, INTERACTION_KEY: 600.0},
        )
        assert wrong_interaction.matches is False
        assert wrong_interaction.sums_to_delta is False

    def test_explicit_without_the_interaction_key_raises(self) -> None:
        with pytest.raises(ValueError, match="do not match"):
            check_attribution(
                _metric("explicit"),
                before=BEFORE,
                after=AFTER,
                reported={"volume": 1750.0, "rate": 1000.0},
            )

    def test_interaction_key_rejected_when_already_distributed(self) -> None:
        # Under split_evenly the residual is inside the contributions;
        # reporting it separately double-counts.
        with pytest.raises(ValueError, match="do not match"):
            check_attribution(
                _metric("split_evenly"),
                before=BEFORE,
                after=AFTER,
                reported={"volume": 2000.0, "rate": 1250.0, INTERACTION_KEY: 500.0},
            )

    def test_tolerance_is_scaled_to_delta_parent(self) -> None:
        # rel_tol * |delta| = 1e-3 * 3250 = 3.25, so a 3.0 deviation passes
        # and a 4.0 deviation does not.
        inside = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2003.0, "rate": 1250.0},
            rel_tol=1e-3,
        )
        assert inside.matches is True
        outside = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2004.0, "rate": 1250.0},
            rel_tol=1e-3,
        )
        assert outside.matches is False

    def test_expected_fields_are_still_populated(self) -> None:
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2000.0, "rate": 1250.0},
        )
        assert abs(r.contributions["volume"] - 2000.0) < 1e-6
        assert r.rel_tol == 1e-4
        assert r.abs_tol == 0.0


def test_attribution_is_exported_from_the_validation_package() -> None:
    from agentic_data_contracts.validation import (
        AttributionResult as Exported,
    )
    from agentic_data_contracts.validation import attribute_change, check_attribution

    assert Exported is AttributionResult
    assert attribute_change is not None
    assert check_attribution is not None


class TestDegenerateDelta:
    """A metric whose factors offset is flat, and flat broke every scale.

    ``delta_parent`` was the module's only reference magnitude, which assumes
    the total change is non-zero. Offsetting factors — customers double while
    spend halves — are the attribution question most worth asking, and there
    the delta is zero while the contributions are ±100. Found by review of the
    v0.43.0 PR.
    """

    _FLAT_BEFORE = {"customers": 10.0, "spend": 10.0}
    _FLAT_AFTER = {"customers": 20.0, "spend": 5.0}

    def test_flat_metric_still_attributes(self) -> None:
        r = attribute_change(
            _metric("explicit", operands=["customers", "spend"]),
            before=self._FLAT_BEFORE,
            after=self._FLAT_AFTER,
        )
        assert abs(r.delta_parent) < 1e-9
        assert abs(r.contributions["customers"] - 100.0) < 1e-9
        assert abs(r.contributions["spend"] - (-50.0)) < 1e-9
        assert abs(r.interaction - (-50.0)) < 1e-9

    def test_flat_metric_tolerance_is_not_exact_equality(self) -> None:
        # Previously the tolerance was rel_tol * abs(delta_parent) == 0.0, so a
        # 1e-7 rounding in a written answer failed the check.
        r = check_attribution(
            _metric("explicit", operands=["customers", "spend"]),
            before=self._FLAT_BEFORE,
            after=self._FLAT_AFTER,
            reported={
                "customers": 100.0,
                "spend": -50.0,
                INTERACTION_KEY: -50.0000001,
            },
        )
        assert r.matches is True

    def test_flat_metric_still_rejects_a_real_deviation(self) -> None:
        # The looser scale must not make the check toothless: scale is 100, so
        # rel_tol 1e-4 gives a tolerance of 0.01 — 5.0 is far outside it.
        r = check_attribution(
            _metric("explicit", operands=["customers", "spend"]),
            before=self._FLAT_BEFORE,
            after=self._FLAT_AFTER,
            reported={
                "customers": 105.0,
                "spend": -50.0,
                INTERACTION_KEY: -50.0,
            },
        )
        assert r.matches is False

    def test_float_noise_delta_yields_undefined_shares(self) -> None:
        # 0.1*3.0 and 0.3*1.0 are equal in decimal but differ by 5.6e-17 in
        # binary. An exact `delta == 0` guard missed it and produced shares of
        # ~1e16 where the docstring promises None.
        r = attribute_change(
            _metric("explicit", operands=["a", "b"]),
            before={"a": 0.1, "b": 3.0},
            after={"a": 0.3, "b": 1.0},
        )
        assert r.delta_parent != 0.0  # genuinely noise, not exactly zero
        assert r.shares is None

    def test_real_delta_still_produces_shares(self) -> None:
        r = attribute_change(_metric("split_evenly"), before=BEFORE, after=AFTER)
        assert r.shares is not None
        assert abs(r.shares["volume"] - 2000.0 / 3250.0) < 1e-9


class TestInteractionKeyCollision:
    def test_operand_named_interaction_is_refused_under_explicit(self) -> None:
        # Nothing at load time forbids a metric named `interaction` from being
        # an operand, but under `explicit` the residual is reported under that
        # same key — so the operand's own contribution is overwritten, a correct
        # breakdown is inexpressible, and the verdict came back self-
        # contradictory (matches True with sums_to_delta False).
        with pytest.raises(ValueError, match="collides"):
            check_attribution(
                _metric("explicit", operands=[INTERACTION_KEY, "users"]),
                before={INTERACTION_KEY: 2.0, "users": 10.0},
                after={INTERACTION_KEY: 3.0, "users": 20.0},
                reported={INTERACTION_KEY: 10.0, "users": 20.0},
            )

    def test_operand_named_interaction_is_fine_without_explicit(self) -> None:
        # Only `explicit` reports a separate residual line, so only `explicit`
        # collides. The other conventions stay usable.
        r = check_attribution(
            _metric("split_evenly", operands=[INTERACTION_KEY, "users"]),
            before={INTERACTION_KEY: 2.0, "users": 10.0},
            after={INTERACTION_KEY: 3.0, "users": 20.0},
            reported={INTERACTION_KEY: 15.0, "users": 25.0},
        )
        assert r.matches is True
