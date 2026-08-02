from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError


def test_session_tracks_retries(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    assert session.retries == 0
    session.record_retry()
    session.record_retry()
    assert session.retries == 2


def test_session_blocks_on_max_retries(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_retry()
    session.record_retry()
    session.record_retry()
    with pytest.raises(LimitExceededError, match="retries"):
        session.check_limits()


def test_session_tracks_tokens(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_tokens(10000)
    session.record_tokens(20000)
    assert session.tokens_used == 30000


def test_session_blocks_on_token_budget(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_tokens(50001)
    with pytest.raises(LimitExceededError, match="token"):
        session.check_limits()


def test_session_tracks_cost(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_cost(2.50)
    session.record_cost(1.50)
    assert session.cost_usd == pytest.approx(4.0)


def test_session_blocks_on_cost_limit(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_cost(5.01)
    with pytest.raises(LimitExceededError, match="cost"):
        session.check_limits()


def test_session_elapsed_seconds(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.check_limits()  # starts the timer
    assert session.elapsed_seconds >= 0.0


def test_session_no_limits_when_none_configured(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    session = ContractSession(dc)
    session.record_retry()
    session.record_retry()
    session.record_retry()
    session.record_retry()
    session.record_tokens(999999)
    session.record_cost(999.0)
    session.check_limits()  # Should not raise


def test_session_remaining_budget(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.record_tokens(10000)
    session.record_cost(1.50)
    session.record_retry()
    info = session.remaining()
    assert info["retries_remaining"] == 2
    assert info["tokens_remaining"] == 40000
    assert info["cost_remaining_usd"] == pytest.approx(3.50)


# --- Lazy timer tests (issue #16) ---


def test_timer_not_started_at_construction(fixtures_dir: Path) -> None:
    """Timer should not start at construction — elapsed should be 0."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    assert session.elapsed_seconds == 0.0


def test_timer_starts_on_first_check_limits(fixtures_dir: Path) -> None:
    """Timer should start lazily on the first check_limits() call."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    assert session._start_time is None
    session.check_limits()
    assert session._start_time is not None
    assert session.elapsed_seconds >= 0.0


def test_reset_timer(fixtures_dir: Path) -> None:
    """reset_timer() should clear the timer so elapsed returns 0."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.check_limits()  # starts timer
    session.reset_timer()
    assert session.elapsed_seconds == 0.0
    assert session._start_time is None


def test_reset_timer_restarts_on_next_check(fixtures_dir: Path) -> None:
    """After reset, timer should restart on the next check_limits() call."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.check_limits()  # starts timer
    session.reset_timer()
    session.check_limits()  # restarts timer
    assert session._start_time is not None


def test_reset_timer_before_started_is_noop(fixtures_dir: Path) -> None:
    """reset_timer() on an unstarted timer should be a safe no-op."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    session.reset_timer()  # should not raise
    assert session._start_time is None
    assert session.elapsed_seconds == 0.0


def test_remaining_before_timer_started(fixtures_dir: Path) -> None:
    """remaining() should show full duration budget when timer not started."""
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    session = ContractSession(dc)
    info = session.remaining()
    assert "seconds_remaining" in info
    assert info["seconds_remaining"] == 300.0


class TestObserveTokens:
    """Cumulative usage from a framework counter, accrued as deltas.

    Framework counters report a running total for *their* scope — a Pydantic AI
    run, a LangGraph thread — while a ContractSession deliberately spans several
    of them (the deps-aware toolset keeps one session per user across every
    turn, so limits accumulate). Adding the total each time would multiply-count;
    setting it would reset the session at each new run. Only the per-scope delta
    accrues correctly.
    """

    def _session(self, fixtures_dir: Path) -> ContractSession:
        return ContractSession(
            DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        )

    def test_first_observation_counts_in_full(self, fixtures_dir: Path) -> None:
        session = self._session(fixtures_dir)
        session.observe_tokens(1200, scope="run-1")
        assert session.tokens_used == 1200

    def test_repeated_observation_accrues_only_the_delta(
        self, fixtures_dir: Path
    ) -> None:
        # The bug this prevents: a cumulative total added on every tool call
        # multiplies. Three calls in one run at 500/900/1500 is 1500 used, not
        # 2900.
        session = self._session(fixtures_dir)
        for total in (500, 900, 1500):
            session.observe_tokens(total, scope="run-1")
        assert session.tokens_used == 1500

    def test_unchanged_total_adds_nothing(self, fixtures_dir: Path) -> None:
        session = self._session(fixtures_dir)
        session.observe_tokens(800, scope="run-1")
        session.observe_tokens(800, scope="run-1")
        assert session.tokens_used == 800

    def test_scopes_accumulate_independently(self, fixtures_dir: Path) -> None:
        # The cross-turn case: one session, several runs. Each run's counter
        # starts from zero, and the session must total them rather than reset.
        session = self._session(fixtures_dir)
        session.observe_tokens(1000, scope="run-1")
        session.observe_tokens(700, scope="run-2")
        session.observe_tokens(1300, scope="run-1")
        assert session.tokens_used == 2000  # 1300 (run-1) + 700 (run-2)

    def test_a_lower_total_never_subtracts(self, fixtures_dir: Path) -> None:
        # Defensive: a counter that resets under a reused scope key must not
        # hand back budget the session already spent.
        session = self._session(fixtures_dir)
        session.observe_tokens(5000, scope="run-1")
        session.observe_tokens(10, scope="run-1")
        assert session.tokens_used == 5000

    def test_composes_with_record_tokens(self, fixtures_dir: Path) -> None:
        # record_tokens stays the delta-based path for framework-free callers;
        # the two must not fight over the same counter.
        session = self._session(fixtures_dir)
        session.record_tokens(400)
        session.observe_tokens(600, scope="run-1")
        assert session.tokens_used == 1000

    def test_observed_usage_reaches_the_limit_check(self, fixtures_dir: Path) -> None:
        session = self._session(fixtures_dir)
        session.observe_tokens(50_001, scope="run-1")
        with pytest.raises(LimitExceededError, match="token"):
            session.check_limits()

    def test_remaining_reflects_observed_usage(self, fixtures_dir: Path) -> None:
        # Before this existed, remaining() reported the full budget forever and
        # every BLOCKED envelope told the model a false number.
        session = self._session(fixtures_dir)
        session.observe_tokens(20_000, scope="run-1")
        assert session.remaining()["tokens_remaining"] == 30_000
