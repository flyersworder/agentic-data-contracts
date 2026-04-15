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
