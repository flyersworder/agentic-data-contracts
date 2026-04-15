"""ContractSession — lightweight enforcement via counters and timers."""

from __future__ import annotations

import time
from typing import Any

from agentic_data_contracts.core.contract import DataContract


class LimitExceededError(Exception):
    """Raised when a contract resource limit is exceeded."""


class ContractSession:
    """Tracks enforcement state for a single agent run."""

    def __init__(self, contract: DataContract) -> None:
        self.contract = contract
        self.retries: int = 0
        self.tokens_used: int = 0
        self.cost_usd: float = 0.0
        self._start_time: float | None = None

    def _ensure_timer(self) -> None:
        """Start the timer if not already running."""
        if self._start_time is None:
            self._start_time = time.monotonic()

    @property
    def elapsed_seconds(self) -> float:
        if self._start_time is None:
            return 0.0
        return time.monotonic() - self._start_time

    def reset_timer(self) -> None:
        """Reset the timer so it restarts on the next check_limits() call."""
        self._start_time = None

    def record_retry(self) -> None:
        self.retries += 1

    def record_tokens(self, count: int) -> None:
        self.tokens_used += count

    def record_cost(self, amount: float) -> None:
        self.cost_usd += amount

    def check_limits(self) -> None:
        self._ensure_timer()
        res = self.contract.schema.resources
        if res is None:
            return

        if res.max_retries is not None and self.retries >= res.max_retries:
            raise LimitExceededError(
                f"Max retries exceeded: {self.retries} >= {res.max_retries}"
            )

        if res.token_budget is not None and self.tokens_used > res.token_budget:
            raise LimitExceededError(
                f"token budget exceeded: {self.tokens_used} > {res.token_budget}"
            )

        if res.cost_limit_usd is not None and self.cost_usd > res.cost_limit_usd:
            raise LimitExceededError(
                f"cost limit exceeded: ${self.cost_usd:.2f} > ${res.cost_limit_usd:.2f}"
            )

        temporal = self.contract.schema.temporal
        if temporal and temporal.max_duration_seconds is not None:
            if self.elapsed_seconds > temporal.max_duration_seconds:
                max_dur = temporal.max_duration_seconds
                raise LimitExceededError(
                    f"Duration exceeded: {self.elapsed_seconds:.1f}s > {max_dur}s"
                )

    def remaining(self) -> dict[str, Any]:
        res = self.contract.schema.resources
        result: dict[str, Any] = {
            "elapsed_seconds": round(self.elapsed_seconds, 1),
        }
        if res:
            if res.max_retries is not None:
                result["retries_remaining"] = res.max_retries - self.retries
            if res.token_budget is not None:
                result["tokens_remaining"] = res.token_budget - self.tokens_used
            if res.cost_limit_usd is not None:
                result["cost_remaining_usd"] = res.cost_limit_usd - self.cost_usd

        temporal = self.contract.schema.temporal
        if temporal and temporal.max_duration_seconds is not None:
            result["seconds_remaining"] = round(
                temporal.max_duration_seconds - self.elapsed_seconds, 1
            )
        return result
