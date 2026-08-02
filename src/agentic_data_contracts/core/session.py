"""ContractSession — lightweight enforcement via counters and timers."""

from __future__ import annotations

import time
from typing import Any

from agentic_data_contracts.core.contract import DataContract


class LimitExceededError(Exception):
    """Raised when a contract resource limit is exceeded."""


class ContractSessionLimitError(RuntimeError):
    """Terminal enforcement error surfaced when a session budget is exhausted.

    Distinct from :class:`LimitExceededError`, which is the internal signal
    raised by :meth:`ContractSession.check_limits`. Framework adapters raise
    ``ContractSessionLimitError`` to *propagate* a budget breach out of an
    agent run — it is terminal because retrying a breached
    ``max_retries`` / ``max_duration`` / cost cap cannot recover. Adapters
    that distinguish recoverable from terminal failures (e.g. the Pydantic AI
    adapter, which uses ``ModelRetry`` for recoverable validation blocks) raise
    this instead so the run ends rather than spending another retry.
    """


class ContractSession:
    """Tracks enforcement state for a single agent run."""

    def __init__(self, contract: DataContract) -> None:
        self.contract = contract
        self.retries: int = 0
        self.tokens_used: int = 0
        self.cost_usd: float = 0.0
        self._start_time: float | None = None
        # Last cumulative total seen per external counter — see observe_tokens.
        self._observed_totals: dict[str, int] = {}

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
        """Add a *delta* of tokens to the session tally.

        For callers holding an incremental count. If you have a framework's
        running total instead, use :meth:`observe_tokens` — adding a cumulative
        figure here multiplies it.
        """
        self.tokens_used += count

    def observe_tokens(self, cumulative: int, *, scope: str = "") -> None:
        """Record a *cumulative* token total reported by an external counter.

        Framework usage counters report a running total for **their** scope — a
        Pydantic AI run (``ctx.usage``), a LangGraph thread (message
        ``usage_metadata``) — while a ``ContractSession`` deliberately spans
        several of them: :class:`~...tools.pydantic_ai.ContractDeps` instructs
        callers to keep one session per user across every turn so limits
        accumulate. That mismatch rules out both obvious implementations —
        adding the total on each tool call multiplies it, and assigning it
        resets the session's tally at each new run, silently discarding earlier
        turns. So only the per-scope delta accrues.

        ``scope`` identifies the counter (Pydantic AI's ``ctx.run_id``). A
        total lower than the last one seen for that scope adds nothing rather
        than subtracting, so a counter that restarts under a reused key cannot
        hand back budget the session already spent.
        """
        previous = self._observed_totals.get(scope, 0)
        if cumulative > previous:
            self.tokens_used += cumulative - previous
            self._observed_totals[scope] = cumulative

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
