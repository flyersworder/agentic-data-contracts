"""Records which contract tools an agent actually called during one run.

Standard library only, and deliberately so: ``core.session`` imports this at
runtime, and anything reaching into ``validation`` from here would close the
cycle ``core.session -> validation.conformance -> validation/__init__ ->
examples -> adapters -> core.session``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

OUTCOMES = frozenset({"ok", "miss", "blocked", "error"})


@dataclass(frozen=True)
class ToolCall:
    """One recorded contract-tool call.

    ``outcome`` is the load-bearing field. ``ok`` means the call resolved
    exactly; ``miss`` means a lookup did not (fuzzy fallback fired, or nothing
    matched); ``blocked`` means governance rejected the SQL; ``error`` means the
    tool raised *or returned an error payload without raising*.
    """

    sequence: int
    tool: str
    args: dict[str, Any] = field(default_factory=dict)
    outcome: str = "ok"
    detail: str | None = None
    scalar: float | None = None
    row_count: int | None = None
    relative_time: str | None = None


class ToolRecorder:
    """Collects ``ToolCall`` records for a single agent attempt.

    Times itself from construction rather than reading
    ``ContractSession.elapsed_seconds``: the session timer starts in
    ``_ensure_timer``, reached only from ``check_limits``, which only
    ``run_query`` calls -- so a session-derived duration reads 0.0 for exactly
    the attempts worth timing, the ones where the agent never reached a
    successful query.
    """

    def __init__(self) -> None:
        self.calls: list[ToolCall] = []
        self._start = time.monotonic()
        self._consumed = False

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start

    def log(
        self,
        tool: str,
        args: dict[str, Any],
        outcome: str,
        *,
        detail: str | None = None,
        scalar: float | None = None,
        row_count: int | None = None,
        relative_time: str | None = None,
    ) -> None:
        if outcome not in OUTCOMES:
            raise ValueError(
                f"outcome must be one of {sorted(OUTCOMES)}, got {outcome!r}"
            )
        self.calls.append(
            ToolCall(
                sequence=len(self.calls),
                tool=tool,
                args=dict(args),
                outcome=outcome,
                detail=detail,
                scalar=scalar,
                row_count=row_count,
                relative_time=relative_time,
            )
        )

    def consume(self) -> list[ToolCall]:
        """Hand over the call log exactly once.

        A recorder reused across two questions would merge their call logs and
        silently produce wrong protocol verdicts. Raising here follows the same
        rule as ``VerifiedExample.__post_init__``: a loud error beats a quiet
        wrong answer.
        """
        if self._consumed:
            raise ValueError("ToolRecorder already consumed — use one per attempt")
        self._consumed = True
        return list(self.calls)
