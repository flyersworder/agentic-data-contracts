"""Pass 3 over a verified-examples corpus: can an agent reproduce the certified
answer from the contract alone, through the governed path?

``validate_examples`` asks whether certified SQL is still allowed and plannable.
``check_example_answers`` asks whether it still returns the right number. Both
check SQL a human already got right. Neither can see a contract that stays
enforceable and accurate while quietly ceasing to *teach* -- rename a metric or
trim a domain description and both stay green.

Nothing here calls a model or touches a database. The consumer runs their own
agent and hands back ``Attempt`` records; every function below is pure.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from agentic_data_contracts.core.recorder import ToolCall
from agentic_data_contracts.validation.examples import VerifiedExample

if TYPE_CHECKING:
    from agentic_data_contracts.core.session import ContractSession


@dataclass
class Attempt:
    """One agent run against one corpus question."""

    example: VerifiedExample
    calls: list[ToolCall] = field(default_factory=list)
    final_text: str = ""
    final_answer: float | None = None
    foreign_tool_calls: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None

    @classmethod
    def from_session(
        cls,
        example: VerifiedExample,
        session: ContractSession,
        *,
        final_text: str = "",
        final_answer: float | None = None,
        foreign_tool_calls: Iterable[str] = (),
        error: str | None = None,
    ) -> Attempt:
        """Snapshot one attempt off the session that served it.

        ``foreign_tool_calls`` is coerced with ``list()``: the parameter takes
        any iterable and defaults to ``()``, while the field is ``list[str]``.
        """
        if session.recorder is None:
            raise ValueError(
                "Attempt.from_session needs a session built with a recorder: "
                "ContractSession(contract, recorder=ToolRecorder())"
            )
        return cls(
            example=example,
            calls=session.recorder.consume(),
            final_text=final_text,
            final_answer=final_answer,
            foreign_tool_calls=list(foreign_tool_calls),
            cost_usd=session.cost_usd,
            elapsed_seconds=session.recorder.elapsed_seconds,
            error=error,
        )
