"""Contract middleware — wraps existing tool functions with contract enforcement."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from agentic_data_contracts.adapters.base import DatabaseAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.session import ContractSession, LimitExceededError
from agentic_data_contracts.validation.validator import Validator


def contract_middleware(
    contract: DataContract,
    *,
    adapter: DatabaseAdapter | None = None,
    session: ContractSession | None = None,
) -> Callable:
    if session is None:
        session = ContractSession(contract)

    dialect = adapter.dialect if adapter else None
    validator = Validator(contract, dialect=dialect)

    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(args: dict[str, Any]) -> dict[str, Any]:
            try:
                session.check_limits()
            except LimitExceededError as e:
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"BLOCKED — Session limit exceeded: {e}",
                        }
                    ]
                }

            sql = args.get("sql", "")
            if sql:
                result = validator.validate(sql)
                if result.blocked:
                    session.record_retry()
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": "BLOCKED — Violations:\n"
                                + "\n".join(f"- {r}" for r in result.reasons),
                            }
                        ]
                    }

            return await fn(args)

        return wrapper

    return decorator
