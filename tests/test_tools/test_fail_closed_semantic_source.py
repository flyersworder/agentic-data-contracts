"""Fail-closed enforcement: building tools from a contract that declares a
semantic source which cannot be loaded must raise, not silently under-enforce.

This is the factory-level half of Path A — every adapter (SDK, LangChain,
Pydantic AI) funnels through ``create_tools``, so guaranteeing it here covers
all of them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
from agentic_data_contracts.tools.factory import create_tools


def test_create_tools_fails_closed_on_declared_but_missing_source(
    fixtures_dir: Path,
) -> None:
    contract = DataContract.from_yaml(fixtures_dir / "missing_source_contract.yml")
    with pytest.raises(SemanticSourceUnavailableError):
        create_tools(contract)
