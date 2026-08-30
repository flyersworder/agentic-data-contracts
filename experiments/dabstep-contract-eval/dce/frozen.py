"""The frozen contract under test.

Its digest is stamped into every result row so a post-hoc edit is detectable
against git history rather than merely promised in prose.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from agentic_data_contracts import DataContract, contract_digest

CONTRACT_PATH = Path(__file__).parent.parent / "contract" / "contract.yml"


@lru_cache(maxsize=1)
def load_contract() -> DataContract:
    return DataContract.from_yaml(CONTRACT_PATH)


def digest() -> str:
    return contract_digest(load_contract())
