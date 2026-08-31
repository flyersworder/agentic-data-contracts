"""The frozen contract under test.

Its digest is stamped into every result row so a post-hoc edit is detectable
against git history rather than merely promised in prose.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from agentic_data_contracts import DataContract, contract_digest

CONTRACT_PATH = Path(__file__).parent.parent / "contract" / "contract.yml"

#: Arm C's scaffolding with its payments knowledge stripped — the control that
#: separates contract CONTENT from contract TOOLING. Generated from
#: `CONTRACT_PATH` by `dce.hollow`; see that module for what is removed and why.
HOLLOW_CONTRACT_PATH = Path(__file__).parent.parent / "contract_hollow" / "contract.yml"


@lru_cache(maxsize=1)
def load_contract() -> DataContract:
    return DataContract.from_yaml(CONTRACT_PATH)


@lru_cache(maxsize=1)
def load_hollow_contract() -> DataContract:
    return DataContract.from_yaml(HOLLOW_CONTRACT_PATH)


def digest() -> str:
    return contract_digest(load_contract())


def hollow_digest() -> str:
    return contract_digest(load_hollow_contract())
