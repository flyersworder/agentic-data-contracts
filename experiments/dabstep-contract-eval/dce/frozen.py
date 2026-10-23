"""The frozen contract under test.

Its digest is stamped into every result row so a post-hoc edit is detectable
against git history rather than merely promised in prose.
"""

from __future__ import annotations

from functools import cache, lru_cache
from pathlib import Path

from agentic_data_contracts import DataContract, contract_digest

CONTRACT_PATH = Path(__file__).parent.parent / "contract" / "contract.yml"

#: Arm C's scaffolding with its payments knowledge stripped — the control that
#: separates contract CONTENT from contract TOOLING. Generated from
#: `CONTRACT_PATH` by `dce.hollow`; see that module for what is removed and why.
HOLLOW_CONTRACT_PATH = Path(__file__).parent.parent / "contract_hollow" / "contract.yml"

#: Arm C with its own reading of the manual removed -- the control that
#: separates DELIVERY of what the manual states from RESOLUTION of what it
#: leaves open. Generated from `CONTRACT_PATH` by `dce.uninterpreted`.
UNINTERPRETED_CONTRACT_PATH = (
    Path(__file__).parent.parent / "contract_uninterpreted" / "contract.yml"
)


@lru_cache(maxsize=1)
def load_contract() -> DataContract:
    return DataContract.from_yaml(CONTRACT_PATH)


@lru_cache(maxsize=1)
def load_hollow_contract() -> DataContract:
    return DataContract.from_yaml(HOLLOW_CONTRACT_PATH)


@lru_cache(maxsize=1)
def load_uninterpreted_contract() -> DataContract:
    return DataContract.from_yaml(UNINTERPRETED_CONTRACT_PATH)


def digest() -> str:
    return contract_digest(load_contract())


def hollow_digest() -> str:
    return contract_digest(load_hollow_contract())


def uninterpreted_digest() -> str:
    return contract_digest(load_uninterpreted_contract())


@cache
def load_contract_at(path: Path) -> DataContract:
    """A compiled contract, by path (`contracts/<benchmark>/...`)."""
    return DataContract.from_yaml(path)


#: DABStep's contract compiled by `dce.compile` from the same two documents
#: (spec, Part 5). Written by the compile commit; `DABStep.check_arms` stops
#: a sweep that asks for `contract_compiled` before it exists.
COMPILED_CONTRACT_PATH = (
    Path(__file__).parent.parent / "contracts" / "dabstep" / "contract.yml"
)


#: The other compiled DABStep contracts, by arm: claudeopus5.5 as the
#: compiler model, without and with declared joins (88484eb).
COMPILED_ARM_DIRS: dict[str, str] = {
    "contract_compiled_opus": "contracts-opus",
    "contract_compiled_opus_joins": "contracts-opus-joins",
}


def compiled_contract_path(arm: str = "contract_compiled") -> Path:
    # Read at call time, so a test can point COMPILED_CONTRACT_PATH elsewhere.
    if arm == "contract_compiled":
        return COMPILED_CONTRACT_PATH
    return (
        Path(__file__).parent.parent
        / COMPILED_ARM_DIRS[arm]
        / "dabstep"
        / "contract.yml"
    )


def load_compiled_contract(arm: str = "contract_compiled") -> DataContract:
    return load_contract_at(compiled_contract_path(arm))


def compiled_digest(arm: str = "contract_compiled") -> str:
    return contract_digest(load_compiled_contract(arm))
