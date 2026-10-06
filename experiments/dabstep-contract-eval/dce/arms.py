"""DABStep's arms. The machinery every benchmark shares -- the bounded
adapter, the tool sets, the working-copy discipline and its CALL ORDER --
lives in `dce.tools`; read its module docstring before changing anything an
arm does with the database.
"""

from __future__ import annotations

from pathlib import Path

from dce.frozen import (
    load_contract,
    load_hollow_contract,
    load_uninterpreted_contract,
)
from dce.tools import (  # noqa: F401 - re-exported for existing importers
    HARNESS_MEMORY_LIMIT,
    HARNESS_QUERY_SECONDS,
    MAX_ROWS,
    ArmSetup,
    IntegrityCheck,
    _BoundedDuckDBAdapter,
    _governed_tools,
    _ungoverned_tools,
    check_and_restore,
    make_working_copy,
)

ARMS: tuple[str, ...] = (
    "schema_only",
    "manual_prompt",
    "contract",
    "contract_hollow",
)

#: Arms run on purpose, one model at a time, and never by default. Kept out of
#: `ARMS` so that a sweep without `--arms`, and every report on the four-arm
#: ablation, stay exactly what they were.
#:
#: `contract_uninterpreted` is the fifth arm the paper's Threats section names:
#: the contract with its `INTERPRETATION` clauses stripped and the empty-list
#: wildcard reverted to the manual's reading (`dce.uninterpreted`).
#:
#: `manual_resolved` is `manual_prompt` plus `DATA_NOTE`, the one fact the
#: contract's author took from the data rather than from the manual. It gives
#: both arms the same knowledge, so what separates it from `contract` is
#: delivery alone.
EXTRA_ARMS: tuple[str, ...] = ("contract_uninterpreted", "manual_resolved")
ALL_ARMS: tuple[str, ...] = ARMS + EXTRA_ARMS

#: What the contract knows that the manual does not. The manual says a null
#: field applies to all values; the annexed `fees` data never uses null for its
#: list-typed fields and stores "all" as an empty list instead (720 of 1,000
#: rules for `account_type`). The contract states this as an INTERPRETATION of
#: `fee_rule_matches_transaction` and builds it into that metric's SQL. The note
#: carries the fact only, once, in words: writing the SQL for it is part of what
#: the contract delivers, and repeating it is a treatment of its own (see
#: `dce.uninterpreted`). Fixed before the arm's first run; never reworded after.
DATA_NOTE = (
    "In the `fees` table, the list-typed fields `account_type`, `aci` and "
    "`merchant_category_code` are never null. A rule that applies to all values "
    "of such a field stores an empty list instead. Treat an empty list the way "
    "the manual treats null: the rule applies to all values of that field."
)

#: Every arm built through `_governed_tools`, and so carrying a contract, a
#: session and the governed-tool counters.
GOVERNED_ARMS: frozenset[str] = frozenset(
    {"contract", "contract_hollow", "contract_uninterpreted"}
)

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "Your final message must be the answer alone, formatted exactly as the "
    "question's guidelines require. Do not show working in the final message."
)


def build_arm(arm: str, db_path: Path, docs: dict[str, str]) -> ArmSetup:
    if arm == "schema_only":
        return ArmSetup(BASE_PROMPT, _ungoverned_tools(db_path), None)

    if arm in ("manual_prompt", "manual_resolved"):
        prompt = (
            f"{BASE_PROMPT}\n\n## Domain manual\n\n{docs['manual']}\n\n"
            f"## Payments table reference\n\n{docs['payments_readme']}"
        )
        if arm == "manual_resolved":
            prompt += f"\n\n## Data note\n\n{DATA_NOTE}"
        return ArmSetup(prompt, _ungoverned_tools(db_path), None)

    if arm in GOVERNED_ARMS:
        # IDENTICAL EXCEPT FOR THE CONTRACT OBJECT. The procedural sentence
        # below is the thing `contract_hollow` exists to control for, so it is
        # written once and shared rather than copied — a divergence here would
        # silently reintroduce the confound the control was built to remove.
        contract = {
            "contract": load_contract,
            "contract_hollow": load_hollow_contract,
            "contract_uninterpreted": load_uninterpreted_contract,
        }[arm]()
        tools, session, adapter = _governed_tools(db_path, contract=contract)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{contract.to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session, adapter)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ALL_ARMS}")
