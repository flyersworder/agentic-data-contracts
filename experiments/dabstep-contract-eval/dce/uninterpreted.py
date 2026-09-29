"""Derive the *uninterpreted* contract: arm C minus its own reading of the manual.

WHY THIS EXISTS. The contract arm is matched to `manual_prompt` on the sources
it may consult, not on how those sources read. Seven metric descriptions carry
an `INTERPRETATION` sentence, where writing the contract forced a call the
manual does not make. Four restate or apply what the manual supplies; three go
further. The furthest-reaching of those is the empty list: the manual says a
NULL field "applies to all possible values", but the annexed `fees` data
encodes that wildcard for list-typed fields as an empty list, which the manual
never mentions. The contract resolves it -- in prose, and in the SQL of
`fee_rule_matches_transaction` -- so the contract arm's margin over
`manual_prompt` combines DELIVERY of what the manual states with RESOLUTION of
what it leaves open. This arm keeps the delivery and removes the resolution:

  * uninterpreted ~ contract       -> delivery carries the margin.
  * uninterpreted ~ manual_prompt  -> resolution does, and the contract's lead
                                      is the author's reading, not its format.

WHAT IS REMOVED, EXACTLY.
  1. Every `INTERPRETATION:` sentence, from the marker to the end of its
     metric description. The manual's own words before it stay.
  2. The wildcard sentence of the three list-typed `fees` column
     descriptions ("Empty or null means all ..."), dropped whole. The manual
     states its null rule once, in its Notes, and never per column; the
     contract keeps that statement where the manual has it, in the fees
     domain text and in `fee_rule_matches_transaction`'s description.
  3. The SQL of `fee_rule_matches_transaction`, emptied as `dce.hollow`
     empties every metric's, and the fees domain's sentence saying that
     metric "states this predicate as SQL" reworded to match. Its description
     -- the manual's own statement of the rule -- stays.

WHY 3 EMPTIES THE SQL RATHER THAN REWRITING IT. The first version of this arm
(commit fb2579c) reverted the SQL to the manual's literal reading, dropping
only the `OR len(f.<field>) = 0` disjuncts. That is not an unresolved
contract; it is a contract asserting a wrong rule. The list-typed fields are
never NULL in the annexed data, so the literal predicate matches almost no
rule, and gpt-6-sol trusted it: 1 of 37 hard fee tasks right, against 15 for
`schema_only`, which is told nothing. Any SQL for this predicate must decide
the empty list one way or the other, so there is no neutral version to keep.
Emptying it leaves the agent where `manual_prompt` stands -- the manual's rule
in words, the data to work out how it is encoded -- while keeping every other
thing the contract delivers.

WHY 2 DROPS THE SENTENCE RATHER THAN REWORDING IT. The second version
(commit 7785733) reworded it to "Null means all ...", the manual's rule in
the manual's sense. Its first run showed why that is not neutral either:
`describe_table` puts the sentence beside the data, three times, and
gpt-6-sol looked at the list columns on 85 of 97 hard fee tasks, saw the
empty lists and concluded that no rule applies on 83 -- against 9 for
`manual_prompt`, which reads the same rule once, in the manual's Notes. The
arm would have measured where the rule is repeated, not whether it is
resolved.

WHAT IS KEPT, AND WHY. The two band tie-breaks lose their sentences but keep
their SQL: a CASE must return one band for a boundary value, so there is no
neutral version to revert to. The other four interpretations' SQL restates the
manual and stays for the same reason the sentences went -- the code is the
manual's rule, the sentence was the author's commentary on it.

DERIVED, NOT AUTHORED, as `dce.hollow` is: every field this module does not
touch is byte-identical to the contract under test, and
`tests/test_uninterpreted.py` asserts the diff is exactly the above.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

SOURCE_DIR = Path(__file__).parent.parent / "contract"
UNINTERPRETED_DIR = Path(__file__).parent.parent / "contract_uninterpreted"

MARKER = "INTERPRETATION:"
#: The one metric whose SQL cannot be written without deciding the empty list.
UNRESOLVABLE_METRIC = "fee_rule_matches_transaction"
_STATES_AS_SQL = re.compile(
    r"(`fee_rule_matches_transaction` )states this predicate\s+as SQL"
)
_LIST_FIELDS = ("aci", "account_type", "merchant_category_code")
_WILDCARD_SENTENCE = re.compile(r"\s*Empty or null means all [^.]*\.")


def uninterpret_contract(raw: dict[str, Any]) -> dict[str, Any]:
    raw["name"] = f"{raw['name']}-uninterpreted"
    for domain in raw.get("semantic", {}).get("domains", []):
        if "description" in domain:
            domain["description"] = _STATES_AS_SQL.sub(
                r"\1names this predicate", domain["description"]
            )
    return raw


def uninterpret_semantic(raw: dict[str, Any]) -> dict[str, Any]:
    for metric in raw.get("metrics", []):
        desc = metric.get("description", "")
        if MARKER in desc:
            metric["description"] = desc[: desc.index(MARKER)].rstrip() + "\n"
        if metric.get("name") == UNRESOLVABLE_METRIC:
            metric["sql_expression"] = ""
    for table in raw.get("tables", []):
        if table.get("table") != "fees":
            continue
        for column in table.get("columns", []):
            if column.get("name") in _LIST_FIELDS and "description" in column:
                column["description"] = _WILDCARD_SENTENCE.sub(
                    "", column["description"]
                )
    return raw


def build(source_dir: Path = SOURCE_DIR, out_dir: Path = UNINTERPRETED_DIR) -> Path:
    """Write the uninterpreted contract beside the real one. Idempotent."""
    out_dir.mkdir(parents=True, exist_ok=True)
    contract = yaml.safe_load((source_dir / "contract.yml").read_text(encoding="utf-8"))
    semantic = yaml.safe_load((source_dir / "semantic.yml").read_text(encoding="utf-8"))
    header = (
        "# GENERATED by dce/uninterpreted.py from ../contract/ -- do not edit.\n"
        "# Arm C with its INTERPRETATION clauses removed and the empty-list\n"
        "# wildcard reverted to the manual's null-only reading.\n"
    )
    (out_dir / "contract.yml").write_text(
        header + yaml.safe_dump(uninterpret_contract(contract), sort_keys=False),
        encoding="utf-8",
    )
    (out_dir / "semantic.yml").write_text(
        header + yaml.safe_dump(uninterpret_semantic(semantic), sort_keys=False),
        encoding="utf-8",
    )
    return out_dir


if __name__ == "__main__":
    print(build())
