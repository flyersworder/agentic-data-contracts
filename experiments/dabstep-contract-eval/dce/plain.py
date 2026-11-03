"""Derive the *plain* contract: the two DABStep facts written into the contract.

WHY THIS EXISTS (experiment 5e). Experiment 5d pasted the two notes
(`DATA_NOTE`, `SUM_NOTE`) into both arms' prompts, so neither arm had to reach
the deciding facts through the contract, and the route test measured the
governed delivery of the rest of it. Here the facts sit inside the contract,
at the point where each is used, in the notes' exact words, and in no prompt:
`contract_plain` looks the contract up through the governed tools,
`contract_plain_text` pastes its two files.

TWO EDITS, BY TEXT SUBSTITUTION. Not a YAML round-trip, which would drop the
comments and reflow every block scalar, so that the pasted arm would differ
from `contract_text` far beyond the two edits. Each anchor is matched across
the folded-scalar line breaks and must occur exactly once:

  E1  `contract.yml`, the fees domain's MATCHING RULE paragraph: `DATA_NOTE`
      inserted after the sentence stating the manual's null rule ("... a null
      `aci` means the rule applies for every ACI.").
  E2  `semantic.yml`, `total_transaction_fees`' description: its first
      sentence replaced by `SUM_NOTE`. The next sentence and the SQL stay.

The inserted text is wrapped at the block's own indent, never at a hyphen (a
folded line break reads as a space), so the parsed descriptions read as the
original with the note in place. The contract keeps its name: a suffix would
show in the governed prompt. No generated-file header either: the pasted arm
shows these files verbatim. `tests/test_plain.py` asserts the diff is exactly
the two edits.
"""

from __future__ import annotations

import re
import textwrap
from functools import lru_cache
from pathlib import Path

from agentic_data_contracts import DataContract, contract_digest

SOURCE_DIR = Path(__file__).parent.parent / "contract"
PLAIN_DIR = Path(__file__).parent.parent / "contract_plain"
PLAIN_CONTRACT_PATH = PLAIN_DIR / "contract.yml"

#: E1's anchor: the end of the sentence stating the manual's null rule.
NULL_RULE = "a null `aci` means the rule applies for every ACI."
#: E2's anchor: the first sentence of `total_transaction_fees`' description.
FIRST_SUM_SENTENCE = (
    "Total fees over a set of transactions: the per-transaction fee summed "
    "after each transaction has been joined to the fee rule that matches it."
)
WIDTH = 80


def _anchor(text: str) -> re.Pattern[str]:
    """`text` with each run of whitespace matching any whitespace, so a
    sentence matches across a folded scalar's line breaks."""
    return re.compile(r"\s+".join(re.escape(word) for word in text.split()))


def _line_start(raw: str, pos: int) -> int:
    return raw.rfind("\n", 0, pos) + 1


def splice(raw: str, anchor: str, new: str, *, keep: bool) -> str:
    """Insert `new` after `anchor` (`keep=True`) or replace `anchor` with it,
    in a block scalar's raw text. `anchor` must occur exactly once. The new
    text is wrapped on lines of its own at the indent of the line the anchor
    starts on; the text before and after it on those lines stays, each on
    its own line at the same indent."""
    matches = list(_anchor(anchor).finditer(raw))
    if len(matches) != 1:
        raise ValueError(
            f"anchor must occur exactly once, found {len(matches)}: {anchor!r}"
        )
    match = matches[0]
    first = raw[_line_start(raw, match.start()) :]
    indent = first[: len(first) - len(first.lstrip(" "))]
    cut, resume = (match.end(), match.end()) if keep else match.span()

    start = _line_start(raw, cut)
    head = raw[:start] if not raw[start:cut].strip() else raw[:cut].rstrip(" ") + "\n"
    end = raw.find("\n", resume)
    end = len(raw) if end < 0 else end
    rest = raw[resume:end]
    tail = raw[end:] if not rest.strip() else "\n" + indent + raw[resume:].lstrip(" ")

    body = textwrap.fill(
        " ".join(new.split()),
        width=WIDTH,
        initial_indent=indent,
        subsequent_indent=indent,
        break_on_hyphens=False,
        break_long_words=False,
    )
    return head + body + tail


def plain_contract(raw: str, data_note: str) -> str:
    """E1 on `contract.yml`'s raw text."""
    return splice(raw, NULL_RULE, data_note, keep=True)


def plain_semantic(raw: str, sum_note: str) -> str:
    """E2 on `semantic.yml`'s raw text."""
    return splice(raw, FIRST_SUM_SENTENCE, sum_note, keep=False)


def build(source_dir: Path = SOURCE_DIR, out_dir: Path = PLAIN_DIR) -> Path:
    """Write the plain contract beside the real one. Idempotent."""
    from dce.benchmarks.dabstep import DATA_NOTE, SUM_NOTE

    out_dir.mkdir(parents=True, exist_ok=True)
    for name, edit, note in (
        ("contract.yml", plain_contract, DATA_NOTE),
        ("semantic.yml", plain_semantic, SUM_NOTE),
    ):
        raw = (source_dir / name).read_text(encoding="utf-8")
        (out_dir / name).write_text(edit(raw, note), encoding="utf-8")
    return out_dir


@lru_cache(maxsize=1)
def load_plain_contract() -> DataContract:
    return DataContract.from_yaml(PLAIN_CONTRACT_PATH)


def plain_digest() -> str:
    """The digest `contract_plain` and `contract_plain_text` rows carry: the
    derived contract's own, not the original's."""
    return contract_digest(load_plain_contract())


if __name__ == "__main__":
    print(build())
