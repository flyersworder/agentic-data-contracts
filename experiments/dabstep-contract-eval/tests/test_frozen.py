from dce.frozen import digest, load_contract

from agentic_data_contracts import DataContract


def test_contract_loads():
    contract = load_contract()
    assert isinstance(contract, DataContract)


def test_digest_is_stable_across_calls():
    assert digest() == digest()


def test_contract_declares_the_five_dabstep_tables():
    contract = load_contract()
    rendered = contract.to_system_prompt()
    for table in (
        "payments",
        "fees",
        "merchant_data",
        "acquirer_countries",
        "merchant_category_codes",
    ):
        assert table in rendered


def test_contract_defines_at_least_one_metric():
    # A contract with no metrics would make arm C a schema-only arm wearing a
    # contract's clothes, and the whole comparison meaningless.
    #
    # The brief spelled this `contract.semantic_source.list_metrics()`. This
    # library version exposes the source through `load_semantic_source()` and
    # the metrics through the `SemanticSource` protocol's `get_metrics()`; the
    # assertion is the brief's, only the accessor names are the library's.
    contract = load_contract()
    source = contract.load_semantic_source()
    assert source is not None
    assert len(source.get_metrics()) >= 1


# The digest of the contract as frozen on 2026-08-30, after the fourth and last
# authoring pass. The fourth was forced from outside the contract: `dce/data.py`
# had been ingesting the annex CSVs' header row as data, so
# `merchant_category_codes` and `acquirer_countries` loaded with positional
# column names. The loader was fixed to match DABStep's official environment —
# the one our gold answers were scored in — the two tables gained real column
# names, and the contract had to stop describing columns that no longer exist.
# No part of the encoding was reconsidered in that pass.
#
# Every scored result row carries this value; a result whose
# digest differs was produced against a different contract and is not comparable
# with the rest of the sweep.
#
# This constant is the enforcement behind the freeze. Without it the suite only
# asserts that the digest is *stable within one process*, which is true of any
# contract, including one edited after the fact — detection would depend on
# somebody reading git history, which nobody does.
#
# If this test fails, the contract changed. That is not a formatting problem and
# updating the constant is not the fix: if any run has been scored, the edit
# invalidates it, and the honest options are to revert the contract or to re-run
# every arm. Change this literal only as a deliberate decision, made before any
# scoring, and say in the commit message what moved and why.
#
# Comments and YAML formatting are outside the digest — it is taken over the
# parsed contract with the semantic source frozen inline — so prose the agent
# actually reads (domain and metric descriptions, table and column
# descriptions) does move it, while a comment-only edit does not.
FROZEN_DIGEST = (
    "sha256:e438ecf7964bb051ce9cd81767006d8dc1cfe4214a2d2fefa8a5b73b554e227e"
)


def test_digest_matches_the_frozen_value():
    assert digest() == FROZEN_DIGEST
