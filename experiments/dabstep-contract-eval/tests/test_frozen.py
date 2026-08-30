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
