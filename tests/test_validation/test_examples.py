from agentic_data_contracts.validation.examples import VerifiedExample


def test_from_dict_maps_known_fields() -> None:
    ex = VerifiedExample.from_dict(
        {"id": "wau", "question": "weekly active users", "sql": "SELECT 1"}
    )
    assert ex.id == "wau"
    assert ex.question == "weekly active users"
    assert ex.sql == "SELECT 1"


def test_from_dict_stashes_unknown_keys_in_metadata() -> None:
    ex = VerifiedExample.from_dict(
        {"sql": "SELECT 1", "verified_by": "jsmith", "type": "sql"}
    )
    assert ex.metadata["verified_by"] == "jsmith"
    assert ex.metadata["type"] == "sql"


def test_from_dict_merges_explicit_metadata() -> None:
    ex = VerifiedExample.from_dict({"sql": "SELECT 1", "metadata": {"a": 1}, "b": 2})
    assert ex.metadata == {"a": 1, "b": 2}


def test_from_dict_requires_sql() -> None:
    import pytest

    with pytest.raises(ValueError, match="sql"):
        VerifiedExample.from_dict({"question": "no sql here"})
