"""The vendored LiveSQLBench helpers are upstream's, byte for byte."""

import ast
import hashlib
from pathlib import Path

VENDOR = Path(__file__).resolve().parents[1] / "vendor" / "livesqlbench_test_utils.py"

UPSTREAM_COMMIT = "5aab9623d6ce58d32e252f8c307f08fbcbdf4a70"
UPSTREAM_SHA256 = {
    "process_decimals_recursive": "a8d6369d031af19ab83abbf264a4a252ef8af68f460c50192a264451a10a458a",  # noqa: E501
    "preprocess_results": "eac3ffe7926d70c7aa5c487daa0bad38e49350c6edab85c8824eb0b2cc4ba8f1",  # noqa: E501
    "remove_distinct": "5d69bd22fd6611da54200d0a9d5aee4747e9a7b882fe238a86b1cdde7df7deeb",  # noqa: E501
    "remove_comments": "0fd625600ab4338141caf97eace39525ce1687b224650b7b58be2fd183c02ed3",  # noqa: E501
    "remove_round_functions": "8379355bbc5cea56697a31203d08bafb8b574b1894fc63985927e188f3dcfc6f",  # noqa: E501
    "remove_round": "7ad9b6e73963e0e276dc6bbb573fa33a64ce98d94eb1e69fb5e57578de93075e",  # noqa: E501
}


def test_every_vendored_function_is_upstream_byte_for_byte():
    src = VENDOR.read_text(encoding="utf-8")
    found = {
        node.name: hashlib.sha256(
            ast.get_source_segment(src, node).encode("utf-8")
        ).hexdigest()
        for node in ast.parse(src).body
        if isinstance(node, ast.FunctionDef)
    }
    assert found == UPSTREAM_SHA256


def test_the_header_names_the_commit_and_the_licence():
    head = VENDOR.read_text(encoding="utf-8")[:2000]
    assert UPSTREAM_COMMIT in head
    assert "MIT License" in head
