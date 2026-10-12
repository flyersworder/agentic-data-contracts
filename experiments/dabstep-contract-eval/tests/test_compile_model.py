"""The compiler model is a compile option (`--model`), recorded in the log
and the contract header; the default is the frozen compiler's."""

import dataclasses
import json

import pytest
from dce import pricing
from dce.benchmarks import livesqlbench
from dce.compile import __main__ as cli
from dce.compile import run as run_module
from dce.compile.emit import build, write
from dce.compile.inputs import COMPILER_MODEL


def test_the_default_is_the_frozen_compiler(lsb_data):
    assert COMPILER_MODEL == "claudesonnet5"
    assert livesqlbench.compile_inputs(lsb_data, "shop").model == COMPILER_MODEL


def test_opus_5_5_is_priced_at_the_gateways_rates():
    spec = pricing.MODELS["claudeopus5.5"]
    assert (spec.price_in, spec.price_out, spec.price_cached) == (4.0, 20.0, 0.2)
    assert spec.route == "litellm_anthropic"
    assert pricing.cost("claudeopus5.5", 1_000_000, 0) == pytest.approx(4.0)


def test_the_header_and_contract_name_the_model(lsb_data, tmp_path):
    inputs = dataclasses.replace(
        livesqlbench.compile_inputs(lsb_data, "shop"), model="claudeopus5.5"
    )
    assert run_module._header(inputs)["model"] == "claudeopus5.5"
    write(tmp_path, *build(inputs, [], {}), inputs)
    head = (tmp_path / "semantic.yml").read_text(encoding="utf-8")
    assert "compiler: claudeopus5.5" in head


def test_a_resume_cannot_switch_the_model(lsb_data, tmp_path):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    header = run_module._header(dataclasses.replace(inputs, model="claudeopus5.5"))
    out = tmp_path / "shop"
    out.mkdir()
    (out / run_module.LOG).write_text(json.dumps(header) + "\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="claudeopus5.5"):
        run_module.compile_database(inputs, out, lambda **_: None, spend_cap=1.0)


def test_the_cli_passes_the_model_to_inputs_and_driver(monkeypatch, tmp_path):
    seen = {}

    def fake_compile(inputs, out_dir, driver, *, spend_cap, spent):
        seen["inputs"], seen["driver"] = inputs, driver
        return "done", spent

    monkeypatch.setattr(run_module, "compile_database", fake_compile)
    cli.main(
        [
            "--benchmark",
            "dabstep",
            "--model",
            "claudeopus5.5",
            "--out",
            str(tmp_path),
            "--max-spend",
            "1",
        ]
    )
    assert seen["inputs"].model == "claudeopus5.5"
    assert seen["driver"].keywords == {"model": "claudeopus5.5"}


def test_the_cli_refuses_an_unpriced_model(tmp_path):
    with pytest.raises(SystemExit):
        cli.main(
            [
                "--benchmark",
                "dabstep",
                "--model",
                "nope",
                "--out",
                str(tmp_path),
                "--max-spend",
                "1",
            ]
        )
