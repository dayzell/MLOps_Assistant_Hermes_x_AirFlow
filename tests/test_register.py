"""Registration contract: manifest <-> register() parity, schema shape, no I/O at load."""

from __future__ import annotations

from pathlib import Path

import respx
import yaml
from harness import ROOT, make_context

MANIFEST = yaml.safe_load((ROOT / "plugin.yaml").read_text(encoding="utf-8"))


def test_manifest_declares_exactly_the_registered_tools() -> None:
    ctx = make_context()
    assert sorted(ctx.tools) == sorted(MANIFEST["provides_tools"])
    assert sorted(ctx.hooks) == sorted(MANIFEST.get("provides_hooks") or [])


def test_tools_use_the_airflow_toolset_and_prefix() -> None:
    ctx = make_context()
    for name, tool in ctx.tools.items():
        assert name.startswith("airflow_")
        assert tool["toolset"] == "airflow"


def test_schemas_are_well_formed() -> None:
    ctx = make_context()
    for name, tool in ctx.tools.items():
        schema = tool["schema"]
        assert schema["name"] == name
        assert len(schema["description"]) > 40
        params = schema["parameters"]
        assert params["type"] == "object"
        assert set(params["required"]) <= set(params["properties"])


def test_register_performs_no_network_call() -> None:
    with respx.mock(assert_all_mocked=True) as router:
        make_context()
        assert router.calls.call_count == 0


def test_manifest_requires_env_rich_format() -> None:
    entries = {e["name"]: e for e in MANIFEST["requires_env"]}
    assert set(entries) == {"AIRFLOW_USERNAME", "AIRFLOW_PASSWORD", "AIRFLOW_TOKEN"}
    assert entries["AIRFLOW_PASSWORD"]["secret"] is True
    assert entries["AIRFLOW_TOKEN"]["secret"] is True


def test_config_schema_defaults_match_code_defaults() -> None:
    from airflow_mlops.config import DEFAULTS

    schema = MANIFEST["config_schema"]
    assert set(schema) == set(DEFAULTS)
    for key, spec in schema.items():
        assert spec["default"] == DEFAULTS[key], key


def test_plugin_never_reads_env_files() -> None:
    for path in Path(ROOT).glob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "os.environ" not in source, path
        assert "auth.json" not in source, path
        assert ".env" not in source.replace(".environ", ""), path
