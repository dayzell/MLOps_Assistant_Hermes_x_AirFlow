"""airflow_get_dag_source: v1/v2 API, local-folder preference, windows, masking, safety.

Motivated by a real Hermes session: without the source, the agent guessed the code and its
first two diffs did not apply in the sandbox.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import payloads as p
import pytest
import respx
from airflow_mlops.dag_source import MAX_SOURCE_LINES, locate_local_file
from airflow_mlops.sandbox import apply_unified_diff
from harness import FakeContext, make_context

SOURCE = (
    "from airflow.decorators import dag, task\n"
    "\n"
    "\n"
    "@dag(schedule=None)\n"
    "def train():\n"
    "    @task\n"
    "    def load() -> dict:\n"
    '        return {"features": [1, 2, 3]}\n'
    "\n"
    "    @task\n"
    "    def fit(frame: dict) -> None:\n"
    '        print(frame["target"])\n'
    "\n"
    "    fit(load())\n"
)
TOKEN = "Ii9vcHQvYWlyZmxvdy9kYWdzL3RyYWluLnB5Ig.abc"


def mock_source(airflow: respx.MockRouter, api_version: str, text: str = SOURCE) -> None:
    details = p.dag_details(api_version, "train")
    details["fileloc"] = "/opt/airflow/dags/team/train.py"
    if api_version == "v2":
        details["relative_fileloc"] = "team/train.py"
    airflow.get("/dags/train/details").respond(json=details)
    if api_version == "v1":
        airflow.get("/dags/train").respond(json={**details, "file_token": TOKEN})
        airflow.get(f"/dagSources/{TOKEN}").respond(json={"content": text})
    else:
        airflow.get("/dagSources/train").respond(
            json={"content": text, "dag_id": "train", "version_number": 1}
        )


def local_dags(tmp_path: Path, text: str = SOURCE) -> Path:
    root = tmp_path / "dags"
    (root / "team").mkdir(parents=True)
    (root / "team" / "train.py").write_text(text, encoding="utf-8")
    return root


# --------------------------------------------------------------------------- API origin


def test_reads_deployed_source_from_airflow(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_source(airflow, api_version)
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["ok"] is True, result
    assert result["origin"] == "airflow_api"
    assert result["content"] == SOURCE.rstrip("\n")
    assert result["total_lines"] == 14
    assert (result["first_line"], result["last_line"]) == (1, 14)
    expected_path = "team/train.py" if api_version == "v2" else "train.py"
    assert result["path"] == expected_path
    assert f"--- a/{expected_path}" in result["diff_guidance"]
    assert "differs_from_deployed" not in result


def test_line_window(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    mock_source(airflow, api_version)
    result = ctx.call("airflow_get_dag_source", dag_id="train", start_line=10, end_line=12)
    assert result["content"].splitlines() == [
        "    @task",
        "    def fit(frame: dict) -> None:",
        '        print(frame["target"])',
    ]
    assert (result["first_line"], result["last_line"]) == (10, 12)
    bad = ctx.call("airflow_get_dag_source", dag_id="train", start_line=99)
    assert bad["error"]["type"] == "invalid_argument"


def test_long_file_is_truncated_with_a_continuation_hint(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    long_source = "\n".join(f"x_{i} = {i}" for i in range(MAX_SOURCE_LINES + 50))
    mock_source(airflow, api_version, long_source)
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["truncated"] is True
    assert result["last_line"] == MAX_SOURCE_LINES
    assert f"start_line={MAX_SOURCE_LINES + 1}" in result["next"]


def test_secrets_in_code_are_masked(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_source(airflow, api_version, SOURCE + 'DB_URI = "postgresql://etl:pa55word@db/raw"\n')
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert "pa55word" not in json.dumps(result)
    assert result["redacted"] is True


def test_dag_outside_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", dag_allowlist=["etl_*"])
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["error"]["type"] == "dag_not_allowed"
    assert len(airflow.calls) == 0


@pytest.mark.parametrize("api_version", ["v1"])
def test_v1_without_file_token_is_not_found(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version="v1", auth_mode="basic")
    airflow.get("/dags/train/details").respond(json=p.dag_details("v1", "train"))
    airflow.get("/dags/train").respond(json=p.dag("v1", "train"))
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["error"]["type"] == "not_found"


# --------------------------------------------------------------------------- local origin


def test_prefers_the_local_dags_folder(
    api_version: str, airflow: respx.MockRouter, tmp_path: Path
) -> None:
    root = local_dags(tmp_path)
    ctx = make_context(api_version=api_version, auth_mode="basic", sandbox_dags_path=str(root))
    mock_source(airflow, api_version)
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["origin"] == "local_dags_folder"
    assert result["path"] == "team/train.py"
    assert result["differs_from_deployed"] is False
    assert "warning" not in result


def test_warns_when_local_code_differs_from_deployed(
    api_version: str, airflow: respx.MockRouter, tmp_path: Path
) -> None:
    root = local_dags(tmp_path, SOURCE.replace('frame["target"]', 'frame.get("target")'))
    ctx = make_context(api_version=api_version, auth_mode="basic", sandbox_dags_path=str(root))
    mock_source(airflow, api_version)
    result = ctx.call("airflow_get_dag_source", dag_id="train")
    assert result["differs_from_deployed"] is True
    assert "differs from the code Airflow currently runs" in result["warning"]


def test_locate_local_file_never_leaves_the_root(tmp_path: Path) -> None:
    root = local_dags(tmp_path)
    (tmp_path / "secret.py").write_text("x", encoding="utf-8")
    assert (
        locate_local_file(root, "/opt/airflow/dags/team/train.py", None)
        == (root / "team" / "train.py").resolve()
    )
    assert locate_local_file(root, None, "team/train.py") is not None
    assert locate_local_file(root, "/opt/airflow/../secret.py", None) is None
    assert locate_local_file(root, "/somewhere/else/missing.py", None) is None


# --------------------------------------------------------------------------- the point of it


def test_a_diff_built_from_the_returned_source_applies(
    api_version: str, airflow: respx.MockRouter, tmp_path: Path
) -> None:
    """End-to-end intent: read the code, write the diff from 'content', the sandbox's
    patcher accepts it (this is what failed twice in the real session)."""
    root = local_dags(tmp_path)
    ctx = make_context(api_version=api_version, auth_mode="basic", sandbox_dags_path=str(root))
    mock_source(airflow, api_version)
    src: dict[str, Any] = ctx.call(
        "airflow_get_dag_source", dag_id="train", start_line=11, end_line=12
    )
    old = src["content"].splitlines()
    patch = (
        f"--- a/{src['path']}\n+++ b/{src['path']}\n@@ -{src['first_line']},2 "
        f"+{src['first_line']},2 @@\n {old[0]}\n-{old[1]}\n+        print(frame.get('target'))\n"
    )
    copy = tmp_path / "copy"
    (copy / "team").mkdir(parents=True)
    (copy / "team" / "train.py").write_text(SOURCE, encoding="utf-8")
    assert apply_unified_diff(copy, patch) == ["team/train.py"]
    assert "frame.get('target')" in (copy / "team" / "train.py").read_text(encoding="utf-8")
