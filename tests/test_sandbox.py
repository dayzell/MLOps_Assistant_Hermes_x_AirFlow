"""Sandbox: diff application, workspace isolation, bounded docker command, result parsing,
guardrails (disabled, heavy operators, budget, lock), approval hook and audit journal.

Docker is never started: ``dispatch_tool("terminal")`` is faked and inspects the workspace
that the real command would mount.
"""

from __future__ import annotations

import importlib.util
import json
import shlex
import shutil
from pathlib import Path
from typing import Any

import payloads as p
import pytest
import respx
from airflow_mlops import storage
from airflow_mlops.config import Settings
from airflow_mlops.guardrails import make_pre_tool_call_hook
from airflow_mlops.sandbox import (
    SandboxError,
    apply_unified_diff,
    build_docker_command,
    copy_dags_folder,
    parse_terminal_result,
)
from harness import ROOT, FakeContext, make_context

BUGGY = 'import os\n\n\ndef train(frame):\n    return frame["target"]\n\n\nVALUE = 1\n'
PATCH = """--- a/train.py
+++ b/train.py
@@ -40,2 +40,2 @@
 def train(frame):
-    return frame["target"]
+    return frame.get("target")
"""


def sentinel(passed: bool = True, **stages: Any) -> str:
    body = {"passed": passed, "stages": stages or {"import": {"passed": passed}}}
    return "booting...\nSANDBOX_RESULT " + json.dumps(body)


def terminal(output: str, exit_code: int = 0, err: str | None = None) -> str:
    return json.dumps({"output": output, "exit_code": exit_code, "error": err})


def mounted_workspace(command: str) -> Path:
    argv = shlex.split(command.removeprefix("MSYS_NO_PATHCONV=1 "))
    volume = argv[argv.index("-v") + 1]
    return Path(volume.rsplit(":/sandbox:ro", 1)[0])


@pytest.fixture
def dags(tmp_path: Path) -> Path:
    folder = tmp_path / "my-dags"
    (folder / "lib").mkdir(parents=True)
    (folder / "train.py").write_text(BUGGY, encoding="utf-8")
    (folder / "lib" / "helpers.py").write_text("X = 1\n", encoding="utf-8")
    (folder / ".git").mkdir()
    (folder / ".git" / "config").write_text("secret", encoding="utf-8")
    return folder


def sandbox_ctx(dags: Path, **config: Any) -> FakeContext:
    settings = {
        "api_version": "v2",
        "auth_mode": "jwt",
        "sandbox_enabled": True,
        "sandbox_image": "my/airflow:3.1-deps",
        "sandbox_dags_path": str(dags),
        **config,
    }
    return make_context(**settings)


@pytest.fixture
def api() -> Any:
    with respx.mock(base_url="http://airflow.test/api/v2", assert_all_called=False) as router:
        router.post("http://airflow.test/auth/token").respond(201, json={"access_token": "j"})
        router.get("/dags/train/tasks").respond(json=p.tasks("v2"))
        yield router


# --------------------------------------------------------------------------- unified diff


def test_patch_applies_despite_wrong_line_numbers(tmp_path: Path) -> None:
    (tmp_path / "train.py").write_text(BUGGY, encoding="utf-8")
    assert apply_unified_diff(tmp_path, PATCH) == ["train.py"]
    assert 'frame.get("target")' in (tmp_path / "train.py").read_text(encoding="utf-8")


def test_patch_tolerates_extra_leading_folder(tmp_path: Path) -> None:
    (tmp_path / "train.py").write_text(BUGGY, encoding="utf-8")
    apply_unified_diff(
        tmp_path,
        PATCH.replace("a/train.py", "a/dags/train.py").replace("b/train.py", "b/dags/train.py"),
    )
    assert 'frame.get("target")' in (tmp_path / "train.py").read_text(encoding="utf-8")


def test_patch_multiple_hunks_new_file_and_crlf(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_bytes(b"one\r\ntwo\r\nthree\r\nfour\r\n")
    patch = (
        "--- a/a.py\n+++ b/a.py\n@@ -1,1 +1,1 @@\n-one\n+ONE\n@@ -4,1 +4,1 @@\n-four\n+FOUR\n"
        "--- /dev/null\n+++ b/new/util.py\n@@ -0,0 +1,2 @@\n+A = 1\n+B = 2\n"
    )
    assert apply_unified_diff(tmp_path, patch) == ["a.py", "new/util.py"]
    assert (tmp_path / "a.py").read_bytes() == b"ONE\r\ntwo\r\nthree\r\nFOUR\r\n"
    assert (tmp_path / "new" / "util.py").read_text(encoding="utf-8") == "A = 1\nB = 2\n"


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ("--- a/train.py\n+++ b/train.py\n@@ -1 +1 @@\n-not there\n+x\n", "does not apply"),
        ("--- a/train.py\n+++ /dev/null\n@@ -1 +0,0 @@\n-import os\n", "deletes"),
        ("--- a/../evil.py\n+++ b/../evil.py\n@@ -0,0 +1 @@\n+x\n", "outside"),
        ("--- /dev/null\n+++ /etc/cron.d/x\n@@ -0,0 +1 @@\n+x\n", "outside"),
        ("--- a/missing.py\n+++ b/missing.py\n@@ -1 +1 @@\n-a\n+b\n", "does not exist"),
        ("just some text", "no file change"),
    ],
)
def test_bad_patches_are_refused(tmp_path: Path, patch: str, message: str) -> None:
    (tmp_path / "train.py").write_text(BUGGY, encoding="utf-8")
    with pytest.raises(SandboxError, match=message):
        apply_unified_diff(tmp_path, patch)


def test_copy_skips_vcs_and_caches(dags: Path, tmp_path: Path) -> None:
    (dags / "__pycache__").mkdir()
    (dags / "__pycache__" / "x.pyc").write_bytes(b"\x00")
    target = tmp_path / "copy"
    assert copy_dags_folder(dags, target) == 2
    assert (target / "lib" / "helpers.py").is_file()
    assert not (target / ".git").exists()


# --------------------------------------------------------------------------- docker command


def test_docker_command_is_bounded_and_isolated(tmp_path: Path) -> None:
    settings = Settings(
        sandbox_image="img:1",
        sandbox_cpus=1.0,
        sandbox_memory="2g",
        sandbox_timeout_seconds=300,
        sandbox_sample_rows=500,
    )
    command = build_docker_command(
        settings,
        tmp_path,
        level="task",
        dag_id="train",
        task_id="fit; rm -rf /",
        container_name="sbx-1",
    )
    assert command.startswith("MSYS_NO_PATHCONV=1 docker run --rm ")
    argv = shlex.split(command.removeprefix("MSYS_NO_PATHCONV=1 "))
    joined = " ".join(argv)
    for flag in (
        "--network none",
        "--cpus 1",
        "--memory 2g",
        "--memory-swap 2g",
        "--pids-limit 256",
        "--read-only",
        "--cap-drop ALL",
        "--security-opt no-new-privileges",
        "--entrypoint timeout",
    ):
        assert flag in joined, flag
    assert f"{tmp_path.resolve().as_posix()}:/sandbox:ro" in argv
    assert "AIRFLOW_SANDBOX_SAMPLE_ROWS=500" in argv
    assert argv[argv.index("--task-id") + 1] == "fit; rm -rf /"  # quoted, not executed
    assert not any(a.startswith("AIRFLOW_CONN") or "PASSWORD" in a for a in argv)
    assert argv[argv.index("img:1") + 1 : argv.index("img:1") + 4] == ["--signal", "KILL", "300"]


# --------------------------------------------------------------------------- result parsing


def test_parse_success() -> None:
    result = parse_terminal_result(terminal(sentinel(True)), Settings())
    assert result["passed"] is True and result["stages"]["import"]["passed"] is True


@pytest.mark.parametrize(
    ("exit_code", "key", "text"),
    [
        (137, "oom", "memory limit"),
        (124, "timed_out", "Timed out"),
        (125, None, "Docker"),
        (127, None, "Docker"),
    ],
)
def test_parse_failures(exit_code: int, key: str | None, text: str) -> None:
    result = parse_terminal_result(terminal("Killed", exit_code), Settings())
    assert result["passed"] is False and text in result["error"]
    if key:
        assert result[key] is True


def test_parse_blocked_terminal_and_redaction() -> None:
    raw = json.dumps(
        {"output": "password=hunter2", "exit_code": -1, "error": "BLOCKED: denied by user"}
    )
    result = parse_terminal_result(raw, Settings())
    assert "denied by user" in result["error"]
    assert "hunter2" not in json.dumps(result)


# --------------------------------------------------------------------------- tool end to end


def test_tool_hidden_and_refused_when_disabled(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags, sandbox_enabled=False)
    assert ctx.tools["airflow_sandbox_test"]["check_fn"]() is False
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert result["error"]["type"] == "sandbox" and "disabled" in result["error"]["message"]


def test_import_level_end_to_end(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags)
    seen: dict[str, Any] = {}

    def fake_terminal(tool: str, args: dict[str, Any]) -> str:
        workspace = mounted_workspace(args["command"])
        seen["tool"] = tool
        seen["timeout"] = args["timeout"]
        seen["patched"] = (workspace / "dags" / "train.py").read_text(encoding="utf-8")
        seen["runner"] = (workspace / "check_dag.py").is_file()
        seen["expected"] = json.loads((workspace / "expected_structure.json").read_text("utf-8"))
        seen["workspace"] = workspace
        return terminal(sentinel(True))

    ctx.dispatch_handler = fake_terminal
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert result["ok"] is True and result["passed"] is True, result
    assert seen["tool"] == "terminal" and seen["timeout"] == 360
    assert 'frame.get("target")' in seen["patched"]
    assert seen["runner"] is True
    assert seen["expected"]["transform"] == ["load", "quality_check"]
    assert result["patched_files"] == ["train.py"]
    assert result["limits"]["network"] == "none"
    assert not seen["workspace"].exists()  # cleaned up
    assert 'frame["target"]' in (dags / "train.py").read_text(encoding="utf-8")  # untouched
    entry = storage.list_audit_entries(0)[0]
    assert (entry["tool"], entry["outcome"], entry["dry_run"]) == (
        "airflow_sandbox_test",
        "passed",
        True,
    )


def test_failed_import_is_reported(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags)
    ctx.dispatch_handler = lambda tool, args: terminal(
        sentinel(
            False, **{"import": {"passed": False, "import_errors": {"train.py": "SyntaxError"}}}
        ),
        1,
    )
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert result["ok"] is True and result["passed"] is False
    assert result["stages"]["import"]["import_errors"] == {"train.py": "SyntaxError"}
    assert storage.list_audit_entries(0)[0]["outcome"] == "failed"
    # a failed patch is not kept: only patches that passed can be applied
    assert not Path(result["patch_file"]).exists() and "apply_commands" not in result


def test_task_level_with_fixtures(dags: Path, api: Any) -> None:
    api.get("/dags/train/dagRuns/run_1/taskInstances/extract/xcomEntries").respond(
        json={"xcom_entries": [{"key": "return_value", "map_index": -1}], "total_entries": 1}
    )
    api.get("/dags/train/dagRuns/run_1/taskInstances/extract/xcomEntries/return_value").respond(
        json={"value": {"rows": 10, "api_token": "abc"}}
    )
    ctx = sandbox_ctx(dags)
    fixtures: dict[str, Any] = {}

    def fake_terminal(tool: str, args: dict[str, Any]) -> str:
        workspace = mounted_workspace(args["command"])
        fixtures.update(json.loads((workspace / "fixtures" / "xcoms.json").read_text("utf-8")))
        assert "--task-id" in args["command"]
        return terminal(sentinel(True))

    ctx.dispatch_handler = fake_terminal
    result = ctx.call(
        "airflow_sandbox_test",
        dag_id="train",
        patch=PATCH,
        level="task",
        task_id="transform",
        run_id="run_1",
    )
    assert result["passed"] is True
    assert fixtures == {"extract": {"return_value": {"rows": 10, "api_token": "[REDACTED]"}}}


def test_heavy_operator_is_never_run(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags, sandbox_heavy_operators=["EmailOperator"])
    result = ctx.call(
        "airflow_sandbox_test", dag_id="train", patch=PATCH, level="task", task_id="notify"
    )
    assert result["error"]["type"] == "sandbox" and "EmailOperator" in result["error"]["message"]
    assert not getattr(ctx, "dispatched", [])
    # import / structure checks stay allowed for that DAG
    assert ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)["ok"] is True


def test_task_level_requires_task_id(dags: Path, api: Any) -> None:
    result = sandbox_ctx(dags).call(
        "airflow_sandbox_test", dag_id="train", patch=PATCH, level="task"
    )
    assert result["error"]["type"] == "sandbox"


def test_budget_per_session(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags, sandbox_max_tests_per_session=1)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(True))
    assert ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)["ok"] is True
    second = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert second["error"]["type"] == "sandbox" and "budget" in second["error"]["message"]


def test_one_test_at_a_time(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags)
    outcome: dict[str, Any] = {}

    def nested(tool: str, args: dict[str, Any]) -> str:
        outcome["inner"] = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
        return terminal(sentinel(True))

    ctx.dispatch_handler = nested
    assert ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)["ok"] is True
    assert "already running" in outcome["inner"]["error"]["message"]


def test_bad_patch_is_journaled_as_refused(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags)
    result = ctx.call(
        "airflow_sandbox_test",
        dag_id="train",
        patch="--- a/x.py\n+++ b/x.py\n@@ -1 +1 @@\n-a\n+b\n",
    )
    assert result["error"]["type"] == "sandbox"
    assert storage.list_audit_entries(0)[0]["outcome"] == "refused"


def test_allowlist_applies(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags, dag_allowlist=["etl_*"])
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert result["error"]["type"] == "dag_not_allowed"


# --------------------------------------------------------------------------- approval hook


def test_hook_for_sandbox_levels() -> None:
    enabled = Settings(sandbox_enabled=True, sandbox_max_level="structure")
    hook = make_pre_tool_call_hook(lambda: enabled)
    assert hook(tool_name="airflow_sandbox_test", args={"dag_id": "train"}) is None
    assert (
        hook(tool_name="airflow_sandbox_test", args={"dag_id": "train", "level": "structure"})
        is None
    )
    decision = hook(
        tool_name="airflow_sandbox_test",
        args={"dag_id": "train", "level": "task", "task_id": "fit"},
    )
    assert decision["action"] == "approve" and "no network" in decision["message"]
    disabled = make_pre_tool_call_hook(lambda: Settings())
    assert disabled(tool_name="airflow_sandbox_test", args={"dag_id": "t"})["action"] == "block"
    outside = make_pre_tool_call_hook(
        lambda: Settings(sandbox_enabled=True, dag_allowlist=("etl_*",))
    )
    assert outside(tool_name="airflow_sandbox_test", args={"dag_id": "t"})["action"] == "block"


# --------------------------------------------------------------------------- settings / runner


def test_sandbox_settings_are_bounded() -> None:
    ctx = make_context(
        sandbox_memory="999g",
        sandbox_timeout_seconds=99999,
        sandbox_cpus=64,
        sandbox_max_level="everything",
    )
    settings = Settings.from_source(ctx)
    assert settings.sandbox_memory == "2g"
    assert settings.sandbox_timeout_seconds == 540
    assert settings.sandbox_cpus == 8.0
    assert settings.sandbox_max_level == "structure"


def test_runner_compare_structure() -> None:
    spec = importlib.util.spec_from_file_location(
        "check_dag", ROOT / "sandbox_runner" / "check_dag.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # stdlib-only at import time (Airflow is imported lazily)
    diff = module.compare_structure({"a": ["b"], "b": []}, {"a": ["c"], "c": []})
    assert diff == {
        "added_tasks": ["c"],
        "removed_tasks": ["b"],
        "added_edges": ["a>>c"],
        "removed_edges": ["a>>b"],
    }


# --------------------------------------------------------------------------- real-run findings


def test_task_killed_by_memory_limit_is_reported_as_oom() -> None:
    out = sentinel(False, task={"passed": False, "exit_code": -9, "output": "Executing"})
    result = parse_terminal_result(terminal(out, 1), Settings(sandbox_memory="512m"))
    assert result["oom"] is True and "512m" in result["error"]


def test_network_failure_gets_a_sandbox_hint() -> None:
    out = sentinel(
        False,
        task={
            "passed": False,
            "exit_code": 1,
            "output": "URLError: Temporary failure in name resolution",
        },
    )
    result = parse_terminal_result(terminal(out, 1), Settings())
    assert "never allows" in result["hint"] and result["oom"] is False


def test_docker_command_disables_log_colours(tmp_path: Path) -> None:
    command = build_docker_command(
        Settings(sandbox_image="i"),
        tmp_path,
        level="import",
        dag_id="d",
        task_id=None,
        container_name="c",
    )
    assert "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=False" in command


def test_runner_strips_ansi() -> None:
    spec = importlib.util.spec_from_file_location(
        "check_dag_ansi", ROOT / "sandbox_runner" / "check_dag.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module._clean("\x1b[34mINFO\x1b[0m - Done") == "INFO - Done"


def test_stale_workspaces_are_purged(tmp_path: Path) -> None:
    import os
    import time

    from airflow_mlops.sandbox import purge_stale_workspaces

    old, fresh = tmp_path / "old", tmp_path / "fresh"
    for folder in (old, fresh):
        (folder / "dags").mkdir(parents=True)
    two_hours_ago = time.time() - 7200
    os.utime(old, (two_hours_ago, two_hours_ago))
    assert purge_stale_workspaces(tmp_path) == 1
    assert not old.exists() and fresh.exists()
    assert purge_stale_workspaces(tmp_path / "missing") == 0


def test_refused_patches_do_not_consume_the_budget(dags: Path, api: Any) -> None:
    """Found in a real Hermes session: rejected diffs used up the test budget."""
    ctx = sandbox_ctx(dags, sandbox_max_tests_per_session=1)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(True))
    bad = "--- a/train.py\n+++ b/train.py\n@@ -1 +1 @@\n-not in the file\n+x\n"
    for _ in range(3):  # refused before any container runs
        refused = ctx.call("airflow_sandbox_test", dag_id="train", patch=bad)
        assert "does not apply" in refused["error"]["message"]
    assert ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)["ok"] is True
    over = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert "budget" in over["error"]["message"]


def test_checker_crash_is_reported_with_its_real_error() -> None:
    """Found on Airflow 3.4: the checker crashed (DagBag API change) and the plugin only said
    'no result line'. The checker now always emits a result carrying the real error."""
    out = 'SANDBOX_RESULT {"passed": false, "stages": {}, "error": "TypeError: bad kwarg"}'
    result = parse_terminal_result(terminal(out, 1), Settings())
    assert result["passed"] is False
    assert result["error"] == "Sandbox checker error: TypeError: bad kwarg"


def test_runner_safe_main_emits_a_result_line(capsys: pytest.CaptureFixture[str]) -> None:
    spec = importlib.util.spec_from_file_location(
        "check_dag_safe", ROOT / "sandbox_runner" / "check_dag.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    def boom() -> int:
        raise RuntimeError("airflow exploded")

    module.main = boom
    assert module._safe_main() == 1
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert line.startswith("SANDBOX_RESULT ")
    assert "RuntimeError: airflow exploded" in line


@pytest.mark.parametrize(
    ("output", "returncode", "state", "passed"),
    [
        # Airflow 3.4: exit code 0 although the task failed (found on a real 3.4.0 run)
        (
            "URLError ...\nTask instance state updated new_state=failed rows_affected=1\n"
            "[DAG TEST] end task",
            0,
            "failed",
            False,
        ),
        ("... new_state=success rows_affected=1", 0, "success", True),
        # Airflow 2.x
        ("INFO - Marking task as SUCCESS. dag_id=d", 0, "success", True),
        ("ERROR - Task failed\nINFO - Marking task as FAILED. dag_id=d", 1, "failed", False),
        # the LAST announced state wins (e.g. up_for_retry then failed)
        ("new_state=up_for_retry ... new_state=failed", 0, "failed", False),
        # no state announced: fall back to the exit code
        ("killed", -9, None, False),
        ("done", 0, None, True),
    ],
)
def test_task_outcome_uses_the_announced_state(
    output: str, returncode: int, state: str | None, passed: bool
) -> None:
    spec = importlib.util.spec_from_file_location(
        "check_dag_state", ROOT / "sandbox_runner" / "check_dag.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.task_final_state(output) == state
    assert module.task_passed(returncode, state) is passed


# --------------------------------------------------------------------------- presenting the fix


def test_result_shows_exact_location_and_a_ready_patch(dags: Path, api: Any) -> None:
    """User request: easy-to-find file and lines, and an easy way to apply the tested fix."""
    ctx = sandbox_ctx(dags)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(True))
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert result["your_files_modified"] is False
    change = result["changes"][0]
    assert change["file"] == "train.py"
    assert change["local_path"] == (dags / "train.py").resolve().as_posix()
    block = change["blocks"][0]
    # exact line, computed from the files although the LLM's hunk header said line 40
    assert block["kind"] == "modified" and block["lines_before"] == [5, 5]
    assert block["before"] == '   5 |     return frame["target"]'
    assert block["after"] == '   5 |     return frame.get("target")'
    patch_file = Path(result["patch_file"])
    assert patch_file.is_file() and patch_file.suffix == ".patch"
    assert dags.resolve() not in patch_file.resolve().parents  # never inside the user's repo
    commands = result["apply_commands"]
    assert commands["check"].startswith("git -C ") and "apply --check" in commands["check"]
    assert commands["apply"].endswith(shlex.quote(patch_file.resolve().as_posix()))
    # the saved patch is a clean diff that reproduces the tested change
    copy = dags.parent / "verify"
    shutil.copytree(dags, copy)
    apply_unified_diff(copy, patch_file.read_text(encoding="utf-8"))
    assert 'frame.get("target")' in (copy / "train.py").read_text(encoding="utf-8")
    assert 'frame["target"]' in (dags / "train.py").read_text(encoding="utf-8")  # untouched


def test_describe_changes_insert_and_crlf(tmp_path: Path) -> None:
    from airflow_mlops.sandbox import describe_changes

    src, copy = tmp_path / "src", tmp_path / "copy"
    src.mkdir()
    copy.mkdir()
    (src / "d.py").write_bytes(b"a = 1\r\nb = 2\r\n")
    (copy / "d.py").write_bytes(b"a = 1\r\nx = 9\r\nb = 2\r\n")
    changes, patch = describe_changes(src, copy, ["d.py"])
    block = changes[0]["blocks"][0]
    assert block["kind"] == "added" and block["after_line"] == 1
    assert block["after"] == "   2 | x = 9"
    assert "+x = 9\r\n" in patch  # CRLF kept so `git apply` matches the real file
