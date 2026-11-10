# ruff: noqa: F811  (pytest fixtures imported from test_sandbox)
"""Plan v2, step 3: the DAG write lock.

A DAG file is changed only with the exact change that passed the sandbox, after a human
approval; everywhere else Hermes' file and shell tools are left alone.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from airflow_mlops import storage
from airflow_mlops import workflow as wf
from airflow_mlops.dag_guard import fingerprint, record_tested
from airflow_mlops.dag_guard import tested_changes as recorded_changes
from harness import FakeContext
from test_sandbox import BUGGY, PATCH, api, dags, sandbox_ctx, sentinel, terminal  # noqa: F401

FIXED = BUGGY.replace('frame["target"]', 'frame.get("target")')


def hook(ctx: FakeContext, tool: str, **args: Any) -> dict[str, Any] | None:
    return ctx.hooks["pre_tool_call"][0](tool_name=tool, args=args)


def record_fix(dags: Path, after: str = FIXED) -> str:
    """Record a passing sandbox test of train.py; returns the patch file."""
    patch = str(dags.parent / "patches" / "20261109-train.patch")
    record_tested(
        "train", "structure", patch, [(dags / "train.py", BUGGY.encode(), after.encode())]
    )
    return patch


# ---------------------------------------------------------------------------- write_file / patch


def test_files_outside_the_dags_folder_are_free(dags: Path, tmp_path: Path) -> None:
    ctx = sandbox_ctx(dags)
    assert hook(ctx, "write_file", path=str(tmp_path / "notes.md"), content="x") is None
    assert hook(ctx, "read_file", path=str(dags / "train.py")) is None  # reading is free


def test_untested_write_to_a_dag_file_is_blocked(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    decision = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED)
    assert decision is not None and decision["action"] == "block"
    assert "was not tested" in decision["message"]
    assert "airflow_sandbox_test" in decision["message"]


def test_the_tested_change_needs_a_human_approval(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    record_fix(dags)
    decision = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED)
    assert decision is not None and decision["action"] == "approve"
    assert "sandbox-tested fix to train.py" in decision["message"]
    assert decision["rule_key"] == f"dag_write:{fingerprint(FIXED)[:12]}"  # this change only
    # same content with Windows line endings: still the tested change
    crlf = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED.replace("\n", "\r\n"))
    assert crlf is not None and crlf["action"] == "approve"


def test_a_different_change_is_blocked_with_the_way_out(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    patch = record_fix(dags)
    decision = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED + "X = 2\n")
    assert decision is not None and decision["action"] == "block"
    assert "differs from the one that passed the sandbox" in decision["message"]
    assert (
        f"git apply {Path(patch).as_posix()}".lower()
        in decision["message"].replace("\\", "/").lower()
    )


def test_patch_tool_replace_mode_is_simulated(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    record_fix(dags)
    ok = hook(
        ctx,
        "patch",
        path=str(dags / "train.py"),
        old_string='frame["target"]',
        new_string='frame.get("target")',
    )
    assert ok is not None and ok["action"] == "approve"
    other = hook(
        ctx, "patch", path=str(dags / "train.py"), old_string='frame["target"]', new_string="0"
    )
    assert other is not None and other["action"] == "block"
    missing = hook(ctx, "patch", path=str(dags / "train.py"), old_string="nope", new_string="x")
    assert missing is not None and "cannot check" in missing["message"]


def test_v4a_patches_cannot_be_checked(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    record_fix(dags)
    v4a = f"*** Begin Patch\n*** Update File: {dags / 'train.py'}\n@@\n-a\n+b\n*** End Patch"
    decision = hook(ctx, "patch", mode="patch", patch=v4a)
    assert decision is not None and decision["action"] == "block"


def test_relative_paths_use_the_session_folder(dags: Path) -> None:
    from airflow_mlops.config import Settings
    from airflow_mlops.dag_guard import DagWriteGuard

    settings = Settings(sandbox_enabled=True, sandbox_image="x", sandbox_dags_path=str(dags))
    guard = DagWriteGuard(lambda: settings, lambda: str(dags))  # the session works in dags/
    decision = guard.decide("write_file", {"path": "train.py", "content": "x"})
    assert decision is not None and decision["action"] == "block"


def test_file_changed_since_the_test_is_blocked(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    record_fix(dags)
    (dags / "train.py").write_text(BUGGY + "# edited by hand\n", encoding="utf-8")
    decision = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED)
    assert decision is not None and "changed since the sandbox test" in decision["message"]


# ---------------------------------------------------------------------------- terminal


def test_git_apply_of_the_tested_patch(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    patch = Path(record_fix(dags)).as_posix()
    root = dags.as_posix()
    applied = hook(ctx, "terminal", command=f"git -C {root} apply {patch}")
    assert applied is not None and applied["action"] == "approve"
    assert applied["message"].startswith("Apply the sandbox-tested patch on train.py")
    assert hook(ctx, "terminal", command=f"git -C {root} apply --check {patch}") is None
    # undo is only possible once the fix is in the file
    (dags / "train.py").write_text(FIXED, encoding="utf-8")
    undo = hook(ctx, "terminal", command=f"git -C {root} apply -R {patch}")
    assert undo is not None and undo["action"] == "approve"
    assert undo["message"].startswith("Undo the sandbox-tested patch")


def test_untested_patch_or_raw_edit_in_the_dags_folder(dags: Path) -> None:
    ctx = sandbox_ctx(dags)
    root = dags.as_posix()
    untested = hook(ctx, "terminal", command=f"git -C {root} apply /tmp/other.patch")
    assert untested is not None and untested["action"] == "block"
    sed = hook(ctx, "terminal", command=f"sed -i 's/a/b/' {root}/train.py")
    assert sed is not None and sed["action"] == "approve" and "cannot check" in sed["message"]
    assert hook(ctx, "terminal", command=f"cat {root}/train.py") is None  # reading is free
    assert hook(ctx, "terminal", command="ls -la /tmp > /tmp/list.txt") is None  # not the DAGs
    code = hook(ctx, "execute_code", code=f"open('{root}/train.py', 'w').write('x')")
    assert code is not None and code["action"] == "approve"


# ---------------------------------------------------------------------------- modes


def test_modes(dags: Path) -> None:
    write = {"path": str(dags / "train.py"), "content": "x"}
    approval = sandbox_ctx(dags, dag_write_guard="approval_only")
    decision = approval.hooks["pre_tool_call"][0](tool_name="write_file", args=write)
    assert decision is not None and decision["action"] == "approve"
    off = sandbox_ctx(dags, dag_write_guard="off")
    assert off.hooks["pre_tool_call"][0](tool_name="write_file", args=write) is None
    # no sandbox: nothing can be tested, a human approves every write
    no_sandbox = sandbox_ctx(dags, sandbox_enabled=False)
    decision = no_sandbox.hooks["pre_tool_call"][0](tool_name="write_file", args=write)
    assert decision is not None and decision["action"] == "approve"


def test_guard_errors_fall_back_to_a_human(dags: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from airflow_mlops import dag_guard

    ctx = sandbox_ctx(dags)

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("bug")

    monkeypatch.setattr(dag_guard.DagWriteGuard, "_file_targets", boom)
    decision = hook(ctx, "write_file", path=str(dags / "train.py"), content="x")
    assert decision is not None and decision["action"] == "approve"


# ---------------------------------------------------------------------------- after the write


def test_written_fix_is_journaled(dags: Path) -> None:
    """After the audit, Hermes applies the tested fix: the write is checked and journaled."""
    ctx = sandbox_ctx(dags)
    record_fix(dags)
    (dags / "train.py").write_text(FIXED, encoding="utf-8")  # what Hermes wrote
    ctx.hooks["post_tool_call"][0](
        tool_name="write_file", args={"path": str(dags / "train.py"), "content": FIXED}
    )
    entries = storage.list_audit_entries(0.0, dag_ids=["train"])
    assert entries[0]["tool"] == "dag_write" and entries[0]["outcome"] == "matches_tested_fix"


def test_no_code_change_during_an_audit(dags: Path) -> None:
    """User rule: an audit never changes code, not even a tested fix."""
    ctx = sandbox_ctx(dags)
    patch = Path(record_fix(dags)).as_posix()
    wf.create_audit("all", "24h", [], "s-1")  # an audit in progress
    write = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED)
    assert write is not None and write["action"] == "block"
    assert "an audit is in progress" in write["message"]
    apply = hook(ctx, "terminal", command=f"git -C {dags.as_posix()} apply {patch}")
    assert apply is not None and apply["action"] == "block"
    assert hook(ctx, "terminal", command=f"cat {dags.as_posix()}/train.py") is None  # reading
    # once the audit is archived, the tested fix can be applied (with the user's approval)
    ctx.call("airflow_audit_progress", state="stop")
    after = hook(ctx, "write_file", path=str(dags / "train.py"), content=FIXED)
    assert after is not None and after["action"] == "approve"


def test_a_passing_sandbox_test_unlocks_its_change(dags: Path, api: Any) -> None:
    """Real session: only the 'import' level ran and the lock let it through. Now the change
    must pass 'structure' too (tasks and dependencies checked)."""
    ctx = sandbox_ctx(dags)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(True))
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)  # level import
    assert result["passed"] is True
    recorded = recorded_changes(dags / "train.py")
    assert recorded and recorded[0].after == fingerprint(FIXED)
    assert recorded[0].before == fingerprint(BUGGY)
    write = {"path": str(dags / "train.py"), "content": FIXED}
    blocked = hook(ctx, "write_file", **write)
    assert blocked is not None and blocked["action"] == "block"
    assert "only passed the 'import' level" in blocked["message"]
    import_patch = Path(result["patch_file"]).as_posix()
    apply = hook(ctx, "terminal", command=f"git -C {dags.as_posix()} apply {import_patch}")
    assert apply is not None and apply["action"] == "block"

    ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH, level="structure")
    decision = hook(ctx, "write_file", **write)
    assert decision is not None and decision["action"] == "approve"
    # the patch saved by the import run is the same change: it may now be applied too
    apply = hook(ctx, "terminal", command=f"git -C {dags.as_posix()} apply {import_patch}")
    assert apply is not None and apply["action"] == "approve"


def test_the_sandbox_flags_a_fix_that_hides_the_error(dags: Path, api: Any) -> None:
    """User question: the .get() 'fix' passed the sandbox; the user should have been warned."""
    ctx = sandbox_ctx(dags)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(True))
    result = ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    warning = result["masking_warnings"][0]
    assert warning["rule"] == "key_lookup_to_get" and warning["line"] == 5
    assert "no longer fails" in warning["why"]
    assert "masking_warnings" in result["note"]


def test_a_failing_sandbox_test_unlocks_nothing(dags: Path, api: Any) -> None:
    ctx = sandbox_ctx(dags)
    ctx.dispatch_handler = lambda tool, args: terminal(sentinel(False))
    ctx.call("airflow_sandbox_test", dag_id="train", patch=PATCH)
    assert recorded_changes(dags / "train.py") == []
