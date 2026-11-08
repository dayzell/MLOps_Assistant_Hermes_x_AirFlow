"""Plan v2, step 2: batch handling (fixes of one DAG together, retries together) and the
progress board updated in place."""

from __future__ import annotations

from typing import Any

from airflow_mlops import workflow as wf
from airflow_mlops.audit_tools import AuditTools
from airflow_mlops.runtime import PluginRuntime
from airflow_mlops.tools import ReadTools
from harness import FakeContext, make_context
from test_workflow import CANDIDATES, critical_diagnosis, planned_audit, root


def select(audit: wf.Audit, *numbers: str) -> None:
    for item in audit.items:
        item.selected = item.number in numbers
    audit.current = numbers[0]
    wf.save_audit(audit)


def set_state(audit: wf.Audit, state: str, *numbers: str) -> None:
    for number in numbers:
        item = audit.item(number)
        assert item is not None
        item.state = state


# ---------------------------------------------------------------------------- groups


def test_fixes_of_one_dag_are_proposed_and_tested_together() -> None:
    audit = planned_audit()
    select(audit, "1.1", "1.2")
    step = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)
    assert step["next_action"].startswith("Fixes 1.1 + 1.2 are in the same DAG")
    assert 'item="1.1, 1.2", state="fix_proposed"' in step["next_action"]

    set_state(audit, "fix_proposed", "1.1", "1.2")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert question["question"] == "Test fixes 1.1 + 1.2 in the sandbox (one run)?"
    assert question["choices"][0] == "Test them (one run)"
    assert "ONE diff containing every fix" in question["options"][0]["then"]

    set_state(audit, "sandbox_passed", "1.1", "1.2")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert (
        question["question"] == "Fixes 1.1 + 1.2 passed the sandbox test. Apply them to your file?"
    )
    assert question["choices"][0] == "Apply them (your approval required)"


def test_a_group_applied_together_unlocks_the_retry() -> None:
    audit = planned_audit()
    select(audit, "1.1", "1.2")
    set_state(audit, "applied", "1.1", "1.2")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert question["question"].startswith("Fixes 1.1 + 1.2 are applied and demo_train_critical")
    assert question["choices"][0] == "Preview the retry of demo_train_critical (dry run)"


def test_a_single_selected_fix_keeps_the_singular_wording() -> None:
    audit = planned_audit()
    select(audit, "1.1")
    set_state(audit, "fix_proposed", "1.1")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert question["question"] == "Test fix 1.1 in the sandbox?"


def test_transient_failures_are_retried_with_one_question() -> None:
    cands = [dict(CANDIDATES[1]), {**CANDIDATES[1], "dag_id": "other_fetch", "run_id": "r3"}]
    audit = wf.create_audit("all", "24h", cands, None)
    for dag_id, run_id in (("demo_fetch_transient", "r2"), ("other_fetch", "r3")):
        audit.items += wf.items_from_diagnosis(
            audit,
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "root_failures": [root("fetch", "transient", "ConnectionResetError: x")],
            },
        )
    select(audit, "1.1", "2.1")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert question["question"] == "Retry the 2 transient failures (1.1 + 2.1)?"
    assert question["choices"] == [
        "Preview all retries (dry run)",
        "One by one",
        "Skip them",
        "Stop here",
    ]
    then = question["options"][0]["then"]
    assert 'dag_id="demo_fetch_transient"' in then and 'dag_id="other_fetch"' in then
    assert "ONE combined list" in then


def test_a_blocked_transient_is_left_out_of_the_retry_group() -> None:
    audit = wf.create_audit("all", "24h", [dict(CANDIDATES[0]), dict(CANDIDATES[1])], None)
    diagnosis = critical_diagnosis()
    diagnosis["root_failures"].append(root("fetch", "transient", "ConnectionResetError: x"))
    audit.items += wf.items_from_diagnosis(audit, diagnosis)  # 1.1 train, 1.2 fetch, 1.3 masked
    audit.items += wf.items_from_diagnosis(
        audit,
        {
            "dag_id": "demo_fetch_transient",
            "run_id": "r2",
            "root_failures": [root("fetch", "transient", "ConnectionResetError: y")],
        },
    )
    select(audit, "2.1", "1.2")
    group = wf.retry_group(audit, audit.item("2.1"))  # type: ignore[arg-type]
    assert [i.number for i in group] == ["2.1"]  # 1.2 waits for the fixes of DAG 1


# ---------------------------------------------------------------------------- tools


class Board:
    """``post_message`` stand-in: records the message and every in-place edit."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    def post_message(self, text: str) -> Any:
        self.texts.append(text)
        board = self

        class Handle:
            ok = True

            def edit(self, new: str) -> dict[str, Any]:
                board.texts.append(new)
                return {"ok": True}

        return Handle()


def audit_tools(ctx: FakeContext) -> AuditTools:
    runtime = PluginRuntime(ctx)
    return AuditTools(runtime, ReadTools(runtime))


def test_progress_accepts_a_group_and_the_board_follows() -> None:
    ctx = make_context()
    board = Board()
    ctx.post_message = board.post_message  # type: ignore[attr-defined]
    tools = audit_tools(ctx)
    planned_audit()

    selected = tools.select({"selection": "DAG 1"})
    assert '"selected": ["1.1", "1.2"]' in selected
    assert board.texts[0].startswith("Audit #") and "[-------] 0/2 done" in board.texts[0]
    assert "starting with 1.1" in board.texts[0]

    tools.progress({"item": "1.1, 1.2", "state": "fix_proposed"})
    assert board.texts[-1].count("fix proposed") == 3  # header + both lines

    tools.observe_sandbox_start("demo_train_critical", None, "structure")
    assert "testing 1.1, 1.2 in the sandbox (structure)..." in board.texts[-1]
    step = tools.observe_sandbox("demo_train_critical", None, "structure", {"passed": True})
    assert step is not None and step["items"] == ["1.1", "1.2"]
    assert step["next_question"]["choices"][0] == "Apply them (your approval required)"
    audit = wf.load_current()
    assert audit is not None
    assert {i.number: i.state for i in audit.items if i.dag_id == "demo_train_critical"} == {
        "1.1": "sandbox_passed",
        "1.2": "sandbox_passed",
    }

    tools.progress({"item": "1.1 + 1.2", "state": "verified"})
    assert "[#######] 2/2 done" in board.texts[-1]
    tools.progress({"state": "stop"})
    assert "stopped" in board.texts[-1]


def test_a_single_selection_posts_no_board() -> None:
    ctx = make_context()
    board = Board()
    ctx.post_message = board.post_message  # type: ignore[attr-defined]
    planned_audit()
    audit_tools(ctx).select({"selection": "1.1"})
    assert board.texts == []


def test_unknown_items_in_a_group_are_reported() -> None:
    ctx = make_context()
    planned_audit()
    result = ctx.call("airflow_audit_progress", item="1.1, 7.7", state="applied")
    assert result["error"]["type"] == "invalid_argument" and "7.7" in result["error"]["message"]


def test_board_is_silent_without_post_message() -> None:
    tools = audit_tools(make_context())  # FakeContext has no post_message
    planned_audit()
    assert '"selected"' in tools.select({"selection": "all"})
