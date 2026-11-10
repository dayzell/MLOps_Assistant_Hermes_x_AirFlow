"""Batch handling in an audit: fixes of one DAG proposed and tested together (one sandbox
run), retries together (one preview, one decision), and the progress board."""

from __future__ import annotations

from typing import Any

from airflow_mlops import workflow as wf
from airflow_mlops.audit_tools import AuditTools
from airflow_mlops.runtime import PluginRuntime
from airflow_mlops.tools import ReadTools
from harness import make_context
from test_workflow import CANDIDATES, planned_audit, root


def select(audit: wf.Audit, *numbers: str) -> None:
    for item in audit.items:
        item.selected = item.number in numbers
    audit.current = numbers[0]
    wf.save_audit(audit)


def test_fixes_of_one_dag_are_tested_in_one_sandbox_run() -> None:
    audit = planned_audit()
    select(audit, "1.1", "1.2")
    action = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_action"]
    assert action.startswith("Fixes 1.1 + 1.2: read the code")
    assert "ONE diff containing every fix" in action


def test_a_single_fix_keeps_the_singular_wording() -> None:
    audit = planned_audit()
    select(audit, "1.1")
    action = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_action"]
    assert action.startswith("Fix 1.1: read the code") and 'task_id="train"' in action


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
    assert question["choices"] == ["Preview all retries (dry run)", "Skip them", "Stop here"]
    then = question["options"][0]["then"]
    assert 'dag_id="demo_fetch_transient"' in then and 'dag_id="other_fetch"' in then


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


def audit_tools(ctx: Any) -> AuditTools:
    runtime = PluginRuntime(ctx)
    return AuditTools(runtime, ReadTools(runtime))


def test_a_group_passes_together_and_the_board_follows() -> None:
    ctx = make_context()
    board = Board()
    ctx.post_message = board.post_message  # type: ignore[attr-defined]
    tools = audit_tools(ctx)
    planned_audit()

    assert '"selected": ["1.1", "1.2"]' in tools.select({"selection": "DAG 1"})
    assert "[-------] 0/2 tested" in board.texts[0] and "starting with 1.1" in board.texts[0]

    tools.observe_sandbox_start("demo_train_critical", None, "structure")
    assert "testing 1.1, 1.2 in the sandbox (structure)..." in board.texts[-1]
    step = tools.observe_sandbox("demo_train_critical", None, "structure", {"passed": True})
    assert step is not None and step["items"] == ["1.1", "1.2"] and step["state"] == "passed"
    # 2.1 is left: it is offered, or Stop here
    assert step["next_question"]["choices"][0].startswith("2.1 fetch")
    assert "[#######] 2/2 tested" in board.texts[-1]


def test_progress_accepts_a_group() -> None:
    ctx = make_context()
    planned_audit()
    result = ctx.call("airflow_audit_progress", item="1.1, 1.2", state="skipped")
    assert [i["state"] for i in result["items"]] == ["skipped", "skipped"]
    unknown = ctx.call("airflow_audit_progress", item="1.1, 7.7", state="passed")
    assert unknown["error"]["type"] == "invalid_argument" and "7.7" in unknown["error"]["message"]


def test_a_single_selection_posts_no_board() -> None:
    ctx = make_context()
    board = Board()
    ctx.post_message = board.post_message  # type: ignore[attr-defined]
    planned_audit()
    audit_tools(ctx).select({"selection": "1.1"})
    assert board.texts == []
