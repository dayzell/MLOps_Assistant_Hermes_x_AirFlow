"""Guided audit end to end: tools, observers, /airflow-audit, /airflow-status, pre_llm_call."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import payloads as p
import respx
from airflow_mlops import workflow as wf
from airflow_mlops.audit_tools import AuditTools
from airflow_mlops.runtime import PluginRuntime
from airflow_mlops.tools import ReadTools
from harness import FakeContext
from test_diagnosis import mock_run
from test_reporting import mock_airflow

CRITICAL = {
    "extract": "success",
    "transform": "failed",
    "load": "upstream_failed",
    "quality_check": "upstream_failed",
    "notify": "success",
}


def plan_train(airflow: respx.MockRouter, api_version: str, ctx: FakeContext) -> dict[str, Any]:
    mock_run(airflow, api_version, CRITICAL, logs={"transform": p.log_body(api_version)})
    airflow.get("/dags/train/dagRuns").respond(
        json={
            "dag_runs": [p.dag_run(api_version, "train", "run_1", "failed")],
            "total_entries": 1,
        }
    )
    return ctx.call("airflow_audit_plan", dag_ids=["train"])


def hook_context(ctx: FakeContext, session_id: str = "s-1") -> str:
    result = ctx.hooks["pre_llm_call"][0](session_id=session_id)
    return str((result or {}).get("context", ""))


# ---------------------------------------------------------------------------- tools


def test_plan_numbers_the_problems_and_asks_which_first(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    result = plan_train(airflow, api_version, ctx)
    assert result["ok"] is True, result
    assert [i["number"] for i in result["items"]] == ["1.1"]
    item = result["items"][0]
    assert (item["task_id"], item["category"], item["kind"]) == (
        "transform",
        "critical",
        "code_fix",
    )
    assert result["plan_text"].startswith("DAG 1 — train")
    assert result["next_question"]["choices"][-1] == "Stop here"


def test_select_progress_and_stop(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    selected = ctx.call("airflow_audit_select", selection="1.1")
    assert selected["current"]["number"] == "1.1"
    assert "airflow_get_dag_source" in selected["next_action"]

    proposed = ctx.call("airflow_audit_progress", item="1.1", state="fix_proposed")
    # the sandbox is disabled by default: no test is offered
    assert proposed["next_question"]["question"].startswith("Fix 1.1 cannot be tested")
    done = ctx.call("airflow_audit_progress", item="1.1", state="verified")
    assert done["progress"]["done"] == 1
    assert 'state="stop"' in done["next_action"]  # the only item is handled

    stopped = ctx.call("airflow_audit_progress", state="stop")
    assert stopped["stopped"] is True
    assert "Resume any time with /airflow-audit." in stopped["summary"]
    # resume: no dag_ids returns the audit in progress
    resumed = ctx.call("airflow_audit_plan")
    assert resumed["status"] == "open" and resumed["items"][0]["state"] == "verified"


def test_bad_inputs_are_explained(ctx: FakeContext) -> None:
    assert ctx.call("airflow_audit_plan")["error"]["type"] == "invalid_argument"
    assert ctx.call("airflow_audit_select", selection="1.1")["error"]["type"] == "invalid_argument"
    assert ctx.call("airflow_audit_progress", state="weird")["error"]["type"] == "invalid_argument"


def test_plan_respects_the_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    from harness import make_context

    ctx = make_context(api_version=api_version, dag_allowlist=["etl*"])
    result = ctx.call("airflow_audit_plan", dag_ids=["train"])
    assert result["error"]["type"] == "dag_not_allowed"


def test_sandbox_and_retry_results_move_the_item(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    ctx.call("airflow_audit_select", selection="1.1")
    audits = AuditTools(PluginRuntime(ctx), ReadTools(PluginRuntime(ctx)))

    step = audits.observe_sandbox("train", "transform", "import", {"passed": True})
    assert step is not None and "structure" in step["next_action"]
    step = audits.observe_sandbox("train", "transform", "structure", {"passed": True})
    assert step is not None and step["state"] == "sandbox_passed"
    assert step["next_question"]["choices"][0] == "Apply it (your approval required)"

    ctx.call("airflow_audit_progress", item="1.1", state="applied")
    assert audits.observe_clear("train", ["transform"], {"outcome": "dry_run"}) is None
    step = audits.observe_clear("train", ["transform"], {"outcome": "cleared"})
    assert step is not None and step["state"] == "retried"
    assert "airflow_get_run_status" in step["next_action"]
    audit = wf.load_current()
    assert audit is not None and audit.item("1.1").state == "retried"  # type: ignore[union-attr]


def test_one_run_check_verifies_every_retried_item_of_the_dag(ctx: FakeContext) -> None:
    audit = wf.create_audit("all", "24h", [], "s-1")
    for position, task in enumerate(("train", None), start=1):
        audit.items.append(
            wf.Item(
                f"1.{position}",
                1,
                position,
                "d",
                "r",
                task,
                "critical",
                "code_fix",
                "t",
                state="retried",
            )
        )
    audit.candidates.append({"dag_id": "d", "status": "failing", "reason": "x"})
    wf.save_audit(audit)
    result = ctx.call("airflow_audit_progress", item="1.1", state="verified")
    assert result["progress"]["done"] == 2


def test_observers_never_fail_without_an_audit(ctx: FakeContext) -> None:
    audits = AuditTools(PluginRuntime(ctx), ReadTools(PluginRuntime(ctx)))
    assert audits.observe_sandbox("x", None, "import", {"passed": True}) is None
    assert audits.observe_clear("x", ["t"], {"outcome": "cleared"}) is None
    assert audits.context("s") is None


def test_report_tool_offers_the_first_question(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    result = ctx.call("airflow_build_report", period="24h")
    question = result["next_question"]
    assert question["question"] == "Which DAG should I diagnose?"
    assert question["choices"] == ["etl", "Stop here"]


# ---------------------------------------------------------------------------- commands


def menu(ctx: FakeContext, answer: str | None, inject_ok: bool = True) -> dict[str, list[Any]]:
    """Give the fake Hermes context the menu / message features of Hermes v0.23."""
    seen: dict[str, list[Any]] = {"asked": [], "posted": [], "injected": []}

    def ask_choice(question: str, choices: list[Any]) -> str | None:
        seen["asked"].append((question, choices))
        return answer

    def post_message(text: str) -> Any:
        seen["posted"].append(text)
        return SimpleNamespace(ok=True)

    def inject_message(content: str, **kwargs: Any) -> bool:
        seen["injected"].append(content)
        return inject_ok

    ctx.ask_choice = ask_choice  # type: ignore[attr-defined]
    ctx.post_message = post_message  # type: ignore[attr-defined]
    ctx.inject_message = inject_message  # type: ignore[attr-defined]
    return seen


def test_audit_command_report_menu_and_hand_over(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    seen = menu(ctx, "etl")
    text = ctx.commands["airflow-audit"]["handler"]("all 24h", session_id="s-1")
    assert seen["posted"][0].startswith("# Airflow monitoring report")  # shown before the menu
    question, choices = seen["asked"][0]
    assert question == "Which DAG should I diagnose?"
    assert [c["label"] for c in choices] == ["etl", "Stop here"]
    assert 'airflow_audit_plan with dag_ids=["etl"]' in seen["injected"][0]
    assert text == "➡️ etl: starting."
    # the model is reminded of the choice before its next reply
    assert "Pending choice from a menu: etl" in hook_context(ctx)


def test_audit_command_without_a_menu_falls_back_to_text(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    text = ctx.commands["airflow-audit"]["handler"]("all 24h")  # no ask_choice on this host
    assert text.startswith("# Airflow monitoring report")
    assert "Which DAG should I diagnose?\n  1. etl" in text
    assert text.rstrip().endswith("Reply with a number or a name.")


def test_audit_command_stop_here(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    menu(ctx, "Stop here")
    text = ctx.commands["airflow-audit"]["handler"]("all 24h")
    assert "stopped" in text and "Not diagnosed: etl" in text
    assert hook_context(ctx) == ""  # a stopped audit is not pushed to the model


def test_audit_command_when_injection_is_refused(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    menu(ctx, "etl", inject_ok=False)
    text = ctx.commands["airflow-audit"]["handler"]("all 24h")
    assert text.startswith("Choice saved: etl. Send any message")


def test_resume_and_status(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    plan_train(airflow, api_version, ctx)
    status = ctx.commands["airflow-status"]["handler"]("")
    assert "[-------] 0/1 done · next: 1.1" in status and "DAG 1 — train" in status
    seen = menu(ctx, "Resume (1 left)")
    text = ctx.commands["airflow-audit"]["handler"]("")
    assert seen["asked"][0][0].startswith("Resume audit #")
    assert "airflow_audit_plan with no arguments" in seen["injected"][0]
    assert text.endswith("➡️ Resume (1 left): starting.")


def test_status_without_audit(ctx: FakeContext) -> None:
    text = ctx.commands["airflow-status"]["handler"]("")
    assert text == "No audit in progress. Start one with /airflow-audit."
