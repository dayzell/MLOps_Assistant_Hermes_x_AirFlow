"""Guided audit end to end: tools, observers, /airflow-audit, /airflow-status, pre_llm_call.
Rule: report → diagnosis → test → problems left or Stop; no code change; archived when left."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import payloads as p
import respx
from airflow_mlops import workflow as wf
from airflow_mlops.audit_tools import AuditTools
from airflow_mlops.runtime import PluginRuntime
from airflow_mlops.tools import ReadTools
from harness import FakeContext, make_context
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


def tools_for(ctx: FakeContext) -> AuditTools:
    runtime = PluginRuntime(ctx)
    return AuditTools(runtime, ReadTools(runtime))


# ---------------------------------------------------------------------------- tools


def test_plan_numbers_the_problems_and_asks_which_first(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    result = plan_train(airflow, api_version, ctx)
    assert result["ok"] is True, result
    item = result["items"][0]
    assert (item["number"], item["task_id"], item["kind"]) == ("1.1", "transform", "code_fix")
    assert result["plan_text"].startswith("DAG 1 — train")
    assert result["next_question"]["question"] == "Which problem first?"


def test_select_test_and_end_of_audit(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    selected = ctx.call("airflow_audit_select", selection="1.1")
    assert "Never edit a file during an audit" in selected["next_action"]
    # the only problem passes its test: the audit is archived by itself
    done = ctx.call("airflow_audit_progress", item="1.1", state="passed")
    assert done["archived"] is True
    assert done["summary"].startswith("🗂️ Audit #")
    assert wf.load_current() is None  # no audit in progress any more
    # nothing to resume: the agent is told to start a new audit
    assert ctx.call("airflow_audit_plan")["error"]["type"] == "invalid_argument"


def test_stop_here_archives_and_summarizes(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    stopped = ctx.call("airflow_audit_progress", state="stop")
    assert stopped["archived"] is True and "• 1.1" in stopped["summary"]  # not tested
    assert ctx.call("airflow_audit_select", selection="1.1")["error"]["type"] == "invalid_argument"


def test_bad_inputs_are_explained(ctx: FakeContext) -> None:
    assert ctx.call("airflow_audit_plan")["error"]["type"] == "invalid_argument"
    assert ctx.call("airflow_audit_select", selection="1.1")["error"]["type"] == "invalid_argument"
    assert ctx.call("airflow_audit_progress", state="weird")["error"]["type"] == "invalid_argument"


def test_plan_respects_the_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, dag_allowlist=["etl*"])
    result = ctx.call("airflow_audit_plan", dag_ids=["train"])
    assert result["error"]["type"] == "dag_not_allowed"


def test_the_sandbox_result_is_the_test(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    ctx.call("airflow_audit_select", selection="1.1")
    tools = tools_for(ctx)
    step = tools.observe_sandbox("train", "transform", "import", {"passed": True})
    assert step is not None and "structure" in step["next_action"]
    commands = {"apply": "git -C d apply x.patch", "check": "c", "undo": "u"}
    step = tools.observe_sandbox(
        "train",
        "transform",
        "structure",
        {"passed": True, "patch_file": "x.patch", "apply_commands": commands},
    )
    assert step is not None and step["state"] == "passed"
    assert step["archived"] is True  # last problem tested: the audit ends
    assert "apply it on request: git -C d apply x.patch" in step["summary"]
    last = wf.load_audit()
    assert last is not None and last.item("1.1").result["level"] == "structure"  # type: ignore[union-attr]


def test_a_failed_sandbox_test_offers_to_revise(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    ctx.call("airflow_audit_select", selection="1.1")
    step = tools_for(ctx).observe_sandbox("train", "transform", "structure", {"passed": False})
    assert step is not None and step["state"] == "failed"
    assert step["next_question"]["choices"][0] == "Revise the fix"


def test_a_real_retry_is_recorded_then_checked(ctx: FakeContext) -> None:
    audit = wf.create_audit("all", "24h", [], "s-1")
    audit.items += wf.items_from_diagnosis(
        audit,
        {
            "dag_id": "fetch_dag",
            "run_id": "r1",
            "root_failures": [
                {
                    "task_id": "fetch",
                    "classification": {"category": "transient", "matched_rules": []},
                    "error_block": "ConnectionResetError: reset",
                }
            ],
        },
    )
    wf.save_audit(audit)
    tools = tools_for(ctx)
    assert tools.observe_clear("fetch_dag", ["fetch"], {"outcome": "dry_run"}) is None
    step = tools.observe_clear("fetch_dag", ["fetch"], {"outcome": "cleared"})
    assert step is not None and step["state"] == "retried"
    assert "airflow_get_run_status" in step["next_action"]


def test_observers_never_fail_without_an_audit(ctx: FakeContext) -> None:
    tools = tools_for(ctx)
    assert tools.observe_sandbox("x", None, "import", {"passed": True}) is None
    assert tools.observe_clear("x", ["t"], {"outcome": "cleared"}) is None
    assert tools.context("s") is None
    assert tools.audit_in_progress() is False


def test_the_report_tool_answers_without_a_question(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """Real session (10/11, Desktop): "list my DAGs" returned a report with a question to ask
    through a menu tool; the model called the menu without writing anything, and the report
    was displayed three minutes later. The report alone is the answer."""
    mock_airflow(airflow, api_version)
    result = ctx.call("airflow_build_report", period="24h")
    assert "next_question" not in result
    assert "tell me which DAG(s) to diagnose (etl)" in result["markdown"]


def test_questions_are_asked_as_text_never_through_a_menu_tool(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    question = plan_train(airflow, api_version, ctx)["next_question"]
    assert "clarify" not in question["how_to_ask"]
    assert "END your reply with this question" in question["how_to_ask"]
    assert "clarify" not in hook_context(ctx)


# ---------------------------------------------------------------------------- the command


def hermes_features(ctx: FakeContext) -> dict[str, list[Any]]:
    """Give the fake Hermes context a menu and a message channel, and record their use."""
    seen: dict[str, list[Any]] = {"asked": [], "posted": []}

    def ask_choice(question: str, choices: list[Any]) -> str | None:
        seen["asked"].append(question)
        return None

    def post_message(text: str) -> Any:
        seen["posted"].append(text)
        return SimpleNamespace(ok=True)

    ctx.ask_choice = ask_choice  # type: ignore[attr-defined]
    ctx.post_message = post_message  # type: ignore[attr-defined]
    return seen


def test_the_command_answers_at_once_with_the_report_and_the_question(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """Hermes Desktop gives up on a slash command after 30 s and then runs it again: the
    command never waits for a menu and never posts a separate message (on Desktop it would be
    a notification, not a message of the conversation)."""
    mock_airflow(airflow, api_version)
    seen = hermes_features(ctx)
    text = ctx.commands["airflow-audit"]["handler"]("all 24h", platform="local", session_id="s-1")
    assert text.startswith("# Airflow monitoring report")
    assert text.count("# Airflow monitoring report") == 1  # once, not twice
    assert "Which DAG should I diagnose?\n  1. etl" in text
    assert text.rstrip().endswith("Reply with a number or a name.")
    assert seen == {"asked": [], "posted": []}
    current = wf.load_current()
    assert current is not None and current.status == wf.AUDIT_OPEN


def test_the_question_of_the_command_stays_known_with_its_choices(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """Real session (10/11): the saved question had lost its choices, so the model made up
    its own ("the options aren't clearly specified")."""
    mock_airflow(airflow, api_version)
    ctx.commands["airflow-audit"]["handler"]("all 24h", session_id="s-1")
    reminder = hook_context(ctx, "s-1")
    assert "Which DAG should I diagnose?" in reminder
    assert '1. etl -> Call airflow_audit_plan with dag_ids=["etl"].' in reminder
    assert "Stop here -> Call airflow_audit_progress" in reminder


def test_the_reminder_stays_in_the_conversation_of_the_audit(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    ctx.commands["airflow-audit"]["handler"]("all 24h", session_id="s-1")
    assert hook_context(ctx, "s-1") != ""
    assert hook_context(ctx, "another-chat") == ""  # nor a scheduled job


def test_no_resume_a_new_audit_archives_the_open_one(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    old = wf.load_current()
    assert old is not None
    mock_airflow(airflow, api_version)
    text = ctx.commands["airflow-audit"]["handler"]("")
    assert "Resume" not in text
    archived = wf.load_audit(old.id)
    assert archived is not None and archived.status == wf.AUDIT_ARCHIVED


def test_status(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    assert ctx.commands["airflow-status"]["handler"]("") == (
        "No audit yet. Start one with /airflow-audit."
    )
    plan_train(airflow, api_version, ctx)
    status = ctx.commands["airflow-status"]["handler"]("")
    assert "in progress" in status and "DAG 1 — train" in status
    ctx.call("airflow_audit_progress", state="stop")
    after = ctx.commands["airflow-status"]["handler"]("")
    assert after.startswith("No audit in progress. Last one:\n🗂️ Audit #")


def test_the_archived_results_stay_known_in_the_same_conversation(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    plan_train(airflow, api_version, ctx)
    audit = wf.load_current()
    assert audit is not None
    audit.session_id = "s-1"
    wf.save_audit(audit)
    ctx.call("airflow_audit_progress", state="stop")
    assert "last audit" in hook_context(ctx, "s-1")
    assert hook_context(ctx, "another-chat") == ""


def test_the_stop_tool_offers_to_apply(ctx: FakeContext) -> None:
    audit = wf.create_audit("all", "24h", [], "s-1")
    audit.items.append(
        wf.Item("1.1", 1, 1, "train", "r", "train", "critical", "code_fix", "KeyError", "passed")
    )
    audit.items[0].result = {"level": "structure", "apply": "git -C d apply a.patch"}
    wf.save_audit(audit)
    result = ctx.call("airflow_audit_progress", state="stop")
    assert result["archived"] is True
    assert result["next_question"]["choices"][0] == "Apply 1.1 (your approval required)"
