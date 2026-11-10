"""Plan v2, step 5: scheduled watch (news only), postmortem facts, night-watch blueprint."""

from __future__ import annotations

from typing import Any

import payloads as p
import respx
from airflow_mlops import storage
from airflow_mlops import workflow as wf
from airflow_mlops.watch import BLUEPRINT
from harness import FakeContext, make_context


def coll(key: str, items: list[Any]) -> dict[str, Any]:
    return {key: items, "total_entries": len(items)}


def mock_dags(airflow: respx.MockRouter, api_version: str, states: dict[str, str]) -> None:
    """``states``: dag_id -> 'failed' | 'success' | 'masked' (success run, a task failed)."""
    airflow.get("/dags").respond(json=coll("dags", [p.dag(api_version, d) for d in states]))
    for dag_id, state in states.items():
        run_state = "failed" if state == "failed" else "success"
        run_id = f"{dag_id}_{state}"
        airflow.get(f"/dags/{dag_id}/dagRuns").respond(
            json=coll("dag_runs", [p.dag_run(api_version, dag_id, run_id, run_state)])
        )
        tis = [p.task_instance(api_version, "t", "failed" if state != "success" else "success")]
        airflow.get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances").respond(
            json=coll("task_instances", tis)
        )


def test_first_check_reports_incidents_then_stays_silent(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_dags(airflow, api_version, {"etl": "failed", "train": "masked", "ok": "success"})
    first = ctx.call("airflow_watch_check")
    assert first["first_check"] is True and first["silent"] is False
    kinds = {i["dag_id"]: i["kind"] for i in first["new_incidents"]}
    assert kinds == {"etl": "failed run", "train": "hidden failure (run marked success)"}
    assert first["alert_text"].startswith("Airflow watch\n- NEW failed run: etl")
    assert "airflow_diagnose_run" in first["next"]

    again = ctx.call("airflow_watch_check")  # nothing changed
    assert again["silent"] is True and again["alert_text"] is None
    assert sorted(again["still_failing"]) == ["etl", "train"]
    assert "[SILENT]" in again["next"]


def test_recovery_and_new_failure_are_reported(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_dags(airflow, api_version, {"etl": "failed", "ok": "success"})
    ctx.call("airflow_watch_check")
    airflow.routes.clear()
    mock_dags(airflow, api_version, {"etl": "success", "ok": "failed"})
    result = ctx.call("airflow_watch_check")
    assert result["recovered"] == ["etl"]
    assert [i["dag_id"] for i in result["new_incidents"]] == ["ok"]
    assert "- RECOVERED: etl" in result["alert_text"]


def test_reset_reports_everything_again(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_dags(airflow, api_version, {"etl": "failed"})
    ctx.call("airflow_watch_check")
    assert ctx.call("airflow_watch_check")["silent"] is True
    assert ctx.call("airflow_watch_check", reset=True)["new_incidents"][0]["dag_id"] == "etl"


def test_allowlist_applies(api_version: str, airflow: respx.MockRouter) -> None:
    auth = "jwt" if api_version == "v2" else "basic"
    ctx = make_context(api_version=api_version, auth_mode=auth, dag_allowlist=["etl"])
    mock_dags(airflow, api_version, {"etl": "failed", "secret_dag": "failed"})
    result = ctx.call("airflow_watch_check")
    assert [i["dag_id"] for i in result["new_incidents"]] == ["etl"]


# ---------------------------------------------------------------------------- postmortem


def test_timeline_gives_the_facts_in_time_order(ctx: FakeContext) -> None:
    audit = wf.create_audit(
        "all", "24h", [{"dag_id": "train", "status": "failing", "reason": "x"}], "s"
    )
    audit.items.append(
        wf.Item("1.1", 1, 1, "train", "r1", "train", "critical", "code_fix", "KeyError", "passed")
    )
    wf.save_audit(audit)
    for tool, outcome, dry in (
        ("airflow_sandbox_test", "passed", True),
        ("dag_write", "matches_tested_fix", False),
        ("airflow_clear_tasks", "cleared", False),
    ):
        storage.add_audit_entry(
            tool=tool,
            dag_id="train",
            run_id="r1",
            task_ids=["train"],
            dry_run=dry,
            auto=False,
            reason="sandbox structure" if tool == "airflow_sandbox_test" else "mlops_demo.py",
            outcome=outcome,
        )
    result = ctx.call("airflow_audit_timeline")
    assert result["audit_id"] == audit.id and result["items"][0]["state"] == "passed"
    assert [e["what"] for e in result["events"]] == [
        "sandbox test (sandbox structure)",
        "DAG file written: mlops_demo.py",
        "retry",
    ]
    assert result["events"][2]["approval"] == "human"
    assert result["started_at"].endswith("Z")


def test_timeline_without_audit(ctx: FakeContext) -> None:
    assert ctx.call("airflow_audit_timeline")["error"]["type"] == "invalid_argument"


# ---------------------------------------------------------------------------- blueprint


def test_night_watch_blueprint_is_registered_when_hermes_supports_it() -> None:
    import airflow_mlops

    seen: dict[str, Any] = {}
    ctx = FakeContext({"base_url": "http://airflow.test"})
    ctx.register_automation_blueprint = (  # type: ignore[attr-defined]
        lambda key, **spec: seen.update(key=key, **spec)
    )
    airflow_mlops.register(ctx)
    assert seen["key"] == "night-watch"
    assert seen["schedule_template"] == "*/{interval_min} * * * *"
    assert "[SILENT]" in seen["prompt_template"]
    assert seen["skills"] == ("airflow-mlops:airflow-watch",)
    assert {s["name"] for s in seen["slots"]} == {"interval_min", "deliver"}
    assert BLUEPRINT["category"] == "monitoring"
