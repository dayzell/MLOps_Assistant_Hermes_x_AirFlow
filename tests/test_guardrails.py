"""Protected remediation: guardrails, dry-run default, critical refusal, auto-retry policy,
approval hook and audit journal.

DAG graph (payloads.tasks): extract -> transform -> [load, quality_check]; load -> notify.
"""

from __future__ import annotations

import inspect
import json
from typing import Any

import httpx
import payloads as p
import pytest
import respx
from airflow_mlops import storage
from airflow_mlops.config import Settings
from airflow_mlops.guardrails import evaluate_auto_policy, make_pre_tool_call_hook, required_backoff
from harness import FakeContext, make_context

RUN = "/dags/train/dagRuns/run_1"
DOWNSTREAM = {
    "extract": ["transform"],
    "transform": ["load", "quality_check"],
    "load": ["notify"],
    "quality_check": [],
    "notify": [],
}
TRANSIENT_LOG = "Traceback (most recent call last):\nConnectionResetError: Connection reset by peer"
CRITICAL_LOG = "Traceback (most recent call last):\nKeyError: 'target'"


def descendants(task_id: str) -> set[str]:
    found: set[str] = set()
    stack = list(DOWNSTREAM[task_id])
    while stack:
        current = stack.pop()
        if current not in found:
            found.add(current)
            stack.extend(DOWNSTREAM[current])
    return found


def mock_env(
    airflow: respx.MockRouter,
    api_version: str,
    states: dict[str, str],
    logs: dict[str, str] | None = None,
) -> respx.Route:
    """Mock tasks, task instances, logs and a realistic clearTaskInstances endpoint."""
    airflow.get("/dags/train/tasks").respond(json=p.tasks(api_version))
    tis = [p.task_instance(api_version, t, s, try_number=1) for t, s in states.items()]
    airflow.get(f"{RUN}/taskInstances").respond(
        json={"task_instances": tis, "total_entries": len(tis)}
    )
    for task_id, text in (logs or {}).items():
        airflow.get(f"{RUN}/taskInstances/{task_id}/logs/1").respond(
            text=text, headers={"content-type": "text/plain"}
        )

    def clear(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        targets = list(body["task_ids"])
        if body["include_downstream"]:
            for task_id in list(targets):
                targets.extend(sorted(descendants(task_id)))
        if body["only_failed"]:
            targets = [t for t in targets if states.get(t) in ("failed", "upstream_failed")]
        targets = list(dict.fromkeys(targets))
        if api_version == "v1":
            items = [
                {
                    "task_id": t,
                    "dag_id": "train",
                    "dag_run_id": "run_1",
                    "execution_date": "2026-11-07T00:00:00+00:00",
                }
                for t in targets
            ]
        else:
            items = [p.task_instance(api_version, t, states.get(t, "failed")) for t in targets]
        return httpx.Response(200, json={"task_instances": items, "total_entries": len(items)})

    return airflow.post("/dags/train/clearTaskInstances").mock(side_effect=clear)


def writable(api_version: str, **config: Any) -> FakeContext:
    auth = "jwt" if api_version == "v2" else "basic"
    return make_context(api_version=api_version, auth_mode=auth, read_only=False, **config)


def bodies(route: respx.Route) -> list[dict[str, Any]]:
    return [json.loads(call.request.content) for call in route.calls]


def transient_cache(task_id: str = "transform") -> None:
    storage.save_diagnosis(
        "train",
        "run_1",
        task_id,
        map_index=-1,
        try_number=1,
        category="transient",
        rule_ids=["connection_reset"],
        justification="j",
    )


# --------------------------------------------------------------------------- exposure


def test_read_only_default_hides_and_refuses(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    tool = ctx.tools["airflow_clear_tasks"]
    assert tool["check_fn"]() is False  # read_only defaults to true
    route = mock_env(airflow, api_version, {"transform": "failed"})
    result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["transform"])
    assert result["ok"] is False and result["error"]["type"] == "guardrail"
    assert "read_only" in result["error"]["message"]
    assert route.call_count == 0


def test_write_tool_visible_when_not_read_only(api_version: str, airflow: respx.MockRouter) -> None:
    assert writable(api_version).tools["airflow_clear_tasks"]["check_fn"]() is True


# --------------------------------------------------------------------------- hard guardrails


def test_empty_task_ids_refused(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(airflow, api_version, {"transform": "failed"})
    for task_ids in ([], None):
        result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=task_ids)
        assert result["error"]["type"] == "invalid_argument"
    assert route.call_count == 0


def test_dry_run_is_the_default(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(
        airflow,
        api_version,
        {"transform": "failed", "load": "upstream_failed"},
        logs={"transform": TRANSIENT_LOG},
    )
    result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["transform"])
    assert result["ok"] is True, result
    assert result["dry_run"] is True and result["outcome"] == "dry_run"
    assert [ti["task_id"] for ti in result["task_instances"]] == ["transform"]
    assert [b["dry_run"] for b in bodies(route)] == [True]  # nothing was changed
    assert result["classification"] == {"transform": "transient"}
    entry = storage.list_audit_entries(0)[0]
    assert entry["outcome"] == "dry_run" and entry["dry_run"] is True


def test_whole_dag_is_refused(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(airflow, api_version, {t: "failed" for t in DOWNSTREAM})
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=list(DOWNSTREAM),
        dry_run=False,
        reason="retry all",
        confirm_critical=True,
    )
    assert result["error"]["type"] == "guardrail"
    assert "whole DAG" in result["error"]["message"] or "every task" in result["error"]["message"]
    assert route.call_count == 0
    assert storage.list_audit_entries(0)[0]["outcome"] == "refused"


def test_downstream_expansion_to_whole_dag_is_refused(
    api_version: str, airflow: respx.MockRouter
) -> None:
    ctx = writable(api_version)
    transient_cache("extract")
    route = mock_env(airflow, api_version, {t: "failed" for t in DOWNSTREAM})
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["extract"],
        include_downstream=True,
        only_failed=True,
        dry_run=False,
        reason="retry",
    )
    assert result["error"]["type"] == "guardrail"
    assert [b["dry_run"] for b in bodies(route)] == [True]  # previewed, never executed


def test_unknown_task_refused(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    mock_env(airflow, api_version, {"transform": "failed"})
    result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["nope"])
    assert result["error"]["type"] == "guardrail" and "Unknown task" in result["error"]["message"]


def test_reason_required_for_real_clear(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(airflow, api_version, {"transform": "failed"})
    result = ctx.call(
        "airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["transform"], dry_run=False
    )
    assert result["error"]["type"] == "guardrail" and "reason" in result["error"]["message"]
    assert route.call_count == 0


def test_dag_outside_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version, dag_allowlist=["etl_*"])
    route = mock_env(airflow, api_version, {"transform": "failed"})
    result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["transform"])
    assert result["error"]["type"] == "dag_not_allowed"
    assert route.call_count == 0


# --------------------------------------------------------------------------- critical failures


def test_critical_failure_refused_without_confirmation(
    api_version: str, airflow: respx.MockRouter
) -> None:
    ctx = writable(api_version)
    route = mock_env(
        airflow, api_version, {"transform": "failed"}, logs={"transform": CRITICAL_LOG}
    )
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="retry",
    )
    assert result["error"]["type"] == "guardrail"
    assert "CRITICAL" in result["error"]["message"]
    assert route.call_count == 0
    # classified on the fly and cached for the next calls
    assert storage.get_diagnosis("train", "run_1", "transform").category == "critical"  # type: ignore[union-attr]


def test_critical_failure_with_confirmation(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(
        airflow, api_version, {"transform": "failed"}, logs={"transform": CRITICAL_LOG}
    )
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="fix deployed",
        confirm_critical=True,
    )
    assert result["ok"] is True and result["outcome"] == "cleared"
    assert [b["dry_run"] for b in bodies(route)] == [True, False]


def test_dry_run_warns_about_critical(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    mock_env(airflow, api_version, {"transform": "failed"}, logs={"transform": CRITICAL_LOG})
    result = ctx.call("airflow_clear_tasks", dag_id="train", run_id="run_1", task_ids=["transform"])
    assert result["ok"] is True
    assert any("critical" in w for w in result["warnings"])


# --------------------------------------------------------------------------- real clear


def test_real_clear_is_targeted_counted_and_journaled(
    api_version: str, airflow: respx.MockRouter
) -> None:
    ctx = writable(api_version)
    transient_cache()
    route = mock_env(airflow, api_version, {"transform": "failed", "load": "upstream_failed"})
    raw = ctx.tools["airflow_clear_tasks"]["handler"](
        {
            "dag_id": "train",
            "run_id": "run_1",
            "task_ids": ["transform"],
            "dry_run": False,
            "reason": "network blip",
        },
        session_id="sess-42",
        conversation_id="conv-7",
        task_id="t-1",
    )
    result = json.loads(raw)
    assert result["ok"] is True and result["outcome"] == "cleared"
    assert result["approval"] == "human (Hermes approval gate)"
    real = bodies(route)[-1]
    assert real == {
        "dry_run": False,
        "dag_run_id": "run_1",
        "task_ids": ["transform"],
        "only_failed": True,
        "only_running": False,
        "include_upstream": False,
        "include_downstream": False,
        "include_future": False,
        "include_past": False,
        "reset_dag_runs": True,
    }
    assert result["retry_attempts"] == {"transform": {"attempts": 1, "auto_attempts": 0}}
    entry = storage.list_audit_entries(0)[0]
    assert entry["id"] == result["audit_id"]
    assert entry["outcome"] == "cleared"  # the pending entry was completed
    assert (entry["session_id"], entry["conversation_id"]) == ("sess-42", "conv-7")
    assert entry["reason"] == "network blip" and entry["auto"] is False
    assert entry["task_ids"] == ["transform"]


def test_nothing_to_clear(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    route = mock_env(airflow, api_version, {"transform": "success"})
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="retry",
    )
    assert result["outcome"] == "nothing_to_clear"
    assert [b["dry_run"] for b in bodies(route)] == [True]


def test_no_write_without_audit_log(
    api_version: str, airflow: respx.MockRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = writable(api_version)
    transient_cache()
    route = mock_env(airflow, api_version, {"transform": "failed"})

    def broken(**kwargs: Any) -> int:
        raise OSError("disk full")

    monkeypatch.setattr(storage, "add_audit_entry", broken)
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="retry",
    )
    assert result["error"]["type"] == "guardrail" and "audit log" in result["error"]["message"]
    assert [b["dry_run"] for b in bodies(route)] == [True]  # the real clear never happened


def test_airflow_error_during_clear_is_journaled(
    api_version: str, airflow: respx.MockRouter
) -> None:
    ctx = writable(api_version)
    transient_cache()
    mock_env(airflow, api_version, {"transform": "failed"})
    airflow.post("/dags/train/clearTaskInstances").mock(
        side_effect=[
            httpx.Response(200, json={"task_instances": [{"task_id": "transform"}]}),
            httpx.Response(500, json={"detail": "db down"}),
        ]
    )
    result = ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="retry",
    )
    assert result["error"]["type"] == "server"
    assert storage.list_audit_entries(0)[0]["outcome"] == "error"


# --------------------------------------------------------------------------- auto-retry policy


def test_required_backoff() -> None:
    settings = Settings(auto_retry_backoff_seconds=300)
    assert [required_backoff(settings, n) for n in range(4)] == [0, 300, 600, 1200]


def test_policy_disabled_by_default() -> None:
    transient_cache()
    decision = evaluate_auto_policy(Settings(), "train", "run_1", ["transform"], only_failed=True)
    assert decision.allowed is False and "disabled" in decision.reason


def test_policy_limits_backoff_and_escalation() -> None:
    settings = Settings(
        auto_remediation_enabled=True, auto_retry_max_attempts=2, auto_retry_backoff_seconds=300
    )
    args = ("train", "run_1", ["transform"])
    assert "not diagnosed" in evaluate_auto_policy(settings, *args, only_failed=True).reason
    transient_cache()
    assert evaluate_auto_policy(settings, *args, only_failed=True, now=1000).allowed
    assert not evaluate_auto_policy(settings, *args, only_failed=False, now=1000).allowed

    storage.record_retry("train", "run_1", ["transform"], auto=True, now=1000)
    early = evaluate_auto_policy(settings, *args, only_failed=True, now=1100)
    assert not early.allowed and "backoff" in early.reason
    assert evaluate_auto_policy(settings, *args, only_failed=True, now=1301).allowed

    storage.record_retry("train", "run_1", ["transform"], auto=True, now=1301)
    capped = evaluate_auto_policy(settings, *args, only_failed=True, now=10_000)
    assert not capped.allowed and "escalate to a human" in capped.reason


def test_policy_refuses_critical() -> None:
    settings = Settings(auto_remediation_enabled=True)
    storage.save_diagnosis(
        "train",
        "run_1",
        "transform",
        map_index=-1,
        try_number=1,
        category="critical",
        rule_ids=[],
        justification="",
    )
    decision = evaluate_auto_policy(settings, "train", "run_1", ["transform"], only_failed=True)
    assert not decision.allowed and "critical" in decision.reason


def test_auto_retry_end_to_end(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version, auto_remediation_enabled=True, auto_retry_max_attempts=1)
    hook = ctx.hooks["pre_tool_call"][0]
    transient_cache()
    mock_env(airflow, api_version, {"transform": "failed"})
    args = {
        "dag_id": "train",
        "run_id": "run_1",
        "task_ids": ["transform"],
        "dry_run": False,
        "reason": "auto retry of transient failure",
    }
    assert hook(tool_name="airflow_clear_tasks", args=args, task_id="x") is None  # no approval
    result = ctx.call("airflow_clear_tasks", **args)
    assert result["approval"] == "auto-policy"
    assert result["retry_attempts"]["transform"]["auto_attempts"] == 1
    assert storage.list_audit_entries(0)[0]["auto"] is True
    # Limit reached: the next call is escalated to a human, never looped automatically.
    decision = hook(tool_name="airflow_clear_tasks", args=args)
    assert decision["action"] == "approve"
    assert "escalate to a human" in decision["message"]


# --------------------------------------------------------------------------- approval hook


def test_hook_signature_accepts_kwargs() -> None:
    hook = make_pre_tool_call_hook(Settings)
    params = inspect.signature(hook).parameters.values()
    assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params)


def test_hook_ignores_other_tools_and_dry_runs() -> None:
    hook = make_pre_tool_call_hook(lambda: Settings(read_only=False))
    assert hook(tool_name="terminal", args={"command": "ls"}) is None
    assert hook(tool_name="airflow_get_dag", args={"dag_id": "x"}) is None
    assert hook(tool_name="airflow_clear_tasks", args={"dag_id": "x", "task_ids": ["a"]}) is None
    assert hook(tool_name="airflow_clear_tasks", args={"dag_id": "x", "dry_run": True}) is None


def test_hook_requires_human_approval_for_real_clears() -> None:
    hook = make_pre_tool_call_hook(lambda: Settings(read_only=False))
    decision = hook(
        tool_name="airflow_clear_tasks",
        args={
            "dag_id": "train",
            "run_id": "run_1",
            "task_ids": ["transform"],
            "dry_run": False,
            "reason": "blip",
        },
        session_id="s",
    )
    assert decision["action"] == "approve"
    assert decision["rule_key"] == "airflow_clear_tasks:train:run_1"
    assert "[transform]" in decision["message"] and "blip" in decision["message"]


def test_hook_blocks_read_only_and_allowlist() -> None:
    args = {"dag_id": "train", "run_id": "r", "task_ids": ["t"], "dry_run": False}
    read_only = make_pre_tool_call_hook(lambda: Settings(read_only=True))
    assert read_only(tool_name="airflow_clear_tasks", args=args)["action"] == "block"
    outside = make_pre_tool_call_hook(lambda: Settings(read_only=False, dag_allowlist=("etl_*",)))
    assert outside(tool_name="airflow_clear_tasks", args=args)["action"] == "block"


def test_hook_fails_closed() -> None:
    def broken() -> Settings:
        raise RuntimeError("boom")

    hook = make_pre_tool_call_hook(broken)
    decision = hook(tool_name="airflow_clear_tasks", args={"dry_run": False})
    assert decision["action"] == "block"


# --------------------------------------------------------------------------- report integration


def test_actions_show_up_in_report(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = writable(api_version)
    transient_cache()
    mock_env(airflow, api_version, {"transform": "failed"})
    ctx.call(
        "airflow_clear_tasks",
        dag_id="train",
        run_id="run_1",
        task_ids=["transform"],
        dry_run=False,
        reason="blip",
    )
    entries = storage.list_audit_entries(0, dag_ids=["train"])
    assert entries[0]["tool"] == "airflow_clear_tasks" and entries[0]["outcome"] == "cleared"
