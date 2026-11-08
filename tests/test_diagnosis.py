"""airflow_diagnose_run end to end against mocked Airflow v1 / v2 APIs.

DAG graph (see payloads.tasks): extract -> transform -> [load, quality_check];
load -> notify (trigger_rule all_done).
"""

from __future__ import annotations

import json
from typing import Any

import payloads as p
import respx
from airflow_mlops import storage
from harness import FakeContext, make_context

RUN = "/dags/train/dagRuns/run_1"

TRANSIENT_LOG = (
    "INFO - fetching features\n"
    "ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    '  File "/opt/airflow/dags/train.py", line 10, in fetch\n'
    "ConnectionResetError: [Errno 104] Connection reset by peer\n"
)


def mock_run(
    airflow: respx.MockRouter,
    api_version: str,
    states: dict[str, str],
    *,
    logs: dict[str, Any] | None = None,
    run_state: str = "failed",
) -> None:
    """Mock run, task instances, tasks, details, logs and XComs of the 'train' DAG."""
    airflow.get(RUN).respond(json=p.dag_run(api_version, "train", "run_1", run_state))
    tis = [
        p.task_instance(
            api_version,
            task_id,
            state,
            try_number=0 if state in ("upstream_failed", "skipped") else 2,
        )
        for task_id, state in states.items()
    ]
    airflow.get(f"{RUN}/taskInstances").respond(
        json={"task_instances": tis, "total_entries": len(tis)}
    )
    airflow.get("/dags/train/tasks").respond(json=p.tasks(api_version))
    airflow.get("/dags/train/details").respond(json=p.dag_details(api_version, "train"))
    for task_id, body in (logs or {}).items():
        route = airflow.get(f"{RUN}/taskInstances/{task_id}/logs/2")
        if isinstance(body, str):
            route.respond(text=body, headers={"content-type": "text/plain"})
        else:
            route.respond(json=body)
    # XComs: extract pushed its output; every other task has none.
    airflow.get(f"{RUN}/taskInstances/extract/xcomEntries").respond(
        json={"xcom_entries": [{"key": "return_value", "map_index": -1}], "total_entries": 1}
    )
    airflow.get(f"{RUN}/taskInstances/extract/xcomEntries/return_value").respond(
        json={"key": "return_value", "value": {"rows": 1200, "db_password": "hunter2"}}
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(
        json={"xcom_entries": [], "total_entries": 0}
    )


def test_critical_failure_root_secondary_and_impacted(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_run(
        airflow,
        api_version,
        {
            "extract": "success",
            "transform": "failed",
            "load": "upstream_failed",
            "quality_check": "upstream_failed",
            "notify": "failed",
        },
        logs={"transform": p.log_body(api_version), "notify": "ERROR - smtp boom"},
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["ok"] is True, result
    assert result["status"] == "failed"

    # Root vs secondary vs impacted
    assert [r["task_id"] for r in result["root_failures"]] == ["transform"]
    assert result["secondary_failures"] == [
        {
            "task_id": "notify",
            "map_index": -1,
            "state": "failed",
            "trigger_rule": "all_done",
            "failed_upstream": ["transform"],
        }
    ]
    impacted = {i["task_id"]: i for i in result["impacted"]}
    assert set(impacted) == {"load", "quality_check"}
    assert impacted["load"]["caused_by"] == ["transform"]

    root = result["root_failures"][0]
    assert root["classification"]["category"] == "critical"
    assert root["classification"]["matched_rules"][0]["rule_id"] == "python_code_error"
    assert "KeyError" in root["error_block"]
    assert root["upstream"] == [
        {"task_id": "extract", "trigger_rule": "all_success", "operator": "PythonOperator"}
    ]
    assert {d["task_id"] for d in root["downstream"]} == {"load", "quality_check"}
    # XComs of the failed task and of its input producer, secrets masked
    assert root["xcoms"]["extract"]["entries"][0]["value"] == {
        "rows": 1200,
        "db_password": "[REDACTED]",
    }
    assert root["xcoms"]["transform"]["entries"] == []

    summary = result["summary"]
    assert summary["category"] == "critical"
    assert summary["requires_human"] is True
    assert summary["retry_candidates"] == []
    assert result["dag_params"]["db_password"] == "[REDACTED]"
    payload = json.dumps(result)
    assert "hunter2" not in payload and "pa55word" not in payload

    cached = storage.get_diagnosis("train", "run_1", "transform")
    assert cached is not None and cached.category == "critical"
    assert result["cached"] is True


def test_transient_failure_lists_retry_candidates(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_run(
        airflow,
        api_version,
        {"extract": "failed", "transform": "upstream_failed"},
        logs={"extract": TRANSIENT_LOG},
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    summary = result["summary"]
    assert summary["category"] == "transient"
    assert summary["requires_human"] is False
    assert summary["retry_candidates"] == [{"task_id": "extract", "map_index": -1}]
    assert "never the whole DAG" in summary["guidance"]
    assert storage.get_diagnosis("train", "run_1", "extract").category == "transient"  # type: ignore[union-attr]


def test_no_failure(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    mock_run(
        airflow, api_version, {"extract": "success", "transform": "success"}, run_state="success"
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["ok"] is True
    assert result["status"] == "no_failure"
    assert result["state_counts"] == {"success": 2}
    assert "root_failures" not in result


def test_retrying_only(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    mock_run(
        airflow,
        api_version,
        {"extract": "up_for_retry"},
        logs={"extract": TRANSIENT_LOG},
        run_state="running",
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["status"] == "retrying"
    assert result["root_failures"] == []
    assert result["retrying"][0]["classification"]["category"] == "transient"
    assert result["summary"]["category"] == "transient"


def test_log_error_gives_unknown(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_run(airflow, api_version, {"extract": "failed"})
    airflow.get(f"{RUN}/taskInstances/extract/logs/2").respond(404, json={"detail": "no log"})
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    root = result["root_failures"][0]
    assert root["log_error"]["type"] == "not_found"
    assert root["classification"]["category"] == "unknown"
    assert result["summary"]["requires_human"] is True


def test_skipped_without_failed_upstream_is_flagged_as_possibly_intentional(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_run(
        airflow,
        api_version,
        {
            "extract": "success",
            "transform": "success",
            "quality_check": "skipped",
            "load": "failed",
        },
        logs={"load": TRANSIENT_LOG},
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    skipped = next(i for i in result["impacted"] if i["task_id"] == "quality_check")
    assert skipped["caused_by"] == []
    assert "intentional" in skipped["note"]


def test_details_are_bounded(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext, monkeypatch: Any
) -> None:
    from airflow_mlops import diagnosis

    monkeypatch.setattr(diagnosis, "_MAX_DETAILED_FAILURES", 1)
    mock_run(
        airflow,
        api_version,
        {"extract": "failed", "transform": "up_for_retry"},
        logs={"extract": TRANSIENT_LOG},
    )
    transform_log = airflow.get(f"{RUN}/taskInstances/transform/logs/2").respond(text="x")
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["root_failures"][0]["classification"]["category"] == "transient"
    omitted = result["retrying"][0]
    assert omitted["details_omitted"] is True and omitted["classification"] is None
    assert transform_log.call_count == 0  # no API call spent on omitted failures


def test_diagnose_refuses_dag_outside_allowlist(
    api_version: str, airflow: respx.MockRouter
) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", dag_allowlist=["etl_*"])
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["ok"] is False and result["error"]["type"] == "dag_not_allowed"
    assert len(airflow.calls) == 0


def test_run_not_found(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    airflow.get(RUN).respond(404, json={"detail": "DAGRun not found"})
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["ok"] is False and result["error"]["type"] == "not_found"


def test_masked_failure_is_flagged(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_run(
        airflow,
        api_version,
        {"extract": "success", "transform": "failed", "notify": "success"},
        logs={"transform": TRANSIENT_LOG},
        run_state="success",
    )
    result = ctx.call("airflow_diagnose_run", dag_id="train", run_id="run_1")
    assert result["status"] == "failed"
    assert "trigger_rule" in result["masked_failure"]
