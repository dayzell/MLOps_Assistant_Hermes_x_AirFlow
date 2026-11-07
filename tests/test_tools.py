"""One test (at least) per read-only tool, against mocked Airflow v1 and v2 APIs."""

from __future__ import annotations

import json
from typing import Any

import payloads as p
import respx
from harness import FakeContext, make_context


def _collection(key: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {key: items, "total_entries": len(items)}


# --------------------------------------------------------------------------- list_dags


def test_list_dags_filters_by_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(
        api_version=api_version,
        auth_mode="basic",
        dag_allowlist=["train*", "etl_daily"],
    )
    airflow.get("/dags").respond(
        json=_collection(
            "dags",
            [
                p.dag(api_version, "train_model", tags=["ml"]),
                p.dag(api_version, "etl_daily", paused=True),
                p.dag(api_version, "finance_secret"),
            ],
        )
    )
    result = ctx.call("airflow_list_dags")
    assert result["ok"] is True
    assert [d["dag_id"] for d in result["dags"]] == ["train_model", "etl_daily"]
    assert result["hidden_by_allowlist"] == 1
    first = result["dags"][0]
    assert first["schedule"] == "0 * * * *"
    assert first["tags"] == ["ml"]
    assert first["next_run"].startswith("2026-11-08")
    assert result["dags"][1]["is_paused"] is True


def test_list_dags_passes_filters(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    route = airflow.get("/dags").respond(json=_collection("dags", []))
    result = ctx.call("airflow_list_dags", tags=["ml", "prod"], paused=False, limit=5)
    assert result["ok"] is True and result["dags"] == []
    params = route.calls.last.request.url.params
    assert params.get_list("tags") == ["ml", "prod"]
    assert params["paused"] == "false"
    stale_param = "exclude_stale" if api_version == "v2" else "only_active"
    assert params[stale_param] == "true"


def test_list_dags_paginates(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", page_size=2)
    dags = [p.dag(api_version, f"dag_{i}") for i in range(5)]

    def page(request: Any) -> Any:
        offset = int(request.url.params["offset"])
        limit = int(request.url.params["limit"])
        return respx.MockResponse(
            200, json={"dags": dags[offset : offset + limit], "total_entries": 5}
        )

    route = airflow.get("/dags").mock(side_effect=page)
    result = ctx.call("airflow_list_dags")
    assert result["total_visible"] == 5
    assert route.call_count == 3


# --------------------------------------------------------------------------- get_dag


def test_get_dag_returns_params_redacted_and_tasks(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/details").respond(json=p.dag_details(api_version, "train"))
    airflow.get("/dags/train/tasks").respond(json=p.tasks(api_version))
    result = ctx.call("airflow_get_dag", dag_id="train")
    assert result["ok"] is True
    assert result["dag"]["dag_id"] == "train"
    assert result["dag"]["catchup"] is False
    params = result["params"]
    assert params["model"]["value"] == "xgboost"
    assert params["db_password"] == "[REDACTED]"
    assert "pa55word" not in json.dumps(params)
    assert result["task_count"] == 5
    notify = next(t for t in result["tasks"] if t["task_id"] == "notify")
    assert notify["trigger_rule"] == "all_done"
    assert notify["operator"] == "EmailOperator"


def test_get_dag_refuses_dag_outside_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", dag_allowlist=["etl_*"])
    route = airflow.get("/dags/train/details").respond(json={})
    result = ctx.call("airflow_get_dag", dag_id="train")
    assert result["ok"] is False
    assert result["error"]["type"] == "dag_not_allowed"
    assert route.call_count == 0  # nothing is sent to Airflow


# --------------------------------------------------------------------------- get_dag_runs


def test_get_dag_runs(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    route = airflow.get("/dags/train/dagRuns").respond(
        json=_collection(
            "dag_runs",
            [
                p.dag_run(api_version, "train", "run_2", "failed"),
                p.dag_run(api_version, "train", "run_1", "success"),
            ],
        )
    )
    result = ctx.call("airflow_get_dag_runs", dag_id="train", limit=2, state=["failed", "success"])
    assert result["ok"] is True
    assert [r["run_id"] for r in result["dag_runs"]] == ["run_2", "run_1"]
    assert result["dag_runs"][0]["duration_s"] == 600.0
    params = route.calls.last.request.url.params
    assert params["order_by"] == ("-run_after" if api_version == "v2" else "-execution_date")
    assert params.get_list("state") == ["failed", "success"]
    assert params["limit"] == "2"


# --------------------------------------------------------------------------- get_run_status


def test_get_run_status(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    airflow.get("/dags/train/dagRuns/run_1/taskInstances").respond(
        json=_collection(
            "task_instances",
            [
                p.task_instance(api_version, "extract", "success"),
                p.task_instance(api_version, "transform", "failed", try_number=3),
                p.task_instance(api_version, "load", "upstream_failed", try_number=0),
            ],
        )
    )
    result = ctx.call("airflow_get_run_status", dag_id="train", run_id="run_1")
    assert result["ok"] is True
    assert result["state_counts"] == {"success": 1, "failed": 1, "upstream_failed": 1}
    transform = next(t for t in result["task_instances"] if t["task_id"] == "transform")
    assert transform["try_number"] == 3
    assert transform["duration_s"] == 90.0
    assert transform["queued_s"] == 10.0


def test_run_id_is_url_encoded(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    route = airflow.get(path__regex=r".*/taskInstances$").respond(
        json=_collection("task_instances", [])
    )
    ctx.call(
        "airflow_get_run_status", dag_id="train", run_id="scheduled__2026-11-07T00:00:00+00:00"
    )
    raw_path = route.calls.last.request.url.raw_path.decode()
    assert "scheduled__2026-11-07T00%3A00%3A00%2B00%3A00" in raw_path


# --------------------------------------------------------------------------- get_task_logs


def test_get_task_logs_extracts_error_and_redacts(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/dagRuns/run_1/taskInstances/transform").respond(
        json=p.task_instance(api_version, "transform", "failed", try_number=2)
    )
    log_route = airflow.get("/dags/train/dagRuns/run_1/taskInstances/transform/logs/2").respond(
        json=p.log_body(api_version)
    )
    result = ctx.call("airflow_get_task_logs", dag_id="train", run_id="run_1", task_id="transform")
    assert result["ok"] is True
    assert result["try_number"] == 2
    assert log_route.calls.last.request.url.params["full_content"] == "true"
    assert "KeyError" in result["error_block"]
    assert "Traceback (most recent call last):" in result["error_block"]
    assert "hunter2" not in json.dumps(result)


def test_get_task_logs_tail_is_bounded(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", log_max_lines=5)
    big_log = "\n".join(f"line {i}" for i in range(1000))
    airflow.get("/dags/train/dagRuns/run_1/taskInstances/t/logs/1").respond(
        text=big_log, headers={"content-type": "text/plain"}
    )
    result = ctx.call(
        "airflow_get_task_logs",
        dag_id="train",
        run_id="run_1",
        task_id="t",
        try_number=1,
        max_lines=500,
    )
    assert result["returned_lines"] == 5  # capped by configuration, not by the LLM argument
    assert result["tail"].splitlines()[-1] == "line 999"
    assert result["truncated"] is True
    assert result["total_lines"] == 1000


def test_get_task_logs_task_never_ran(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/dagRuns/run_1/taskInstances/load").respond(
        json=p.task_instance(api_version, "load", "upstream_failed", try_number=0)
    )
    result = ctx.call("airflow_get_task_logs", dag_id="train", run_id="run_1", task_id="load")
    assert result["ok"] is False
    assert result["error"]["type"] == "not_found"


# --------------------------------------------------------------------------- get_xcoms


def test_get_xcoms_truncates_and_redacts(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", xcom_max_chars=100)
    base = "/dags/train/dagRuns/run_1/taskInstances/transform/xcomEntries"
    airflow.get(base).respond(
        json=_collection(
            "xcom_entries",
            [
                {"key": "return_value", "map_index": -1, "timestamp": "2026-11-07T00:02:00Z"},
                {"key": "api_token", "map_index": -1, "timestamp": "2026-11-07T00:02:00Z"},
                {"key": "big", "map_index": -1, "timestamp": "2026-11-07T00:02:00Z"},
            ],
        )
    )
    airflow.get(f"{base}/return_value").respond(json={"key": "return_value", "value": {"rows": 42}})
    airflow.get(f"{base}/api_token").respond(json={"key": "api_token", "value": "abc123xyz"})
    airflow.get(f"{base}/big").respond(json={"key": "big", "value": "x" * 5000})
    result = ctx.call("airflow_get_xcoms", dag_id="train", run_id="run_1", task_id="transform")
    assert result["ok"] is True
    by_key = {x["key"]: x for x in result["xcoms"]}
    assert by_key["return_value"]["value"] == {"rows": 42}
    assert by_key["return_value"]["truncated"] is False
    assert by_key["api_token"]["value"] == "[REDACTED]"
    assert by_key["big"]["truncated"] is True
    assert len(by_key["big"]["value"]) < 200


def test_get_xcoms_key_filter_and_entry_error(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    base = "/dags/train/dagRuns/run_1/taskInstances/transform/xcomEntries"
    route = airflow.get(base).respond(
        json=_collection("xcom_entries", [{"key": "metrics", "map_index": -1}])
    )
    airflow.get(f"{base}/metrics").respond(500, json={"detail": "boom"})
    result = ctx.call(
        "airflow_get_xcoms", dag_id="train", run_id="run_1", task_id="transform", key="metrics"
    )
    assert route.calls.last.request.url.params["xcom_key"] == "metrics"
    assert result["ok"] is True
    assert result["xcoms"][0]["error"]["type"] == "server"


# --------------------------------------------------------------------------- get_dependencies


def test_get_dependencies(api_version: str, airflow: respx.MockRouter, ctx: FakeContext) -> None:
    airflow.get("/dags/train/tasks").respond(json=p.tasks(api_version))
    result = ctx.call("airflow_get_dependencies", dag_id="train", task_id="transform", depth=2)
    assert result["ok"] is True
    assert [t["task_id"] for t in result["upstream"]] == ["extract"]
    downstream = {t["task_id"]: t for t in result["downstream"]}
    assert set(downstream) == {"load", "quality_check", "notify"}
    assert downstream["notify"]["distance"] == 2
    assert downstream["notify"]["trigger_rule"] == "all_done"


def test_get_dependencies_unknown_task(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/tasks").respond(json=p.tasks(api_version))
    result = ctx.call("airflow_get_dependencies", dag_id="train", task_id="nope")
    assert result["ok"] is False and result["error"]["type"] == "not_found"


# --------------------------------------------------------------------------- generic contract


def test_invalid_arguments_return_json_error(ctx: FakeContext) -> None:
    result = ctx.call("airflow_get_dag")
    assert result == {
        "ok": False,
        "error": {
            "type": "invalid_argument",
            "message": "'dag_id' is required and must be a non-empty string.",
            "status": None,
        },
    }
    assert ctx.call("airflow_get_dag_runs", dag_id="train", limit=0)["error"]["type"] == (
        "invalid_argument"
    )
    assert ctx.call("airflow_list_dags", paused="yes")["error"]["type"] == "invalid_argument"


def test_handlers_never_raise_on_garbage(ctx: FakeContext) -> None:
    for name, tool in ctx.tools.items():
        raw = tool["handler"](["not", "a", "dict"])  # type: ignore[arg-type]
        assert json.loads(raw)["ok"] is False, name


def test_http_errors_are_mapped(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/ghost/details").respond(404, json={"detail": "DAG ghost not found"})
    result = ctx.call("airflow_get_dag", dag_id="ghost")
    assert result["ok"] is False
    assert result["error"]["type"] == "not_found"
    assert result["error"]["status"] == 404
    assert "Traceback" not in json.dumps(result)


def test_unexpected_exception_is_hidden(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/tasks").mock(side_effect=RuntimeError("secret internals"))
    result = ctx.call("airflow_get_dependencies", dag_id="train", task_id="extract")
    assert result["ok"] is False
    assert result["error"]["type"] == "internal"
    assert "secret internals" not in json.dumps(result)
