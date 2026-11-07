"""Version-specific Airflow API payloads used by the tests (shapes follow the official specs)."""

from __future__ import annotations

from typing import Any


def dag(
    api_version: str, dag_id: str, *, paused: bool = False, tags: list[str] | None = None
) -> dict[str, Any]:
    tag_objs = [{"name": t} for t in (tags or [])]
    if api_version == "v1":
        return {
            "dag_id": dag_id,
            "is_paused": paused,
            "is_active": True,
            "description": f"{dag_id} pipeline",
            "schedule_interval": {"__type": "CronExpression", "value": "0 * * * *"},
            "timetable_description": "At minute 0",
            "tags": tag_objs,
            "owners": ["data"],
            "next_dagrun": "2026-11-08T00:00:00+00:00",
            "has_import_errors": False,
            "max_active_runs": 1,
            "max_active_tasks": 16,
        }
    return {
        "dag_id": dag_id,
        "is_paused": paused,
        "is_stale": False,
        "description": f"{dag_id} pipeline",
        "timetable_summary": "0 * * * *",
        "timetable_description": "At minute 0",
        "tags": [{**t, "dag_id": dag_id} for t in tag_objs],
        "owners": ["data"],
        "next_dagrun_logical_date": "2026-11-08T00:00:00Z",
        "has_import_errors": False,
        "max_active_runs": 1,
        "max_active_tasks": 16,
    }


def dag_details(api_version: str, dag_id: str) -> dict[str, Any]:
    details = dag(api_version, dag_id, tags=["ml"])
    details.update(
        {
            "catchup": False,
            "start_date": "2026-01-01T00:00:00Z",
            "timezone": "UTC",
            "dag_run_timeout": None,
            "doc_md": "# Training pipeline",
            "fileloc": "/opt/airflow/dags/train.py",
            "params": {
                "model": {"value": "xgboost", "description": "model family"},
                "db_password": "hunter2-very-secret",
                "source_uri": "postgresql://etl:pa55word@db.internal:5432/raw",
            },
        }
    )
    return details


def task(
    api_version: str,
    task_id: str,
    downstream: list[str],
    *,
    trigger_rule: str = "all_success",
    operator: str = "PythonOperator",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "trigger_rule": trigger_rule,
        "retries": 2,
        "pool": "default_pool",
        "is_mapped": False,
        "downstream_task_ids": downstream,
        "class_ref": {"module_path": "airflow.operators.python", "class_name": operator},
    }
    if api_version == "v2":
        payload["operator_name"] = operator
    return payload


def tasks(api_version: str) -> dict[str, Any]:
    """extract -> transform -> [load, quality_check] ; load -> notify (all_done)."""
    items = [
        task(api_version, "extract", ["transform"]),
        task(api_version, "transform", ["load", "quality_check"]),
        task(api_version, "load", ["notify"]),
        task(api_version, "quality_check", []),
        task(api_version, "notify", [], trigger_rule="all_done", operator="EmailOperator"),
    ]
    body: dict[str, Any] = {"tasks": items}
    if api_version == "v2":
        body["total_entries"] = len(items)
    return body


def dag_run(api_version: str, dag_id: str, run_id: str, state: str) -> dict[str, Any]:
    run: dict[str, Any] = {
        "dag_run_id": run_id,
        "dag_id": dag_id,
        "state": state,
        "run_type": "scheduled",
        "logical_date": "2026-11-07T00:00:00Z",
        "start_date": "2026-11-07T00:00:05Z",
        "end_date": "2026-11-07T00:10:05Z",
    }
    if api_version == "v1":
        run["execution_date"] = run["logical_date"]
    else:
        run["duration"] = 600.0
        run["run_after"] = run["logical_date"]
    return run


def task_instance(
    api_version: str, task_id: str, state: str, *, try_number: int = 1
) -> dict[str, Any]:
    ti: dict[str, Any] = {
        "task_id": task_id,
        "dag_id": "train",
        "dag_run_id": "run_1",
        "map_index": -1,
        "state": state,
        "try_number": try_number,
        "max_tries": 2,
        "start_date": "2026-11-07T00:01:00Z",
        "end_date": "2026-11-07T00:02:30Z",
        "duration": 90.0,
        "queued_when": "2026-11-07T00:00:50Z",
        "pool": "default_pool",
        "operator": "PythonOperator",
        "hostname": "worker-1",
    }
    if api_version == "v2":
        ti["operator_name"] = "PythonOperator"
    return ti


V1_LOG_TEXT = (
    "[2026-11-07, 00:01:00 UTC] {taskinstance.py:1} INFO - Starting attempt 2 of 3\n"
    "[2026-11-07, 00:01:01 UTC] {train.py:12} INFO - connecting with password=hunter2\n"
    "[2026-11-07, 00:01:02 UTC] {taskinstance.py:2} ERROR - Task failed with exception\n"
    "Traceback (most recent call last):\n"
    '  File "/opt/airflow/dags/train.py", line 40, in train\n'
    "    df = load(source)\n"
    '  File "/opt/airflow/dags/train.py", line 22, in load\n'
    "    return frame[cfg['target']]\n"
    "KeyError: 'target'\n"
    "[2026-11-07, 00:01:03 UTC] {taskinstance.py:3} INFO - Marking task as UP_FOR_RETRY\n"
)


def v1_log_body() -> dict[str, Any]:
    # Airflow 2.x returns a stringified list of (host, text) tuples.
    return {"content": repr([("worker-1", V1_LOG_TEXT)]), "continuation_token": None}


def v2_log_body() -> dict[str, Any]:
    return {
        "content": [
            {"timestamp": "2026-11-07T00:01:00Z", "level": "info", "event": "Starting attempt 2"},
            {
                "timestamp": "2026-11-07T00:01:01Z",
                "level": "info",
                "event": "connecting with password=hunter2",
            },
            {
                "timestamp": "2026-11-07T00:01:02Z",
                "level": "error",
                "event": "Task failed with exception",
                "logger": "task",
                "error_detail": [
                    {
                        "exc_type": "KeyError",
                        "exc_value": "'target'",
                        "frames": [
                            {
                                "filename": "/opt/airflow/dags/train.py",
                                "lineno": 40,
                                "name": "train",
                            },
                            {
                                "filename": "/opt/airflow/dags/train.py",
                                "lineno": 22,
                                "name": "load",
                            },
                        ],
                    }
                ],
            },
            {"timestamp": "2026-11-07T00:01:03Z", "level": "info", "event": "Marking task"},
        ],
        "continuation_token": None,
    }


def log_body(api_version: str) -> dict[str, Any]:
    return v1_log_body() if api_version == "v1" else v2_log_body()
