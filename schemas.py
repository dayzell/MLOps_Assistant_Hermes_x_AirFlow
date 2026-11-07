"""Tool schemas exposed to the LLM (name, description, JSON-schema parameters)."""

from __future__ import annotations

from typing import Any

_DAG_ID = {"type": "string", "description": "Airflow DAG id (must be in the plugin allowlist)."}
_RUN_ID = {
    "type": "string",
    "description": "DAG run id, e.g. 'scheduled__2026-01-01T00:00:00+00:00' or 'manual__...'.",
}
_TASK_ID = {"type": "string", "description": "Task id inside the DAG."}
_MAP_INDEX = {
    "type": "integer",
    "description": "Map index for dynamically mapped tasks. Omit for regular tasks.",
    "minimum": -1,
}

AIRFLOW_LIST_DAGS: dict[str, Any] = {
    "name": "airflow_list_dags",
    "description": (
        "List Airflow DAGs visible to this plugin (allowlist applied) with paused state, "
        "schedule, tags, owners and next run. Read-only. Use it to discover DAG ids."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Only DAGs having at least one of these tags.",
            },
            "paused": {
                "type": "boolean",
                "description": "true = only paused DAGs, false = only unpaused. Omit for both.",
            },
            "only_active": {
                "type": "boolean",
                "description": "Exclude stale/removed DAGs (default true).",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 500,
                "description": "Maximum number of DAGs to return (default 100).",
            },
        },
        "required": [],
    },
}

AIRFLOW_GET_DAG: dict[str, Any] = {
    "name": "airflow_get_dag",
    "description": (
        "Get one DAG's details: schedule, owners, catchup, params (secrets masked) and the "
        "list of tasks with operator, trigger_rule, retries and downstream tasks. Read-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {"dag_id": _DAG_ID},
        "required": ["dag_id"],
    },
}

AIRFLOW_GET_DAG_RUNS: dict[str, Any] = {
    "name": "airflow_get_dag_runs",
    "description": (
        "List the most recent runs of a DAG (newest first) with state, dates and duration in "
        "seconds. Read-only. Use it to find the run_id of a failed run."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "description": "Number of runs to return (default 10).",
            },
            "state": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["queued", "running", "success", "failed"],
                },
                "description": "Only runs in these states.",
            },
        },
        "required": ["dag_id"],
    },
}

AIRFLOW_GET_RUN_STATUS: dict[str, Any] = {
    "name": "airflow_get_run_status",
    "description": (
        "Get every task instance of one DAG run: state, try number, duration, queue time and "
        "pool, plus a count per state. Read-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {"dag_id": _DAG_ID, "run_id": _RUN_ID},
        "required": ["dag_id", "run_id"],
    },
}

AIRFLOW_GET_TASK_LOGS: dict[str, Any] = {
    "name": "airflow_get_task_logs",
    "description": (
        "Get the log of one task try, truncated to its last lines, plus the extracted "
        "error/traceback block. Secrets are masked. Read-only. Prefer reading 'error_block' "
        "first; the tail is bounded by the plugin configuration."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "run_id": _RUN_ID,
            "task_id": _TASK_ID,
            "try_number": {
                "type": "integer",
                "minimum": 1,
                "description": "Try to read. Defaults to the latest try.",
            },
            "max_lines": {
                "type": "integer",
                "minimum": 1,
                "description": "Lines of tail to return (capped by the plugin configuration).",
            },
            "map_index": _MAP_INDEX,
        },
        "required": ["dag_id", "run_id", "task_id"],
    },
}

AIRFLOW_GET_XCOMS: dict[str, Any] = {
    "name": "airflow_get_xcoms",
    "description": (
        "Get the XCom entries pushed by one task instance. Values are secret-masked and "
        "truncated above the configured size. Read-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "run_id": _RUN_ID,
            "task_id": _TASK_ID,
            "key": {"type": "string", "description": "Only this XCom key (e.g. return_value)."},
            "map_index": _MAP_INDEX,
        },
        "required": ["dag_id", "run_id", "task_id"],
    },
}

AIRFLOW_GET_DEPENDENCIES: dict[str, Any] = {
    "name": "airflow_get_dependencies",
    "description": (
        "Get the upstream and downstream tasks of one task (up to 'depth' hops) with their "
        "trigger_rule and operator. Read-only. Use it to see what a failure impacts."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "task_id": _TASK_ID,
            "depth": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10,
                "description": "Number of hops to follow in each direction (default 2).",
            },
        },
        "required": ["dag_id", "task_id"],
    },
}

AIRFLOW_DIAGNOSE_RUN: dict[str, Any] = {
    "name": "airflow_diagnose_run",
    "description": (
        "Diagnose a failed or retrying DAG run in one call. Separates root failures (failed "
        "tasks with no failed upstream) from secondary failures and impacted tasks "
        "(upstream_failed / skipped). For each root failure it returns the error block, a log "
        "tail, XComs of the task and its inputs, its dependencies, and a classification "
        "(transient / critical / unknown) with the matched rules as justification. Also "
        "returns DAG params (secrets masked), retry candidates and guidance. Read-only. "
        "Start every failure investigation with this tool, then follow the airflow-triage skill."
    ),
    "parameters": {
        "type": "object",
        "properties": {"dag_id": _DAG_ID, "run_id": _RUN_ID},
        "required": ["dag_id", "run_id"],
    },
}

READ_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    schema["name"]: schema
    for schema in (
        AIRFLOW_LIST_DAGS,
        AIRFLOW_GET_DAG,
        AIRFLOW_GET_DAG_RUNS,
        AIRFLOW_GET_RUN_STATUS,
        AIRFLOW_GET_TASK_LOGS,
        AIRFLOW_GET_XCOMS,
        AIRFLOW_GET_DEPENDENCIES,
        AIRFLOW_DIAGNOSE_RUN,
    )
}
