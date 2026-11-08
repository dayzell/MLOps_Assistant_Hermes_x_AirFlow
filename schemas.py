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

AIRFLOW_BUILD_REPORT: dict[str, Any] = {
    "name": "airflow_build_report",
    "description": (
        "Build the standard Airflow monitoring report for a scope and period. Always returns "
        "the same six sections, as JSON ('report') and Markdown ('markdown'): 1 scope and "
        "period, 2 global health (success rate, runs, incidents), 3 per-task performance "
        "(p50/p95 vs stored baseline, retries, queue time, SLA misses), 4 data quality (from "
        "the data-quality XCom convention, never invented), 5 incidents and actions taken, "
        "6 recommendations. Read-only, except that it refreshes the stored duration "
        "baselines. Follow the airflow-report skill when presenting it."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "scope": {
                "type": "string",
                "description": "'all' (default), a DAG id, or 'tag:<tag>'.",
            },
            "period": {
                "type": "string",
                "description": "Look-back window: number + m/h/d/w, e.g. '24h', '7d'. "
                "Defaults to the configured report period.",
            },
            "update_baseline": {
                "type": "boolean",
                "description": "Refresh the stored duration baselines (default true).",
            },
        },
        "required": [],
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
        AIRFLOW_BUILD_REPORT,
    )
}


AIRFLOW_CLEAR_TASKS: dict[str, Any] = {
    "name": "airflow_clear_tasks",
    "description": (
        "Re-run (clear) specific failed task instances of ONE DAG run - never a whole DAG. "
        "Defaults to dry_run=true: returns the exact task instances that would be cleared, "
        "without changing anything. Always dry-run first and show the list to the user. A real "
        "clear (dry_run=false) needs a 'reason', goes through human approval (unless the "
        "bounded auto-retry policy for transient failures applies) and is journaled. Failures "
        "classified critical are refused unless confirm_critical=true after explicit human "
        "confirmation. Retry only the root failures listed by airflow_diagnose_run."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "run_id": _RUN_ID,
            "task_ids": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "maxItems": 20,
                "description": "Explicit task ids to clear (the root failures). Required.",
            },
            "include_downstream": {
                "type": "boolean",
                "description": "Also clear the downstream tasks (default true). With "
                "only_failed=true this re-runs only the downstream tasks blocked by the failure "
                "(upstream_failed), never one that succeeded. Without it they stay "
                "upstream_failed and the run stays failed.",
            },
            "only_failed": {
                "type": "boolean",
                "description": "Only clear task instances in a failed state (default true).",
            },
            "dry_run": {
                "type": "boolean",
                "description": "Preview only (default true). Set false only after the user "
                "agreed with the previewed list.",
            },
            "reason": {
                "type": "string",
                "description": "Why this retry is needed (required when dry_run=false; journaled).",
            },
            "confirm_critical": {
                "type": "boolean",
                "description": "Allow retrying a failure classified critical. Only set after "
                "a human explicitly confirmed it (default false).",
            },
        },
        "required": ["dag_id", "run_id", "task_ids"],
    },
}

WRITE_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {AIRFLOW_CLEAR_TASKS["name"]: AIRFLOW_CLEAR_TASKS}


AIRFLOW_SANDBOX_TEST: dict[str, Any] = {
    "name": "airflow_sandbox_test",
    "description": (
        "Test a proposed DAG fix in a throw-away Docker sandbox BEFORE presenting it. Applies "
        "the unified diff to a COPY of the DAGs folder (the user's files are never modified) "
        "and runs a bounded check in a container with no network, no secrets and CPU/memory/"
        "time limits. Levels, lightest first: 'import' (the patched DAG loads), 'structure' "
        "(tasks and dependencies vs production), 'task' (runs ONE task on a data sample; only "
        "when the user asks, and never for heavy operators like Spark or Kubernetes). Start "
        "with 'import'. Never changes Airflow. Budgeted per session."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "dag_id": _DAG_ID,
            "patch": {
                "type": "string",
                "description": "Unified diff of the fix, with paths relative to the DAGs "
                "folder (e.g. '--- a/train.py' / '+++ b/train.py').",
            },
            "level": {
                "type": "string",
                "enum": ["import", "structure", "task"],
                "description": "How far to test (default 'import').",
            },
            "task_id": {
                "type": "string",
                "description": "Task to run (required for level 'task'; checked for 'structure').",
            },
            "run_id": {
                "type": "string",
                "description": "Failed run whose upstream XComs are given to the task as "
                "fixtures (level 'task' only, optional).",
            },
        },
        "required": ["dag_id", "patch"],
    },
}

SANDBOX_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    AIRFLOW_SANDBOX_TEST["name"]: AIRFLOW_SANDBOX_TEST
}
