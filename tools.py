"""Tool handlers: validate arguments, call the business modules, return a JSON string.

Contract (Hermes): ``handler(args: dict, **kwargs) -> str``. Handlers always return a JSON
string and never raise.
"""

from __future__ import annotations

import functools
import json
import logging
from collections import Counter, deque
from collections.abc import Callable
from typing import Any

from .airflow_client import AirflowError
from .config import Settings
from .redaction import redact_text, redact_value
from .runtime import PluginRuntime
from .textutils import excerpt_log, truncate_text, truncate_value

logger = logging.getLogger(__name__)

Handler = Callable[..., str]
_MAX_TASKS_LISTED = 200
_MAX_XCOM_ENTRIES = 50


class ToolInputError(ValueError):
    """Invalid tool arguments (reported to the LLM as ``invalid_argument``)."""


class DagNotAllowedError(Exception):
    """The DAG is outside the configured allowlist."""


# ---------------------------------------------------------------------------- helpers


def ok(**payload: Any) -> str:
    return json.dumps({"ok": True, **payload}, default=str, ensure_ascii=False)


def error(error_type: str, message: str, status: int | None = None) -> str:
    return json.dumps(
        {"ok": False, "error": {"type": error_type, "message": message, "status": status}},
        ensure_ascii=False,
    )


def safe_handler(func: Callable[[dict[str, Any]], str]) -> Handler:
    """Wrap a handler so that it always returns JSON and never raises."""

    @functools.wraps(func)
    def wrapper(args: dict[str, Any] | None = None, **kwargs: Any) -> str:
        try:
            if args is not None and not isinstance(args, dict):
                raise ToolInputError("Arguments must be a JSON object.")
            return func(args or {})
        except ToolInputError as exc:
            return error("invalid_argument", str(exc))
        except DagNotAllowedError as exc:
            return error("dag_not_allowed", str(exc), 403)
        except AirflowError as exc:
            return error(exc.error_type, exc.message, exc.status)
        except Exception:  # last resort: no traceback leaks to the LLM
            logger.exception("airflow-mlops: unexpected error in %s", func.__name__)
            return error("internal", "Unexpected plugin error; see the Hermes agent log.")

    return wrapper


def _require_str(args: dict[str, Any], name: str) -> str:
    value = args.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ToolInputError(f"'{name}' is required and must be a non-empty string.")
    return value.strip()


def _optional_int(
    args: dict[str, Any], name: str, default: int | None, minimum: int, maximum: int | None = None
) -> int | None:
    value = args.get(name)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise ToolInputError(f"'{name}' must be an integer.")
    try:
        number = int(value)
    except ValueError as exc:
        raise ToolInputError(f"'{name}' must be an integer.") from exc
    if number < minimum or (maximum is not None and number > maximum):
        bound = f"between {minimum} and {maximum}" if maximum is not None else f">= {minimum}"
        raise ToolInputError(f"'{name}' must be {bound}.")
    return number


def _optional_bool(args: dict[str, Any], name: str) -> bool | None:
    value = args.get(name)
    if value is None or isinstance(value, bool):
        return value
    raise ToolInputError(f"'{name}' must be a boolean.")


def _optional_str_list(args: dict[str, Any], name: str) -> list[str]:
    value = args.get(name)
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ToolInputError(f"'{name}' must be a list of strings.")
    return [item.strip() for item in value if item.strip()]


def _check_allowed(settings: Settings, dag_id: str) -> None:
    if not settings.dag_allowed(dag_id):
        raise DagNotAllowedError(
            f"DAG '{dag_id}' is outside the plugin allowlist (dag_allowlist); access refused."
        )


# ---------------------------------------------------------------------------- handlers


class ReadTools:
    """Read-only tools bound to a :class:`PluginRuntime`."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self.runtime = runtime

    def handlers(self) -> dict[str, Handler]:
        return {
            "airflow_list_dags": safe_handler(self.list_dags),
            "airflow_get_dag": safe_handler(self.get_dag),
            "airflow_get_dag_runs": safe_handler(self.get_dag_runs),
            "airflow_get_run_status": safe_handler(self.get_run_status),
            "airflow_get_task_logs": safe_handler(self.get_task_logs),
            "airflow_get_xcoms": safe_handler(self.get_xcoms),
            "airflow_get_dependencies": safe_handler(self.get_dependencies),
        }

    # -- airflow_list_dags
    def list_dags(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        tags = _optional_str_list(args, "tags")
        paused = _optional_bool(args, "paused")
        only_active = _optional_bool(args, "only_active")
        limit = _optional_int(args, "limit", 100, 1, 500) or 100
        dags = self.runtime.client(settings).list_dags(
            tags=tags or None,
            paused=paused,
            only_active=True if only_active is None else only_active,
        )
        allowed = [dag for dag in dags if dag.get("dag_id") and settings.dag_allowed(dag["dag_id"])]
        hidden = len(dags) - len(allowed)
        return ok(
            count=min(len(allowed), limit),
            total_visible=len(allowed),
            hidden_by_allowlist=hidden,
            truncated=len(allowed) > limit,
            dags=redact_value(allowed[:limit]),
        )

    # -- airflow_get_dag
    def get_dag(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        _check_allowed(settings, dag_id)
        client = self.runtime.client(settings)
        dag = client.get_dag_details(dag_id)
        tasks = client.get_tasks(dag_id)
        params, params_truncated = truncate_value(
            redact_value(dag.pop("params", {}) or {}), settings.xcom_max_chars * 2
        )
        dag["doc_md"] = truncate_text(dag.get("doc_md"), 1000)
        return ok(
            dag=redact_value(dag),
            params=params,
            params_truncated=params_truncated,
            task_count=len(tasks),
            tasks_truncated=len(tasks) > _MAX_TASKS_LISTED,
            tasks=tasks[:_MAX_TASKS_LISTED],
        )

    # -- airflow_get_dag_runs
    def get_dag_runs(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        _check_allowed(settings, dag_id)
        limit = _optional_int(args, "limit", 10, 1, 100) or 10
        states = _optional_str_list(args, "state")
        runs = self.runtime.client(settings).list_dag_runs(dag_id, limit=limit, states=states)
        return ok(dag_id=dag_id, count=len(runs), dag_runs=runs)

    # -- airflow_get_run_status
    def get_run_status(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        run_id = _require_str(args, "run_id")
        _check_allowed(settings, dag_id)
        tis = self.runtime.client(settings).list_task_instances(dag_id, run_id)
        tis.sort(key=lambda ti: (ti.get("start_date") or "~", ti.get("task_id") or ""))
        counts = Counter(str(ti.get("state") or "none") for ti in tis)
        return ok(
            dag_id=dag_id,
            run_id=run_id,
            state_counts=dict(counts),
            task_instances=tis,
        )

    # -- airflow_get_task_logs
    def get_task_logs(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        run_id = _require_str(args, "run_id")
        task_id = _require_str(args, "task_id")
        _check_allowed(settings, dag_id)
        map_index = _optional_int(args, "map_index", None, -1)
        try_number = _optional_int(args, "try_number", None, 1)
        max_lines = _optional_int(args, "max_lines", settings.log_max_lines, 1)
        max_lines = min(max_lines or settings.log_max_lines, settings.log_max_lines)

        client = self.runtime.client(settings)
        if try_number is None:
            ti = client.get_task_instance(dag_id, run_id, task_id, map_index)
            latest = ti.get("try_number") or 0
            if latest < 1:
                return error(
                    "not_found",
                    f"Task '{task_id}' has not run yet in run '{run_id}' (no try to read).",
                    404,
                )
            try_number = int(latest)

        text = client.get_task_log(dag_id, run_id, task_id, try_number, map_index)
        excerpt = excerpt_log(
            redact_text(text),
            max_lines=max_lines,
            max_chars=settings.log_max_chars,
            context_lines=settings.error_context_lines,
        )
        return ok(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            try_number=try_number,
            map_index=map_index,
            total_lines=excerpt.total_lines,
            returned_lines=excerpt.returned_lines,
            truncated=excerpt.truncated,
            error_block=excerpt.error_block,
            tail=excerpt.tail,
        )

    # -- airflow_get_xcoms
    def get_xcoms(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        run_id = _require_str(args, "run_id")
        task_id = _require_str(args, "task_id")
        _check_allowed(settings, dag_id)
        key = args.get("key")
        if key is not None and (not isinstance(key, str) or not key.strip()):
            raise ToolInputError("'key' must be a non-empty string when provided.")
        map_index = _optional_int(args, "map_index", None, -1)

        client = self.runtime.client(settings)
        entries = client.list_xcom_entries(
            dag_id,
            run_id,
            task_id,
            key=key.strip() if isinstance(key, str) else None,
            map_index=map_index,
            max_items=_MAX_XCOM_ENTRIES,
        )
        results: list[dict[str, Any]] = []
        for entry in entries:
            entry_key = str(entry.get("key"))
            entry_map_index = entry.get("map_index", map_index)
            item: dict[str, Any] = {
                "key": entry_key,
                "map_index": entry_map_index,
                "timestamp": entry.get("timestamp"),
            }
            try:
                value = client.get_xcom_value(
                    dag_id,
                    run_id,
                    task_id,
                    entry_key,
                    entry_map_index if isinstance(entry_map_index, int) else None,
                )
                masked = redact_value({entry_key: value})[entry_key]
                item["value"], item["truncated"] = truncate_value(masked, settings.xcom_max_chars)
            except AirflowError as exc:
                item["value"], item["truncated"] = None, False
                item["error"] = exc.to_dict()
            results.append(item)
        return ok(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            count=len(results),
            entries_truncated=len(entries) >= _MAX_XCOM_ENTRIES,
            xcoms=results,
        )

    # -- airflow_get_dependencies
    def get_dependencies(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        task_id = _require_str(args, "task_id")
        _check_allowed(settings, dag_id)
        depth = _optional_int(args, "depth", 2, 1, 10) or 2
        tasks = self.runtime.client(settings).get_tasks(dag_id)
        graph = build_dependency_view(tasks, task_id, depth)
        if graph is None:
            return error("not_found", f"Task '{task_id}' does not exist in DAG '{dag_id}'.", 404)
        return ok(dag_id=dag_id, depth=depth, **graph)


def build_dependency_view(
    tasks: list[dict[str, Any]], task_id: str, depth: int
) -> dict[str, Any] | None:
    """Upstream/downstream neighbourhood of ``task_id`` (BFS, ``depth`` hops each way)."""
    by_id = {str(task["task_id"]): task for task in tasks if task.get("task_id")}
    if task_id not in by_id:
        return None
    downstream: dict[str, list[str]] = {
        tid: [str(d) for d in task.get("downstream_task_ids") or []] for tid, task in by_id.items()
    }
    upstream: dict[str, list[str]] = {tid: [] for tid in by_id}
    for tid, children in downstream.items():
        for child in children:
            upstream.setdefault(child, []).append(tid)

    def walk(edges: dict[str, list[str]]) -> list[dict[str, Any]]:
        seen: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque([(task_id, 0)])
        while queue:
            current, distance = queue.popleft()
            if distance >= depth:
                continue
            for neighbour in edges.get(current, []):
                if neighbour not in seen and neighbour != task_id:
                    seen[neighbour] = distance + 1
                    queue.append((neighbour, distance + 1))
        return [
            {
                "task_id": tid,
                "distance": dist,
                "trigger_rule": by_id.get(tid, {}).get("trigger_rule"),
                "operator": by_id.get(tid, {}).get("operator"),
                "direct_upstream": upstream.get(tid, []),
            }
            for tid, dist in sorted(seen.items(), key=lambda kv: (kv[1], kv[0]))
        ]

    task = by_id[task_id]
    return {
        "task": {
            "task_id": task_id,
            "operator": task.get("operator"),
            "trigger_rule": task.get("trigger_rule"),
            "retries": task.get("retries"),
        },
        "upstream": walk(upstream),
        "downstream": walk(downstream),
    }
