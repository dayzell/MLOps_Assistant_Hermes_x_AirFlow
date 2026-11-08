"""Tool handlers: validate arguments, call the business modules, return a JSON string.

Contract (Hermes): ``handler(args: dict, **kwargs) -> str``. Handlers always return a JSON
string and never raise.
"""

from __future__ import annotations

import functools
import json
import logging
from collections import Counter
from collections.abc import Callable
from typing import Any

from .airflow_client import AirflowError
from .config import DagNotAllowedError, Settings
from .diagnosis import RunDiagnoser, load_rules
from .guardrails import ClearRequest, ClearTasksAction, GuardrailError
from .redaction import redact_text, redact_value
from .reporting import ReportBuilder, parse_period, parse_scope
from .runtime import PluginRuntime
from .sandbox import SandboxError, SandboxRequest, SandboxRunner
from .textutils import excerpt_log, truncate_text, truncate_value
from .views import build_dependency_view, collect_xcoms

logger = logging.getLogger(__name__)

Handler = Callable[..., str]
_MAX_TASKS_LISTED = 200
_MAX_XCOM_ENTRIES = 50


class ToolInputError(ValueError):
    """Invalid tool arguments (reported to the LLM as ``invalid_argument``)."""


# ---------------------------------------------------------------------------- helpers


def ok(**payload: Any) -> str:
    return json.dumps({"ok": True, **payload}, default=str, ensure_ascii=False)


def error(error_type: str, message: str, status: int | None = None) -> str:
    return json.dumps(
        {"ok": False, "error": {"type": error_type, "message": message, "status": status}},
        ensure_ascii=False,
    )


def safe_handler(func: Callable[..., str], *, context_aware: bool = False) -> Handler:
    """Wrap a handler so that it always returns JSON and never raises.

    ``context_aware`` handlers also receive Hermes' context kwargs (session_id, ...).
    """

    @functools.wraps(func)
    def wrapper(args: dict[str, Any] | None = None, **kwargs: Any) -> str:
        try:
            if args is not None and not isinstance(args, dict):
                raise ToolInputError("Arguments must be a JSON object.")
            if context_aware:
                return func(args or {}, **kwargs)
            return func(args or {})
        except ToolInputError as exc:
            return error("invalid_argument", str(exc))
        except GuardrailError as exc:
            return error("guardrail", str(exc))
        except SandboxError as exc:
            return error("sandbox", str(exc))
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
            "airflow_diagnose_run": safe_handler(self.diagnose_run),
            "airflow_build_report": safe_handler(self.build_report),
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

        results, entries_truncated = collect_xcoms(
            self.runtime.client(settings),
            dag_id,
            run_id,
            task_id,
            max_chars=settings.xcom_max_chars,
            key=key.strip() if isinstance(key, str) else None,
            map_index=map_index,
            max_entries=_MAX_XCOM_ENTRIES,
        )
        return ok(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            count=len(results),
            entries_truncated=entries_truncated,
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

    # -- airflow_diagnose_run
    def diagnose_run(self, args: dict[str, Any]) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        run_id = _require_str(args, "run_id")
        _check_allowed(settings, dag_id)
        diagnoser = RunDiagnoser(
            self.runtime.client(settings), settings, load_rules(settings.error_patterns_path)
        )
        return ok(**diagnoser.diagnose(dag_id, run_id))

    # -- airflow_build_report
    def build_report(self, args: dict[str, Any]) -> str:
        scope = args.get("scope", "all")
        period = args.get("period")
        if not isinstance(scope, str) or (period is not None and not isinstance(period, str)):
            raise ToolInputError("'scope' and 'period' must be strings.")
        update_baseline = _optional_bool(args, "update_baseline")
        result = self._report(scope, period, True if update_baseline is None else update_baseline)
        return ok(**result)

    def _report(self, scope: str, period: str | None, update_baseline: bool) -> dict[str, Any]:
        settings = self.runtime.settings()
        try:  # validate before any network call
            parsed_scope = parse_scope(scope)
            parse_period(period or settings.report_default_period)
        except ValueError as exc:
            raise ToolInputError(str(exc)) from exc
        if parsed_scope.kind == "dag":
            _check_allowed(settings, str(parsed_scope.value))
        builder = ReportBuilder(self.runtime.client(settings), settings, now=self.runtime.now())
        return builder.build(scope, period, update_baseline=update_baseline)

    # -- /airflow-report slash command
    def report_command(self, raw_args: str = "", **kwargs: Any) -> str:
        """``/airflow-report [scope] [period]`` -> Markdown report (plain text on error)."""
        scope, period = parse_command_args(raw_args)
        result = json.loads(safe_handler(self.build_report)({"scope": scope, "period": period}))
        if result.get("ok"):
            return str(result["markdown"])
        err = result["error"]
        return f"❌ /airflow-report : {err['message']} ({err['type']})"


def parse_command_args(raw_args: str | None) -> tuple[str, str | None]:
    """Split ``/airflow-report`` arguments: a token starting with a digit (``24h``, ``7d``)
    is the period (validated later), anything else is the scope."""
    scope, period = "all", None
    for token in (raw_args or "").split():
        if token[0].isdigit():
            period = token
        else:
            scope = token
    return scope, period


class WriteTools:
    """Remediation tools (exposed only when ``read_only`` is false)."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self.runtime = runtime

    def handlers(self) -> dict[str, Handler]:
        return {"airflow_clear_tasks": safe_handler(self.clear_tasks, context_aware=True)}

    def available(self) -> bool:
        """``check_fn``: hide write tools in read-only mode."""
        try:
            return not self.runtime.settings().read_only
        except Exception:
            return False

    # -- airflow_clear_tasks
    def clear_tasks(self, args: dict[str, Any], **context: Any) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        run_id = _require_str(args, "run_id")
        task_ids = _optional_str_list(args, "task_ids")
        if not task_ids:
            raise ToolInputError("'task_ids' must list at least one task (never a whole DAG).")
        _check_allowed(settings, dag_id)
        reason = args.get("reason") or ""
        if not isinstance(reason, str):
            raise ToolInputError("'reason' must be a string.")
        flags: dict[str, bool] = {}
        for name, default in (
            ("include_downstream", True),
            ("only_failed", True),
            ("dry_run", True),
            ("confirm_critical", False),
        ):
            value = _optional_bool(args, name)
            flags[name] = default if value is None else value
        request = ClearRequest(
            dag_id=dag_id,
            run_id=run_id,
            task_ids=list(dict.fromkeys(task_ids)),
            reason=reason.strip(),
            **flags,
        )
        action = ClearTasksAction(
            self.runtime.client(settings), settings, load_rules(settings.error_patterns_path)
        )
        result = action.run(
            request,
            session_id=_context_str(context, "session_id"),
            conversation_id=_context_str(context, "conversation_id"),
        )
        return ok(**result)


def _context_str(context: dict[str, Any], key: str) -> str | None:
    value = context.get(key)
    return str(value) if value not in (None, "") else None


class SandboxTools:
    """``airflow_sandbox_test`` (exposed only when ``sandbox_enabled`` is true)."""

    def __init__(self, runtime: PluginRuntime) -> None:
        self.runtime = runtime

    def handlers(self) -> dict[str, Handler]:
        return {"airflow_sandbox_test": safe_handler(self.sandbox_test, context_aware=True)}

    def available(self) -> bool:
        """``check_fn``: hide the sandbox tool until it is enabled."""
        try:
            return self.runtime.settings().sandbox_enabled
        except Exception:
            return False

    def sandbox_test(self, args: dict[str, Any], **context: Any) -> str:
        settings = self.runtime.settings()
        dag_id = _require_str(args, "dag_id")
        patch = args.get("patch")
        if not isinstance(patch, str) or not patch.strip():
            raise ToolInputError("'patch' must be a non-empty unified diff.")
        level = args.get("level", "import")
        if level not in ("import", "structure", "task"):
            raise ToolInputError("'level' must be 'import', 'structure' or 'task'.")
        task_id = args.get("task_id")
        run_id = args.get("run_id")
        for name, value in (("task_id", task_id), ("run_id", run_id)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ToolInputError(f"'{name}' must be a non-empty string when provided.")
        _check_allowed(settings, dag_id)
        if not self.runtime.sandbox_lock.acquire(blocking=False):
            raise SandboxError("Another sandbox test is already running; wait for it to finish.")
        try:
            runner = SandboxRunner(
                self.runtime.client(settings),
                settings,
                self.runtime.dispatch,
                session_id=_context_str(context, "session_id"),
                conversation_id=_context_str(context, "conversation_id"),
            )
            result = runner.run(
                SandboxRequest(
                    dag_id=dag_id,
                    patch=patch,
                    level=level,
                    task_id=task_id.strip() if isinstance(task_id, str) else None,
                    run_id=run_id.strip() if isinstance(run_id, str) else None,
                )
            )
        finally:
            self.runtime.sandbox_lock.release()
        return ok(**result)
