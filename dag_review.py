"""DAG review before deployment (plan v2, step 4): deterministic checks, exact lines.

``airflow_review_dag`` reads the DAG code (local folder first, like ``airflow_get_dag_source``)
and the task list from Airflow, then looks for the problems that caused real incidents:

* a final task with a permissive ``trigger_rule`` (``all_done``…) that marks the run
  ``success`` while a task failed, with no "watcher" task;
* a secret written in the code (found by the plugin's masking: ``[REDACTED]`` in the source);
* heavy work at the top level of the file (run at every parse of the DAG, every ~30 s);
* an exception swallowed (``except: pass``), a task with no retry;
* ``catchup=True``.

Heuristics on the code: they point at lines to review, they never change anything.
"""

from __future__ import annotations

import ast
import re
from typing import Any

# Rules that let a final task succeed after an upstream failure. ``none_failed*`` are not
# here: with them a failed upstream makes the task ``upstream_failed`` and the run fails.
PERMISSIVE_RULES = frozenset({"all_done", "always", "one_success", "dummy"})
_HEAVY_CALLS = re.compile(
    r"(^|\.)(get|post|put|request|urlopen|read_csv|read_parquet|read_sql|read_excel|read_json|"
    r"connect|get_connection|get_records|sleep|run|check_output|Popen|query)$"
)
_HEAVY_ROOTS = frozenset(
    {
        "requests",
        "pd",
        "pandas",
        "urllib",
        "psycopg2",
        "sqlalchemy",
        "time",
        "subprocess",
        "boto3",
        "Variable",
        "BaseHook",
        "spark",
        "bigquery",
        "snowflake",
    }
)
SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2, "info": 3}


def review_dag(source: dict[str, Any], tasks: list[dict[str, Any]]) -> dict[str, Any]:
    """``source`` = result of ``read_dag_source``; ``tasks`` = ``client.get_tasks``."""
    content = str(source.get("content") or "")
    first = int(source.get("first_line") or 1)
    lines = content.splitlines()
    findings: list[dict[str, Any]] = []
    findings += _trigger_rule_findings(tasks, lines, first)
    findings += _secret_findings(lines, first)
    findings += _retry_findings(tasks, lines, first)
    notes: list[str] = []
    if source.get("truncated"):
        notes.append("Only the first lines were read: the code checks cover that part of the file.")
    try:
        tree = ast.parse(content)
    except SyntaxError as exc:
        findings.append(
            _finding(
                "high",
                "syntax_error",
                (exc.lineno or 1) + first - 1,
                f"The file does not parse: {exc.msg}.",
                "Fix the syntax: Airflow cannot load the DAG at all.",
            )
        )
        tree = None
    if tree is not None and not source.get("truncated"):
        findings += _top_level_findings(tree, first)
        findings += _swallowed_findings(tree, first)
        findings += _catchup_findings(tree, first)
    findings.sort(key=lambda f: (SEVERITY_ORDER[f["severity"]], f.get("line") or 0))
    counts = {s: sum(1 for f in findings if f["severity"] == s) for s in SEVERITY_ORDER}
    return {
        "dag_id": source.get("dag_id"),
        "path": source.get("path"),
        "counts": counts,
        "findings": findings,
        "verdict": "fix before deploying"
        if counts["high"]
        else ("review the warnings" if counts["medium"] else "no blocking issue found"),
        "notes": notes,
        "next": "Propose a fix for each 'high' finding (airflow-fix-proposal template), test it "
        "with airflow_sandbox_test (level 'structure'); the DAG write lock then lets the tested "
        "change be applied after the user's approval. These checks are heuristics, not a proof.",
    }


def _finding(
    severity: str, rule: str, line: int | None, message: str, suggestion: str, **extra: Any
) -> dict[str, Any]:
    return {
        "severity": severity,
        "rule": rule,
        "line": line,
        "message": message,
        "suggestion": suggestion,
        **extra,
    }


def _task_line(lines: list[str], first: int, task_id: str, near: str | None = None) -> int | None:
    """Line of a task's definition (``def task_id(`` or ``task_id="..."``); with ``near``, the
    line holding it within 3 lines (e.g. the decorator with the trigger_rule). A file can hold
    several DAGs: the first match of ``near`` in the file could belong to another one."""
    marks = (f"def {task_id}(", f'task_id="{task_id}"', f"task_id='{task_id}'")
    for index, text in enumerate(lines):
        if any(m in text for m in marks):
            if near:
                for offset in range(max(0, index - 3), min(len(lines), index + 4)):
                    if near in lines[offset]:
                        return offset + first
            return index + first
    return None


def _trigger_rule_findings(
    tasks: list[dict[str, Any]], lines: list[str], first: int
) -> list[dict[str, Any]]:
    leaves = [t for t in tasks if not t.get("downstream_task_ids")]
    has_watcher = any(t.get("trigger_rule") == "one_failed" for t in tasks)
    found = []
    for task in leaves:
        rule = str(task.get("trigger_rule") or "")
        if rule not in PERMISSIVE_RULES or has_watcher:
            continue
        line = _task_line(lines, first, str(task["task_id"]), near=rule)
        found.append(
            _finding(
                "high",
                "masking_trigger_rule",
                line,
                f"Final task {task['task_id']!r} has trigger_rule={rule!r}: the run is marked "
                "success even when an upstream task fails, so failures go unnoticed.",
                "Keep the final task (cleanup / notification) and add a leaf 'watcher' task "
                "with trigger_rule='one_failed' that raises, so the run fails.",
                task_id=task["task_id"],
            )
        )
    return found


def _secret_findings(lines: list[str], first: int) -> list[dict[str, Any]]:
    found = []
    for index, text in enumerate(lines):
        if "[REDACTED]" in text:
            found.append(
                _finding(
                    "high",
                    "hardcoded_secret",
                    index + first,
                    "A secret is written in the code (masked here).",
                    "Move it to an Airflow Connection or Variable (secrets backend) and read "
                    "it at run time; rotate the exposed secret.",
                )
            )
    return found[:5]


def _retry_findings(
    tasks: list[dict[str, Any]], lines: list[str], first: int
) -> list[dict[str, Any]]:
    no_retry = [t for t in tasks if t.get("retries") == 0]
    if not no_retry:
        return []
    names = ", ".join(str(t["task_id"]) for t in no_retry[:6])
    return [
        _finding(
            "medium",
            "no_retries",
            _task_line(lines, first, str(no_retry[0]["task_id"]), near="retries"),
            f"No retry for: {names}. A network hiccup fails the whole run.",
            "Set retries (e.g. default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}) "
            "on tasks that call external systems.",
            tasks=[t["task_id"] for t in no_retry],
        )
    ]


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_call_name(node.value)}.{node.attr}"
    if isinstance(node, ast.Call):
        return _call_name(node.func)
    return ""


def _top_level_findings(tree: ast.Module, first: int) -> list[dict[str, Any]]:
    """Calls run at import time: module level and ``with DAG(...)`` bodies (not functions)."""
    found = []

    def scan(statements: list[ast.stmt]) -> None:
        for stmt in statements:
            if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                continue
            if isinstance(stmt, ast.With):
                scan(stmt.body)
                continue
            for node in ast.walk(stmt):
                if not isinstance(node, ast.Call):
                    continue
                name = _call_name(node.func)
                root = name.split(".")[0]
                if root in _HEAVY_ROOTS and (_HEAVY_CALLS.search(name) or root == "Variable"):
                    found.append(
                        _finding(
                            "medium",
                            "top_level_work",
                            node.lineno + first - 1,
                            f"`{name}(...)` runs at the top level of the file: Airflow re-runs "
                            "it at every parse of the DAG (about every 30 s).",
                            "Move it inside a task (or a function called by a task); use "
                            "templates such as {{ var.value.x }} for Variables.",
                        )
                    )
                    break

    scan(tree.body)
    return found[:5]


def _swallowed_findings(tree: ast.Module, first: int) -> list[dict[str, Any]]:
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        body = node.body
        silent = all(
            isinstance(s, ast.Pass | ast.Continue)
            or (isinstance(s, ast.Return) and (s.value is None or _is_none(s.value)))
            for s in body
        )
        if silent:
            found.append(
                _finding(
                    "medium",
                    "swallowed_exception",
                    node.lineno + first - 1,
                    "An exception is caught and ignored: the task can succeed with a wrong or "
                    "empty result.",
                    "Let it fail, or log it and re-raise with a clear message.",
                )
            )
    return found[:5]


def _is_none(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _catchup_findings(tree: ast.Module, first: int) -> list[dict[str, Any]]:
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == "catchup":
            value = node.value
            if isinstance(value, ast.Constant) and value.value is True:
                return [
                    _finding(
                        "info",
                        "catchup_enabled",
                        getattr(value, "lineno", None) and value.lineno + first - 1,
                        "catchup=True: after a pause or a new start_date, Airflow runs every "
                        "missed interval.",
                        "Keep it only if backfilling is wanted; otherwise catchup=False.",
                    )
                ]
    return []
