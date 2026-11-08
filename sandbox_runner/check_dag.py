"""In-container checker for the airflow-mlops sandbox. NOT imported by the plugin.

The plugin copies this file into the throw-away workspace and runs it inside the sandbox
container (no network, no secrets, CPU/memory/time limits). It only needs the Python
standard library and Airflow.

Levels (each includes the previous ones):
    import     the patched DAG folder loads without import errors and the DAG exists
    structure  compare tasks / dependencies with the current production structure
    task       run ONE task with ``airflow tasks test`` (sample mode, see env vars)

The result is printed on one line prefixed with ``SANDBOX_RESULT `` (JSON).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SENTINEL = "SANDBOX_RESULT "
MAX_OUTPUT_CHARS = 6000
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _clean(text: str) -> str:
    """Last part of a command output, without terminal colour codes."""
    return _ANSI.sub("", text)[-MAX_OUTPUT_CHARS:]


def compare_structure(
    expected: dict[str, list[str]], actual: dict[str, list[str]]
) -> dict[str, Any]:
    """Diff two ``{task_id: [downstream ids]}`` graphs (pure function, unit-tested)."""
    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))
    expected_edges = {(a, b) for a, children in expected.items() for b in children}
    actual_edges = {(a, b) for a, children in actual.items() for b in children}
    return {
        "added_tasks": added,
        "removed_tasks": removed,
        "added_edges": sorted(f"{a}>>{b}" for a, b in actual_edges - expected_edges),
        "removed_edges": sorted(f"{a}>>{b}" for a, b in expected_edges - actual_edges),
    }


def _emit(result: dict[str, Any]) -> None:
    print(SENTINEL + json.dumps(result, default=str), flush=True)


def _load_dagbag(dags_folder: str) -> Any:
    try:  # Airflow 3.1+
        from airflow.dag_processing.dagbag import DagBag  # type: ignore[import-not-found]
    except ImportError:  # Airflow 2.x / 3.0
        from airflow.models.dagbag import DagBag  # type: ignore[import-not-found]
    return DagBag(dag_folder=dags_folder, include_examples=False)


def _graph(dag: Any) -> tuple[dict[str, list[str]], dict[str, str]]:
    graph: dict[str, list[str]] = {}
    operators: dict[str, str] = {}
    for task in dag.tasks:
        graph[task.task_id] = sorted(task.downstream_task_ids)
        operators[task.task_id] = type(task).__name__
    return graph, operators


def _run_task(dag_id: str, task_id: str, budget_s: float) -> dict[str, Any]:
    started = time.monotonic()
    env = dict(os.environ)
    migrate = subprocess.run(
        ["airflow", "db", "migrate"],
        capture_output=True,
        text=True,
        timeout=max(30.0, budget_s / 3),
        env=env,
        check=False,
    )
    if migrate.returncode != 0:
        return {
            "passed": False,
            "error": "airflow db migrate failed in the sandbox",
            "output": _clean(migrate.stdout + migrate.stderr),
        }
    remaining = max(10.0, budget_s - (time.monotonic() - started))
    logical_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")  # noqa: UP017 (Python < 3.11 images)
    try:
        proc = subprocess.run(
            ["airflow", "tasks", "test", dag_id, task_id, logical_date],
            capture_output=True,
            text=True,
            timeout=remaining,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"passed": False, "timed_out": True, "error": "task test timed out"}
    return {
        "passed": proc.returncode == 0,
        "exit_code": proc.returncode,
        "duration_s": round(time.monotonic() - started, 1),
        "output": _clean(proc.stdout + proc.stderr),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--level", choices=("import", "structure", "task"), required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--task-id")
    parser.add_argument("--dags-folder", default="/sandbox/dags")
    parser.add_argument("--expected", default="/sandbox/expected_structure.json")
    parser.add_argument("--budget", type=float, default=240.0)
    args = parser.parse_args()

    result: dict[str, Any] = {"level": args.level, "dag_id": args.dag_id, "stages": {}}
    started = time.monotonic()

    bag = _load_dagbag(args.dags_folder)
    import_errors = {str(k): str(v)[-2000:] for k, v in dict(bag.import_errors).items()}
    dag = bag.dags.get(args.dag_id)
    import_ok = not import_errors and dag is not None
    result["stages"]["import"] = {
        "passed": import_ok,
        "import_errors": import_errors,
        "dag_found": dag is not None,
        "dag_ids": sorted(bag.dags)[:50],
        "duration_s": round(time.monotonic() - started, 1),
    }
    if not import_ok or args.level == "import":
        result["passed"] = import_ok
        _emit(result)
        return 0 if import_ok else 1

    graph, operators = _graph(dag)
    structure: dict[str, Any] = {"passed": True, "task_count": len(graph), "operators": operators}
    expected_path = Path(args.expected)
    if expected_path.is_file():
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
        structure["changes"] = compare_structure(expected, graph)
        if args.task_id and args.task_id not in graph:
            structure["passed"] = False
            structure["error"] = f"task {args.task_id!r} is missing from the patched DAG"
    if not graph:
        structure["passed"] = False
        structure["error"] = "the patched DAG has no task"
    result["stages"]["structure"] = structure
    if not structure["passed"] or args.level == "structure":
        result["passed"] = structure["passed"]
        _emit(result)
        return 0 if structure["passed"] else 1

    if not args.task_id:
        result["stages"]["task"] = {"passed": False, "error": "--task-id is required"}
        result["passed"] = False
        _emit(result)
        return 1
    budget = args.budget - (time.monotonic() - started)
    result["stages"]["task"] = _run_task(args.dag_id, args.task_id, budget)
    result["passed"] = bool(result["stages"]["task"]["passed"])
    _emit(result)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
