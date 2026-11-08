"""Manual smoke test of the airflow-mlops sandbox on REAL Docker containers.

Not part of the plugin, and not run by pytest. It drives the plugin's real sandbox code
(copy, patch, docker command, result parsing) through six scenarios. Only Hermes' ``terminal``
tool is replaced by a direct bash call, the same shell Hermes uses (Git Bash on Windows).

Requirements: Docker running, an Airflow image available locally, and the dev venv
(``pip install -r requirements-dev.txt``).

Usage (from the plugin root):
    .venv/Scripts/python demo-local-airflow/sandbox_smoke_test.py            # Windows
    .venv/bin/python demo-local-airflow/sandbox_smoke_test.py --image apache/airflow:3.1.0
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, ClassVar

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

import harness  # noqa: E402  (installs the Hermes host stubs, loads the plugin package)
from airflow_mlops.config import Settings  # noqa: E402
from airflow_mlops.sandbox import SandboxRequest, SandboxRunner  # noqa: E402

DAGS = PLUGIN_ROOT / "demo-local-airflow" / "dags"
WINDOWS_GIT_BASH = r"C:\Program Files\Git\bin\bash.exe"
BASH = WINDOWS_GIT_BASH if os.name == "nt" and Path(WINDOWS_GIT_BASH).is_file() else "bash"


def dispatch(tool: str, args: dict[str, Any]) -> str:
    assert tool == "terminal"
    proc = subprocess.run(
        [BASH, "-c", args["command"]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=args["timeout"],
        check=False,
    )
    return json.dumps(
        {"output": proc.stdout + proc.stderr, "exit_code": proc.returncode, "error": None}
    )


class StubClient:
    """Production structure of the demo DAGs, as the Airflow API would return it."""

    TASKS: ClassVar[dict[str, list[dict[str, Any]]]] = {
        "demo_train_critical": [
            {
                "task_id": "load",
                "operator": "_PythonDecoratedOperator",
                "downstream_task_ids": ["train"],
            },
            {
                "task_id": "train",
                "operator": "_PythonDecoratedOperator",
                "downstream_task_ids": ["notify"],
            },
            {
                "task_id": "notify",
                "operator": "_PythonDecoratedOperator",
                "downstream_task_ids": [],
            },
        ],
        "demo_train_ok": [
            {
                "task_id": "extract",
                "operator": "_PythonDecoratedOperator",
                "downstream_task_ids": ["quality_check"],
            },
            {
                "task_id": "quality_check",
                "operator": "_PythonDecoratedOperator",
                "downstream_task_ids": [],
            },
        ],
    }

    def get_tasks(self, dag_id: str) -> list[dict[str, Any]]:
        return self.TASKS.get(dag_id, [])


FIX = """--- a/mlops_demo.py
+++ b/mlops_demo.py
@@ -1,2 +1,2 @@
     def train(frame: dict) -> None:
-        print(frame["target"])  # KeyError -> critical
+        print(frame.get("target"))  # fixed: optional key
"""
BROKEN = """--- a/mlops_demo.py
+++ b/mlops_demo.py
@@ -1,2 +1,2 @@
     def train(frame: dict) -> None:
-        print(frame["target"])  # KeyError -> critical
+        print(frame["target"]  # missing parenthesis
"""
HOG = """--- /dev/null
+++ b/sbx_limits.py
@@ -0,0 +1,22 @@
+from datetime import datetime
+
+try:
+    from airflow.sdk import dag, task
+except ImportError:
+    from airflow.decorators import dag, task
+
+
+@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
+def sbx_limits():
+    @task
+    def memory_hog() -> None:
+        blob = bytearray(3 * 1024 * 1024 * 1024)  # 3 GB, above the 512m limit
+        print(len(blob))
+
+    @task
+    def network_call() -> None:
+        import urllib.request
+        urllib.request.urlopen("https://example.com", timeout=10)
+
+    memory_hog()
+    network_call()
+sbx_limits()
"""

SCENARIOS = [
    # title, dag_id, patch, level, task_id, settings overrides, expected result fields
    ("A. fix KeyError - import", "demo_train_critical", FIX, "import", None, {}, {"passed": True}),
    (
        "B. fix KeyError - structure",
        "demo_train_critical",
        FIX,
        "structure",
        "train",
        {},
        {"passed": True},
    ),
    (
        "C. broken patch - import",
        "demo_train_critical",
        BROKEN,
        "import",
        None,
        {},
        {"passed": False},
    ),
    (
        "D. task run (sample) - extract",
        "demo_train_ok",
        FIX,
        "task",
        "extract",
        {},
        {"passed": True},
    ),
    (
        "E. memory limit 512m - memory_hog",
        "sbx_limits",
        HOG,
        "task",
        "memory_hog",
        {"sandbox_memory": "512m"},
        {"passed": False, "oom": True},
    ),
    (
        "F. no network - network_call",
        "sbx_limits",
        HOG,
        "task",
        "network_call",
        {},
        {"passed": False, "oom": False},
    ),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--image", default="apache/airflow:2.9.3", help="Airflow image")
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="airflow-mlops-smoke-"))
    harness._DATA_DIR["root"] = work / "plugin-data"
    failures = 0
    try:
        for title, dag_id, patch, level, task_id, overrides, expect in SCENARIOS:
            settings = Settings(
                sandbox_enabled=True,
                sandbox_image=args.image,
                sandbox_dags_path=str(DAGS),
                sandbox_max_tests_per_session=100,
                sandbox_timeout_seconds=300,
                **overrides,
            )
            runner = SandboxRunner(StubClient(), settings, dispatch, session_id="smoke")  # type: ignore[arg-type]
            started = time.monotonic()
            try:
                result = runner.run(
                    SandboxRequest(dag_id=dag_id, patch=patch, level=level, task_id=task_id)
                )
            except Exception as exc:  # report and continue
                result = {"passed": None, "error": f"{type(exc).__name__}: {exc}"}
            ok = all(result.get(key) == value for key, value in expect.items())
            failures += 0 if ok else 1
            print(f"{'OK  ' if ok else 'FAIL'} {title}  ({round(time.monotonic() - started, 1)}s)")
            print(
                f"     passed={result.get('passed')} oom={result.get('oom')} "
                f"error={result.get('error')!r} hint={'yes' if result.get('hint') else 'no'}"
            )
            errors = (result.get("stages") or {}).get("import", {}).get("import_errors")
            if errors:
                print("     import error:", next(iter(errors.values())).strip().splitlines()[-1])
        source_ok = 'print(frame["target"])' in (DAGS / "mlops_demo.py").read_text("utf-8")
        leftovers = list((work / "plugin-data").rglob("sandbox/*"))
        print(f"source DAG untouched: {source_ok} | workspaces left: {len(leftovers)}")
        failures += 0 if source_ok and not leftovers else 1
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print("ALL SCENARIOS BEHAVED AS EXPECTED" if not failures else f"{failures} UNEXPECTED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
