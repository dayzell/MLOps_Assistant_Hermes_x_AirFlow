"""Manual end-to-end walkthrough of the plugin tools against a REAL Airflow (no LLM needed).

Not part of the plugin, not run by pytest. It calls the plugin's tool handlers exactly as
Hermes would (same arguments, same JSON results), in the order an agent following the
``airflow-triage`` / ``airflow-fix-proposal`` / ``airflow-report`` skills would.

Prerequisites:
    docker compose -f demo-local-airflow/docker-compose.airflow2.yml up -d
    (then trigger the demo DAGs: see the README of demo-local-airflow)

Usage (from the plugin root):
    .venv/Scripts/python demo-local-airflow/e2e_walkthrough.py                     # Airflow 2.9
    .venv/Scripts/python demo-local-airflow/e2e_walkthrough.py --base-url http://localhost:8081 \\
        --api-version v2 --auth jwt --image apache/airflow:3.4.0                    # Airflow 3.4
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_ROOT / "tests"))

import harness  # noqa: E402  (Hermes host stubs + plugin package)

WINDOWS_GIT_BASH = r"C:\Program Files\Git\bin\bash.exe"
BASH = WINDOWS_GIT_BASH if os.name == "nt" and Path(WINDOWS_GIT_BASH).is_file() else "bash"
FIX = """--- a/mlops_demo.py
+++ b/mlops_demo.py
@@ -1,2 +1,2 @@
     def train(frame: dict) -> None:
-        print(frame["target"])  # KeyError -> critical
+        print(frame.get("target"))  # fixed: optional key
"""


def terminal(tool: str, args: dict[str, Any]) -> str:
    """Stand-in for Hermes' terminal tool (same shell, same JSON shape)."""
    proc = subprocess.run(
        [BASH, "-c", args["command"]],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=args["timeout"],
        check=False,
    )
    output = proc.stdout + proc.stderr
    return json.dumps({"output": output, "exit_code": proc.returncode, "error": None})


def step(title: str) -> None:
    print(f"\n=== {title}")


def show(result: dict[str, Any], keys: list[str]) -> None:
    if not result.get("ok"):
        print("  ERROR:", result.get("error"))
        return
    for key in keys:
        value = result.get(key)
        text = json.dumps(value, ensure_ascii=False, default=str)
        print(f"  {key}: {text[:400]}{' ...' if len(text) > 400 else ''}")


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # Windows consoles default to cp1252
    parser = argparse.ArgumentParser(description="End-to-end walkthrough on a real Airflow")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--api-version", choices=("v1", "v2"), default="v1")
    parser.add_argument("--auth", choices=("basic", "token", "jwt"), default="basic")
    parser.add_argument("--image", default="apache/airflow:2.9.3", help="sandbox image")
    options = parser.parse_args()
    print(f"Airflow {options.base_url} (API {options.api_version}, auth {options.auth}), "
          f"sandbox image {options.image}")
    work = Path(tempfile.mkdtemp(prefix="airflow-mlops-e2e-"))
    harness._DATA_DIR["root"] = work / "plugin-data"
    harness.SECRETS.update({"AIRFLOW_USERNAME": "admin", "AIRFLOW_PASSWORD": "admin"})
    ctx = harness.FakeContext(
        {
            "base_url": options.base_url,
            "api_version": options.api_version,
            "auth_mode": options.auth,
            "read_only": False,
            "sandbox_enabled": True,
            "sandbox_image": options.image,
            "sandbox_dags_path": str(PLUGIN_ROOT / "demo-local-airflow" / "dags"),
        }
    )
    import airflow_mlops

    airflow_mlops.register(ctx)
    ctx.dispatch_handler = terminal
    hook = ctx.hooks["pre_tool_call"][0]

    step("1. airflow_list_dags")
    show(ctx.call("airflow_list_dags"), ["count", "dags"])

    step("2. airflow_get_dag_runs - latest runs of each demo DAG")
    runs: dict[str, dict[str, Any]] = {}
    for dag_id in ("demo_train_ok", "demo_train_critical", "demo_fetch_transient"):
        result = ctx.call("airflow_get_dag_runs", dag_id=dag_id, limit=1)
        runs[dag_id] = result["dag_runs"][0]
        print(f"  {dag_id}: {runs[dag_id]['state']} ({runs[dag_id]['run_id']})")

    step("3. airflow_diagnose_run - demo_train_critical")
    critical_run = runs["demo_train_critical"]["run_id"]
    diag = ctx.call("airflow_diagnose_run", dag_id="demo_train_critical", run_id=critical_run)
    show(diag, ["status", "summary"])
    for root in diag.get("root_failures", []):
        print("  root:", root["task_id"], root["classification"]["category"], "|",
              root["classification"]["justification"][:200])
        print("  error_block:", (root.get("error_block") or "")[-300:].replace("\n", " | "))

    step("4. airflow_diagnose_run - demo_fetch_transient")
    transient_run = runs["demo_fetch_transient"]["run_id"]
    diag_t = ctx.call("airflow_diagnose_run", dag_id="demo_fetch_transient", run_id=transient_run)
    show(diag_t, ["status", "summary", "impacted"])

    step("5. airflow_get_task_logs / airflow_get_xcoms / airflow_get_dependencies")
    show(ctx.call("airflow_get_task_logs", dag_id="demo_train_critical", run_id=critical_run,
                  task_id="train", max_lines=5), ["try_number", "total_lines", "error_block"])
    ok_run = runs["demo_train_ok"]["run_id"]
    show(ctx.call("airflow_get_xcoms", dag_id="demo_train_ok", run_id=ok_run,
                  task_id="quality_check"), ["xcoms"])
    show(ctx.call("airflow_get_dependencies", dag_id="demo_train_critical", task_id="train"),
         ["upstream", "downstream"])

    step("6. airflow_sandbox_test - the fix proposal, levels import then structure")
    for level in ("import", "structure"):
        res = ctx.call("airflow_sandbox_test", dag_id="demo_train_critical", patch=FIX,
                       level=level, task_id="train")
        show(res, ["passed", "stages", "patched_files", "error"])

    step("7. airflow_clear_tasks - dry run then real retry of the transient failure")
    retry = [c["task_id"] for c in diag_t["summary"]["retry_candidates"]]
    show(ctx.call("airflow_clear_tasks", dag_id="demo_fetch_transient", run_id=transient_run,
                  task_ids=retry), ["outcome", "count", "task_instances", "classification"])
    args = {"dag_id": "demo_fetch_transient", "run_id": transient_run, "task_ids": retry,
            "dry_run": False, "reason": "e2e walkthrough: transient ConnectionResetError"}
    print("  pre_tool_call hook decision:", json.dumps(hook(tool_name="airflow_clear_tasks",
                                                             args=args))[:300])
    print("  (the human approves in Hermes; here we proceed as if approved)")
    show(ctx.call("airflow_clear_tasks", **args), ["outcome", "approval", "count",
                                                   "retry_attempts", "audit_id"])

    step("8. airflow_build_report - all DAGs, last 24h")
    report = ctx.call("airflow_build_report", period="24h")
    if report.get("ok"):
        print(report["markdown"])
    else:
        print("  ERROR:", report.get("error"))


if __name__ == "__main__":
    main()
