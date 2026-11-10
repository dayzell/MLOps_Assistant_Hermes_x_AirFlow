"""Plan v2, step 4: airflow_review_dag (deterministic checks, exact lines)."""

from __future__ import annotations

from typing import Any

from airflow_mlops.dag_review import review_dag
from harness import ROOT, make_context

# A frozen copy of the demo DAG: the real one changes when fixes are tried on it.
DEMO = (ROOT / "tests" / "fixtures" / "mlops_demo.py").read_text(encoding="utf-8")


def source(content: str, **extra: Any) -> dict[str, Any]:
    return {"dag_id": "d", "path": "d.py", "content": content, "first_line": 1, **extra}


def task(task_id: str, downstream: list[str], rule: str = "all_success", retries: int = 2) -> dict:
    return {
        "task_id": task_id,
        "downstream_task_ids": downstream,
        "trigger_rule": rule,
        "retries": retries,
    }


def rules(result: dict[str, Any]) -> dict[str, int | None]:
    return {f["rule"]: f["line"] for f in result["findings"]}


def test_demo_dag_with_a_masking_final_task() -> None:
    """The incident of the demo: notify (all_done) hides the failure of train."""
    tasks = [task("load", ["train"]), task("train", ["notify"]), task("notify", [], "all_done")]
    result = review_dag(source(DEMO), tasks)
    assert rules(result)["masking_trigger_rule"] == 41  # @task(trigger_rule="all_done")
    first = result["findings"][0]
    assert first["severity"] == "high" and first["task_id"] == "notify"
    assert "one_failed" in first["suggestion"]
    assert result["verdict"] == "fix before deploying"


def test_a_watcher_task_makes_it_fine() -> None:
    tasks = [
        task("train", ["notify", "watcher"]),
        task("notify", [], "all_done"),
        task("watcher", [], "one_failed"),
    ]
    assert "masking_trigger_rule" not in rules(review_dag(source(DEMO), tasks))


def test_no_retries_points_at_the_line() -> None:
    tasks = [task("fetch", ["publish"], retries=0), task("publish", [])]
    result = review_dag(source(DEMO), tasks)
    assert rules(result)["no_retries"] == 50  # @task(retries=0)
    assert result["verdict"] == "review the warnings"


def test_code_checks() -> None:
    code = (
        "import requests\n"
        "from airflow.models import Variable\n"
        'TOKEN = "[REDACTED]"\n'
        'DATA = requests.get("https://api")\n'
        'LIMIT = Variable.get("limit")\n'
        "def f():\n"
        "    try:\n"
        "        g()\n"
        "    except Exception:\n"
        "        pass\n"
        "with DAG('d', catchup=True) as dag:\n"
        "    rows = requests.get('https://x')\n"
    )
    result = review_dag(source(code), [])
    found = [(f["rule"], f["line"]) for f in result["findings"]]
    assert ("hardcoded_secret", 3) in found
    assert ("top_level_work", 4) in found and ("top_level_work", 5) in found
    assert ("top_level_work", 12) in found  # inside `with DAG(...)`: still parse time
    assert ("swallowed_exception", 9) in found
    assert ("catchup_enabled", 11) in found
    assert result["counts"]["high"] == 1


def test_work_inside_a_task_is_fine() -> None:
    code = "import requests\n\ndef fetch():\n    return requests.get('https://api')\n"
    assert review_dag(source(code), [])["findings"] == []


def test_syntax_error_and_truncated_file() -> None:
    broken = review_dag(source("def f(:\n    pass\n"), [])
    assert broken["findings"][0]["rule"] == "syntax_error"
    truncated = review_dag(source(DEMO, truncated=True), [])
    assert truncated["notes"] and truncated["verdict"] == "no blocking issue found"


def test_tool_is_registered_and_read_only() -> None:
    ctx = make_context()
    tool = ctx.tools["airflow_review_dag"]
    assert tool["check_fn"] is None  # always available, even in read-only mode
    assert tool["schema"]["parameters"]["required"] == ["dag_id"]


def test_lines_belong_to_the_reviewed_dag() -> None:
    """The demo file holds 3 DAGs: lines must come from the task's own definition."""
    tasks = [
        task("load", ["train"], retries=0),
        task("train", ["notify"], retries=0),
        task("notify", [], "all_done", retries=0),
    ]
    found = rules(review_dag(source(DEMO), tasks))
    assert found["masking_trigger_rule"] == 41
    assert found["no_retries"] == 34  # def load(), not fetch's @task(retries=0) at line 50
