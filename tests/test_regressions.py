"""Non-regression tests of the bugs found by the audit of 10/11/2026 (one test per bug)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from airflow_mlops import dag_guard, dag_review, storage
from airflow_mlops import workflow as wf
from airflow_mlops.airflow_client import AirflowClient, Credentials
from airflow_mlops.config import Settings
from airflow_mlops.guardrails import make_pre_tool_call_hook
from airflow_mlops.sandbox import SandboxError, apply_unified_diff

CLEAR = {"dag_id": "d", "run_id": "r", "task_ids": ["t"], "reason": "x"}


@pytest.mark.parametrize("dry_run", ["false", "False", " FALSE ", "no", 0, None, "0"])
def test_only_a_real_true_is_a_dry_run_for_the_approval_hook(dry_run: Any) -> None:
    """The hook sees the arguments before Hermes converts "false" to False: a text "false"
    passed for a dry run, then the handler ran a real clear without any approval."""
    hook = make_pre_tool_call_hook(lambda: Settings(read_only=False))
    decision = hook("airflow_clear_tasks", {**CLEAR, "dry_run": dry_run})
    assert decision is not None and decision["action"] == "approve"


@pytest.mark.parametrize("dry_run", [True, "true", "TRUE"])
def test_a_dry_run_needs_no_approval(dry_run: Any) -> None:
    hook = make_pre_tool_call_hook(lambda: Settings(read_only=False))
    assert hook("airflow_clear_tasks", {**CLEAR, "dry_run": dry_run}) is None
    assert hook("airflow_clear_tasks", CLEAR) is None  # dry_run defaults to true


def guard_in(dags: Path, *, busy: bool = False) -> dag_guard.DagWriteGuard:
    settings = Settings(sandbox_enabled=True, sandbox_dags_path=str(dags))
    return dag_guard.DagWriteGuard(
        lambda: settings, lambda: str(dags), audit_in_progress=lambda session: busy
    )


def test_the_lock_sees_a_relative_command_run_from_the_dags_folder(tmp_path: Path) -> None:
    """`sed -i ... x.py` from inside the DAGs folder never names the folder: it went through."""
    (tmp_path / "x.py").write_text("a = 1\n", encoding="utf-8")
    guard = guard_in(tmp_path)
    edit = guard.decide("terminal", {"command": "sed -i 's/a = 1/a = 2/' x.py"})
    assert edit is not None and edit["action"] == "approve"
    patch = guard.decide("terminal", {"command": "git apply fix.patch"})
    assert patch is not None and patch["action"] == "block"
    assert guard.decide("terminal", {"command": "ls -la"}) is None  # reading stays free


def test_during_an_audit_the_relative_command_is_blocked(tmp_path: Path) -> None:
    guard = guard_in(tmp_path, busy=True)
    decision = guard.decide("terminal", {"command": "sed -i 's/a/b/' x.py"})
    assert decision == {"action": "block", "message": dag_guard.AUDIT_BLOCK}


def test_the_lock_follows_a_symbolic_link_to_the_dags_folder(tmp_path: Path) -> None:
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "x.py").write_text("a = 1\n", encoding="utf-8")
    link = tmp_path / "alias"
    try:
        link.symlink_to(dags, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links are not allowed on this machine")
    guard = guard_in(dags)
    decision = guard.decide("write_file", {"path": str(link / "x.py"), "content": "a = 2\n"})
    assert decision is not None and decision["action"] == "block"


def test_a_patch_that_changes_nothing_is_refused(tmp_path: Path) -> None:
    """Real case (09/11): an empty fix "passed" the sandbox and was recorded as tested."""
    (tmp_path / "x.py").write_text("a = 1\nb = 2\n", encoding="utf-8")
    noop = "--- a/x.py\n+++ b/x.py\n@@ -1,2 +1,2 @@\n a = 1\n-b = 2\n+b = 2\n"
    with pytest.raises(SandboxError, match="changes nothing"):
        apply_unified_diff(tmp_path, noop)
    real = noop.replace("+b = 2", "+b = 3")
    assert apply_unified_diff(tmp_path, real) == ["x.py"]


def test_long_journal_details_stay_readable() -> None:
    """The JSON text was cut at 4000 characters: reading the journal then raised."""
    details = {"task_instances": [f"a_rather_long_task_name_{i}[{i}]" for i in range(400)]}
    storage.add_audit_entry(
        tool="airflow_clear_tasks",
        dag_id="d",
        run_id="r",
        task_ids=["t"],
        dry_run=False,
        auto=False,
        reason="x",
        outcome="cleared",
        details=details,
    )
    (entry,) = storage.list_audit_entries(0)
    assert entry["details"]["truncated"] is True


def test_an_unreadable_journal_row_of_an_older_version_is_tolerated() -> None:
    entry_id = storage.add_audit_entry(
        tool="t",
        dag_id="d",
        run_id=None,
        task_ids=[],
        dry_run=True,
        auto=False,
        reason="",
        outcome="x",
    )
    with storage.connect() as conn:
        conn.execute("UPDATE audit_log SET details = ? WHERE id = ?", ('{"cut": "abc', entry_id))
    assert storage.list_audit_entries(0)[0]["details"] == {}


def test_pagination_goes_on_when_airflow_caps_the_page_size() -> None:
    """page_size above Airflow's maximum_page_limit: the first short page ended the listing."""

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        limit = min(int(request.url.params["limit"]), 100)  # the server-side cap
        dags = [{"dag_id": f"d{i}"} for i in range(offset, min(offset + limit, 250))]
        return httpx.Response(200, json={"dags": dags, "total_entries": 250})

    settings = Settings(auth_mode="basic", api_version="v1", page_size=200)
    client = AirflowClient(settings, Credentials("u", "p"), transport=httpx.MockTransport(handler))
    assert len(client.list_dags()) == 250


class _Config:
    def __init__(self, allowlist: Any) -> None:
        self.allowlist = allowlist

    def get_config(self, key: str, default: Any = None) -> Any:
        return self.allowlist if key == "dag_allowlist" else default


@pytest.mark.parametrize("malformed", [None, {"a": 1}, 5])
def test_a_malformed_allowlist_allows_nothing(malformed: Any) -> None:
    """It became "*" (everything allowed): an empty YAML key opened every DAG."""
    settings = Settings.from_source(_Config(malformed))
    assert settings.dag_allowlist == () and not settings.dag_allowed("any_dag")


def test_the_default_allowlist_is_still_everything() -> None:
    class Empty:
        def get_config(self, key: str, default: Any = None) -> Any:
            return default

    assert Settings.from_source(Empty()).dag_allowed("any_dag")


def test_none_failed_on_a_final_task_is_not_a_masking_rule() -> None:
    """With none_failed a failed upstream makes the task upstream_failed: the run fails."""
    tasks = [
        {"task_id": "a", "downstream_task_ids": ["end"], "trigger_rule": "all_success"},
        {"task_id": "end", "downstream_task_ids": [], "trigger_rule": "none_failed"},
    ]
    review = dag_review.review_dag({"content": "x = 1\n", "first_line": 1}, tasks)
    assert "masking_trigger_rule" not in [f["rule"] for f in review["findings"]]
    tasks[1]["trigger_rule"] = "all_done"
    review = dag_review.review_dag({"content": "x = 1\n", "first_line": 1}, tasks)
    assert "masking_trigger_rule" in [f["rule"] for f in review["findings"]]


def test_a_dag_id_with_and_inside_is_one_selection() -> None:
    audit = wf.Audit(1, 0, 0, None, "all", "24h", "open", [])
    assert wf.parse_selection("extract-and-load", audit) == ([], ["extract-and-load"])
    assert wf.parse_selection("Import_Et_Export", audit) == ([], ["Import_Et_Export"])
    assert wf.parse_selection("dag_a and dag_b", audit) == ([], ["dag_a", "dag_b"])


def test_source_code_keeps_its_names_and_masks_its_literal_secrets() -> None:
    """On code the free-text rules masked names (`def f(token: str)` became a fake secret in
    the DAG review, and the line was unusable as diff context)."""
    from airflow_mlops.redaction import redact_code

    for line in (
        "def fetch(token: str, retries=3):",
        'password = Variable.get("db_password")',
        "client = Api(api_key=api_key)",
    ):
        assert redact_code(line) == line
    assert "hunter2" not in redact_code('password = "hunter2"')
    assert "s3cr3t" not in redact_code('uri = "postgresql://user:s3cr3t@db:5432/x"')


def test_a_scheduled_airflow2_run_is_in_the_period_it_became_due() -> None:
    """Airflow 2: logical_date is the START of the interval. A daily run due at 00:00 today has
    logical_date yesterday 00:00 and fell out of a 24h report."""
    from datetime import UTC, datetime

    from airflow_mlops.reporting import ReportBuilder

    now = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
    builder = ReportBuilder(None, Settings(), now=now)  # type: ignore[arg-type]
    run = {
        "logical_date": "2026-11-09T00:00:00+00:00",
        "data_interval_end": "2026-11-10T00:00:00+00:00",
        "start_date": "2026-11-10T00:00:05+00:00",
    }
    start = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
    assert builder._in_window(run, start)
    assert not builder._in_window({"logical_date": run["logical_date"]}, start)


def test_a_cached_classification_is_only_reused_for_the_same_try(monkeypatch: Any) -> None:
    """A first OOM is transient; the same error on the next try must be classified again."""
    from airflow_mlops import guardrails
    from airflow_mlops.guardrails import ClearRequest, ClearTasksAction

    storage.save_diagnosis(
        "d",
        "r",
        "t",
        map_index=-1,
        try_number=1,
        category="transient",
        rule_ids=[],
        justification="first try",
    )

    class Client:
        try_number = 1

        def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
            return [{"task_id": "t", "state": "failed", "try_number": self.try_number}]

    seen: list[int] = []

    def classify_again(client: Any, settings: Any, rules: Any, dag: str, run: str, ti: Any) -> Any:
        seen.append(ti["try_number"])
        from airflow_mlops.diagnosis import Classification, TaskEvidence

        return TaskEvidence(Classification("critical", "same error again"))

    monkeypatch.setattr(guardrails, "read_and_classify", classify_again)
    client = Client()
    action = ClearTasksAction(client, Settings(), None)  # type: ignore[arg-type]
    request = ClearRequest(dag_id="d", run_id="r", task_ids=["t"])
    assert action._classifications(request, {}) == {"t": "transient"}
    assert seen == []
    client.try_number = 2
    assert action._classifications(request, {}) == {"t": "critical"}
    assert seen == [2]


def test_a_forgotten_audit_stops_locking_other_conversations() -> None:
    """An open audit blocked every DAG write, in every conversation, for 24 hours."""
    import time

    from airflow_mlops.audit_tools import AuditTools
    from airflow_mlops.runtime import PluginRuntime
    from airflow_mlops.tools import ReadTools
    from harness import FakeContext

    runtime = PluginRuntime(FakeContext())
    tools = AuditTools(runtime, ReadTools(runtime))
    audit = wf.create_audit("all", "24h", [], "s-audit")
    assert tools.audit_in_progress("s-audit") and tools.audit_in_progress("another")
    wf.save_audit(audit, now=time.time() - 2 * 3600)  # idle for two hours
    assert tools.audit_in_progress("s-audit")  # its own conversation stays locked
    assert not tools.audit_in_progress("another")


def test_a_changed_secret_gets_a_new_client() -> None:
    from airflow_mlops.runtime import PluginRuntime
    from harness import SECRETS, FakeContext

    runtime = PluginRuntime(FakeContext({"auth_mode": "basic", "api_version": "v1"}))
    first = runtime.client()
    assert runtime.client() is first
    SECRETS["AIRFLOW_PASSWORD"] = "rotated"
    assert runtime.client() is not first


def test_a_mapped_task_with_a_succeeded_instance_is_not_a_whole_dag_rerun() -> None:
    """Instances were keyed by task_id: the last map_index decided for every instance."""
    from airflow_mlops.guardrails import ClearRequest, ClearTasksAction

    class Client:
        def get_tasks(self, dag_id: str) -> list[dict[str, Any]]:
            return [{"task_id": "a"}, {"task_id": "m"}]

        def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
            return [
                {"task_id": "a", "state": "failed", "map_index": -1, "try_number": 1},
                {"task_id": "m", "state": "failed", "map_index": 0, "try_number": 1},
                {"task_id": "m", "state": "success", "map_index": 1, "try_number": 1},
            ]

        def clear_task_instances(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return [
                {"task_id": "a", "map_index": -1, "run_id": "r", "state": "failed"},
                {"task_id": "m", "map_index": 0, "run_id": "r", "state": "failed"},
            ]

    for task in ("a", "m"):
        storage.save_diagnosis(
            "d",
            "r",
            task,
            map_index=-1,
            try_number=1,
            category="transient",
            rule_ids=[],
            justification="x",
        )
    action = ClearTasksAction(Client(), Settings(read_only=False), None)  # type: ignore[arg-type]
    result = action.run(ClearRequest(dag_id="d", run_id="r", task_ids=["a", "m"]))
    assert result["outcome"] == "dry_run" and result["rerun_succeeded_tasks"] == []
