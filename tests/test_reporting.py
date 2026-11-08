"""Standard report: fixed shape, metrics, data-quality convention, baselines, slash command."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import payloads as p
import pytest
import respx
from airflow_mlops import storage
from airflow_mlops.reporting import (
    SECTION_KEYS,
    SECTION_TITLES,
    parse_period,
    parse_scope,
    percentile,
    validate_data_quality,
)
from airflow_mlops.tools import parse_command_args
from harness import FakeContext, make_context

NOW = datetime.now(UTC)


def ts(hours_ago: float) -> str:
    return (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(run_id: str, state: str, hours_ago: float, duration: float = 600.0) -> dict[str, Any]:
    return {
        "dag_run_id": run_id,
        "state": state,
        "run_type": "scheduled",
        "logical_date": ts(hours_ago),
        "start_date": ts(hours_ago),
        "end_date": ts(hours_ago - duration / 3600),
    }


def ti(
    task_id: str,
    state: str,
    duration: float,
    *,
    tries: int = 1,
    queued: float = 5.0,
) -> dict[str, Any]:
    start = NOW - timedelta(hours=1)
    return {
        "task_id": task_id,
        "state": state,
        "try_number": tries,
        "max_tries": 3,
        "duration": duration,
        "map_index": -1,
        "start_date": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_date": (start + timedelta(seconds=duration)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "queued_when": (start - timedelta(seconds=queued)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def collection(key: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {key: items, "total_entries": len(items)}


def mock_airflow(airflow: respx.MockRouter, api_version: str) -> None:
    """Two DAGs: 'train' (healthy, data-quality instrumented) and 'etl' (1 failure in 3)."""
    airflow.get("/dags").respond(
        json=collection(
            "dags", [p.dag(api_version, "train", tags=["ml"]), p.dag(api_version, "etl")]
        )
    )
    airflow.get("/dags/train/details").respond(json=p.dag_details(api_version, "train"))
    airflow.get("/dags/etl/details").respond(json=p.dag_details(api_version, "etl"))
    airflow.get("/dags/train/dagRuns").respond(
        json=collection(
            "dag_runs",
            [
                run("t3", "success", 2),
                run("t2", "success", 6),
                run("t1", "success", 10),
                run("old", "success", 24 * 30),
            ],
        )  # outside the window
    )
    for run_id, dur in (("t3", 100.0), ("t2", 110.0), ("t1", 90.0)):
        airflow.get(f"/dags/train/dagRuns/{run_id}/taskInstances").respond(
            json=collection(
                "task_instances", [ti("fit", "success", dur), ti("quality_check", "success", 10.0)]
            )
        )
    airflow.get("/dags/etl/dagRuns").respond(
        json=collection(
            "dag_runs",
            [run("e3", "failed", 1, 4000.0), run("e2", "success", 5), run("e1", "success", 9)],
        )
    )
    for run_id, state in (("e3", "failed"), ("e2", "success"), ("e1", "success")):
        airflow.get(f"/dags/etl/dagRuns/{run_id}/taskInstances").respond(
            json=collection("task_instances", [ti("load", state, 50.0, tries=2, queued=400.0)])
        )
    # Data-quality XCom on train.quality_check (latest successful run t3), none elsewhere.
    dq_base = "/dags/train/dagRuns/t3/taskInstances/quality_check/xcomEntries"
    airflow.get(dq_base).respond(
        json=collection("xcom_entries", [{"key": "data_quality", "map_index": -1}])
    )
    airflow.get(f"{dq_base}/data_quality").respond(
        json={
            "key": "data_quality",
            "value": {"rows": 1200, "null_ratio": 0.01, "freshness": "PT1H", "checks_passed": True},
        }
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(json=collection("xcom_entries", []))


# --------------------------------------------------------------------------- unit helpers


@pytest.mark.parametrize(
    ("text", "seconds"), [("30m", 1800), ("24h", 86400), ("7d", 604800), ("2W", 1209600)]
)
def test_parse_period(text: str, seconds: int) -> None:
    assert parse_period(text).total_seconds() == seconds


@pytest.mark.parametrize("text", ["", "24", "h", "0h", "-1d", "91d", "1y"])
def test_parse_period_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        parse_period(text)


def test_parse_scope() -> None:
    assert parse_scope(None).kind == "all"
    assert parse_scope("*").kind == "all"
    assert parse_scope("tag:ml").value == "ml"
    assert parse_scope("train").kind == "dag"
    with pytest.raises(ValueError):
        parse_scope("tag:")


def test_percentile() -> None:
    assert percentile([], 50) is None
    assert percentile([5.0], 95) == 5.0
    assert percentile([1, 2, 3, 4], 50) == 2.5
    assert percentile(list(range(1, 101)), 95) == 95.05


@pytest.mark.parametrize(
    ("value", "ok", "problem"),
    [
        ({"rows": 10, "null_ratio": 0.1, "freshness": "PT1H", "checks_passed": True}, True, None),
        ('{"rows": 10, "null_ratio": 0, "freshness": 3600, "checks_passed": false}', True, None),
        ("{'rows': 1, 'null_ratio': 0.5, 'freshness': 'x', 'checks_passed': True}", True, None),
        ({"rows": 10, "checks_passed": True}, False, "missing 'null_ratio'"),
        (
            {"rows": True, "null_ratio": 0, "freshness": "x", "checks_passed": True},
            False,
            "'rows' has the wrong type",
        ),
        (
            {"rows": 1, "null_ratio": 2, "freshness": "x", "checks_passed": True},
            False,
            "'null_ratio' must be between 0 and 1",
        ),
        ("not json", False, "value is not an object"),
    ],
)
def test_validate_data_quality(value: Any, ok: bool, problem: str | None) -> None:
    normalised, problems = validate_data_quality(value)
    assert (normalised is not None) is ok
    if problem:
        assert problem in problems


def test_parse_command_args() -> None:
    assert parse_command_args("") == ("all", None)
    assert parse_command_args("train 7d") == ("train", "7d")
    assert parse_command_args("24h tag:ml") == ("tag:ml", "24h")


# --------------------------------------------------------------------------- full report


def test_report_has_the_fixed_shape_and_metrics(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    result = ctx.call("airflow_build_report", scope="all", period="24h")
    assert result["ok"] is True, result
    report = result["report"]
    assert tuple(report) == SECTION_KEYS  # same keys, same order, always

    scope = report["scope_period"]
    assert scope["dags"] == ["etl", "train"]
    assert scope["period"] == "24h"

    health = report["health"]
    assert health["total_runs"] == 6  # the 30-day-old run is excluded
    assert health["failed"] == 1 and health["success"] == 5
    assert health["success_rate"] == pytest.approx(5 / 6, abs=1e-4)
    # per-DAG statuses; global = derived from DAGs (1 of 2 failing -> half -> failing)
    statuses = {d["dag_id"]: (d["status"], d["status_reason"]) for d in health["per_dag"]}
    assert statuses["train"] == ("healthy", "aucun incident")
    assert statuses["etl"] == ("failing", "dernier run en échec")
    assert health["dag_status_counts"] == {"failing": 1, "degraded": 0, "healthy": 1, "no_data": 0}
    assert health["status"] == "failing"
    assert health["status_reason"] == "1 DAG(s) sur 2 en échec : etl (dernier run en échec)"
    assert health["incidents"] == 1

    tasks = {(t["dag_id"], t["task_id"]): t for t in report["performance"]["tasks"]}
    fit = tasks[("train", "fit")]
    assert fit["duration_p50_s"] == 100.0
    assert fit["trend"] == "no_baseline"
    load = tasks[("etl", "load")]
    assert load["retries"] == 3
    assert load["queue_p95_s"] == 400.0

    quality = {d["dag_id"]: d for d in report["data_quality"]["per_dag"]}
    assert quality["train"]["status"] == "instrumented"
    assert quality["train"]["checks"][0]["rows"] == 1200
    assert quality["etl"]["status"] == "not_instrumented"

    incidents = report["incidents"]
    assert incidents["failed_run_count"] == 1
    assert incidents["failed_runs"][0]["failed_tasks"] == ["load"]
    assert incidents["actions"] == []

    messages = " ".join(r["message"] for r in report["recommendations"])
    assert "airflow_diagnose_run" in messages
    assert "SLA" in messages  # not configured
    assert report["recommendations"][0]["severity"] in ("high", "medium")


def test_markdown_always_has_the_six_sections_in_order(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    markdown = ctx.call("airflow_build_report", period="24h")["markdown"]
    positions = [markdown.index(f"## {SECTION_TITLES[key]}") for key in SECTION_KEYS]
    assert positions == sorted(positions)
    for key in SECTION_KEYS:
        assert markdown.count(f"## {SECTION_TITLES[key]}") == 1
    assert "non instrumenté" in markdown  # etl has no data-quality XCom
    assert "SLA : non configuré" in markdown


def test_empty_scope_keeps_the_same_structure(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags").respond(json=collection("dags", []))
    result = ctx.call("airflow_build_report")
    report = result["report"]
    assert tuple(report) == SECTION_KEYS
    assert report["health"]["status"] == "no_data"
    assert report["data_quality"]["status"] == "not_instrumented"
    assert report["recommendations"][0]["message"].startswith("Aucune action requise")
    for key in SECTION_KEYS:
        assert f"## {SECTION_TITLES[key]}" in result["markdown"]


def test_baseline_trend_and_update(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    storage.update_baseline("train", "fit", p50=50.0, p95=60.0, samples=10)
    mock_airflow(airflow, api_version)
    report = ctx.call("airflow_build_report", scope="train", period="24h")["report"]
    fit = next(t for t in report["performance"]["tasks"] if t["task_id"] == "fit")
    assert fit["baseline_p50_s"] == 50.0
    assert fit["trend"] == "slower"
    assert fit["change_vs_baseline"] == 1.0  # 100 s vs 50 s
    assert any("plus lente" in r["message"] for r in report["recommendations"])
    updated = storage.get_baselines("train")["fit"]
    assert updated.p50 == pytest.approx(0.7 * 50 + 0.3 * 100)
    assert updated.samples == 13


def test_update_baseline_false_leaves_baselines(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    ctx.call("airflow_build_report", scope="train", period="24h", update_baseline=False)
    assert storage.get_baselines("train") == {}


def test_sla_thresholds(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(
        api_version=api_version, auth_mode="basic", sla_thresholds={"etl": 3600, "train.fit": 105}
    )
    mock_airflow(airflow, api_version)
    sla = ctx.call("airflow_build_report", period="24h")["report"]["performance"]["sla"]
    assert sla["configured"] is True
    levels = sorted((m["level"], m["dag_id"], m.get("task_id")) for m in sla["misses"])
    assert levels == [("dag", "etl", None), ("task", "train", "fit")]


def test_actions_from_audit_log_and_cached_diagnosis(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    storage.add_audit_entry(
        tool="airflow_clear_tasks",
        dag_id="etl",
        run_id="e3",
        task_ids=["load"],
        dry_run=False,
        auto=False,
        reason="retry",
        outcome="cleared",
    )
    storage.add_audit_entry(
        tool="airflow_clear_tasks",
        dag_id="other",
        run_id="x",
        task_ids=["t"],
        dry_run=False,
        auto=False,
        reason="",
        outcome="x",
    )
    storage.save_diagnosis(
        "etl",
        "e3",
        "load",
        map_index=-1,
        try_number=2,
        category="transient",
        rule_ids=["connection_reset"],
        justification="j",
    )
    mock_airflow(airflow, api_version)
    result = ctx.call("airflow_build_report", period="24h")
    incidents = result["report"]["incidents"]
    assert [a["dag_id"] for a in incidents["actions"]] == ["etl"]  # scope-filtered
    assert incidents["failed_runs"][0]["diagnosis"] == {"load": "transient"}
    assert "load=transient" in result["markdown"]


def test_non_conformant_data_quality(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    airflow.get("/dags/train/details").respond(json=p.dag_details(api_version, "train"))
    airflow.get("/dags/train/dagRuns").respond(
        json=collection("dag_runs", [run("t1", "success", 1)])
    )
    airflow.get("/dags/train/dagRuns/t1/taskInstances").respond(
        json=collection("task_instances", [ti("check", "success", 1.0)])
    )
    base = "/dags/train/dagRuns/t1/taskInstances/check/xcomEntries"
    airflow.get(base).respond(json=collection("xcom_entries", [{"key": "data_quality"}]))
    airflow.get(f"{base}/data_quality").respond(json={"value": {"rows": 3}})
    report = ctx.call("airflow_build_report", scope="train", period="24h")["report"]
    dq = report["data_quality"]["per_dag"][0]
    assert dq["status"] == "non_conformant"
    assert "missing 'checks_passed'" in dq["invalid"][0]["problems"]


def test_tag_scope_is_forwarded(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    route = airflow.get("/dags").respond(json=collection("dags", []))
    ctx.call("airflow_build_report", scope="tag:ml")
    assert route.calls.last.request.url.params.get_list("tags") == ["ml"]


def test_report_refuses_dag_outside_allowlist(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", dag_allowlist=["etl*"])
    result = ctx.call("airflow_build_report", scope="train")
    assert result["error"]["type"] == "dag_not_allowed"
    assert len(airflow.calls) == 0


def test_report_rejects_bad_period(ctx: FakeContext) -> None:
    result = ctx.call("airflow_build_report", period="forever")
    assert result["error"]["type"] == "invalid_argument"


def test_report_allowlist_filters_all_scope(api_version: str, airflow: respx.MockRouter) -> None:
    ctx = make_context(api_version=api_version, auth_mode="basic", dag_allowlist=["train"])
    mock_airflow(airflow, api_version)
    report = ctx.call("airflow_build_report", period="24h")["report"]
    assert report["scope_period"]["dags"] == ["train"]


# --------------------------------------------------------------------------- slash command


def test_slash_command_returns_markdown(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    command = ctx.commands["airflow-report"]
    assert command["args_hint"]
    text = command["handler"]("train 24h")
    assert text.startswith("# Rapport de monitoring Airflow — train — 24h")


def test_slash_command_reports_errors_as_text(ctx: FakeContext) -> None:
    text = ctx.commands["airflow-report"]["handler"]("all 99y")
    assert text.startswith("❌ /airflow-report")


def test_masked_failures_in_success_runs(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """Found on a real Airflow: an all_done final task makes a failed run look successful."""
    airflow.get("/dags/train/details").respond(json=p.dag_details(api_version, "train"))
    airflow.get("/dags/train/dagRuns").respond(
        json=collection("dag_runs", [run("m1", "success", 1)])
    )
    airflow.get("/dags/train/dagRuns/m1/taskInstances").respond(
        json=collection(
            "task_instances", [ti("train", "failed", 2.0), ti("notify", "success", 1.0)]
        )
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(json=collection("xcom_entries", []))
    result = ctx.call("airflow_build_report", scope="train", period="24h")
    report = result["report"]
    assert report["health"]["masked_failures"] == 1
    assert report["health"]["incidents"] == 1
    # the latest run hides a failed task: this DAG is broken right now
    assert report["health"]["status"] == "failing"
    assert report["health"]["per_dag"][0]["status_reason"].startswith("échec masqué")
    masked = report["incidents"]["failed_runs"][0]
    assert masked["masked"] is True and masked["failed_tasks"] == ["train"]
    assert report["incidents"]["masked_run_count"] == 1
    assert report["recommendations"][0]["severity"] == "high"
    assert "trigger_rule" in report["recommendations"][0]["message"]
    assert "échec masqué" in result["markdown"]


def test_one_recovered_dag_among_several_is_only_degraded(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """DAGs are independent: an old failure on one DAG, now recovered, must not make the
    whole platform 'failing'; the reason names the DAG concerned."""
    airflow.get("/dags").respond(
        json=collection("dags", [p.dag(api_version, d) for d in ("a", "b", "c")])
    )
    airflow.get("/dags/a/dagRuns").respond(
        json=collection("dag_runs", [run("a2", "success", 1), run("a1", "failed", 5)])
    )
    for dag_id in ("b", "c"):
        airflow.get(f"/dags/{dag_id}/dagRuns").respond(
            json=collection("dag_runs", [run(f"{dag_id}1", "success", 2)])
        )
    airflow.get(path__regex=r".*/taskInstances$").respond(
        json=collection("task_instances", [ti("t", "success", 1.0)])
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(json=collection("xcom_entries", []))
    result = ctx.call("airflow_build_report", period="24h")
    health = result["report"]["health"]
    assert health["status"] == "degraded"
    assert health["status_reason"].startswith("1 DAG(s) dégradé(s) : a (1 incident(s)")
    assert "🟠 degraded" in result["markdown"]
    assert "- Statut : **degraded** — 1 DAG(s) dégradé(s)" in result["markdown"]


def test_section5_lists_each_run_under_its_own_heading(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    """Found in a real report: a failed run was listed under the masked-runs heading."""
    airflow.get("/dags").respond(
        json=collection("dags", [p.dag(api_version, "a"), p.dag(api_version, "b")])
    )
    airflow.get("/dags/a/dagRuns").respond(json=collection("dag_runs", [run("a1", "failed", 1)]))
    airflow.get("/dags/b/dagRuns").respond(json=collection("dag_runs", [run("b1", "success", 2)]))
    airflow.get("/dags/a/dagRuns/a1/taskInstances").respond(
        json=collection("task_instances", [ti("fetch", "failed", 1.0)])
    )
    airflow.get("/dags/b/dagRuns/b1/taskInstances").respond(
        json=collection(
            "task_instances", [ti("train", "failed", 1.0), ti("notify", "success", 1.0)]
        )
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(json=collection("xcom_entries", []))
    storage.add_audit_entry(
        tool="airflow_sandbox_test",
        dag_id="b",
        run_id=None,
        task_ids=[],
        dry_run=True,
        auto=False,
        reason="",
        outcome="passed",
    )
    markdown = ctx.call("airflow_build_report", period="24h")["markdown"]
    section = markdown.split("## 5.")[1].split("## 6.")[0]
    lines = [line for line in section.splitlines() if line.strip()]
    failed_at = next(i for i, line in enumerate(lines) if line.startswith("- Runs en échec"))
    masked_at = next(i for i, line in enumerate(lines) if "masquent un échec" in line)
    assert "a / a1" in lines[failed_at + 1]
    assert "b / b1" in lines[masked_at + 1]
    assert "airflow_sandbox_test b (bac à sable)" in section  # no double space


@pytest.mark.parametrize("api_version", ["v2"])
def test_airflow3_manual_run_without_logical_date_is_counted(
    api_version: str, airflow: respx.MockRouter
) -> None:
    """Found on Airflow 3.4: a manual run has logical_date=None and, right after a retry, a
    start_date later than the report; run_after is the stable reference."""
    ctx = make_context(api_version="v2", auth_mode="jwt")
    airflow.get("/dags").respond(json=collection("dags", [p.dag("v2", "manual_dag")]))
    manual = run("m1", "failed", 2)
    manual.update(logical_date=None, run_after=ts(2), start_date=None, end_date=None)
    airflow.get("/dags/manual_dag/dagRuns").respond(json=collection("dag_runs", [manual]))
    airflow.get(path__regex=r".*/taskInstances$").respond(
        json=collection("task_instances", [ti("fetch", "failed", 1.0)])
    )
    airflow.get(path__regex=r".*/xcomEntries$").respond(json=collection("xcom_entries", []))
    health = ctx.call("airflow_build_report", period="24h")["report"]["health"]
    assert health["total_runs"] == 1
    assert health["per_dag"][0]["status"] == "failing"
