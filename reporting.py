"""Standard monitoring report: always the same six sections, returned as JSON and Markdown.

Sections (fixed order and keys):
    1. scope_period     scope and period
    2. health           success rate, runs, incidents
    3. performance      per-task p50/p95 vs stored baseline, retries, queue time, SLA misses
    4. data_quality     read from the data-quality XCom convention (never invented)
    5. incidents        failed runs of the period and actions taken (audit log)
    6. recommendations  rule-based, evidence attached
"""

from __future__ import annotations

import ast
import json
import logging
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from . import storage
from .airflow_client import AirflowClient, AirflowError, parse_datetime
from .config import DagNotAllowedError, Settings
from .display import FAIL, NEXT, OK, SEVERITY_ICONS, STATUS_ICONS, WARN
from .redaction import redact_value

logger = logging.getLogger(__name__)

SECTION_KEYS = (
    "scope_period",
    "health",
    "performance",
    "data_quality",
    "incidents",
    "recommendations",
)
SECTION_TITLES = {
    "scope_period": "1. Scope and period",
    "health": "2. Overall health",
    "performance": "3. Performance per task",
    "data_quality": "4. Data quality",
    "incidents": "5. Incidents and actions",
    "recommendations": "6. Recommendations",
}

DQ_FIELDS: dict[str, tuple[type, ...]] = {
    "rows": (int,),
    "null_ratio": (int, float),
    "freshness": (str, int, float),
    "checks_passed": (bool,),
}

_PERIOD_RE = re.compile(r"^\s*(\d+)\s*([mhdw])\s*$", re.IGNORECASE)
_PERIOD_UNITS = {"m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}
MAX_PERIOD = timedelta(days=90)

MAX_DAGS = 50
MAX_RUNS_PER_DAG = 200
MAX_RUNS_DETAILED = 30  # runs whose task instances are fetched, per DAG
MAX_TASKS_DQ = 30  # task instances scanned for the data-quality XCom
MAX_TASK_ROWS = 50  # rows of the performance table
MIN_BASELINE_SAMPLES = 3
TREND_THRESHOLD = 0.10  # +/-10 % vs baseline p50 is "stable"
BASELINE_ALPHA = 0.3
_FATAL_ERRORS = frozenset({"auth", "network", "timeout", "config", "forbidden"})


# ============================================================================ parsing helpers


def parse_period(text: str) -> timedelta:
    """Parse ``30m``, ``24h``, ``7d``, ``2w`` (max 90 days)."""
    match = _PERIOD_RE.match(text or "")
    if not match:
        raise ValueError(f"Invalid period {text!r}: use a number followed by m, h, d or w (24h).")
    delta = timedelta(seconds=int(match.group(1)) * _PERIOD_UNITS[match.group(2).lower()])
    if delta <= timedelta(0) or delta > MAX_PERIOD:
        raise ValueError("The period must be greater than 0 and at most 90 days.")
    return delta


@dataclass(frozen=True)
class Scope:
    kind: str  # all | dag | tag
    value: str | None

    @property
    def label(self) -> str:
        if self.kind == "all":
            return "all"
        if self.kind == "tag":
            return f"tag:{self.value}"
        return str(self.value)


def parse_scope(text: str | None) -> Scope:
    raw = (text or "all").strip()
    if not raw or raw.lower() in {"all", "*"}:
        return Scope("all", None)
    if raw.lower().startswith("tag:"):
        tag = raw[4:].strip()
        if not tag:
            raise ValueError("Empty tag in scope; use tag:<name>.")
        return Scope("tag", tag)
    return Scope("dag", raw)


def percentile(values: list[float], q: float) -> float | None:
    """Linear-interpolated percentile (numpy's default method); ``None`` for no data."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * q / 100
    low, high = math.floor(rank), math.ceil(rank)
    result = ordered[low] + (ordered[high] - ordered[low]) * (rank - low)
    return round(result, 3)


def validate_data_quality(value: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Check an XCom value against the data-quality convention.

    Returns ``(normalised, problems)``; ``normalised`` is ``None`` when unusable.
    """
    if isinstance(value, str):
        text = value.strip()
        for loader in (json.loads, ast.literal_eval):
            try:
                value = loader(text)
                break
            except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
                continue
    if not isinstance(value, dict):
        return None, ["value is not an object"]
    problems = []
    normalised: dict[str, Any] = {}
    for key, types in DQ_FIELDS.items():
        if key not in value:
            problems.append(f"missing '{key}'")
            continue
        item = value[key]
        if key != "checks_passed" and isinstance(item, bool):
            problems.append(f"'{key}' has the wrong type")
            continue
        if not isinstance(item, types):
            problems.append(f"'{key}' has the wrong type")
            continue
        if key == "null_ratio" and isinstance(item, int | float) and not 0 <= item <= 1:
            problems.append("'null_ratio' must be between 0 and 1")
            continue
        normalised[key] = item
    return (normalised if not problems else None), problems


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _round(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(value, digits)


# ============================================================================ builder


@dataclass
class _DagData:
    dag: dict[str, Any]
    runs: list[dict[str, Any]]
    tis_by_run: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    sampled: bool = False
    error: dict[str, Any] | None = None

    @property
    def dag_id(self) -> str:
        return str(self.dag["dag_id"])

    def masked_runs(self) -> list[dict[str, Any]]:
        """Runs marked ``success`` that contain failed task instances.

        Airflow derives a run's state from its LEAF tasks: a final task with
        ``trigger_rule="all_done"`` (cleanup, notification) succeeds even when an upstream
        task failed, so the whole run is reported ``success`` and the failure is masked.
        """
        masked = []
        for run in self.runs:
            if run.get("state") != "success":
                continue
            tis = self.tis_by_run.get(str(run["run_id"])) or []
            failed = sorted(str(ti["task_id"]) for ti in tis if ti.get("state") == "failed")
            if failed:
                masked.append({**run, "failed_tasks": failed})
        return masked


class ReportBuilder:
    """Collect Airflow data for a scope/period and render the standard report."""

    def __init__(self, client: AirflowClient, settings: Settings, *, now: datetime) -> None:
        self.client = client
        self.settings = settings
        self.now = now.astimezone(UTC)

    def build(
        self, scope_text: str | None, period_text: str | None, *, update_baseline: bool = True
    ) -> dict[str, Any]:
        scope = parse_scope(scope_text)
        period_label = (period_text or self.settings.report_default_period).strip()
        delta = parse_period(period_label)
        start = self.now - delta

        dags, dags_truncated = self._resolve_dags(scope)
        data = [self._collect(dag, start) for dag in dags]

        performance, perf_stats = self._performance(data)
        report: dict[str, Any] = {
            "scope_period": {
                "scope": scope.label,
                "period": period_label,
                "start": _iso(start),
                "end": _iso(self.now),
                "generated_at": _iso(self.now),
                "dag_count": len(dags),
                "dags": [d.dag_id for d in data],
                "dags_truncated": dags_truncated,
                "sampled_dags": [d.dag_id for d in data if d.sampled],
                "errors": [{"dag_id": d.dag_id, **(d.error or {})} for d in data if d.error],
            },
            "health": self._health(data),
            "performance": performance,
            "data_quality": self._data_quality(data),
            "incidents": self._incidents(data, start),
        }
        report["recommendations"] = self._recommendations(report)
        if update_baseline:
            self._update_baselines(perf_stats)
        return {"report": report, "markdown": render_markdown(report)}

    # ------------------------------------------------------------------ collection

    def _resolve_dags(self, scope: Scope) -> tuple[list[dict[str, Any]], bool]:
        if scope.kind == "dag":
            dag_id = str(scope.value)
            if not self.settings.dag_allowed(dag_id):
                raise DagNotAllowedError(
                    f"DAG '{dag_id}' is outside the plugin allowlist (dag_allowlist); "
                    "access refused."
                )
            return [self.client.get_dag_details(dag_id)], False
        tags = [str(scope.value)] if scope.kind == "tag" else None
        dags = [
            dag
            for dag in self.client.list_dags(tags=tags, only_active=True)
            if dag.get("dag_id") and self.settings.dag_allowed(str(dag["dag_id"]))
        ]
        dags.sort(key=lambda d: str(d["dag_id"]))
        return dags[:MAX_DAGS], len(dags) > MAX_DAGS

    def _collect(self, dag: dict[str, Any], start: datetime) -> _DagData:
        dag_id = str(dag["dag_id"])
        try:
            # Newest first; the window is applied client-side on the logical date (a run
            # cleared for a retry gets a new start_date, or none while queued).
            runs = self.client.list_dag_runs(dag_id, limit=MAX_RUNS_PER_DAG)
            runs = [run for run in runs if self._in_window(run, start)]
            detailed = runs[:MAX_RUNS_DETAILED]
            tis_by_run = {
                str(run["run_id"]): self.client.list_task_instances(dag_id, str(run["run_id"]))
                for run in detailed
            }
            return _DagData(dag, runs, tis_by_run, sampled=len(runs) > len(detailed))
        except AirflowError as exc:
            if exc.error_type in _FATAL_ERRORS:
                raise
            return _DagData(dag, [], error=exc.to_dict())

    def _in_window(self, run: dict[str, Any], start: datetime) -> bool:
        # A stable date: start_date moves when a run is cleared for a retry (and is empty while
        # queued); Airflow 3 manual runs have no logical_date but always a run_after.
        moment = (
            parse_datetime(run.get("logical_date"))
            or parse_datetime(run.get("run_after"))
            or parse_datetime(run.get("start_date"))
        )
        return moment is not None and start <= moment <= self.now

    # ------------------------------------------------------------------ section 2

    def _health(self, data: list[_DagData]) -> dict[str, Any]:
        totals: dict[str, int] = defaultdict(int)
        per_dag = []
        for item in data:
            counts: dict[str, int] = defaultdict(int)
            for run in item.runs:
                counts[str(run.get("state") or "unknown")] += 1
            for state, count in counts.items():
                totals[state] += count
            durations = [r["duration_s"] for r in item.runs if r.get("duration_s") is not None]
            finished = counts["success"] + counts["failed"]
            masked_runs = item.masked_runs()
            masked = len(masked_runs)
            totals["masked"] += masked
            status, reason = _dag_status(item, counts["failed"], masked_runs)
            per_dag.append(
                {
                    "dag_id": item.dag_id,
                    "status": status,
                    "status_reason": reason,
                    "is_paused": item.dag.get("is_paused"),
                    "runs": len(item.runs),
                    "success": counts["success"],
                    "failed": counts["failed"],
                    "masked_failures": masked,
                    "other": len(item.runs) - finished,
                    "success_rate": _round(counts["success"] / finished, 4) if finished else None,
                    "last_run_state": item.runs[0].get("state") if item.runs else None,
                    "last_run_start": item.runs[0].get("start_date") if item.runs else None,
                    "run_duration_p50_s": percentile(durations, 50),
                    "run_duration_p95_s": percentile(durations, 95),
                }
            )
        total_runs = sum(len(item.runs) for item in data)
        finished = totals["success"] + totals["failed"]
        rate = totals["success"] / finished if finished else None
        incidents = totals["failed"] + totals["masked"]
        status, reason, status_counts = _global_status(per_dag)
        return {
            "status": status,
            "status_reason": reason,
            "dag_status_counts": status_counts,
            "total_runs": total_runs,
            "success": totals["success"],
            "failed": totals["failed"],
            "other": total_runs - finished,
            "success_rate": _round(rate, 4),
            "masked_failures": totals["masked"],
            "incidents": incidents,
            "per_dag": per_dag,
        }

    # ------------------------------------------------------------------ section 3

    def _performance(
        self, data: list[_DagData]
    ) -> tuple[dict[str, Any], list[tuple[str, str, float, float, int]]]:
        rows: list[dict[str, Any]] = []
        stats: list[tuple[str, str, float, float, int]] = []
        sla_misses: list[dict[str, Any]] = []
        thresholds = self.settings.sla_thresholds

        for item in data:
            baselines = self._baselines(item.dag_id)
            durations: dict[str, list[float]] = defaultdict(list)
            queues: dict[str, list[float]] = defaultdict(list)
            retries: dict[str, int] = defaultdict(int)
            instances: dict[str, int] = defaultdict(int)
            for run_id, tis in item.tis_by_run.items():
                for ti in tis:
                    task_id = str(ti.get("task_id"))
                    instances[task_id] += 1
                    duration = ti.get("duration_s")
                    if duration is not None and ti.get("state") in ("success", "failed"):
                        durations[task_id].append(float(duration))
                        limit = thresholds.get(f"{item.dag_id}.{task_id}")
                        if limit is not None and duration > limit:
                            sla_misses.append(
                                {
                                    "level": "task",
                                    "dag_id": item.dag_id,
                                    "task_id": task_id,
                                    "run_id": run_id,
                                    "duration_s": duration,
                                    "threshold_s": limit,
                                }
                            )
                    queued = ti.get("queued_s")
                    if queued is not None and queued >= 0:
                        queues[task_id].append(float(queued))
                    retries[task_id] += max(0, int(ti.get("try_number") or 1) - 1)

            dag_limit = thresholds.get(item.dag_id)
            if dag_limit is not None:
                for run in item.runs:
                    duration = run.get("duration_s")
                    if duration is not None and duration > dag_limit:
                        sla_misses.append(
                            {
                                "level": "dag",
                                "dag_id": item.dag_id,
                                "run_id": run.get("run_id"),
                                "duration_s": duration,
                                "threshold_s": dag_limit,
                            }
                        )

            for task_id in sorted(instances):
                values = durations.get(task_id, [])
                p50, p95 = percentile(values, 50), percentile(values, 95)
                baseline = baselines.get(task_id)
                trend, change = "no_baseline", None
                if baseline is not None and p50 is not None and baseline.p50 > 0:
                    change = (p50 - baseline.p50) / baseline.p50
                    if change > TREND_THRESHOLD:
                        trend = "slower"
                    elif change < -TREND_THRESHOLD:
                        trend = "faster"
                    else:
                        trend = "stable"
                rows.append(
                    {
                        "dag_id": item.dag_id,
                        "task_id": task_id,
                        "instances": instances[task_id],
                        "duration_p50_s": p50,
                        "duration_p95_s": p95,
                        "baseline_p50_s": _round(baseline.p50) if baseline else None,
                        "baseline_p95_s": _round(baseline.p95) if baseline else None,
                        "trend": trend,
                        "change_vs_baseline": _round(change, 4),
                        "retries": retries[task_id],
                        "queue_p50_s": percentile(queues.get(task_id, []), 50),
                        "queue_p95_s": percentile(queues.get(task_id, []), 95),
                    }
                )
                if p50 is not None and p95 is not None and len(values) >= MIN_BASELINE_SAMPLES:
                    stats.append((item.dag_id, task_id, p50, p95, len(values)))

        rows.sort(key=lambda r: (-(r["duration_p95_s"] or 0), r["dag_id"], r["task_id"]))
        return (
            {
                "tasks": rows[:MAX_TASK_ROWS],
                "tasks_truncated": len(rows) > MAX_TASK_ROWS,
                "sla": {
                    "configured": bool(thresholds),
                    "miss_count": len(sla_misses),
                    "misses": sla_misses[:100],
                },
            },
            stats,
        )

    @staticmethod
    def _baselines(dag_id: str) -> dict[str, storage.Baseline]:
        try:
            return storage.get_baselines(dag_id)
        except Exception:
            logger.warning("airflow-mlops: cannot read baselines", exc_info=True)
            return {}

    def _update_baselines(self, stats: list[tuple[str, str, float, float, int]]) -> None:
        try:
            for dag_id, task_id, p50, p95, samples in stats:
                storage.update_baseline(
                    dag_id,
                    task_id,
                    p50,
                    p95,
                    samples,
                    alpha=BASELINE_ALPHA,
                    now=self.now.timestamp(),
                )
        except Exception:
            logger.warning("airflow-mlops: cannot update baselines", exc_info=True)

    # ------------------------------------------------------------------ section 4

    def _data_quality(self, data: list[_DagData]) -> dict[str, Any]:
        key = self.settings.data_quality_xcom_key
        per_dag = []
        for item in data:
            per_dag.append(self._dag_quality(item, key))
        instrumented = [d for d in per_dag if d["status"] == "instrumented"]
        failed_checks = sum(
            1 for d in instrumented for c in d["checks"] if c.get("checks_passed") is False
        )
        return {
            "convention_key": key,
            "status": "instrumented" if instrumented else "not_instrumented",
            "instrumented_dags": len(instrumented),
            "failed_checks": failed_checks,
            "per_dag": per_dag,
        }

    def _dag_quality(self, item: _DagData, key: str) -> dict[str, Any]:
        base: dict[str, Any] = {"dag_id": item.dag_id}
        run = next((r for r in item.runs if r.get("state") == "success"), None) or (
            item.runs[0] if item.runs else None
        )
        if run is None:
            return {
                **base,
                "status": "not_instrumented",
                "reason": "no run in the period",
                "checks": [],
            }
        run_id = str(run["run_id"])
        tis = [ti for ti in item.tis_by_run.get(run_id, []) if ti.get("state") == "success"]
        checks: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for ti in tis[:MAX_TASKS_DQ]:
            task_id = str(ti["task_id"])
            map_index = ti.get("map_index")
            map_arg = map_index if isinstance(map_index, int) and map_index >= 0 else None
            try:
                entries = self.client.list_xcom_entries(
                    item.dag_id, run_id, task_id, key=key, map_index=map_arg, max_items=1
                )
                if not entries:
                    continue
                value = self.client.get_xcom_value(item.dag_id, run_id, task_id, key, map_arg)
            except AirflowError as exc:
                if exc.error_type in _FATAL_ERRORS:
                    raise
                continue
            normalised, problems = validate_data_quality(redact_value(value))
            if normalised is None:
                invalid.append({"task_id": task_id, "problems": problems})
            else:
                checks.append({"task_id": task_id, **normalised})
        base["run_id"] = run_id
        if checks:
            return {**base, "status": "instrumented", "checks": checks, "invalid": invalid}
        if invalid:
            return {**base, "status": "non_conformant", "checks": [], "invalid": invalid}
        return {
            **base,
            "status": "not_instrumented",
            "reason": f"no '{key}' XCom found",
            "checks": [],
        }

    # ------------------------------------------------------------------ section 5

    def _incidents(self, data: list[_DagData], start: datetime) -> dict[str, Any]:
        failed_runs = []
        for item in data:
            for run in item.runs:
                if run.get("state") != "failed":
                    continue
                run_id = str(run["run_id"])
                tis = item.tis_by_run.get(run_id)
                failed_tasks = (
                    sorted(str(ti["task_id"]) for ti in tis if ti.get("state") == "failed")
                    if tis is not None
                    else None
                )
                entry: dict[str, Any] = {
                    "dag_id": item.dag_id,
                    "run_id": run_id,
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "failed_tasks": failed_tasks,
                }
                diagnoses = self._cached_diagnoses(item.dag_id, run_id, failed_tasks or [])
                if diagnoses:
                    entry["diagnosis"] = diagnoses
                failed_runs.append(entry)
        for item in data:
            for run in item.masked_runs():
                entry = {
                    "dag_id": item.dag_id,
                    "run_id": str(run["run_id"]),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "failed_tasks": run["failed_tasks"],
                    "masked": True,
                }
                diagnoses = self._cached_diagnoses(
                    item.dag_id, entry["run_id"], run["failed_tasks"]
                )
                if diagnoses:
                    entry["diagnosis"] = diagnoses
                failed_runs.append(entry)
        failed_runs.sort(key=lambda r: str(r.get("start_date") or ""), reverse=True)
        try:
            actions = storage.list_audit_entries(
                start.timestamp(), dag_ids=[d.dag_id for d in data]
            )
            actions_error = None
        except Exception:
            logger.warning("airflow-mlops: cannot read the audit log", exc_info=True)
            actions, actions_error = [], "audit log unavailable"
        result: dict[str, Any] = {
            "failed_run_count": sum(1 for r in failed_runs if not r.get("masked")),
            "masked_run_count": sum(1 for r in failed_runs if r.get("masked")),
            "failed_runs": failed_runs[:50],
            "actions": [
                {
                    "ts": _iso(datetime.fromtimestamp(a["ts"], UTC)),
                    "dag_id": a["dag_id"],
                    "run_id": a["run_id"],
                    "task_ids": a["task_ids"],
                    "tool": a["tool"],
                    "dry_run": a["dry_run"],
                    "auto": a["auto"],
                    "outcome": a["outcome"],
                    "reason": a["reason"],
                }
                for a in actions
            ],
        }
        if actions_error:
            result["actions_error"] = actions_error
        return result

    @staticmethod
    def _cached_diagnoses(dag_id: str, run_id: str, task_ids: list[str]) -> dict[str, str]:
        found: dict[str, str] = {}
        try:
            for task_id in task_ids[:10]:
                cached = storage.get_diagnosis(dag_id, run_id, task_id)
                if cached is not None:
                    found[task_id] = cached.category
        except Exception:
            logger.debug("airflow-mlops: diagnosis cache unavailable", exc_info=True)
        return found

    # ------------------------------------------------------------------ section 6

    def _recommendations(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        recs: list[dict[str, Any]] = []
        for dag in report["health"]["per_dag"]:
            rate = dag["success_rate"]
            if dag["failed"] and rate is not None and rate < 0.95:
                recs.append(
                    {
                        "severity": "high" if rate < 0.8 else "medium",
                        "dag_id": dag["dag_id"],
                        "message": (
                            f"{dag['dag_id']}: {dag['failed']} failed run(s) out of {dag['runs']} "
                            f"(success rate {rate:.0%}). Run airflow_diagnose_run on the latest "
                            "failed run."
                        ),
                    }
                )
        masked = [d for d in report["health"]["per_dag"] if d.get("masked_failures")]
        for dag in masked:
            recs.append(
                {
                    "severity": "high",
                    "dag_id": dag["dag_id"],
                    "message": (
                        f"{dag['dag_id']}: {dag['masked_failures']} run(s) marked success contain "
                        "failed tasks, hidden by the trigger_rule of a final task (e.g. all_done). "
                        "Run airflow_diagnose_run and review the trigger rules."
                    ),
                }
            )
        for task in report["performance"]["tasks"]:
            change = task["change_vs_baseline"]
            if task["trend"] == "slower" and change is not None and change >= 0.25:
                recs.append(
                    {
                        "severity": "medium",
                        "dag_id": task["dag_id"],
                        "task_id": task["task_id"],
                        "message": (
                            f"Task {task['task_id']} is {change:+.0%} slower than its baseline "
                            "(p50): check the data volume and the resources."
                        ),
                    }
                )
            if task["instances"] and task["retries"] / task["instances"] >= 0.2:
                recs.append(
                    {
                        "severity": "medium",
                        "dag_id": task["dag_id"],
                        "task_id": task["task_id"],
                        "message": (
                            f"Task {task['task_id']} is flaky: {task['retries']} retry(ies) for "
                            f"{task['instances']} run(s). Look for a transient cause."
                        ),
                    }
                )
            queue_p95 = task["queue_p95_s"]
            if queue_p95 is not None and queue_p95 >= 300:
                recs.append(
                    {
                        "severity": "low",
                        "dag_id": task["dag_id"],
                        "task_id": task["task_id"],
                        "message": (
                            f"Task {task['task_id']} waits {queue_p95:.0f}s in queue (p95): check "
                            "the pool sizes and the worker capacity."
                        ),
                    }
                )
        sla = report["performance"]["sla"]
        if sla["miss_count"]:
            recs.append(
                {
                    "severity": "medium",
                    "message": f"{sla['miss_count']} SLA miss(es) in the period.",
                }
            )
        elif not sla["configured"]:
            recs.append(
                {
                    "severity": "info",
                    "message": (
                        "No SLA threshold configured (sla_thresholds): SLAs are not tracked."
                    ),
                }
            )
        quality = report["data_quality"]
        if quality["failed_checks"]:
            recs.append(
                {
                    "severity": "high",
                    "message": (
                        f"{quality['failed_checks']} data-quality check(s) failed: do not use the "
                        "affected data before checking it."
                    ),
                }
            )
        missing = [d["dag_id"] for d in quality["per_dag"] if d["status"] != "instrumented"]
        if missing:
            recs.append(
                {
                    "severity": "info",
                    "message": (
                        "Data quality not instrumented for: "
                        + ", ".join(missing[:10])
                        + (" ..." if len(missing) > 10 else "")
                        + f" (publish the '{quality['convention_key']}' XCom)."
                    ),
                }
            )
        if report["scope_period"]["errors"]:
            recs.append(
                {
                    "severity": "medium",
                    "message": "Some DAGs could not be read: see scope_period.errors.",
                }
            )
        if not any(r["severity"] in ("high", "medium", "low") for r in recs):
            recs.insert(
                0,
                {
                    "severity": "info",
                    "message": "No action required: no incident or drift detected.",
                },
            )
        order = {"high": 0, "medium": 1, "low": 2, "info": 3}
        recs.sort(key=lambda r: order[r["severity"]])
        return recs


# ============================================================================ health status

STATUS_ORDER = ("failing", "degraded", "healthy", "no_data")


def _dag_status(
    item: _DagData, failed_runs: int, masked_runs: list[dict[str, Any]]
) -> tuple[str, str]:
    """Status of ONE DAG from its own runs, with a human-readable reason.

    failing  = the DAG is broken right now (latest run failed, or its latest "success" run
               hides a failed task); degraded = incidents in the period but the latest run is
               clean; healthy = no incident; no_data = no run in the period.
    """
    if not item.runs:
        return "no_data", "no run in the period"
    latest = item.runs[0]
    if latest.get("state") == "failed":
        return "failing", "latest run failed"
    if any(str(run["run_id"]) == str(latest.get("run_id")) for run in masked_runs):
        return "failing", "latest run marked success but a task failed"
    incidents = failed_runs + len(masked_runs)
    if incidents:
        return "degraded", f"{incidents} incident(s) in the period, latest run clean"
    return "healthy", "no incident"


def _global_status(per_dag: list[dict[str, Any]]) -> tuple[str, str, dict[str, int]]:
    """Global status derived from the per-DAG statuses (DAGs are independent).

    failing  = at least half of the DAGs with runs are failing; degraded = at least one DAG
    failing or degraded; healthy = none. The reason names the DAGs concerned.
    """
    counts = {status: 0 for status in STATUS_ORDER}
    for dag in per_dag:
        counts[dag["status"]] += 1
    with_data = len(per_dag) - counts["no_data"]
    failing = [d for d in per_dag if d["status"] == "failing"]
    degraded = [d for d in per_dag if d["status"] == "degraded"]

    def names(dags: list[dict[str, Any]]) -> str:
        shown = ", ".join(f"{d['dag_id']} ({d['status_reason']})" for d in dags[:5])
        return shown + (f", +{len(dags) - 5}" if len(dags) > 5 else "")

    if with_data == 0:
        return "no_data", "no run in the period", counts
    parts = []
    if failing:
        parts.append(f"{len(failing)} of {with_data} DAG(s) failing: {names(failing)}")
    if degraded:
        parts.append(f"{len(degraded)} DAG(s) degraded: {names(degraded)}")
    if failing and len(failing) * 2 >= with_data:
        status = "failing"
    elif failing or degraded:
        status = "degraded"
    else:
        return "healthy", f"no incident on the {with_data} DAG(s) with runs", counts
    return status, "; ".join(parts), counts


# ============================================================================ markdown


def _fmt_s(value: float | None) -> str:
    if value is None:
        return "-"
    seconds = float(value)
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(round(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def _fmt_rate(value: float | None) -> str:
    return "-" if value is None else f"{value:.1%}"


def _cell(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _display_width(text: str) -> int:
    """Columns taken in a terminal: emoji / wide characters count 2, combining marks 0."""
    width = 0
    for char in text:
        if unicodedata.combining(char) or char == "\ufe0f":
            continue
        width += 2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
    return width


def _table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    """Markdown table padded with spaces, so it stays aligned when shown as raw text
    (slash-command output is not rendered as Markdown in the terminal)."""
    grid = [[_cell(value) for value in row] for row in [headers, *rows]]
    widths = [max(_display_width(row[i]) for row in grid) for i in range(len(headers))]

    def line(row: list[str]) -> str:
        cells = [c + " " * (widths[i] - _display_width(c)) for i, c in enumerate(row)]
        return "| " + " | ".join(cells) + " |"

    separator = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
    return [line(grid[0]), separator, *(line(row) for row in grid[1:])]


def render_markdown(report: dict[str, Any]) -> str:
    """Render the report with a fixed template (same headings, same order, every time)."""
    scope = report["scope_period"]
    health = report["health"]
    perf = report["performance"]
    quality = report["data_quality"]
    incidents = report["incidents"]
    lines = [
        f"# Airflow monitoring report — {scope['scope']} — {scope['period']}",
        "",
        f"## {SECTION_TITLES['scope_period']}",
        "",
        f"- Scope: `{scope['scope']}` ({scope['dag_count']} DAG(s))",
        f"- Period: {scope['start']} → {scope['end']} ({scope['period']})",
        f"- Generated at: {scope['generated_at']}",
    ]
    if scope["dags_truncated"]:
        lines.append(f"- {WARN} Scope limited to the first {scope['dag_count']} DAGs")
    if scope["sampled_dags"]:
        lines.append(f"- Sampled (recent runs only): {', '.join(scope['sampled_dags'])}")
    for error in scope["errors"]:
        lines.append(f"- {WARN} {error['dag_id']} could not be read: {error.get('message')}")

    counts = health["dag_status_counts"]
    lines += [
        "",
        f"## {SECTION_TITLES['health']}",
        "",
        f"- Status: **{health['status']}** — {health['status_reason']}",
        f"- DAGs: {counts['healthy']} healthy, {counts['degraded']} degraded, "
        f"{counts['failing']} failing, {counts['no_data']} without runs",
        f"- Runs: {health['total_runs']} (success {health['success']}, failed {health['failed']}, "
        f"other {health['other']})",
        f"- Success rate: {_fmt_rate(health['success_rate'])}",
        f"- Incidents: {health['incidents']} (including {health['masked_failures']} masked "
        "failure(s) in runs marked success)",
    ]
    if health["per_dag"]:
        lines.append("")
        lines += _table(
            ["DAG", "Status", "Runs", "Success", "Failed", "Rate", "p50", "p95", "Last state"],
            [
                [
                    dag["dag_id"],
                    f"{STATUS_ICONS[dag['status']]} {dag['status']}",
                    dag["runs"],
                    dag["success"],
                    dag["failed"],
                    _fmt_rate(dag["success_rate"]),
                    _fmt_s(dag["run_duration_p50_s"]),
                    _fmt_s(dag["run_duration_p95_s"]),
                    dag["last_run_state"] or "-",
                ]
                for dag in health["per_dag"]
            ],
        )
        reasons = [d for d in health["per_dag"] if d["status"] in ("failing", "degraded")]
        if reasons:
            lines.append("")
            lines += [
                f"- {STATUS_ICONS[d['status']]} {d['dag_id']}: {d['status_reason']}"
                for d in reasons
            ]

    lines += ["", f"## {SECTION_TITLES['performance']}", ""]
    if perf["tasks"]:
        rows = []
        for task in perf["tasks"]:
            change = task["change_vs_baseline"]
            trend = task["trend"] + (f" ({change:+.0%})" if change is not None else "")
            rows.append(
                [
                    task["dag_id"],
                    task["task_id"],
                    _fmt_s(task["duration_p50_s"]),
                    _fmt_s(task["duration_p95_s"]),
                    _fmt_s(task["baseline_p50_s"]),
                    trend,
                    task["retries"],
                    _fmt_s(task["queue_p95_s"]),
                ]
            )
        lines += _table(
            ["DAG", "Task", "p50", "p95", "Baseline p50", "Trend", "Retries", "Queue p95"], rows
        )
        if perf["tasks_truncated"]:
            lines.append(f"\n_Table limited to the {MAX_TASK_ROWS} slowest tasks (p95)._")
    else:
        lines.append("No task run in the period.")
    sla = perf["sla"]
    if not sla["configured"]:
        lines.append("\n- SLA: not configured")
    else:
        lines.append(f"\n- SLA misses: {sla['miss_count']}")
        for miss in sla["misses"][:10]:
            target = miss["dag_id"] + (f".{miss['task_id']}" if miss.get("task_id") else "")
            lines.append(
                f"  - {target} ({miss['run_id']}): {_fmt_s(miss['duration_s'])} > "
                f"{_fmt_s(miss['threshold_s'])}"
            )

    lines += [
        "",
        f"## {SECTION_TITLES['data_quality']}",
        "",
        f"Convention: XCom `{quality['convention_key']}` "
        "(`rows`, `null_ratio`, `freshness`, `checks_passed`).",
        "",
    ]
    for dag in quality["per_dag"]:
        if dag["status"] == "instrumented":
            for check in dag["checks"]:
                verdict = OK if check["checks_passed"] else FAIL
                lines.append(
                    f"- {verdict} {dag['dag_id']}.{check['task_id']}: {check['rows']} rows, "
                    f"nulls {check['null_ratio']:.1%}, freshness {check['freshness']}"
                )
        elif dag["status"] == "non_conformant":
            problems = "; ".join(
                f"{i['task_id']}: {', '.join(i['problems'])}" for i in dag["invalid"]
            )
            lines.append(f"- {WARN} {dag['dag_id']}: non-conformant XCom ({problems})")
        else:
            lines.append(f"- {dag['dag_id']}: not instrumented")
    if not quality["per_dag"]:
        lines.append("- not instrumented")

    lines += [
        "",
        f"## {SECTION_TITLES['incidents']}",
        "",
    ]

    def run_line(run: dict[str, Any]) -> str:
        tasks = ", ".join(run["failed_tasks"]) if run["failed_tasks"] else "?"
        diagnosis = run.get("diagnosis")
        diag = (
            " — diagnosis: " + ", ".join(f"{k}={v}" for k, v in diagnosis.items())
            if diagnosis
            else ""
        )
        return f"  - {run['dag_id']} / {run['run_id']}: failed tasks {tasks}{diag}"

    # Each run under its own heading: real failures first, then masked ones.
    shown = incidents["failed_runs"][:20]
    lines.append(f"- Failed runs: {incidents['failed_run_count']}")
    lines += [run_line(r) for r in shown if not r.get("masked")]
    if incidents.get("masked_run_count"):
        lines.append(
            f"- {WARN} Runs marked success that hide a failure: {incidents['masked_run_count']}"
        )
        lines += [run_line(r) for r in shown if r.get("masked")]
    lines += _actions_summary(incidents["actions"])

    lines += ["", f"## {SECTION_TITLES['recommendations']}", ""]
    for rec in report["recommendations"]:
        lines.append(f"- {SEVERITY_ICONS[rec['severity']]} {rec['message']}")
    lines += _next_step(incidents)
    return "\n".join(lines) + "\n"


MAX_ACTIONS_LISTED = 5


def _action_mode(action: dict[str, Any]) -> str:
    if action["tool"] == "airflow_sandbox_test":
        return "sandbox"
    if action["dry_run"]:
        return "preview"
    return "auto" if action["auto"] else "manual"


def _actions_summary(actions: list[dict[str, Any]]) -> list[str]:
    """Counts per kind of action, then only the latest real retries and a few others.

    A real session produced 20 sandbox lines here: the counts say the same in one line.
    """
    if not actions:
        return ["- Actions taken: none"]
    sandbox = [a for a in actions if a["tool"] == "airflow_sandbox_test"]
    clears = [a for a in actions if a["tool"] != "airflow_sandbox_test"]
    parts = []
    if sandbox:
        outcomes = Counter(str(a["outcome"]) for a in sandbox)
        detail = ", ".join(f"{name} {count}" for name, count in sorted(outcomes.items()))
        parts.append(f"sandbox tests {len(sandbox)} ({detail})")
    if clears:
        previews = sum(1 for a in clears if a["dry_run"])
        real = len(clears) - previews
        parts.append(f"retries {real} executed, {previews} preview(s)")
    lines = [f"- Actions taken: {len(actions)} — " + " · ".join(parts)]
    # Real retries first (they changed Airflow), then the most recent other actions.
    latest = [a for a in clears if not a["dry_run"]] + [
        a for a in actions if a["tool"] == "airflow_sandbox_test" or a["dry_run"]
    ]
    for action in latest[:MAX_ACTIONS_LISTED]:
        target = " ".join(
            part for part in (action["dag_id"], ", ".join(action["task_ids"])) if part
        )
        lines.append(
            f"  - {action['ts']} {action['tool']} {target} ({_action_mode(action)}) "
            f"→ {action['outcome']}"
        )
    if len(actions) > MAX_ACTIONS_LISTED:
        lines.append(f"  - … {len(actions) - MAX_ACTIONS_LISTED} older action(s) not listed")
    return lines


def _next_step(incidents: dict[str, Any]) -> list[str]:
    """Closing line: the report is the state of play, the user picks what to diagnose."""
    dags = list(dict.fromkeys(run["dag_id"] for run in incidents["failed_runs"]))
    if not dags:
        return []
    return [
        "",
        f"{NEXT} **Next step**: tell me which DAG(s) to diagnose ({', '.join(dags)}), "
        f'e.g. "diagnose {dags[0]}".',
    ]
