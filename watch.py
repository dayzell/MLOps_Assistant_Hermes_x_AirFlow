"""Scheduled watch (plan v2, step 5): what changed since the last check.

``airflow_watch_check`` looks at the latest run of every allowed DAG and compares it with what
it saw last time (``watch_state`` table), so a scheduled job only speaks when something is
new: a new failure, a failure hidden by a final ``all_done`` task, a DAG back to green.
Nothing changes in Airflow here; retries stay with ``airflow_clear_tasks`` and its policy.
"""

from __future__ import annotations

import time
from typing import Any

from . import storage
from .airflow_client import AirflowClient
from .config import Settings

MAX_DAGS = 100
INCIDENT_STATES = frozenset({"failing", "masked"})


def _status(client: AirflowClient, dag_id: str) -> tuple[str, dict[str, Any] | None]:
    runs = client.list_dag_runs(dag_id, limit=3)
    finished = [r for r in runs if r.get("state") in ("success", "failed")]
    if not finished:
        return "no_run", None
    latest = finished[0]
    if latest.get("state") == "failed":
        return "failing", latest
    tis = client.list_task_instances(dag_id, str(latest["run_id"]))
    if any(ti.get("state") == "failed" for ti in tis):
        return "masked", latest
    return "healthy", latest


def _previous() -> dict[str, tuple[str | None, str]]:
    with storage.connect() as conn:
        rows = conn.execute("SELECT dag_id, run_id, status FROM watch_state").fetchall()
    return {str(r[0]): (r[1], str(r[2])) for r in rows}


def _save(states: dict[str, tuple[str | None, str]], now: float) -> None:
    with storage.connect() as conn:
        for dag_id, (run_id, status) in states.items():
            conn.execute(
                """
                INSERT INTO watch_state (dag_id, run_id, status, updated_at) VALUES (?, ?, ?, ?)
                ON CONFLICT (dag_id) DO UPDATE SET run_id = excluded.run_id,
                    status = excluded.status, updated_at = excluded.updated_at
                """,
                (dag_id, run_id, status, now),
            )


def watch_check(
    client: AirflowClient, settings: Settings, now: float | None = None
) -> dict[str, Any]:
    """New incidents and recoveries since the previous check (first check: all incidents)."""
    ts = time.time() if now is None else now
    # Allowlist first, then the cap: DAGs outside the allowlist must not use up the slots.
    dags = [
        d
        for d in client.list_dags(paused=False)
        if d.get("dag_id") and settings.dag_allowed(str(d["dag_id"]))
    ][:MAX_DAGS]
    before = _previous()
    states: dict[str, tuple[str | None, str]] = {}
    new_incidents: list[dict[str, Any]] = []
    recovered: list[str] = []
    still: list[str] = []
    errors: list[dict[str, str]] = []
    for dag in dags:
        dag_id = str(dag["dag_id"])
        try:
            status, run = _status(client, dag_id)
        except Exception as exc:  # one unreadable DAG must not hide the others
            errors.append({"dag_id": dag_id, "message": str(getattr(exc, "message", exc))})
            continue
        run_id = str(run["run_id"]) if run else None
        states[dag_id] = (run_id, status)
        prev_run, prev_status = before.get(dag_id, (None, "unknown"))
        if status in INCIDENT_STATES:
            if prev_status in INCIDENT_STATES and prev_run == run_id:
                still.append(dag_id)  # already reported
            else:
                new_incidents.append(
                    {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "kind": "hidden failure (run marked success)"
                        if status == "masked"
                        else "failed run",
                    }
                )
        elif status == "healthy" and prev_status in INCIDENT_STATES:
            recovered.append(dag_id)
    _save(states, ts)
    silent = not new_incidents and not recovered and not errors
    return {
        "checked": len(states),
        "first_check": not before,
        "new_incidents": new_incidents,
        "recovered": recovered,
        "still_failing": still,
        "errors": errors,
        "silent": silent,
        "alert_text": None if silent else alert_text(new_incidents, recovered, still, errors),
        "next": "Nothing new: respond with [SILENT]."
        if silent
        else "For each new incident call airflow_diagnose_run(dag_id, run_id); a transient "
        "failure may be retried with airflow_clear_tasks (dry run first; the plugin's "
        "auto-retry policy decides if no human is needed). Then send alert_text with the "
        "diagnosis (category, one-line cause) and '/airflow-audit' to start a guided audit.",
    }


def alert_text(
    new: list[dict[str, Any]], recovered: list[str], still: list[str], errors: list[dict[str, str]]
) -> str:
    """Short alert, in English like every pre-made text of the plugin."""
    lines = ["Airflow watch"]
    for item in new:
        lines.append(f"- NEW {item['kind']}: {item['dag_id']} ({item['run_id']})")
    for dag_id in recovered:
        lines.append(f"- RECOVERED: {dag_id}")
    if still:
        lines.append(f"- still failing (already reported): {', '.join(still)}")
    for err in errors:
        lines.append(f"- could not read {err['dag_id']}: {err['message']}")
    lines.append("Run /airflow-audit to diagnose and fix, step by step.")
    return "\n".join(lines)


def reset_watch_state() -> None:
    """Forget what was reported (the next check reports every current incident)."""
    with storage.connect() as conn:
        conn.execute("DELETE FROM watch_state")


BLUEPRINT = {
    "key": "night-watch",
    "title": "Airflow night watch",
    "description": "Check your Airflow DAGs on a schedule and message you ONLY when something "
    "is new: a failed run, a failure hidden by a final all_done task, a DAG back to green. "
    "Transient failures can be retried within the plugin's auto-retry policy.",
    "category": "monitoring",
    "schedule_template": "*/{interval_min} * * * *",
    "prompt_template": (
        "Follow the airflow-mlops:airflow-watch skill. Call airflow_watch_check. If it returns "
        "silent=true, respond with exactly [SILENT] and nothing else. Otherwise diagnose each "
        "new incident with airflow_diagnose_run and send a short alert: alert_text plus, per "
        "incident, the category (transient / critical) and the one-line cause. Never change a "
        "DAG file; never retry a critical failure."
    ),
    "slots": [
        {
            "name": "interval_min",
            "type": "enum",
            "label": "Check every (minutes)",
            "default": "30",
            "options": ("15", "30", "60"),
        },
        {
            "name": "deliver",
            "type": "enum",
            "label": "Where to deliver?",
            "default": "origin",
            "options": ("origin", "local", "telegram", "discord", "slack", "email"),
            "strict": False,
        },
    ],
    "skills": ("airflow-mlops:airflow-watch",),
    "tags": ("airflow", "monitoring"),
}
