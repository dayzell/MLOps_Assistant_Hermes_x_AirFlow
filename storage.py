"""Durable plugin state in Hermes' per-plugin SQLite database (``plugin_db``).

Nothing is ever written inside the plugin directory. The schema is versioned with
``PRAGMA user_version`` so later phases can add tables (audit log, retry counters, baselines)
without breaking existing installs.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from plugins.plugin_storage import plugin_db

from .config import PLUGIN_NAME

SCHEMA_VERSION = 5

_MIGRATIONS: dict[int, list[str]] = {
    5: [
        # DAG write lock: fingerprints of every change that passed a sandbox test.
        """
        CREATE TABLE IF NOT EXISTS tested_changes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            path        TEXT NOT NULL,
            dag_id      TEXT NOT NULL,
            level       TEXT NOT NULL,
            patch_file  TEXT NOT NULL,
            before_fp   TEXT,
            after_fp    TEXT NOT NULL,
            tested_at   REAL NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS tested_changes_path ON tested_changes (path)",
    ],
    4: [
        # Guided audits (plan v2): one dossier per audit, numbered items (1.1, 1.2, 2.1...).
        """
        CREATE TABLE IF NOT EXISTS audits (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at  REAL NOT NULL,
            updated_at  REAL NOT NULL,
            session_id  TEXT,
            scope       TEXT NOT NULL,
            period      TEXT NOT NULL,
            status      TEXT NOT NULL,
            candidates  TEXT NOT NULL,
            pending     TEXT,
            current     TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS audit_items (
            audit_id   INTEGER NOT NULL,
            number     TEXT NOT NULL,
            dag_index  INTEGER NOT NULL,
            position   INTEGER NOT NULL,
            dag_id     TEXT NOT NULL,
            run_id     TEXT,
            task_id    TEXT,
            category   TEXT NOT NULL,
            kind       TEXT NOT NULL,
            title      TEXT NOT NULL,
            state      TEXT NOT NULL,
            selected   INTEGER NOT NULL DEFAULT 0,
            note       TEXT,
            updated_at REAL NOT NULL,
            PRIMARY KEY (audit_id, number)
        )
        """,
    ],
    3: [
        # Retries performed by the plugin, per task instance (auto-retry limit and backoff).
        """
        CREATE TABLE IF NOT EXISTS retry_counters (
            dag_id          TEXT NOT NULL,
            run_id          TEXT NOT NULL,
            task_id         TEXT NOT NULL,
            attempts        INTEGER NOT NULL,
            auto_attempts   INTEGER NOT NULL,
            last_attempt_at REAL NOT NULL,
            PRIMARY KEY (dag_id, run_id, task_id)
        )
        """,
    ],
    2: [
        # Per-task duration baselines used by reports to compute trends.
        """
        CREATE TABLE IF NOT EXISTS baselines (
            dag_id     TEXT NOT NULL,
            task_id    TEXT NOT NULL,
            p50        REAL NOT NULL,
            p95        REAL NOT NULL,
            samples    INTEGER NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (dag_id, task_id)
        )
        """,
        # Journal of every write action (who, what, when, why). Written by remediation tools.
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL NOT NULL,
            session_id      TEXT,
            conversation_id TEXT,
            tool            TEXT NOT NULL,
            dag_id          TEXT NOT NULL,
            run_id          TEXT,
            task_ids        TEXT NOT NULL,
            dry_run         INTEGER NOT NULL,
            auto            INTEGER NOT NULL,
            reason          TEXT,
            outcome         TEXT NOT NULL,
            details         TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS audit_log_ts ON audit_log (ts)",
    ],
    1: [
        """
        CREATE TABLE IF NOT EXISTS diagnosis_cache (
            dag_id        TEXT NOT NULL,
            run_id        TEXT NOT NULL,
            task_id       TEXT NOT NULL,
            map_index     INTEGER NOT NULL DEFAULT -1,
            try_number    INTEGER,
            category      TEXT NOT NULL,
            rule_ids      TEXT NOT NULL,
            justification TEXT NOT NULL,
            diagnosed_at  REAL NOT NULL,
            PRIMARY KEY (dag_id, run_id, task_id, map_index)
        )
        """,
    ],
}


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """Open the plugin database, apply pending migrations, commit on success."""
    conn = plugin_db(PLUGIN_NAME)
    try:
        _migrate(conn)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _migrate(conn: sqlite3.Connection) -> None:
    current = int(conn.execute("PRAGMA user_version").fetchone()[0])
    for version in range(current + 1, SCHEMA_VERSION + 1):
        for statement in _MIGRATIONS[version]:
            conn.execute(statement)
        conn.execute(f"PRAGMA user_version = {version}")
    conn.commit()


@dataclass(frozen=True)
class CachedDiagnosis:
    dag_id: str
    run_id: str
    task_id: str
    map_index: int
    try_number: int | None
    category: str
    rule_ids: list[str]
    justification: str
    diagnosed_at: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "map_index": self.map_index,
            "try_number": self.try_number,
            "category": self.category,
            "rule_ids": self.rule_ids,
            "justification": self.justification,
            "diagnosed_at": self.diagnosed_at,
        }


def save_diagnosis(
    dag_id: str,
    run_id: str,
    task_id: str,
    *,
    map_index: int,
    try_number: int | None,
    category: str,
    rule_ids: list[str],
    justification: str,
    now: float | None = None,
) -> None:
    """Upsert the latest classification of one task instance (read by the approval hook)."""
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO diagnosis_cache
                (dag_id, run_id, task_id, map_index, try_number, category, rule_ids,
                 justification, diagnosed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (dag_id, run_id, task_id, map_index) DO UPDATE SET
                try_number = excluded.try_number,
                category = excluded.category,
                rule_ids = excluded.rule_ids,
                justification = excluded.justification,
                diagnosed_at = excluded.diagnosed_at
            """,
            (
                dag_id,
                run_id,
                task_id,
                map_index,
                try_number,
                category,
                json.dumps(rule_ids),
                justification[:2000],
                time.time() if now is None else now,
            ),
        )


def get_diagnosis(
    dag_id: str, run_id: str, task_id: str, map_index: int = -1
) -> CachedDiagnosis | None:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT dag_id, run_id, task_id, map_index, try_number, category, rule_ids,
                   justification, diagnosed_at
            FROM diagnosis_cache
            WHERE dag_id = ? AND run_id = ? AND task_id = ? AND map_index = ?
            """,
            (dag_id, run_id, task_id, map_index),
        ).fetchone()
    if row is None:
        return None
    try:
        rule_ids = [str(item) for item in json.loads(row[6])]
    except (TypeError, ValueError):
        rule_ids = []
    return CachedDiagnosis(
        dag_id=row[0],
        run_id=row[1],
        task_id=row[2],
        map_index=int(row[3]),
        try_number=row[4],
        category=row[5],
        rule_ids=rule_ids,
        justification=row[7],
        diagnosed_at=float(row[8]),
    )


# ---------------------------------------------------------------------------- baselines


@dataclass(frozen=True)
class Baseline:
    dag_id: str
    task_id: str
    p50: float
    p95: float
    samples: int
    updated_at: float


def get_baselines(dag_id: str) -> dict[str, Baseline]:
    """All task baselines of one DAG, keyed by task_id."""
    with connect() as conn:
        rows = conn.execute(
            "SELECT dag_id, task_id, p50, p95, samples, updated_at FROM baselines WHERE dag_id = ?",
            (dag_id,),
        ).fetchall()
    return {
        row[1]: Baseline(row[0], row[1], float(row[2]), float(row[3]), int(row[4]), float(row[5]))
        for row in rows
    }


def update_baseline(
    dag_id: str,
    task_id: str,
    p50: float,
    p95: float,
    samples: int,
    *,
    alpha: float = 0.3,
    now: float | None = None,
) -> Baseline:
    """Blend new observations into the stored baseline (exponential moving average)."""
    timestamp = time.time() if now is None else now
    with connect() as conn:
        row = conn.execute(
            "SELECT p50, p95, samples FROM baselines WHERE dag_id = ? AND task_id = ?",
            (dag_id, task_id),
        ).fetchone()
        if row is None:
            new_p50, new_p95, total = p50, p95, samples
        else:
            new_p50 = (1 - alpha) * float(row[0]) + alpha * p50
            new_p95 = (1 - alpha) * float(row[1]) + alpha * p95
            total = int(row[2]) + samples
        conn.execute(
            """
            INSERT INTO baselines (dag_id, task_id, p50, p95, samples, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (dag_id, task_id) DO UPDATE SET
                p50 = excluded.p50, p95 = excluded.p95,
                samples = excluded.samples, updated_at = excluded.updated_at
            """,
            (dag_id, task_id, new_p50, new_p95, total, timestamp),
        )
    return Baseline(dag_id, task_id, new_p50, new_p95, total, timestamp)


# ---------------------------------------------------------------------------- audit log


def add_audit_entry(
    *,
    tool: str,
    dag_id: str,
    run_id: str | None,
    task_ids: list[str],
    dry_run: bool,
    auto: bool,
    reason: str | None,
    outcome: str,
    details: dict[str, Any] | None = None,
    session_id: str | None = None,
    conversation_id: str | None = None,
    now: float | None = None,
) -> int:
    """Append one write action to the journal and return its id."""
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO audit_log (ts, session_id, conversation_id, tool, dag_id, run_id,
                                   task_ids, dry_run, auto, reason, outcome, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time() if now is None else now,
                session_id,
                conversation_id,
                tool,
                dag_id,
                run_id,
                json.dumps(task_ids),
                int(dry_run),
                int(auto),
                (reason or "")[:1000],
                outcome,
                json.dumps(details or {}, default=str)[:4000],
            ),
        )
        return int(cursor.lastrowid or 0)


def list_audit_entries(
    since: float, dag_ids: list[str] | None = None, limit: int = 200
) -> list[dict[str, Any]]:
    """Journal entries since ``since`` (epoch seconds), newest first."""
    query = (
        "SELECT id, ts, session_id, conversation_id, tool, dag_id, run_id, task_ids, dry_run, "
        "auto, reason, outcome, details FROM audit_log WHERE ts >= ?"
    )
    params: list[Any] = [since]
    if dag_ids is not None:
        if not dag_ids:
            return []
        query += f" AND dag_id IN ({','.join('?' for _ in dag_ids)})"
        params.extend(dag_ids)
    query += " ORDER BY ts DESC, id DESC LIMIT ?"
    params.append(limit)
    with connect() as conn:
        rows = conn.execute(query, params).fetchall()
    entries = []
    for row in rows:
        entries.append(
            {
                "id": row[0],
                "ts": row[1],
                "session_id": row[2],
                "conversation_id": row[3],
                "tool": row[4],
                "dag_id": row[5],
                "run_id": row[6],
                "task_ids": json.loads(row[7] or "[]"),
                "dry_run": bool(row[8]),
                "auto": bool(row[9]),
                "reason": row[10],
                "outcome": row[11],
                "details": json.loads(row[12] or "{}"),
            }
        )
    return entries


# ---------------------------------------------------------------------------- retry counters


@dataclass(frozen=True)
class RetryState:
    attempts: int = 0
    auto_attempts: int = 0
    last_attempt_at: float | None = None


def get_retry_state(dag_id: str, run_id: str, task_id: str) -> RetryState:
    with connect() as conn:
        row = conn.execute(
            "SELECT attempts, auto_attempts, last_attempt_at FROM retry_counters "
            "WHERE dag_id = ? AND run_id = ? AND task_id = ?",
            (dag_id, run_id, task_id),
        ).fetchone()
    if row is None:
        return RetryState()
    return RetryState(int(row[0]), int(row[1]), float(row[2]))


def record_retry(
    dag_id: str, run_id: str, task_ids: list[str], *, auto: bool, now: float | None = None
) -> dict[str, RetryState]:
    """Count one plugin-initiated retry for each task; returns the new states."""
    timestamp = time.time() if now is None else now
    states: dict[str, RetryState] = {}
    with connect() as conn:
        for task_id in task_ids:
            conn.execute(
                """
                INSERT INTO retry_counters
                    (dag_id, run_id, task_id, attempts, auto_attempts, last_attempt_at)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT (dag_id, run_id, task_id) DO UPDATE SET
                    attempts = attempts + 1,
                    auto_attempts = auto_attempts + excluded.auto_attempts,
                    last_attempt_at = excluded.last_attempt_at
                """,
                (dag_id, run_id, task_id, int(auto), timestamp),
            )
            row = conn.execute(
                "SELECT attempts, auto_attempts, last_attempt_at FROM retry_counters "
                "WHERE dag_id = ? AND run_id = ? AND task_id = ?",
                (dag_id, run_id, task_id),
            ).fetchone()
            states[task_id] = RetryState(int(row[0]), int(row[1]), float(row[2]))
    return states


def update_audit_entry(entry_id: int, *, outcome: str, details: dict[str, Any] | None) -> None:
    """Complete a journal entry written before a write action (``pending`` -> outcome)."""
    with connect() as conn:
        conn.execute(
            "UPDATE audit_log SET outcome = ?, details = ? WHERE id = ?",
            (outcome, json.dumps(details or {}, default=str)[:4000], entry_id),
        )


def count_audit_entries(
    tool: str,
    session_id: str | None,
    since: float = 0.0,
    outcomes: tuple[str, ...] | None = None,
) -> int:
    """Number of journal entries of ``tool`` for one Hermes session (budget checks).

    ``outcomes`` restricts the count (e.g. only tests that actually ran).
    """
    query = "SELECT COUNT(*) FROM audit_log WHERE tool = ? AND ts >= ?"
    params: list[Any] = [tool, since]
    if session_id is None:
        query += " AND session_id IS NULL"
    else:
        query += " AND session_id = ?"
        params.append(session_id)
    if outcomes is not None:
        if not outcomes:
            return 0
        query += f" AND outcome IN ({','.join('?' for _ in outcomes)})"
        params.extend(outcomes)
    with connect() as conn:
        row = conn.execute(query, params).fetchone()
    return int(row[0])
