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

SCHEMA_VERSION = 1

_MIGRATIONS: dict[int, list[str]] = {
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
