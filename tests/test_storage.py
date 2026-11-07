"""SQLite storage (through Hermes' plugin_db): migrations and diagnosis cache."""

from __future__ import annotations

from airflow_mlops import storage


def test_schema_is_migrated_to_current_version() -> None:
    with storage.connect() as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert version == storage.SCHEMA_VERSION
    assert "diagnosis_cache" in tables


def test_diagnosis_upsert_and_read() -> None:
    assert storage.get_diagnosis("d", "r", "t") is None
    storage.save_diagnosis(
        "d",
        "r",
        "t",
        map_index=-1,
        try_number=1,
        category="transient",
        rule_ids=["connection_reset"],
        justification="j1",
        now=100.0,
    )
    storage.save_diagnosis(
        "d",
        "r",
        "t",
        map_index=-1,
        try_number=2,
        category="critical",
        rule_ids=["python_code_error"],
        justification="j2",
        now=200.0,
    )
    cached = storage.get_diagnosis("d", "r", "t")
    assert cached is not None
    assert cached.category == "critical"
    assert cached.try_number == 2
    assert cached.rule_ids == ["python_code_error"]
    assert cached.diagnosed_at == 200.0
    assert cached.to_dict()["justification"] == "j2"


def test_map_index_is_part_of_the_key() -> None:
    storage.save_diagnosis(
        "d",
        "r",
        "t",
        map_index=0,
        try_number=1,
        category="transient",
        rule_ids=[],
        justification="",
        now=1.0,
    )
    assert storage.get_diagnosis("d", "r", "t", map_index=0) is not None
    assert storage.get_diagnosis("d", "r", "t", map_index=1) is None
