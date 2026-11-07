"""Transient / critical / unknown classification against the bundled and custom rules."""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow_mlops.diagnosis import (
    BUNDLED_RULES_PATH,
    CRITICAL,
    TRANSIENT,
    UNKNOWN,
    classify,
    load_rules,
    parse_rules,
)

RULES = load_rules()


def test_bundled_rules_are_valid() -> None:
    assert RULES.warnings == ()
    ids = [rule.id for rule in RULES.rules]
    assert len(ids) == len(set(ids))
    assert {r.category for r in RULES.rules} == {TRANSIENT, CRITICAL}


@pytest.mark.parametrize(
    ("error", "rule_id"),
    [
        ("ReadTimeout: HTTPSConnectionPool(host='api'): Read timed out.", "network_timeout"),
        ("airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 42", "network_timeout"),
        ("ConnectionResetError: [Errno 104] Connection reset by peer", "connection_reset"),
        (
            "requests.exceptions.HTTPError: 429 Client Error: Too Many Requests for url",
            "rate_limited_or_unavailable",
        ),
        ("HTTPError: 503 Server Error: Service Unavailable", "rate_limited_or_unavailable"),
        ("botocore.exceptions.ClientError: ThrottlingException", "rate_limited_or_unavailable"),
        (
            "Not scheduling since there are 0 open slots in pool default_pool",
            "pool_slots_exhausted",
        ),
        ("airflow.exceptions.AirflowSensorTimeout: Sensor has timed out", "sensor_timeout"),
        ("psycopg2.OperationalError: could not connect to server", "external_resource_unavailable"),
        ("socket.gaierror: Temporary failure in name resolution", "external_resource_unavailable"),
        ("Task instance was detected as zombie", "infrastructure_interruption"),
    ],
)
def test_transient_errors(error: str, rule_id: str) -> None:
    result = classify(f"Traceback (most recent call last):\n{error}", None, RULES)
    assert result.category == TRANSIENT, result.justification
    assert rule_id in result.rule_ids


@pytest.mark.parametrize(
    ("error", "rule_id"),
    [
        (
            '  File "dag.py", line 3\n    def f(:\nSyntaxError: invalid syntax',
            "python_syntax_error",
        ),
        ("ModuleNotFoundError: No module named 'sklearn'", "python_import_error"),
        ("KeyError: 'target'", "python_code_error"),
        ("TypeError: unsupported operand type(s) for +: 'int' and 'str'", "python_code_error"),
        ('psycopg2.errors.UndefinedColumn: column "price" does not exist', "schema_mismatch"),
        ("AssertionError: null ratio 0.4 above 0.05", "data_quality_failed"),
        ("great_expectations: Validation failed for suite orders", "data_quality_failed"),
        ("PermissionError: [Errno 13] Permission denied: '/data/out.csv'", "permission_denied"),
        (
            "botocore.exceptions.ClientError: AccessDenied when calling PutObject",
            "permission_denied",
        ),
        (
            "airflow.exceptions.AirflowNotFoundException: The conn_id `warehouse` isn't defined",
            "missing_connection_or_config",
        ),
        ("KeyError: 'Variable model_bucket does not exist'", "missing_connection_or_config"),
    ],
)
def test_critical_errors(error: str, rule_id: str) -> None:
    result = classify(f"Traceback (most recent call last):\n{error}", None, RULES)
    assert result.category == CRITICAL, result.justification
    assert rule_id in result.rule_ids


def test_critical_takes_precedence_over_transient() -> None:
    block = (
        "ConnectionResetError: [Errno 104] Connection reset by peer\n"
        "During handling of the above exception, another exception occurred:\n"
        "KeyError: 'target'"
    )
    result = classify(block, None, RULES)
    assert result.category == CRITICAL
    assert "connection_reset" in result.rule_ids  # still reported, for transparency
    assert "take precedence" in result.justification


def test_error_block_is_preferred_over_log_tail() -> None:
    tail = "WARNING retrying after Connection refused\nKeyError: 'x'"
    result = classify("AirflowSensorTimeout: Sensor has timed out", tail, RULES)
    assert result.category == TRANSIENT
    assert all(m.source == "error_block" for m in result.matches)


def test_log_tail_is_used_when_error_block_matches_nothing() -> None:
    result = classify("something odd happened", "ERROR - Permission denied on bucket", RULES)
    assert result.category == CRITICAL
    assert result.matches[0].source == "log_tail"


def test_first_oom_is_transient_recurring_oom_is_critical() -> None:
    block = "Task exited with return code -9 (Negsignal.SIGKILL)"
    first = classify(block, None, RULES, try_number=1)
    assert first.category == TRANSIENT
    assert "becomes critical" in first.justification
    second = classify(block, None, RULES, try_number=2)
    assert second.category == CRITICAL


def test_unknown_when_nothing_matches() -> None:
    result = classify("ERROR - the flux capacitor is misaligned", None, RULES)
    assert result.category == UNKNOWN
    assert result.matches == ()
    assert "human review" in result.justification


def test_unknown_without_text() -> None:
    assert classify(None, None, RULES).category == UNKNOWN


def test_justification_and_dict_shape() -> None:
    data = classify("KeyError: 'target'", None, RULES).to_dict()
    assert data["category"] == CRITICAL
    match = data["matched_rules"][0]
    assert set(match) >= {"rule_id", "category", "description", "matched", "excerpt", "source"}
    assert match["excerpt"] == "KeyError: 'target'"
    assert "python_code_error" in data["justification"]


def test_parse_rules_reports_invalid_entries() -> None:
    ruleset = parse_rules(
        {
            "rules": [
                {"id": "ok", "category": "transient", "patterns": ["boom"]},
                {"id": "ok", "category": "transient", "patterns": ["dup"]},
                {"id": "bad_cat", "category": "maybe", "patterns": ["x"]},
                {"id": "bad_regex", "category": "critical", "patterns": ["(unclosed"]},
                "not a mapping",
            ]
        },
        "test",
    )
    assert [r.id for r in ruleset.rules] == ["ok"]
    # duplicate id, bad category, invalid regex + resulting "no usable pattern", not a mapping
    assert len(ruleset.warnings) == 5


def test_custom_rules_file(tmp_path: Path) -> None:
    custom = tmp_path / "rules.yaml"
    custom.write_text(
        "rules:\n  - id: vendor_flaky\n    category: transient\n    keywords: [vendor hiccup]\n",
        encoding="utf-8",
    )
    ruleset = load_rules(str(custom))
    assert [r.id for r in ruleset.rules] == ["vendor_flaky"]
    assert classify("ERROR Vendor HICCUP again", None, ruleset).category == TRANSIENT


def test_missing_custom_rules_fall_back_to_bundled(tmp_path: Path) -> None:
    ruleset = load_rules(str(tmp_path / "missing.yaml"))
    assert Path(ruleset.source) == BUNDLED_RULES_PATH
    assert any("using bundled rules" in w for w in ruleset.warnings)
