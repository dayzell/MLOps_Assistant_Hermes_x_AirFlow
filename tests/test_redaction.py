"""Redaction rules: every secret shape must be masked, ordinary text must survive."""

from __future__ import annotations

import pytest
from airflow_mlops.redaction import MASK, is_sensitive_key, redact_text, redact_value

# Sample secrets are assembled at runtime so no token-shaped literal sits in the repository.
AWS_KEY = "AKIA" + "IOSFODNN7EXAMPLE"
GITHUB_TOKEN = "ghp_" + "a1B2c3D4e5F6g7H8i9J0k1L2m3N4o5P6q7R8"
JWT = "eyJ" + "hbGciOiJIUzI1NiJ9" + ".eyJ" + "zdWIiOiJhZG1pbiJ9" + ".c2lnbmF0dXJlLXZhbHVl"
PEM = (
    "-----BEGIN RSA "
    + "PRIVATE KEY-----\nMIIEpAIBAAKCAQEAx\nabcdef\n-----END RSA "
    + "PRIVATE KEY-----"
)


@pytest.mark.parametrize(
    ("text", "secret"),
    [
        ("connecting with password=hunter2 to db", "hunter2"),
        ("PASSWORD: hunter2", "hunter2"),
        ('{"password": "hunter 2 with spaces"}', "hunter 2 with spaces"),
        ("{'api_key': 'k-123456'}", "k-123456"),
        ("aws_secret_access_key=wJalrXUtnFEMI/K7MDENG", "wJalrXUtnFEMI/K7MDENG"),
        ("postgresql://etl:pa55word@db.internal:5432/raw", "pa55word"),
        ("redis://:onlypass@cache:6379/0", "onlypass"),
        ("Authorization: Bearer abcdefghijklmnop", "abcdefghijklmnop"),
        ("authorization=Basic YWRtaW46c2VjcmV0", "YWRtaW46c2VjcmV0"),
        ("export AIRFLOW_CONN_WAREHOUSE=snowflake://u:p@acct/db", "snowflake://u:p@acct"),
        ("AIRFLOW_VAR_SLACK_WEBHOOK='https://hooks.example/T0/B0/xyz'", "hooks.example"),
        (f"using key {AWS_KEY} for s3", AWS_KEY),
        (f"token {GITHUB_TOKEN}", GITHUB_TOKEN),
        (f"jwt is {JWT}", JWT),
        (f"key:\n{PEM}\nend", "MIIEpAIBAAKCAQEAx"),
        ("client_secret = abc.def-ghi", "abc.def-ghi"),
    ],
)
def test_secrets_are_masked(text: str, secret: str) -> None:
    redacted = redact_text(text)
    assert secret not in redacted, redacted
    assert "REDACTED" in redacted


@pytest.mark.parametrize(
    "text",
    [
        "Task exited with return code 0",
        "KeyError: 'target'",
        "Processed 1200 rows in 3.2s",
        "http://airflow.example.com/dags/train/grid",
        "tokenizer loaded: bert-base",  # no assignment: kept
    ],
)
def test_ordinary_text_is_kept(text: str) -> None:
    assert redact_text(text) == text


def test_url_user_is_kept_password_masked() -> None:
    assert redact_text("mysql://reader:topsecret@host/db") == f"mysql://reader:{MASK}@host/db"


def test_redact_value_masks_sensitive_keys_recursively() -> None:
    data = {
        "model": "xgboost",
        "db_password": "hunter2",
        "nested": {"api_token": {"value": "abc"}, "rows": 10},
        "conn_uri": "postgres://u:p@h/d",
        "conn_id": "warehouse_default",
        "items": [{"secret": "s"}, "password=inline"],
        "empty_password": "",
    }
    result = redact_value(data)
    assert result["model"] == "xgboost"
    assert result["db_password"] == MASK
    assert result["nested"]["api_token"] == MASK
    assert result["nested"]["rows"] == 10
    assert result["conn_uri"] == MASK
    assert result["conn_id"] == "warehouse_default"  # an id is not a secret
    assert result["items"][0]["secret"] == MASK
    assert "inline" not in result["items"][1]
    assert result["empty_password"] == ""


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("password", True),
        ("DB_PASSWD", True),
        ("x-api-key", True),
        ("aws_access_key", True),
        ("sqlalchemy_conn_string", True),
        ("dsn", True),
        ("conn_id", False),
        ("rows", False),
        ("null_ratio", False),
    ],
)
def test_is_sensitive_key(key: str, expected: bool) -> None:
    assert is_sensitive_key(key) is expected
