"""Airflow client: auth modes, retries, pagination, error mapping, log flattening."""

from __future__ import annotations

import httpx
import pytest
import respx
from airflow_mlops.airflow_client import (
    AirflowClient,
    AirflowError,
    Credentials,
    flatten_log_content,
)
from airflow_mlops.config import Settings
from harness import BASE_URL, make_context

CREDS = Credentials(username="admin", password="pw", token="static-token")


def client(api_version: str = "v2", auth_mode: str = "jwt", **overrides: object) -> AirflowClient:
    settings = Settings(
        base_url=BASE_URL, api_version=api_version, auth_mode=auth_mode, **overrides
    )  # type: ignore[arg-type]
    return AirflowClient(settings, CREDS, sleep=lambda _s: None)


@respx.mock
def test_basic_auth_header() -> None:
    route = respx.get(f"{BASE_URL}/api/v1/dags/a/tasks").respond(json={"tasks": []})
    client("v1", "basic").get_tasks("a")
    assert route.calls.last.request.headers["authorization"].startswith("Basic ")


@respx.mock
def test_static_token_header() -> None:
    route = respx.get(f"{BASE_URL}/api/v2/dags/a/tasks").respond(json={"tasks": []})
    client("v2", "token").get_tasks("a")
    assert route.calls.last.request.headers["authorization"] == "Bearer static-token"


@respx.mock
def test_jwt_is_fetched_once_and_refreshed_on_401() -> None:
    token_route = respx.post(f"{BASE_URL}/auth/token").mock(
        side_effect=[
            httpx.Response(201, json={"access_token": "jwt-1"}),
            httpx.Response(201, json={"access_token": "jwt-2"}),
        ]
    )
    api = respx.get(f"{BASE_URL}/api/v2/dags/a/tasks").mock(
        side_effect=[
            httpx.Response(200, json={"tasks": []}),
            httpx.Response(401, json={"detail": "expired"}),
            httpx.Response(200, json={"tasks": []}),
        ]
    )
    c = client("v2", "jwt")
    c.get_tasks("a")
    c.get_tasks("a")
    assert token_route.call_count == 2
    assert token_route.calls[0].request.content == b'{"username":"admin","password":"pw"}'
    assert api.calls[0].request.headers["authorization"] == "Bearer jwt-1"
    assert api.calls[2].request.headers["authorization"] == "Bearer jwt-2"


@respx.mock
def test_jwt_failure_is_auth_error() -> None:
    respx.post(f"{BASE_URL}/auth/token").respond(401, json={"detail": "bad credentials"})
    with pytest.raises(AirflowError) as info:
        client("v2", "jwt").get_tasks("a")
    assert info.value.error_type == "auth"


@respx.mock
def test_get_retries_on_503_then_succeeds() -> None:
    route = respx.get(f"{BASE_URL}/api/v1/dags/a/tasks").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={"tasks": []})]
    )
    assert client("v1", "basic", max_retries=2).get_tasks("a") == []
    assert route.call_count == 2


@respx.mock
def test_network_error_after_retries() -> None:
    route = respx.get(f"{BASE_URL}/api/v1/dags/a/tasks").mock(
        side_effect=httpx.ConnectError("refused")
    )
    with pytest.raises(AirflowError) as info:
        client("v1", "basic", max_retries=2).get_tasks("a")
    assert info.value.error_type == "network"
    assert route.call_count == 3


@respx.mock
def test_timeout_is_mapped() -> None:
    respx.get(f"{BASE_URL}/api/v1/dags/a/tasks").mock(side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(AirflowError) as info:
        client("v1", "basic", max_retries=0).get_tasks("a")
    assert info.value.error_type == "timeout"


@pytest.mark.parametrize(
    ("status", "error_type"),
    [(401, "auth"), (403, "forbidden"), (404, "not_found"), (422, "bad_request"), (500, "server")],
)
@respx.mock
def test_http_status_mapping(status: int, error_type: str) -> None:
    respx.get(f"{BASE_URL}/api/v1/dags/a/tasks").respond(
        status, json={"detail": "password=leak in detail"}
    )
    with pytest.raises(AirflowError) as info:
        client("v1", "basic", max_retries=0).get_tasks("a")
    assert info.value.error_type == error_type
    assert info.value.status == status
    assert "leak" not in info.value.message


def test_missing_credentials_raise_config_error() -> None:
    settings = Settings(base_url=BASE_URL, api_version="v2", auth_mode="token")
    with pytest.raises(AirflowError) as info:
        AirflowClient(settings, Credentials(username="u", password="p"))
    assert info.value.error_type == "config"


def test_missing_secret_is_reported_by_tool_and_not_cached(secrets: dict[str, str]) -> None:
    secrets.clear()
    with respx.mock(base_url=f"{BASE_URL}/api/v2", assert_all_called=False) as router:
        router.get("/dags/a/tasks").respond(json={"tasks": [{"task_id": "x"}]})
        router.post(f"{BASE_URL}/auth/token").respond(201, json={"access_token": "j"})
        ctx = make_context(api_version="v2", auth_mode="jwt")
        first = ctx.call("airflow_get_dependencies", dag_id="a", task_id="x")
        assert first["ok"] is False and first["error"]["type"] == "config"
        secrets.update({"AIRFLOW_USERNAME": "admin", "AIRFLOW_PASSWORD": "pw"})
        second = ctx.call("airflow_get_dependencies", dag_id="a", task_id="x")
        assert second["ok"] is True


def test_settings_from_context_coerce_and_default() -> None:
    ctx = make_context(
        api_version="V9",
        timeout_seconds="abc",
        dag_allowlist="a*, b",
        read_only="false",
        sla_thresholds={"a": "60", "bad": "x"},
    )
    settings = Settings.from_source(ctx)
    assert settings.api_version == "v2"
    assert settings.timeout_seconds == 15.0
    assert settings.dag_allowlist == ("a*", "b")
    assert settings.read_only is False
    assert settings.sla_thresholds == {"a": 60.0}
    assert settings.dag_allowed("abc") and not settings.dag_allowed("c")


def test_flatten_v1_tuple_repr() -> None:
    content = repr([("host-a", "line 1\nline 2"), ("host-b", "line 3")])
    assert flatten_log_content(content) == "line 1\nline 2\nline 3"


def test_flatten_v1_plain_string() -> None:
    assert flatten_log_content("just text") == "just text"


def test_flatten_v2_structured() -> None:
    text = flatten_log_content(
        [
            "plain line",
            {
                "timestamp": "T",
                "level": "error",
                "event": "boom",
                "logger": "task",
                "error_detail": [
                    {
                        "exc_type": "ValueError",
                        "exc_value": "bad",
                        "frames": [{"filename": "f.py", "lineno": 3, "name": "fn"}],
                    }
                ],
            },
        ]
    )
    assert text.splitlines() == [
        "plain line",
        "T [ERROR] task: boom",
        "Traceback (most recent call last):",
        '  File "f.py", line 3, in fn',
        "ValueError: bad",
    ]
