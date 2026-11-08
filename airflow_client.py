"""Synchronous Airflow REST client supporting Airflow 2.x (``/api/v1``) and 3.x (``/api/v2``).

Version differences are isolated here; callers receive normalised dictionaries.
Every failure is raised as :class:`AirflowError`, which carries a clean, redacted message
and never a traceback.
"""

from __future__ import annotations

import ast
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from .config import Settings
from .redaction import redact_text

USER_AGENT = "hermes-airflow-mlops/0.1"
_RETRYABLE_STATUS = frozenset({502, 503, 504})
_MAX_PAGES = 50  # hard stop for runaway pagination


class AirflowError(Exception):
    """Clean, user-facing error (type, message, optional HTTP status)."""

    def __init__(self, error_type: str, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.message = redact_text(message)
        self.status = status

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.error_type, "message": self.message, "status": self.status}


@dataclass(frozen=True)
class Credentials:
    """Airflow credentials; ``repr`` never shows the values."""

    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    token: str | None = field(default=None, repr=False)

    def require_for(self, auth_mode: str) -> None:
        """Raise a configuration error when the secrets needed by ``auth_mode`` are missing."""
        if auth_mode == "token" and not self.token:
            raise AirflowError("config", "auth_mode 'token' requires the AIRFLOW_TOKEN secret.")
        if auth_mode in {"basic", "jwt"} and not (self.username and self.password):
            raise AirflowError(
                "config",
                f"auth_mode '{auth_mode}' requires the AIRFLOW_USERNAME and AIRFLOW_PASSWORD "
                "secrets.",
            )


def _seg(value: str) -> str:
    """Encode one URL path segment (run ids contain ':' and '+')."""
    return quote(str(value), safe="")


def parse_datetime(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def seconds_between(start: Any, end: Any) -> float | None:
    start_dt, end_dt = parse_datetime(start), parse_datetime(end)
    if start_dt is None or end_dt is None:
        return None
    return round((end_dt - start_dt).total_seconds(), 3)


class AirflowClient:
    """Thin REST client: auth, retries, pagination and v1/v2 normalisation."""

    def __init__(
        self,
        settings: Settings,
        credentials: Credentials,
        *,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        backoff_base: float = 0.5,
    ) -> None:
        credentials.require_for(settings.auth_mode)
        self.settings = settings
        self._credentials = credentials
        self._sleep = sleep
        self._backoff_base = backoff_base
        self._jwt: str | None = None
        auth: httpx.Auth | None = None
        if settings.auth_mode == "basic":
            auth = httpx.BasicAuth(credentials.username or "", credentials.password or "")
        self._http = httpx.Client(
            base_url=settings.base_url,
            auth=auth,
            timeout=httpx.Timeout(settings.timeout_seconds),
            verify=settings.verify_tls,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            transport=transport,
            follow_redirects=False,
        )

    # ------------------------------------------------------------------ plumbing

    @property
    def is_v2(self) -> bool:
        return self.settings.api_version == "v2"

    def close(self) -> None:
        self._http.close()

    def _auth_headers(self) -> dict[str, str]:
        if self.settings.auth_mode == "token":
            return {"Authorization": f"Bearer {self._credentials.token}"}
        if self.settings.auth_mode == "jwt":
            if self._jwt is None:
                self._jwt = self._fetch_jwt()
            return {"Authorization": f"Bearer {self._jwt}"}
        return {}

    def _fetch_jwt(self) -> str:
        """Exchange username/password for a JWT (Airflow 3: ``POST /auth/token``)."""
        try:
            response = self._http.post(
                "/auth/token",
                json={
                    "username": self._credentials.username,
                    "password": self._credentials.password,
                },
            )
        except httpx.TimeoutException as exc:
            raise AirflowError("timeout", "Airflow did not answer the token request.") from exc
        except httpx.TransportError as exc:
            raise AirflowError(
                "network",
                f"Cannot reach Airflow at {self.settings.base_url} ({type(exc).__name__}).",
            ) from exc
        if response.status_code not in (200, 201):
            raise AirflowError(
                "auth",
                f"JWT token request failed with HTTP {response.status_code}. "
                "Check AIRFLOW_USERNAME / AIRFLOW_PASSWORD and auth_mode.",
                response.status_code,
            )
        try:
            token = response.json().get("access_token")
        except ValueError:
            token = None
        if not token:
            raise AirflowError("auth", "JWT token response did not contain 'access_token'.")
        return str(token)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Send a request under the API prefix with auth, limited retries and error mapping.

        Only idempotent GETs are retried (network errors and 502/503/504).
        """
        url = f"{self.settings.api_prefix}{path}"
        retries = self.settings.max_retries if method == "GET" else 0
        refreshed_jwt = False
        attempt = 0
        while True:
            request_headers = {**self._auth_headers(), **(headers or {})}
            try:
                response = self._http.request(
                    method, url, params=params, json=json_body, headers=request_headers
                )
            except httpx.TimeoutException as exc:
                if attempt < retries:
                    self._backoff(attempt)
                    attempt += 1
                    continue
                raise AirflowError(
                    "timeout",
                    f"Airflow did not answer within {self.settings.timeout_seconds:g}s "
                    f"({method} {path}).",
                ) from exc
            except httpx.TransportError as exc:
                if attempt < retries:
                    self._backoff(attempt)
                    attempt += 1
                    continue
                raise AirflowError(
                    "network",
                    f"Cannot reach Airflow at {self.settings.base_url} ({type(exc).__name__}).",
                ) from exc

            if response.status_code in _RETRYABLE_STATUS and attempt < retries:
                self._backoff(attempt)
                attempt += 1
                continue
            if (
                response.status_code == 401
                and self.settings.auth_mode == "jwt"
                and not refreshed_jwt
            ):
                self._jwt = None  # expired token: fetch a new one once
                refreshed_jwt = True
                continue
            if response.status_code >= 400:
                raise self._http_error(response, method, path)
            return response

    def _backoff(self, attempt: int) -> None:
        self._sleep(min(4.0, self._backoff_base * (2**attempt)))

    @staticmethod
    def _http_error(response: httpx.Response, method: str, path: str) -> AirflowError:
        status = response.status_code
        detail = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                raw_detail = body.get("detail") or body.get("title") or ""
                detail = raw_detail if isinstance(raw_detail, str) else str(raw_detail)
        except ValueError:
            detail = response.text[:300] if response.text else ""
        detail = detail[:500]
        suffix = f": {detail}" if detail else ""
        if status == 401:
            return AirflowError(
                "auth", f"Authentication failed (401). Check credentials and auth_mode{suffix}", 401
            )
        if status == 403:
            return AirflowError("forbidden", f"Permission denied (403) for {path}{suffix}", 403)
        if status == 404:
            return AirflowError("not_found", f"Not found (404): {path}{suffix}", 404)
        if status in (400, 409, 422):
            return AirflowError("bad_request", f"Rejected by Airflow ({status}){suffix}", status)
        if status >= 500:
            return AirflowError(
                "server", f"Airflow server error {status} on {method} {path}{suffix}", status
            )
        return AirflowError("http", f"Unexpected HTTP {status} on {method} {path}{suffix}", status)

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        response = self._request("GET", path, params=params)
        try:
            return response.json()
        except ValueError as exc:
            raise AirflowError("server", f"Airflow returned non-JSON content for {path}.") from exc

    def _paginate(
        self,
        path: str,
        key: str,
        params: dict[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Collect ``key`` items across offset-paginated pages (both API versions)."""
        items: list[dict[str, Any]] = []
        offset = 0
        page_size = self.settings.page_size
        for _ in range(_MAX_PAGES):
            limit = page_size
            if max_items is not None:
                limit = min(page_size, max_items - len(items))
                if limit <= 0:
                    break
            page = self._get_json(path, {**(params or {}), "limit": limit, "offset": offset})
            batch = (page.get(key) or []) if isinstance(page, dict) else []
            items.extend(batch)
            total = page.get("total_entries") if isinstance(page, dict) else None
            offset += len(batch)
            if len(batch) < limit or (isinstance(total, int) and offset >= total):
                break
        return items

    # ------------------------------------------------------------------ DAGs

    def list_dags(
        self,
        *,
        tags: Iterable[str] | None = None,
        paused: bool | None = None,
        only_active: bool = True,
        max_items: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if tags:
            params["tags"] = list(tags)
        if paused is not None:
            params["paused"] = str(paused).lower()
        if self.is_v2:
            params["exclude_stale"] = str(only_active).lower()
        else:
            params["only_active"] = str(only_active).lower()
        raw = self._paginate("/dags", "dags", params, max_items=max_items)
        return [self._normalize_dag(item) for item in raw]

    def get_dag_details(self, dag_id: str) -> dict[str, Any]:
        raw = self._get_json(f"/dags/{_seg(dag_id)}/details")
        dag = self._normalize_dag(raw)
        dag.update(
            {
                "catchup": raw.get("catchup"),
                "start_date": raw.get("start_date"),
                "timezone": raw.get("timezone"),
                "dag_run_timeout": raw.get("dag_run_timeout"),
                "doc_md": raw.get("doc_md"),
                "params": raw.get("params") or {},
                "fileloc": raw.get("fileloc"),
                "relative_fileloc": raw.get("relative_fileloc"),
            }
        )
        return dag

    def get_dag_source(self, dag_id: str) -> str:
        """Python source of the file defining ``dag_id``, as deployed in Airflow.

        v1 (2.x): ``GET /dags/{id}`` -> ``file_token`` -> ``GET /dagSources/{file_token}``
        (verified on Airflow 2.9). v2 (3.x): ``GET /dagSources/{dag_id}``.
        """
        if self.is_v2:
            body = self._get_json(f"/dagSources/{_seg(dag_id)}")
        else:
            dag = self._get_json(f"/dags/{_seg(dag_id)}")
            token = dag.get("file_token") if isinstance(dag, dict) else None
            if not token:
                raise AirflowError("not_found", f"Airflow returned no source file for '{dag_id}'.")
            body = self._get_json(f"/dagSources/{_seg(str(token))}")
        content = body.get("content") if isinstance(body, dict) else body
        return str(content or "")

    def get_tasks(self, dag_id: str) -> list[dict[str, Any]]:
        raw = self._get_json(f"/dags/{_seg(dag_id)}/tasks")
        tasks = (raw.get("tasks") or []) if isinstance(raw, dict) else []
        return [self._normalize_task(task) for task in tasks]

    @staticmethod
    def _normalize_dag(raw: dict[str, Any]) -> dict[str, Any]:
        schedule = raw.get("timetable_summary")
        if schedule is None:
            interval = raw.get("schedule_interval")
            if isinstance(interval, dict):
                schedule = interval.get("value") or {
                    k: v for k, v in interval.items() if k != "__type"
                }
            else:
                schedule = interval
        if schedule is None:
            schedule = raw.get("timetable_description")
        tags = [
            tag.get("name") if isinstance(tag, dict) else str(tag) for tag in raw.get("tags") or []
        ]
        return {
            "dag_id": raw.get("dag_id"),
            "description": raw.get("description"),
            "is_paused": raw.get("is_paused"),
            "is_active": raw.get("is_active", not raw.get("is_stale", False)),
            "schedule": schedule,
            "timetable_description": raw.get("timetable_description"),
            "tags": [tag for tag in tags if tag],
            "owners": raw.get("owners") or [],
            "next_run": raw.get("next_dagrun_logical_date") or raw.get("next_dagrun"),
            "has_import_errors": raw.get("has_import_errors"),
            "max_active_runs": raw.get("max_active_runs"),
            "max_active_tasks": raw.get("max_active_tasks"),
        }

    @staticmethod
    def _normalize_task(raw: dict[str, Any]) -> dict[str, Any]:
        class_ref = raw.get("class_ref") or {}
        operator = raw.get("operator_name") or (
            class_ref.get("class_name") if isinstance(class_ref, dict) else None
        )
        return {
            "task_id": raw.get("task_id"),
            "operator": operator,
            "trigger_rule": raw.get("trigger_rule"),
            "retries": raw.get("retries"),
            "pool": raw.get("pool"),
            "is_mapped": raw.get("is_mapped"),
            "downstream_task_ids": list(raw.get("downstream_task_ids") or []),
        }

    # ------------------------------------------------------------------ runs

    def list_dag_runs(
        self,
        dag_id: str,
        *,
        limit: int = 10,
        states: Iterable[str] | None = None,
        start_date_gte: str | None = None,
    ) -> list[dict[str, Any]]:
        """Most recent runs first. ``start_date_gte`` is an ISO-8601 lower bound."""
        params: dict[str, Any] = {"order_by": "-run_after" if self.is_v2 else "-execution_date"}
        if states:
            params["state"] = list(states)
        if start_date_gte:
            params["start_date_gte"] = start_date_gte
        raw = self._paginate(f"/dags/{_seg(dag_id)}/dagRuns", "dag_runs", params, max_items=limit)
        return [self._normalize_run(run) for run in raw]

    def get_dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return self._normalize_run(self._get_json(f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}"))

    @staticmethod
    def _normalize_run(raw: dict[str, Any]) -> dict[str, Any]:
        duration = raw.get("duration")
        if duration is None:
            duration = seconds_between(raw.get("start_date"), raw.get("end_date"))
        return {
            "run_id": raw.get("dag_run_id"),
            "dag_id": raw.get("dag_id"),
            "state": raw.get("state"),
            "run_type": raw.get("run_type"),
            "logical_date": raw.get("logical_date") or raw.get("execution_date"),
            # Airflow 3: manual runs have no logical_date; run_after (trigger time) is stable
            "run_after": raw.get("run_after"),
            "start_date": raw.get("start_date"),
            "end_date": raw.get("end_date"),
            "duration_s": duration,
        }

    # ------------------------------------------------------------------ task instances

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        raw = self._paginate(
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances", "task_instances"
        )
        return [self._normalize_ti(ti) for ti in raw]

    def get_task_instance(
        self, dag_id: str, run_id: str, task_id: str, map_index: int | None = None
    ) -> dict[str, Any]:
        path = f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances/{_seg(task_id)}"
        if map_index is not None and map_index >= 0:
            path += f"/{map_index}"
        return self._normalize_ti(self._get_json(path))

    @staticmethod
    def _normalize_ti(raw: dict[str, Any]) -> dict[str, Any]:
        duration = raw.get("duration")
        if duration is None:
            duration = seconds_between(raw.get("start_date"), raw.get("end_date"))
        return {
            "task_id": raw.get("task_id"),
            "map_index": raw.get("map_index", -1),
            "state": raw.get("state"),
            "try_number": raw.get("try_number"),
            "max_tries": raw.get("max_tries"),
            "duration_s": duration,
            "start_date": raw.get("start_date"),
            "end_date": raw.get("end_date"),
            "queued_when": raw.get("queued_when"),
            "queued_s": seconds_between(raw.get("queued_when"), raw.get("start_date")),
            "pool": raw.get("pool"),
            "operator": raw.get("operator_name") or raw.get("operator"),
            "hostname": raw.get("hostname"),
        }

    # ------------------------------------------------------------------ logs

    def get_task_log(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int,
        map_index: int | None = None,
    ) -> str:
        """Return the full log text of one try, flattened to plain text."""
        path = (
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances/{_seg(task_id)}"
            f"/logs/{int(try_number)}"
        )
        params: dict[str, Any] = {"full_content": "true"}
        if map_index is not None and map_index >= 0:
            params["map_index"] = map_index
        response = self._request("GET", path, params=params)
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            return response.text
        try:
            body = response.json()
        except ValueError:
            return response.text
        content = body.get("content") if isinstance(body, dict) else body
        return flatten_log_content(content)

    # ------------------------------------------------------------------ xcoms

    def list_xcom_entries(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        *,
        key: str | None = None,
        map_index: int | None = None,
        max_items: int = 50,
    ) -> list[dict[str, Any]]:
        path = (
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances/{_seg(task_id)}/xcomEntries"
        )
        params: dict[str, Any] = {}
        if key:
            params["xcom_key"] = key
        if map_index is not None:
            params["map_index"] = map_index
        return self._paginate(path, "xcom_entries", params, max_items=max_items)

    def get_xcom_value(
        self, dag_id: str, run_id: str, task_id: str, key: str, map_index: int | None = None
    ) -> Any:
        path = (
            f"/dags/{_seg(dag_id)}/dagRuns/{_seg(run_id)}/taskInstances/{_seg(task_id)}"
            f"/xcomEntries/{_seg(key)}"
        )
        params: dict[str, Any] = {}
        if map_index is not None and map_index >= 0:
            params["map_index"] = map_index
        body = self._get_json(path, params or None)
        return body.get("value") if isinstance(body, dict) else body

    # ------------------------------------------------------------------ remediation

    def clear_task_instances(
        self,
        dag_id: str,
        run_id: str,
        task_ids: list[str],
        *,
        dry_run: bool,
        only_failed: bool,
        include_downstream: bool,
    ) -> list[dict[str, Any]]:
        """``POST /dags/{dag_id}/clearTaskInstances`` restricted to ONE run and explicit tasks.

        Scope-widening options are hard-coded off: no upstream, no past/future runs, no date
        range. ``reset_dag_runs`` puts the (failed) run back in a runnable state so the
        scheduler picks the cleared tasks up; tasks that are not cleared keep their state.
        Never retried automatically (not idempotent).
        """
        if not task_ids:
            raise AirflowError("guardrail", "Refusing to clear: task_ids is empty.")
        body = {
            "dry_run": dry_run,
            "dag_run_id": run_id,
            "task_ids": list(task_ids),
            "only_failed": only_failed,
            "only_running": False,
            "include_upstream": False,
            "include_downstream": include_downstream,
            "include_future": False,
            "include_past": False,
            "reset_dag_runs": True,
        }
        response = self._request("POST", f"/dags/{_seg(dag_id)}/clearTaskInstances", json_body=body)
        try:
            payload = response.json()
        except ValueError as exc:
            raise AirflowError("server", "Airflow returned non-JSON content for clear.") from exc
        items = (payload.get("task_instances") or []) if isinstance(payload, dict) else []
        return [
            {
                "task_id": item.get("task_id"),
                "map_index": item.get("map_index", -1),
                "run_id": item.get("dag_run_id") or item.get("run_id") or run_id,
                "state": item.get("state"),
            }
            for item in items
            if isinstance(item, dict)
        ]


def flatten_log_content(content: Any) -> str:
    """Normalise the log payload of both API versions to plain text.

    * v1 (2.x): ``content`` is a string, sometimes the repr of ``[(host, text), ...]``.
    * v2 (3.x): ``content`` is a list of structured messages (``{timestamp, level, event,
      error_detail?...}``) or plain strings.
    """
    if content is None:
        return ""
    if isinstance(content, str):
        stripped = content.lstrip()
        if stripped.startswith("[(") and stripped.rstrip().endswith(")]"):
            try:
                parsed = ast.literal_eval(stripped)
            except (ValueError, SyntaxError, MemoryError, RecursionError):
                return content
            if isinstance(parsed, list):
                parts = [
                    str(item[1]) if isinstance(item, tuple) and len(item) == 2 else str(item)
                    for item in parsed
                ]
                return "\n".join(parts)
        return content
    if isinstance(content, list):
        lines: list[str] = []
        for item in content:
            if isinstance(item, dict):
                lines.extend(_format_structured_log(item))
            else:
                lines.append(str(item))
        return "\n".join(lines)
    return str(content)


def _format_structured_log(item: dict[str, Any]) -> list[str]:
    timestamp = item.get("timestamp") or ""
    level = str(item.get("level") or "").upper()
    event = item.get("event", "")
    logger = item.get("logger") or ""
    head = " ".join(part for part in (str(timestamp), f"[{level}]" if level else "") if part)
    origin = f" {logger}:" if logger else ""
    lines = [f"{head}{origin} {event}".strip()]
    for exc in item.get("error_detail") or []:
        if not isinstance(exc, dict):
            continue
        lines.append("Traceback (most recent call last):")
        for frame in exc.get("frames") or []:
            if isinstance(frame, dict):
                lines.append(
                    f'  File "{frame.get("filename")}", line {frame.get("lineno")}, '
                    f"in {frame.get('name')}"
                )
        lines.append(f"{exc.get('exc_type', 'Exception')}: {exc.get('exc_value', '')}")
    return lines
