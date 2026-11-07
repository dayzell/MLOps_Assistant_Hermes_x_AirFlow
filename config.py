"""Typed plugin settings.

Hermes' ``ctx.get_config`` does not apply ``config_schema`` defaults, so every default
lives here, in one place, and is mirrored in ``plugin.yaml``.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import Any, Protocol

PLUGIN_NAME = "airflow-mlops"
TOOLSET = "airflow"

API_VERSIONS = ("v1", "v2")
AUTH_MODES = ("basic", "token", "jwt")


class ConfigSource(Protocol):
    """Anything exposing Hermes' ``get_config(key, default)`` (the plugin context)."""

    def get_config(self, key: str, default: Any = None) -> Any: ...


DEFAULTS: dict[str, Any] = {
    "base_url": "http://localhost:8080",
    "api_version": "v2",
    "auth_mode": "jwt",
    "verify_tls": True,
    "timeout_seconds": 15.0,
    "max_retries": 2,
    "page_size": 100,
    "dag_allowlist": ["*"],
    "read_only": True,
    "log_max_lines": 200,
    "log_max_chars": 20000,
    "error_context_lines": 40,
    "xcom_max_chars": 2000,
    "data_quality_xcom_key": "data_quality",
    "sla_thresholds": {},
    "report_default_period": "24h",
    "auto_remediation_enabled": False,
    "auto_retry_max_attempts": 2,
    "auto_retry_backoff_seconds": 300,
    "error_patterns_path": "",
}


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, int | float):
        return bool(value)
    return default


def _as_int(value: Any, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(value))
    except (TypeError, ValueError):
        return default


def _as_str_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, str):
        items = [part.strip() for part in value.split(",")]
        return [item for item in items if item]
    if isinstance(value, list | tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    return list(default)


def _as_choice(value: Any, choices: tuple[str, ...], default: str) -> str:
    text = str(value).strip().lower() if value is not None else ""
    return text if text in choices else default


@dataclass(frozen=True)
class Settings:
    """Snapshot of the plugin settings (read fresh on each tool call)."""

    base_url: str = DEFAULTS["base_url"]
    api_version: str = DEFAULTS["api_version"]
    auth_mode: str = DEFAULTS["auth_mode"]
    verify_tls: bool = DEFAULTS["verify_tls"]
    timeout_seconds: float = DEFAULTS["timeout_seconds"]
    max_retries: int = DEFAULTS["max_retries"]
    page_size: int = DEFAULTS["page_size"]
    dag_allowlist: tuple[str, ...] = tuple(DEFAULTS["dag_allowlist"])
    read_only: bool = DEFAULTS["read_only"]
    log_max_lines: int = DEFAULTS["log_max_lines"]
    log_max_chars: int = DEFAULTS["log_max_chars"]
    error_context_lines: int = DEFAULTS["error_context_lines"]
    xcom_max_chars: int = DEFAULTS["xcom_max_chars"]
    data_quality_xcom_key: str = DEFAULTS["data_quality_xcom_key"]
    sla_thresholds: dict[str, float] = field(default_factory=dict)
    report_default_period: str = DEFAULTS["report_default_period"]
    auto_remediation_enabled: bool = DEFAULTS["auto_remediation_enabled"]
    auto_retry_max_attempts: int = DEFAULTS["auto_retry_max_attempts"]
    auto_retry_backoff_seconds: int = DEFAULTS["auto_retry_backoff_seconds"]
    error_patterns_path: str = DEFAULTS["error_patterns_path"]

    @classmethod
    def from_source(cls, source: ConfigSource) -> Settings:
        """Build settings from the plugin context, coercing types and falling back to defaults."""

        def raw(key: str) -> Any:
            try:
                return source.get_config(key, DEFAULTS[key])
            except Exception:  # a broken config must never break a tool call
                return DEFAULTS[key]

        sla_raw = raw("sla_thresholds")
        sla: dict[str, float] = {}
        if isinstance(sla_raw, dict):
            for key, value in sla_raw.items():
                try:
                    sla[str(key)] = float(value)
                except (TypeError, ValueError):
                    continue

        return cls(
            base_url=str(raw("base_url") or DEFAULTS["base_url"]).rstrip("/"),
            api_version=_as_choice(raw("api_version"), API_VERSIONS, DEFAULTS["api_version"]),
            auth_mode=_as_choice(raw("auth_mode"), AUTH_MODES, DEFAULTS["auth_mode"]),
            verify_tls=_as_bool(raw("verify_tls"), DEFAULTS["verify_tls"]),
            timeout_seconds=_as_float(raw("timeout_seconds"), DEFAULTS["timeout_seconds"], 1.0),
            max_retries=_as_int(raw("max_retries"), DEFAULTS["max_retries"], 0),
            page_size=_as_int(raw("page_size"), DEFAULTS["page_size"], 1),
            dag_allowlist=tuple(_as_str_list(raw("dag_allowlist"), DEFAULTS["dag_allowlist"])),
            read_only=_as_bool(raw("read_only"), DEFAULTS["read_only"]),
            log_max_lines=_as_int(raw("log_max_lines"), DEFAULTS["log_max_lines"], 1),
            log_max_chars=_as_int(raw("log_max_chars"), DEFAULTS["log_max_chars"], 200),
            error_context_lines=_as_int(
                raw("error_context_lines"), DEFAULTS["error_context_lines"], 1
            ),
            xcom_max_chars=_as_int(raw("xcom_max_chars"), DEFAULTS["xcom_max_chars"], 50),
            data_quality_xcom_key=str(
                raw("data_quality_xcom_key") or DEFAULTS["data_quality_xcom_key"]
            ),
            sla_thresholds=sla,
            report_default_period=str(
                raw("report_default_period") or DEFAULTS["report_default_period"]
            ),
            auto_remediation_enabled=_as_bool(
                raw("auto_remediation_enabled"), DEFAULTS["auto_remediation_enabled"]
            ),
            auto_retry_max_attempts=_as_int(
                raw("auto_retry_max_attempts"), DEFAULTS["auto_retry_max_attempts"], 0
            ),
            auto_retry_backoff_seconds=_as_int(
                raw("auto_retry_backoff_seconds"), DEFAULTS["auto_retry_backoff_seconds"], 0
            ),
            error_patterns_path=str(raw("error_patterns_path") or ""),
        )

    @property
    def api_prefix(self) -> str:
        return f"/api/{self.api_version}"

    def connection_key(self) -> tuple[Any, ...]:
        """Fields that require a fresh HTTP client when they change."""
        return (
            self.base_url,
            self.api_version,
            self.auth_mode,
            self.verify_tls,
            self.timeout_seconds,
            self.max_retries,
        )

    def dag_allowed(self, dag_id: str) -> bool:
        """True when ``dag_id`` matches one of the allowlist glob patterns."""
        return any(fnmatch.fnmatchcase(dag_id, pattern) for pattern in self.dag_allowlist)
