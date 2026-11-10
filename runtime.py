"""Per-plugin runtime: settings access, secrets and the lazily created Airflow client."""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

from agent.secret_scope import get_secret
from plugins.plugin_utils import SingletonSlot

from .airflow_client import AirflowClient, Credentials
from .config import ConfigSource, Settings

logger = logging.getLogger(__name__)


def load_credentials() -> Credentials:
    """Read Airflow secrets through Hermes' secret scope (never the raw process env or files)."""

    def read(name: str) -> str | None:
        value = get_secret(name)
        return value.strip() if isinstance(value, str) and value.strip() else None

    return Credentials(
        username=read("AIRFLOW_USERNAME"),
        password=read("AIRFLOW_PASSWORD"),
        token=read("AIRFLOW_TOKEN"),
    )


class PluginRuntime:
    """Holds the plugin context and the shared HTTP client.

    The client lives in a :class:`SingletonSlot`; it is rebuilt when connection-related
    settings change. A failing factory (e.g. missing secrets) caches nothing, so the next
    call retries.
    """

    def __init__(self, ctx: ConfigSource) -> None:
        self._ctx = ctx
        self._client_slot: Any = SingletonSlot()
        self._client_key: tuple[Any, ...] | None = None
        self._lock = threading.Lock()
        # One sandbox container at a time (tests are heavy compared with API reads).
        self.sandbox_lock = threading.Lock()

    @property
    def ctx(self) -> ConfigSource:
        return self._ctx

    @staticmethod
    def now() -> datetime:
        return datetime.now(UTC)

    def settings(self) -> Settings:
        return Settings.from_source(self._ctx)

    def client(self, settings: Settings | None = None) -> AirflowClient:
        settings = settings or self.settings()
        credentials = load_credentials()
        # A changed secret (new password or token) gets a new client: the key holds a digest
        # of the credentials, never the values.
        key = (*settings.connection_key(), settings.page_size, credentials.digest())
        with self._lock:
            if self._client_key != key:
                self.reset_client()
                self._client_key = key
        client: AirflowClient = self._client_slot.get(lambda: AirflowClient(settings, credentials))
        return client

    def dispatch(self, tool_name: str, args: dict[str, Any]) -> str:
        """Run another Hermes tool directly (``ctx.dispatch_tool``). The tool's own checks
        apply (the terminal's dangerous-command guards); plugin ``pre_tool_call`` hooks do not:
        callers gate the call themselves."""
        dispatch_tool = getattr(self._ctx, "dispatch_tool", None)
        if dispatch_tool is None:
            raise RuntimeError("this Hermes context cannot dispatch tools")
        return str(dispatch_tool(tool_name, args))

    def reset_client(self) -> None:
        current = self._client_slot.peek()
        if current is not None:
            try:
                current.close()
            except Exception:  # closing is best effort
                logger.debug("airflow-mlops: error while closing HTTP client", exc_info=True)
        self._client_slot.reset()
