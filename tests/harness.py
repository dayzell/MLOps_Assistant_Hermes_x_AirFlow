"""Shared test harness (imported once, by name, from conftest and tests).

* Installs light stand-ins for the Hermes host modules the plugin imports
  (``plugins.plugin_utils``, ``plugins.plugin_storage``, ``agent.secret_scope``) — tests only;
  inside Hermes the real modules are used.
* Loads the plugin directory as the package ``airflow_mlops`` (like Hermes' namespaced import).
* Provides a fake plugin context and respx-mocked Airflow servers for v1 and v2.
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
import threading
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any, Generic, TypeVar

ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "http://airflow.test"
T = TypeVar("T")

# --------------------------------------------------------------------------- host stubs

SECRETS: dict[str, str] = {}
_DATA_DIR: dict[str, Path] = {}


class SingletonSlot(Generic[T]):
    def __init__(self) -> None:
        self._value: T | None = None
        self._lock = threading.Lock()

    def get(self, factory: Callable[[], T]) -> T:
        if self._value is None:
            with self._lock:
                if self._value is None:
                    self._value = factory()
        return self._value

    def peek(self) -> T | None:
        return self._value

    def reset(self) -> None:
        self._value = None


def lazy_singleton(factory: Callable[[], T]) -> Callable[[], T]:
    slot: SingletonSlot[T] = SingletonSlot()

    def accessor() -> T:
        return slot.get(factory)

    accessor.reset = slot.reset  # type: ignore[attr-defined]
    return accessor


def plugin_data_dir(name: str) -> Path:
    path = _DATA_DIR["root"] / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def plugin_db(name: str, filename: str = "data.db") -> sqlite3.Connection:
    conn = sqlite3.connect(plugin_data_dir(name) / filename, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_secret(name: str, default: str | None = None) -> str | None:
    return SECRETS.get(name, default)


def _install_host_stubs() -> None:
    plugins_pkg = types.ModuleType("plugins")
    plugins_pkg.__path__ = []
    utils = types.ModuleType("plugins.plugin_utils")
    utils.SingletonSlot = SingletonSlot  # type: ignore[attr-defined]
    utils.lazy_singleton = lazy_singleton  # type: ignore[attr-defined]
    storage = types.ModuleType("plugins.plugin_storage")
    storage.plugin_data_dir = plugin_data_dir  # type: ignore[attr-defined]
    storage.plugin_db = plugin_db  # type: ignore[attr-defined]
    agent_pkg = types.ModuleType("agent")
    agent_pkg.__path__ = []
    secret_scope = types.ModuleType("agent.secret_scope")
    secret_scope.get_secret = get_secret  # type: ignore[attr-defined]
    sys.modules.update(
        {
            "plugins": plugins_pkg,
            "plugins.plugin_utils": utils,
            "plugins.plugin_storage": storage,
            "agent": agent_pkg,
            "agent.secret_scope": secret_scope,
        }
    )


def _load_plugin_package() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "airflow_mlops", ROOT / "__init__.py", submodule_search_locations=[str(ROOT)]
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["airflow_mlops"] = module
    spec.loader.exec_module(module)
    return module


_install_host_stubs()
_load_plugin_package()


# --------------------------------------------------------------------------- fake context


class FakeContext:
    """Records registrations; ``get_config`` mirrors Hermes (returns the passed default)."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config: dict[str, Any] = dict(config or {})
        self.tools: dict[str, dict[str, Any]] = {}
        self.hooks: dict[str, list[Callable[..., Any]]] = {}
        self.commands: dict[str, dict[str, Any]] = {}
        self.skills: dict[str, Path] = {}

    def get_config(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def register_tool(
        self,
        name: str,
        toolset: str,
        schema: dict[str, Any],
        handler: Any,
        check_fn: Any = None,
        **kwargs: Any,
    ) -> None:
        self.tools[name] = {
            "toolset": toolset,
            "schema": schema,
            "handler": handler,
            "check_fn": check_fn,
            **kwargs,
        }

    def register_hook(self, hook_name: str, callback: Callable[..., Any], **kwargs: Any) -> None:
        self.hooks.setdefault(hook_name, []).append(callback)

    def register_command(
        self, name: str, handler: Any, description: str = "", args_hint: str = "", **kwargs: Any
    ) -> None:
        self.commands[name] = {
            "handler": handler,
            "description": description,
            "args_hint": args_hint,
        }

    def register_skill(self, name: str, path: Path, description: str = "", **kwargs: Any) -> None:
        self.skills[name] = Path(path)

    # ``dispatch_tool`` stand-in: records the call; ``dispatch_handler`` builds the reply
    # (by default a successful empty terminal result).
    dispatched: list[tuple[str, dict[str, Any]]]
    dispatch_handler: Callable[[str, dict[str, Any]], str] | None = None

    def dispatch_tool(self, tool_name: str, args: dict[str, Any], **kwargs: Any) -> str:
        if not hasattr(self, "dispatched"):
            self.dispatched = []
        self.dispatched.append((tool_name, args))
        if self.dispatch_handler is not None:
            return self.dispatch_handler(tool_name, args)
        return json.dumps({"output": "", "exit_code": 0, "error": None})

    def call(self, tool: str, **args: Any) -> dict[str, Any]:
        """Invoke a registered tool like Hermes does and decode its JSON result."""
        raw = self.tools[tool]["handler"](args, task_id="t-1", session_id="s-1")
        assert isinstance(raw, str), "handlers must return a JSON string"
        result: dict[str, Any] = json.loads(raw)
        return result


def make_context(**config: Any) -> FakeContext:
    import airflow_mlops

    ctx = FakeContext({"base_url": BASE_URL, **config})
    airflow_mlops.register(ctx)
    return ctx
