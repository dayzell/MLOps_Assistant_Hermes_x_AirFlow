"""Hermes Agent plugin ``airflow-mlops``: Apache Airflow monitoring, diagnosis and reporting.

``register(ctx)`` only wires tools; it performs no network call (the Airflow client is
created lazily on the first tool call).
"""

from __future__ import annotations

from typing import Any

from .config import TOOLSET
from .runtime import PluginRuntime
from .schemas import READ_TOOL_SCHEMAS
from .tools import ReadTools

__all__ = ["register"]


def register(ctx: Any) -> None:
    """Hermes entry point: register the Airflow tools."""
    runtime = PluginRuntime(ctx)
    handlers = ReadTools(runtime).handlers()
    for name, schema in READ_TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=handlers[name],
            description=schema["description"],
        )
