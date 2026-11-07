"""Hermes Agent plugin ``airflow-mlops``: Apache Airflow monitoring, diagnosis and reporting.

``register(ctx)`` only wires tools, the slash command and the bundled skills; it performs no
network call (the Airflow client is created lazily on the first tool call).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import TOOLSET
from .runtime import PluginRuntime
from .schemas import READ_TOOL_SCHEMAS
from .tools import ReadTools

__all__ = ["register"]

SKILLS_DIR = Path(__file__).resolve().parent / "skills"
SKILLS: dict[str, str] = {
    "airflow-triage": "Diagnose a failed Airflow run and propose the minimal remediation.",
    "airflow-report": "Standard six-section Airflow monitoring report, numbers never invented.",
    "airflow-fix-proposal": "Fix proposal for a critical Airflow failure (cause, diff, risks).",
}


def register(ctx: Any) -> None:
    """Hermes entry point: register the Airflow tools, the slash command and the skills."""
    runtime = PluginRuntime(ctx)
    tools = ReadTools(runtime)
    handlers = tools.handlers()
    for name, schema in READ_TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=handlers[name],
            description=schema["description"],
        )

    ctx.register_command(
        "airflow-report",
        tools.report_command,
        description="Standard Airflow monitoring report (6 sections).",
        args_hint="[all|<dag_id>|tag:<tag>] [24h|7d]",
    )

    for name, description in SKILLS.items():
        ctx.register_skill(name, SKILLS_DIR / name / "SKILL.md", description=description)
