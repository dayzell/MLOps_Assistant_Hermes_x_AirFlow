"""Hermes Agent plugin ``airflow-mlops``: Apache Airflow monitoring, diagnosis and reporting.

``register(ctx)`` only wires tools, the slash command and the bundled skills; it performs no
network call (the Airflow client is created lazily on the first tool call).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .audit_tools import AuditCommands, AuditTools
from .config import TOOLSET
from .guardrails import make_pre_tool_call_hook
from .runtime import PluginRuntime
from .schemas import (
    AUDIT_TOOL_SCHEMAS,
    READ_TOOL_SCHEMAS,
    SANDBOX_TOOL_SCHEMAS,
    WRITE_TOOL_SCHEMAS,
)
from .tools import ReadTools, SandboxTools, WriteTools
from .ui_check import UiCheck

__all__ = ["register"]

SKILLS_DIR = Path(__file__).resolve().parent / "skills"
SEPARATOR = "\n\n"
SKILLS: dict[str, str] = {
    "airflow-triage": "Diagnose a failed Airflow run and propose the minimal remediation.",
    "airflow-report": "Standard six-section Airflow monitoring report, numbers never invented.",
    "airflow-fix-proposal": "Fix proposal for a critical Airflow failure (cause, diff, risks).",
    "airflow-audit": "Guided Airflow audit: report, numbered plan, a question between each step.",
}


def register(ctx: Any) -> None:
    """Hermes entry point: register tools, the approval hook, the slash command and skills."""
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

    writes = WriteTools(runtime)
    write_handlers = writes.handlers()
    for name, schema in WRITE_TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=write_handlers[name],
            check_fn=writes.available,  # hidden while read_only is true
            description=schema["description"],
        )
    sandbox = SandboxTools(runtime)
    sandbox_handlers = sandbox.handlers()
    for name, schema in SANDBOX_TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=sandbox_handlers[name],
            check_fn=sandbox.available,  # hidden while sandbox_enabled is false
            description=schema["description"],
        )
    # Guided audit (plan v2): numbered plan, questions between steps, incident dossier.
    audits = AuditTools(runtime, tools)
    audit_handlers = audits.handlers()
    for name, schema in AUDIT_TOOL_SCHEMAS.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=audit_handlers[name],
            description=schema["description"],
        )
    writes.observer = audits.observe_clear
    sandbox.observer = audits.observe_sandbox

    # Human approval for every real write unless the bounded auto-retry policy allows it,
    # and for sandbox tests above sandbox_max_level.
    ctx.register_hook("pre_tool_call", make_pre_tool_call_hook(runtime.settings))

    ctx.register_command(
        "airflow-report",
        tools.report_command,
        description="Standard Airflow monitoring report (6 sections).",
        args_hint="[all|<dag_id>|tag:<tag>] [24h|7d]",
    )

    commands = AuditCommands(ctx, tools)
    ctx.register_command(
        "airflow-audit",
        commands.audit,
        description="Guided Airflow audit: report, then a question between each step.",
        args_hint="[all|<dag_id>|tag:<tag>] [24h|7d]",
    )
    ctx.register_command(
        "airflow-status",
        commands.status,
        description="Progress of the Airflow audit in progress (numbered plan).",
    )

    # Plan v2, step 0: check which Hermes UI features work on the user's surface.
    ui_check = UiCheck(ctx)
    ctx.register_command(
        "airflow-ui-check",
        ui_check.command,
        description="Check menus, progress bar and context on this Hermes surface.",
    )

    def pre_llm_call(**kwargs: Any) -> dict[str, str] | None:
        """Before each model reply: the audit dossier reminder (and the ui-check code)."""
        probe = ui_check.pre_llm_call(**kwargs) or {}
        parts = [p for p in (probe.get("context"), audits.context(kwargs.get("session_id"))) if p]
        return {"context": SEPARATOR.join(parts)} if parts else None

    ctx.register_hook("pre_llm_call", pre_llm_call)

    for name, description in SKILLS.items():
        ctx.register_skill(name, SKILLS_DIR / name / "SKILL.md", description=description)
