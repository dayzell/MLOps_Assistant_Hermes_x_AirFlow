"""Hermes Agent plugin ``airflow-mlops``: Apache Airflow monitoring, diagnosis and reporting.

``register(ctx)`` only wires tools, the slash command and the bundled skills; it performs no
network call (the Airflow client is created lazily on the first tool call).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .audit_tools import AuditCommands, AuditTools
from .config import TOOLSET
from .dag_guard import DagWriteGuard
from .guardrails import GUARDED_TOOLS, make_pre_tool_call_hook
from .runtime import PluginRuntime
from .schemas import (
    AUDIT_TOOL_SCHEMAS,
    READ_TOOL_SCHEMAS,
    SANDBOX_TOOL_SCHEMAS,
    WRITE_TOOL_SCHEMAS,
)
from .tools import ReadTools, SandboxTools, WriteTools
from .watch import BLUEPRINT

__all__ = ["register"]

logger = logging.getLogger(__name__)

SKILLS_DIR = Path(__file__).resolve().parent / "skills"
SKILLS: dict[str, str] = {
    "airflow-triage": "Diagnose a failed Airflow run and propose the minimal remediation.",
    "airflow-report": "Standard six-section Airflow monitoring report, numbers never invented.",
    "airflow-fix-proposal": "Fix proposal for a critical Airflow failure (cause, diff, risks).",
    "airflow-audit": "Guided Airflow audit: report, numbered plan, a question between each step.",
    "airflow-dag-review": "Review a DAG before deployment: trigger rules, secrets, parse work.",
    "airflow-watch": "Scheduled watch: alerts only on new failures, hidden ones and recoveries.",
    "airflow-postmortem": "Incident postmortem from the audit dossier and the plugin journal.",
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
    sandbox.on_start = audits.observe_sandbox_start

    # Human approval for every real write unless the bounded auto-retry policy allows it,
    # and for sandbox tests above sandbox_max_level.
    guard = DagWriteGuard(
        runtime.settings, _session_cwd(ctx), audit_in_progress=audits.audit_in_progress
    )
    ctx.register_hook("pre_tool_call", make_pre_tool_call_hook(runtime.settings, guard))

    def post_tool_call(tool_name: str = "", args: Any = None, **kwargs: Any) -> None:
        """After Hermes wrote a DAG file: journal whether it is the tested change."""
        if tool_name in GUARDED_TOOLS and isinstance(args, dict):
            guard.after(tool_name, args, kwargs.get("result"))

    ctx.register_hook("post_tool_call", post_tool_call)

    ctx.register_command(
        "airflow-report",
        tools.report_command,
        description="Standard Airflow monitoring report (6 sections).",
        args_hint="[all|<dag_id>|tag:<tag>] [24h|7d]",
    )

    commands = AuditCommands(tools, audits)
    ctx.register_command(
        "airflow-audit",
        commands.audit,
        description="Guided Airflow audit: the report, then the first question.",
        args_hint="[all|<dag_id>|tag:<tag>] [24h|7d]",
    )
    ctx.register_command(
        "airflow-status",
        commands.status,
        description="Progress of the Airflow audit in progress (numbered plan).",
    )

    def pre_llm_call(**kwargs: Any) -> dict[str, str] | None:
        """Before each model reply: the audit dossier reminder."""
        context = audits.context(kwargs.get("session_id"))
        return {"context": context} if context else None

    ctx.register_hook("pre_llm_call", pre_llm_call)

    # A ready-made scheduled job in Hermes' gallery (/blueprint): the night watch. Optional:
    # a Hermes without this API (or with another signature) must not break the plugin.
    register_blueprint = getattr(ctx, "register_automation_blueprint", None)
    if callable(register_blueprint):
        spec = dict(BLUEPRINT)
        try:
            register_blueprint(spec.pop("key"), **spec)
        except Exception:
            logger.warning("airflow-mlops: night-watch blueprint not registered", exc_info=True)

    for name, description in SKILLS.items():
        ctx.register_skill(name, SKILLS_DIR / name / "SKILL.md", description=description)


def _session_cwd(ctx: Any) -> Any:
    """Working directory of the calling session (relative paths of Hermes' file tools)."""
    getter = getattr(ctx, "session_cwd", None)
    return getter if callable(getter) else None
