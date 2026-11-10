"""Display: emoji in the answers (the user's choice); never a terminal escape code, because a
slash command cannot tell the Desktop app from a terminal (both are the surface "local")."""

from __future__ import annotations

import respx
from airflow_mlops.display import STATUS_ICONS
from harness import FakeContext
from test_reporting import mock_airflow


def test_status_icons() -> None:
    assert STATUS_ICONS["failing"] == "🔴"
    assert STATUS_ICONS["no_data"] == "⚪"


def test_no_escape_code_in_any_command_or_report(
    api_version: str, airflow: respx.MockRouter, ctx: FakeContext
) -> None:
    mock_airflow(airflow, api_version)
    for platform in ("local", "cli", "telegram"):
        report = ctx.commands["airflow-report"]["handler"]("all 24h", platform=platform)
        audit = ctx.commands["airflow-audit"]["handler"]("all 24h", platform=platform)
        assert "🔴 failing" in report
        assert "\x1b[" not in report and "\x1b[" not in audit
    assert "\x1b[" not in ctx.call("airflow_build_report", period="24h")["markdown"]
