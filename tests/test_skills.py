"""Bundled skills: registered, files exist, frontmatter and sections follow the Hermes format."""

from __future__ import annotations

import re

import pytest
import yaml
from harness import ROOT, make_context

EXPECTED = ("airflow-triage", "airflow-report", "airflow-fix-proposal", "airflow-audit")


def _split(text: str) -> tuple[dict[str, object], str]:
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.DOTALL)
    assert match, "SKILL.md must start with a YAML frontmatter block"
    return yaml.safe_load(match.group(1)), match.group(2)


def test_all_skills_are_registered() -> None:
    ctx = make_context()
    assert sorted(ctx.skills) == sorted(EXPECTED)
    for path in ctx.skills.values():
        assert path.is_file()


@pytest.mark.parametrize("name", EXPECTED)
def test_skill_format(name: str) -> None:
    text = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8")
    front, body = _split(text)
    assert front["name"] == name
    assert isinstance(front["description"], str) and len(front["description"]) > 80
    hermes = front["metadata"]["hermes"]  # type: ignore[index]
    assert hermes["requires_toolsets"] == ["airflow"]
    assert set(hermes["tags"]) >= {"airflow"}
    for section in ("## When to Use", "## Procedure", "## Pitfalls", "## Verification"):
        assert section in body, f"{name}: missing {section}"


def test_skills_reference_only_existing_tools() -> None:
    ctx = make_context()
    future_tools = {"airflow_clear_tasks"}  # remediation, phase 4
    for name in EXPECTED:
        body = (ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8")
        for tool in set(re.findall(r"\bairflow_[a-z_]+(?=\()", body)):
            assert tool in ctx.tools or tool in future_tools, f"{name} references {tool}"


def test_triage_skill_states_the_minimal_retry_rule() -> None:
    body = (ROOT / "skills" / "airflow-triage" / "SKILL.md").read_text(encoding="utf-8")
    assert "Never re-run the whole DAG" in body
    assert "dry_run=true" in body


def test_fix_procedure_stops_after_the_sandbox_and_hands_over() -> None:
    """User request: report -> pick -> fix -> offer the sandbox -> stop; Hermes' own code
    tools then take over if the user wants the file changed (never blocked by the skill)."""
    body = (ROOT / "skills" / "airflow-fix-proposal" / "SKILL.md").read_text(encoding="utf-8")
    assert "Offer the sandbox test and wait for the answer" in body
    assert "This is where the procedure stops" in body
    assert "this skill does not restrict them" in body
    assert "even if a file or terminal tool is available" not in body
    assert "apply_commands.check" in body  # applying it yourself stays possible
    report = (ROOT / "skills" / "airflow-report" / "SKILL.md").read_text(encoding="utf-8")
    assert "Do not diagnose before the user has chosen" in report


def test_several_problems_are_presented_as_a_numbered_plan() -> None:
    """User request: several bugs -> numbered plan grouped by DAG, then ask where to start."""
    triage = (ROOT / "skills" / "airflow-triage" / "SKILL.md").read_text(encoding="utf-8")
    assert "## Several problems at once" in triage
    assert "1.1" in triage and "2.1" in triage
    assert "Which one should I start with?" in triage
    assert "Wait for the choice" in triage
    fix = (ROOT / "skills" / "airflow-fix-proposal" / "SKILL.md").read_text(encoding="utf-8")
    assert "numbered plan" in fix


def test_fix_display_uses_exact_locations_and_apply_commands() -> None:
    """User request: easy-to-find file/lines, a nice display, and the sandbox announced."""
    fix = (ROOT / "skills" / "airflow-fix-proposal" / "SKILL.md").read_text(encoding="utf-8")
    for needle in (
        "changes[].local_path",
        "block.before",
        "block.after",
        "apply_commands.apply",
        "your_files_modified",
    ):
        assert needle in fix, needle
    triage = (ROOT / "skills" / "airflow-triage" / "SKILL.md").read_text(encoding="utf-8")
    assert "offer to test it in a disposable" in triage
