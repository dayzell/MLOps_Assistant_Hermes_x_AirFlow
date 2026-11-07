"""Bundled skills: registered, files exist, frontmatter and sections follow the Hermes format."""

from __future__ import annotations

import re

import pytest
import yaml
from harness import ROOT, make_context

EXPECTED = ("airflow-triage", "airflow-report", "airflow-fix-proposal")


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


def test_fix_proposal_never_applies_patches() -> None:
    body = (ROOT / "skills" / "airflow-fix-proposal" / "SKILL.md").read_text(encoding="utf-8")
    assert "you do not apply" in body
    assert "```diff" in body
