"""Guided audit, the simple rule (2026-11-09): report → diagnosis → test of each proposal →
the problems left or "Stop here"; an audit never changes code and is archived when left."""

from __future__ import annotations

from typing import Any

import pytest
from airflow_mlops import workflow as wf

CANDIDATES = [
    {
        "dag_id": "demo_train_critical",
        "status": "failing",
        "reason": "latest run marked success but a task failed",
        "run_id": "r1",
        "masked": True,
        "category": "critical",
        "incident": True,
    },
    {
        "dag_id": "demo_fetch_transient",
        "status": "failing",
        "reason": "latest run failed",
        "run_id": "r2",
        "masked": False,
        "category": "transient",
        "incident": True,
    },
]


def root(task: str, category: str, error: str) -> dict[str, Any]:
    return {
        "task_id": task,
        "classification": {"category": category, "matched_rules": [{"rule_id": "rule"}]},
        "error_block": f"Traceback (most recent call last):\n  File x\n{error}",
    }


def critical_diagnosis() -> dict[str, Any]:
    return {
        "dag_id": "demo_train_critical",
        "run_id": "r1",
        "root_failures": [root("train", "critical", "KeyError: 'target'")],
        "masked_failure": "The run is marked 'success' although tasks failed...",
    }


def transient_diagnosis() -> dict[str, Any]:
    return {
        "dag_id": "demo_fetch_transient",
        "run_id": "r2",
        "root_failures": [
            root("fetch", "transient", "ConnectionResetError: [Errno 104] Connection reset")
        ],
    }


def planned_audit(*, both: bool = True) -> wf.Audit:
    audit = wf.create_audit("all", "24h", [dict(c) for c in CANDIDATES], "s-1")
    audit.items += wf.items_from_diagnosis(audit, critical_diagnosis())
    if both:
        audit.items += wf.items_from_diagnosis(audit, transient_diagnosis())
    wf.save_audit(audit)
    loaded = wf.load_current()
    assert loaded is not None
    return loaded


def set_state(audit: wf.Audit, state: str, *numbers: str) -> None:
    for number in numbers:
        item = audit.item(number)
        assert item is not None
        item.state = state


def check_question(question: dict[str, Any]) -> None:
    assert 2 <= len(question["choices"]) <= wf.MAX_CHOICES  # Hermes clarify limit
    assert question["recommended"] == question["choices"][0]
    assert [o["label"] for o in question["options"]] == question["choices"]
    assert question["choices"][-1] == wf.STOP_LABEL


# ---------------------------------------------------------------------------- dossier


def test_numbering_and_reload() -> None:
    audit = planned_audit()
    assert [(i.number, i.task_id, i.kind) for i in audit.ordered_items()] == [
        ("1.1", "train", "code_fix"),
        ("1.2", None, "code_fix"),  # the trigger_rule that hides the failure
        ("2.1", "fetch", "retry"),
    ]
    assert audit.progress()["line"] == "[-------] 0/3 tested · next: 1.1"


def test_a_new_audit_archives_the_previous_one() -> None:
    first = planned_audit()
    second = wf.create_audit("all", "7d", [], None)
    current = wf.load_current()
    assert current is not None and current.id == second.id
    old = wf.load_audit(first.id)
    assert old is not None and old.status == wf.AUDIT_ARCHIVED  # no resume


def test_tested_fix_is_kept_with_the_item() -> None:
    audit = planned_audit()
    item = audit.item("1.1")
    assert item is not None
    item.state, item.result = "passed", {"level": "structure", "apply": "git -C d apply p"}
    wf.save_audit(audit)
    again = wf.load_current()
    assert again is not None and again.item("1.1").result["apply"] == "git -C d apply p"  # type: ignore[union-attr]


def test_plan_text_says_how_each_problem_is_tested() -> None:
    text = wf.plan_text(planned_audit())
    assert text.startswith("DAG 1 — demo_train_critical (latest run marked success")
    assert "test: a fix in the sandbox (copy, no code change)" in text
    assert "test: a real retry, after your approval" in text


# ---------------------------------------------------------------------------- selection


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1.1", ["1.1"]),
        ("1.2, 2.1", ["1.2", "2.1"]),
        ("DAG 2", ["2.1"]),
        ("all", ["1.1", "1.2", "2.1"]),
        ("all critical", ["1.1", "1.2"]),
        ("all except 1.2", ["1.1", "2.1"]),
    ],
)
def test_parse_selection(text: str, expected: list[str]) -> None:
    numbers, to_plan = wf.parse_selection(text, planned_audit())
    assert numbers == expected and to_plan == []


def test_tested_problems_are_not_selected_again() -> None:
    audit = planned_audit()
    set_state(audit, "passed", "1.1")
    assert wf.parse_selection("all", audit)[0] == ["1.2", "2.1"]


@pytest.mark.parametrize("text", ["", "9.9", "DAG 7", "all purple"])
def test_bad_selection_is_explained(text: str) -> None:
    with pytest.raises(wf.SelectionError):
        wf.parse_selection(text, planned_audit())


# ---------------------------------------------------------------------------- steps


def test_first_question_carries_the_state_of_play() -> None:
    question = wf.question_after_report(
        [dict(c) for c in CANDIDATES], "🔴 failing: 2 of 3 DAG(s) failing"
    )
    assert question is not None
    check_question(question)
    assert question["question"] == "🔴 failing: 2 of 3 DAG(s) failing\nWhich DAG should I diagnose?"
    assert question["choices"] == [
        "demo_train_critical",
        "demo_fetch_transient",
        "All (2)",
        "Stop here",
    ]
    assert wf.question_after_report([]) is None


def test_a_code_error_is_proposed_and_tested_never_edited() -> None:
    audit = planned_audit()
    step = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)
    action = step["next_action"]
    assert "airflow_get_dag_source" in action and "airflow_sandbox_test" in action
    assert 'level="structure"' in action
    assert "Never edit a file during an audit" in action


def test_without_sandbox_the_fix_is_shown_and_skipped() -> None:
    audit = planned_audit()
    action = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=False)["next_action"]
    assert 'state="skipped"' in action and "not tested (sandbox disabled)" in action


def test_a_transient_failure_is_tested_by_a_real_retry() -> None:
    audit = planned_audit()
    question = wf.next_step(audit, audit.item("2.1"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["choices"][0] == "Preview the retry (dry run)"
    assert "dry_run=true" in question["options"][0]["then"]
    set_state(audit, "retried", "2.1")
    action = wf.next_step(audit, audit.item("2.1"), sandbox_enabled=True)["next_action"]
    assert "airflow_get_run_status" in action and 'state="passed"' in action


def test_no_retry_on_a_dag_that_has_a_code_error() -> None:
    """User rule: never run a DAG with a code error; and no code change during an audit."""
    audit = wf.create_audit("all", "24h", [dict(CANDIDATES[0])], None)
    diagnosis = critical_diagnosis()
    diagnosis["root_failures"].append(root("fetch", "transient", "ConnectionResetError: x"))
    audit.items += wf.items_from_diagnosis(audit, diagnosis)
    fetch = next(i for i in audit.items if i.task_id == "fetch")
    question = wf.next_step(audit, fetch, sandbox_enabled=True)["next_question"]
    check_question(question)
    assert "cannot be retried during the audit" in question["question"]
    assert not any("retry" in c.lower() for c in question["choices"])


def test_after_a_passing_test_the_problems_left_are_offered() -> None:
    """User request: a test works and problems are left → offer them, or Stop here."""
    audit = planned_audit()
    set_state(audit, "passed", "1.1")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["question"].startswith("✅ 1.1 works")
    assert question["choices"][0].startswith("1.2 run")
    assert question["choices"][1].startswith("2.1 fetch")


def test_everything_tested_ends_the_audit() -> None:
    audit = planned_audit()
    set_state(audit, "passed", "1.1", "1.2", "2.1")
    step = wf.next_step(audit, audit.item("2.1"), sandbox_enabled=True)
    assert step["finished"] is True and 'state="stop"' in step["next_action"]


def test_a_failed_test_offers_revise_next_or_stop() -> None:
    audit = planned_audit()
    set_state(audit, "failed", "1.1")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["question"] == "❌ 1.1: the sandbox test failed. What now?"
    assert question["choices"][0] == "Revise the fix"
    assert question["choices"][1].startswith("Next problem: 1.2")


def test_a_failed_retry_cannot_be_revised_in_the_audit() -> None:
    audit = planned_audit()
    set_state(audit, "failed", "2.1")
    question = wf.next_step(audit, audit.item("2.1"), sandbox_enabled=True)["next_question"]
    assert "probably not transient" in question["question"]
    assert "Revise the fix" not in question["choices"]


def test_masking_fix_asks_first() -> None:
    audit = planned_audit()
    set_state(audit, "passed", "1.1")
    warning = {"file": "d.py", "line": 5, "why": "a missing key no longer fails"}
    question = wf.question_masking(audit, audit.item("1.1"), [warning])  # type: ignore[arg-type]
    check_question(question)
    assert "may hide the error" in question["question"] and "d.py:5" in question["question"]
    assert question["choices"][:2] == ["Revise the fix", "Keep it and go on"]


# ---------------------------------------------------------------------------- summaries


def test_archive_summary_lists_results_and_tested_patches() -> None:
    audit = planned_audit()
    item = audit.item("1.1")
    assert item is not None
    item.state, item.result = "passed", {"level": "structure", "apply": "git -C d apply x.patch"}
    set_state(audit, "failed", "1.2")
    audit.status = wf.AUDIT_ARCHIVED
    text = wf.stop_summary(audit)
    assert text.startswith(f"🗂️ Audit #{audit.id} archived.")
    assert "✅ 1.1 train — KeyError: 'target' — fix tested in the sandbox" in text
    assert "apply it on request: git -C d apply x.patch" in text
    assert "❌ 1.2" in text and "• 2.1" in text
    assert "No code was changed during the audit" in text


def test_context_open_vs_archived() -> None:
    audit = planned_audit()
    open_text = wf.context_summary(audit)
    assert "in progress" in open_text and "no code change during an audit" in open_text
    assert "never overrides an explicit request" in open_text
    now = audit.updated_at
    # an open audit reminds its own conversation only (not another chat, not a scheduled job)
    assert wf.context_is_relevant(audit, "s-1", now=now + 60)
    assert not wf.context_is_relevant(audit, "other", now=now + 60)
    audit.status = wf.AUDIT_ARCHIVED
    archived_text = wf.context_summary(audit)
    assert "archived" in archived_text and "tested patch" in archived_text
    # an archived audit only informs its own conversation
    assert wf.context_is_relevant(audit, "s-1", now=now + 60)
    assert not wf.context_is_relevant(audit, "other", now=now + 60)
    assert not wf.context_is_relevant(audit, "s-1", now=now + 4 * 3600)


# ---------------------------------------------------------------------------- after the audit


def passed_fix(audit: wf.Audit, number: str, apply: str) -> None:
    item = audit.item(number)
    assert item is not None
    item.state, item.result = "passed", {"level": "structure", "apply": apply}


def test_leaving_with_tested_fixes_offers_to_apply_them() -> None:
    """User request: at the final 'Stop here', offer to ask Hermes to apply the tested fixes."""
    audit = planned_audit()
    passed_fix(audit, "1.1", "git -C d apply a.patch")
    passed_fix(audit, "1.2", "git -C d apply a.patch")  # tested together: the same patch
    audit.status = wf.AUDIT_ARCHIVED
    question = wf.question_apply(audit)
    assert question is not None
    assert question["question"] == (
        f"✅ Audit #{audit.id} archived. Ask Hermes to apply the tested fixes 1.1 + 1.2 now?"
    )
    assert question["choices"] == ["Apply 1.1 + 1.2 (your approval required)", "Not now"]
    then = question["options"][0]["then"]
    assert then.count("git -C d apply a.patch") == 1  # one patch, applied once
    assert "lock" in then and "dry run first" in then
    assert question["options"][1]["kind"] == "done"


def test_no_apply_question_without_a_tested_fix() -> None:
    audit = planned_audit()
    set_state(audit, "passed", "2.1")  # a retry that worked: nothing to apply
    set_state(audit, "failed", "1.1")
    assert wf.question_apply(audit) is None
