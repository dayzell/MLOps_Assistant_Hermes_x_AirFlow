"""Guided audit (plan v2, step 1): dossier, numbering, typed selection and questions."""

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


# ---------------------------------------------------------------------------- dossier


def test_numbering_follows_the_dag_order_and_survives_a_reload() -> None:
    audit = planned_audit()
    assert [(i.number, i.task_id, i.kind) for i in audit.ordered_items()] == [
        ("1.1", "train", "code_fix"),
        ("1.2", None, "code_fix"),  # the trigger_rule that hides the failure
        ("2.1", "fetch", "retry"),
    ]
    assert audit.item("1.1").title == "KeyError: 'target'"  # type: ignore[union-attr]
    assert audit.progress()["line"] == "[-------] 0/3 done · next: 1.1"


def test_a_new_audit_supersedes_the_previous_one() -> None:
    first = planned_audit()
    second = wf.create_audit("all", "7d", [], None)
    current = wf.load_current()
    assert current is not None and current.id == second.id != first.id


def test_plan_text_is_grouped_by_dag() -> None:
    text = wf.plan_text(planned_audit())
    assert text.startswith("DAG 1 — demo_train_critical (latest run marked success")
    assert "  1.1 train — critical — KeyError: 'target'" in text
    assert "retry only, no code change" in text


def test_candidates_from_report_put_failing_and_masked_first() -> None:
    report = {
        "health": {
            "per_dag": [
                {"dag_id": "ok", "status": "healthy", "status_reason": "no incident"},
                {"dag_id": "flaky", "status": "degraded", "status_reason": "1 incident"},
                {"dag_id": "fetch", "status": "failing", "status_reason": "latest run failed"},
                {"dag_id": "train", "status": "failing", "status_reason": "masked"},
            ]
        },
        "incidents": {
            "failed_runs": [
                {"dag_id": "fetch", "run_id": "f1", "diagnosis": {"fetch": "transient"}},
                {"dag_id": "train", "run_id": "t1", "masked": True},
                {"dag_id": "flaky", "run_id": "x1"},
            ]
        },
    }
    cands = wf.candidates_from_report(report)
    assert [c["dag_id"] for c in cands] == ["train", "fetch", "flaky"]
    assert cands[1]["run_id"] == "f1" and cands[1]["category"] == "transient"


# ---------------------------------------------------------------------------- selection


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1.1", ["1.1"]),
        ("1.2, 2.1", ["1.2", "2.1"]),
        ("1.1-1.2", ["1.1", "1.2"]),
        ("DAG 2", ["2.1"]),
        ("2", ["2.1"]),
        ("demo_train_critical", ["1.1", "1.2"]),
        ("all", ["1.1", "1.2", "2.1"]),
        ("all critical", ["1.1", "1.2"]),
        ("all transient", ["2.1"]),
        ("all except 1.2", ["1.1", "2.1"]),
        ("1.1 and 2.1", ["1.1", "2.1"]),
    ],
)
def test_parse_selection(text: str, expected: list[str]) -> None:
    numbers, to_plan = wf.parse_selection(text, planned_audit())
    assert numbers == expected and to_plan == []


def test_selection_of_an_unplanned_dag_asks_to_plan_it() -> None:
    audit = planned_audit(both=False)
    assert wf.parse_selection("DAG 2", audit) == ([], ["demo_fetch_transient"])
    assert wf.parse_selection("all", audit) == (["1.1", "1.2"], ["demo_fetch_transient"])


@pytest.mark.parametrize("text", ["", "9.9", "DAG 7", "all purple", "1.1 ; ???"])
def test_bad_selection_is_explained(text: str) -> None:
    with pytest.raises(wf.SelectionError):
        wf.parse_selection(text, planned_audit())


# ---------------------------------------------------------------------------- questions


def check_question(question: dict[str, Any]) -> None:
    assert 2 <= len(question["choices"]) <= wf.MAX_CHOICES  # Hermes clarify limit
    assert question["recommended"] == question["choices"][0]
    assert [o["label"] for o in question["options"]] == question["choices"]
    assert all(o["then"] for o in question["options"])


def test_first_question_after_the_report() -> None:
    question = wf.question_after_report([dict(c) for c in CANDIDATES])
    assert question is not None
    check_question(question)
    assert question["question"] == "Which DAG should I diagnose?"
    assert question["choices"] == [
        "demo_train_critical",
        "demo_fetch_transient",
        "All (2)",
        "Stop here",
    ]
    assert 'dag_ids=["demo_train_critical"]' in question["options"][0]["then"]
    assert question["options"][0]["description"].startswith("recommended")
    assert wf.question_after_report([]) is None


def test_many_dags_are_grouped_to_fit_four_choices() -> None:
    many = [{**CANDIDATES[0], "dag_id": f"crit_{n}"} for n in range(3)] + [
        {**CANDIDATES[1], "dag_id": f"tr_{n}"} for n in range(5)
    ]
    question = wf.question_after_report(many)
    assert question is not None
    check_question(question)
    assert question["choices"] == ["crit_0", "All critical (3)", "All (8)", "Stop here"]
    assert "typed_answers" in question


def test_question_after_the_plan() -> None:
    question = wf.question_after_plan(planned_audit())
    assert question is not None
    check_question(question)
    assert question["choices"][0].startswith("1.1 train — KeyError")
    assert question["choices"][2] == "All of DAG 1 (2)"
    assert question["choices"][-1] == wf.STOP_LABEL


@pytest.mark.parametrize(
    ("state", "kind", "expected"),
    [
        ("fix_proposed", "code_fix", "Test fix 1.1 in the sandbox?"),
        ("sandbox_passed", "code_fix", "Fix 1.1 passed the sandbox test. Apply it to your file?"),
        ("sandbox_failed", "code_fix", "The sandbox test of fix 1.1 failed."),
    ],
)
def test_a_question_between_each_step(state: str, kind: str, expected: str) -> None:
    audit = planned_audit()
    item = audit.item("1.1")
    assert item is not None
    item.state = state
    step = wf.next_step(audit, item, sandbox_enabled=True)
    check_question(step["next_question"])
    assert step["next_question"]["question"] == expected
    assert step["next_question"]["choices"][-1] == wf.STOP_LABEL


def test_new_code_fix_starts_with_an_instruction_not_a_question() -> None:
    audit = planned_audit()
    step = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)
    assert "airflow_get_dag_source" in step["next_action"]
    assert "Do not edit any file" in step["next_action"]


def test_transient_item_asks_for_a_dry_run_retry() -> None:
    audit = planned_audit()
    step = wf.next_step(audit, audit.item("2.1"), sandbox_enabled=True)
    question = step["next_question"]
    check_question(question)
    assert question["choices"][0] == "Preview the retry (dry run)"
    assert "dry_run=true" in question["options"][0]["then"]


def test_sandbox_disabled_offers_no_test() -> None:
    audit = planned_audit()
    item = audit.item("1.1")
    assert item is not None
    item.state = "fix_proposed"
    question = wf.next_step(audit, item, sandbox_enabled=False)["next_question"]
    assert "Test it (copy, no network)" not in question["choices"]


def test_what_next_after_a_closed_item() -> None:
    """Same menu as after the diagnosis: the problems left, then Stop here."""
    audit = planned_audit()
    item = audit.item("1.1")
    assert item is not None
    item.state = "verified"
    question = wf.next_step(audit, None, sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["question"] == "What next? Problems left:"
    assert question["choices"] == [
        "1.2 run — run marked success: a final task's trigger_rule hides the failure",
        "2.1 fetch — ConnectionResetError: [Errno 104] Connection reset",
        "Stop here",
    ]


# ---------------------------------------------------------------------------- retry gate
# User request: no retry of a DAG while another error on the same DAG is still to fix;
# show the problems left with the same menu as after the diagnosis instead.


def applied(audit: wf.Audit, *numbers: str) -> None:
    for number in numbers:
        item = audit.item(number)
        assert item is not None
        item.state = "applied"


def two_crashes() -> wf.Audit:
    """demo_train_critical with two tasks that crash (1.1 train, 1.2 load) + the masking 1.3."""
    audit = wf.create_audit("all", "24h", [dict(c) for c in CANDIDATES], None)
    diagnosis = critical_diagnosis()
    diagnosis["root_failures"].append(root("load", "critical", "ValueError: bad schema"))
    audit.items += wf.items_from_diagnosis(audit, diagnosis)
    audit.items += wf.items_from_diagnosis(audit, transient_diagnosis())
    return audit


def test_no_retry_while_another_task_of_the_dag_crashes() -> None:
    audit = two_crashes()
    applied(audit, "1.1")  # 1.2 (load, same DAG) still crashes
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["question"] == (
        "Fix 1.1 is applied, but demo_train_critical still has 1 problem(s) to fix before a "
        "retry can work. Which one next?"
    )
    assert not any("retry" in c.lower() for c in question["choices"])
    assert question["choices"][0].startswith("1.2 load")
    assert question["options"][0]["description"] == (
        "recommended, same DAG: needed before the retry"
    )
    assert question["choices"][-1] == wf.STOP_LABEL


def test_the_masking_trigger_rule_does_not_block_the_retry() -> None:
    """User rule: only errors that make a task fail block the retry; 1.2 (all_done hiding the
    failure) stays on the list."""
    audit = planned_audit()
    applied(audit, "1.1")  # 1.2 is the masking item, still to do
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["choices"][0] == "Preview the retry of demo_train_critical (dry run)"
    assert question["choices"][1].startswith("1.2 run")  # still offered, not blocking


def test_retry_offered_once_the_dag_is_clean_with_the_other_problems() -> None:
    audit = planned_audit()
    applied(audit, "1.1", "1.2")
    question = wf.next_step(audit, audit.item("1.2"), sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["choices"][0] == "Preview the retry of demo_train_critical (dry run)"
    assert 'task_ids=["train"]' in question["options"][0]["then"]
    assert question["choices"][1].startswith("2.1 fetch")  # other DAG still listed
    assert question["choices"][-1] == wf.STOP_LABEL


def test_retry_when_nothing_else_is_left() -> None:
    audit = planned_audit(both=False)
    applied(audit, "1.1", "1.2")
    question = wf.next_step(audit, audit.item("1.1"), sandbox_enabled=True)["next_question"]
    assert question["question"] == (
        "Fix 1.1 is applied. Retry the failed tasks of demo_train_critical?"
    )
    assert question["choices"] == [
        "Preview the retry of demo_train_critical (dry run)",
        "Not now",
        "Stop here",
    ]


def test_transient_retry_waits_for_the_code_fixes_of_its_dag() -> None:
    audit = wf.create_audit("all", "24h", [dict(CANDIDATES[0])], None)
    diagnosis = critical_diagnosis()
    diagnosis["root_failures"].append(root("fetch", "transient", "ConnectionResetError: x"))
    audit.items += wf.items_from_diagnosis(audit, diagnosis)
    fetch = next(i for i in audit.items if i.task_id == "fetch")
    question = wf.next_step(audit, fetch, sandbox_enabled=True)["next_question"]
    check_question(question)
    assert question["question"].startswith(f"{fetch.number}: fetch can only be retried once")
    assert not any("retry" in c.lower() for c in question["choices"])


def test_what_next_puts_a_ready_retry_first() -> None:
    audit = planned_audit()
    applied(audit, "1.1", "1.2")
    question = wf.question_what_next(audit)
    assert question["choices"][0] == "Preview the retry of demo_train_critical (dry run)"
    assert question["choices"][1].startswith("2.1 fetch")


def test_everything_done_ends_with_the_summary() -> None:
    audit = planned_audit()
    for item in audit.items:
        item.state = "verified"
    step = wf.next_step(audit, None, sandbox_enabled=True)
    assert 'state="stop"' in step["next_action"]


# ---------------------------------------------------------------------------- summaries


def test_stop_summary_lists_what_is_left() -> None:
    audit = planned_audit(both=False)
    item = audit.item("1.1")
    assert item is not None
    item.state = "verified"
    text = wf.stop_summary(audit)
    assert "1/2 done" in text
    assert "Done: 1.1 (verified)" in text
    assert "Left: 1.2 run (new)" in text
    assert "Not diagnosed: demo_fetch_transient" in text
    assert "Resume any time with /airflow-audit." in text


def test_context_reminder_is_short_and_only_where_it_matters() -> None:
    audit = planned_audit()
    text = wf.context_summary(audit)
    assert text.startswith(f"[airflow-mlops audit #{audit.id} — open")
    assert "1.1 train (critical, code_fix) new" in text
    assert len(text) <= 1200
    now = audit.updated_at
    assert wf.context_is_relevant(audit, "s-1", now=now + 10_000)  # its own session
    assert wf.context_is_relevant(audit, "other", now=now + 60)  # right after activity
    assert not wf.context_is_relevant(audit, "other", now=now + 10_000)
    audit.status = wf.AUDIT_STOPPED
    assert not wf.context_is_relevant(audit, "s-1", now=now)
