"""Guided audit (plan v2, step 1): the ``airflow_audit_*`` tools, ``/airflow-audit``,
``/airflow-status`` and the dossier reminder given to the model before each reply.

The dossier lives in ``plugin_db`` (see :mod:`workflow`). The plugin decides every question;
the model displays it with Hermes' ``clarify`` tool and reports each step back.
"""

from __future__ import annotations

import logging
from typing import Any

from . import workflow as wf
from .diagnosis import RunDiagnoser, load_rules
from .runtime import PluginRuntime
from .tools import (
    Handler,
    ReadTools,
    ToolInputError,
    _check_allowed,
    _context_str,
    _optional_str_list,
    ok,
    parse_command_args,
    safe_handler,
)

logger = logging.getLogger(__name__)

MAX_DAGS_PER_PLAN = 10
_RUNS_SCANNED = 10


class AuditTools:
    """``airflow_audit_plan`` / ``airflow_audit_select`` / ``airflow_audit_progress``."""

    def __init__(self, runtime: PluginRuntime, reads: ReadTools) -> None:
        self.runtime = runtime
        self.reads = reads

    def handlers(self) -> dict[str, Handler]:
        return {
            "airflow_audit_plan": safe_handler(self.plan, context_aware=True),
            "airflow_audit_select": safe_handler(self.select, context_aware=True),
            "airflow_audit_progress": safe_handler(self.progress, context_aware=True),
        }

    # ------------------------------------------------------------------ tools

    def plan(self, args: dict[str, Any], **context: Any) -> str:
        dag_ids = _optional_str_list(args, "dag_ids")
        session_id = _context_str(context, "session_id")
        audit = wf.load_current()
        if audit is None:
            audit = wf.create_audit("selected", self._period(), [], session_id)
        audit.status, audit.session_id = wf.AUDIT_OPEN, session_id or audit.session_id
        if not dag_ids:
            if not audit.items and not audit.candidates:
                raise ToolInputError(
                    "'dag_ids' is required (no audit in progress): list the DAGs to diagnose, "
                    "or build a report first."
                )
            if not audit.items:  # a report was made, nothing diagnosed yet
                wf.save_audit(audit)
                return ok(
                    **audit.to_dict(),
                    candidates=audit.candidates,
                    next_question=wf.question_after_report(audit.unplanned_candidates()),
                )
            wf.save_audit(audit)
            return ok(**self._state(audit, wf.question_after_plan(audit)))
        notes = self._plan_dags(audit, dag_ids)
        audit.pending = None
        wf.save_audit(audit)
        return ok(**self._state(audit, wf.question_after_plan(audit)), notes=notes)

    def select(self, args: dict[str, Any], **context: Any) -> str:
        selection = args.get("selection")
        if not isinstance(selection, str) or not selection.strip():
            raise ToolInputError("'selection' is required, e.g. '1.1, 2.3', 'DAG 2' or 'all'.")
        audit = self._require_audit()
        try:
            numbers, to_plan = wf.parse_selection(selection, audit)
        except wf.SelectionError as exc:
            raise ToolInputError(str(exc)) from exc
        notes: list[str] = []
        if to_plan:
            notes = self._plan_dags(audit, to_plan)
            numbers += [i.number for d in to_plan for i in audit.items_of(d) if i.is_open]
        if not numbers:
            raise ToolInputError(f"Nothing left to handle in {selection!r}. " + " ".join(notes))
        chosen = set(numbers)
        for entry in audit.items:
            entry.selected = entry.number in chosen
        audit.current = numbers[0]
        audit.status, audit.pending = wf.AUDIT_OPEN, None
        session_id = _context_str(context, "session_id")
        audit.session_id = session_id or audit.session_id
        wf.save_audit(audit)
        item = audit.item(numbers[0])
        return ok(
            selected=numbers,
            current=item.to_dict() if item else None,
            queue=[i.to_dict() for i in audit.ordered_items() if i.number in numbers[1:]],
            progress=audit.progress(),
            notes=notes,
            **wf.next_step(audit, item, sandbox_enabled=self._sandbox_enabled()),
        )

    def progress(self, args: dict[str, Any], **context: Any) -> str:
        state = args.get("state")
        if state == "stop":
            audit = self._require_audit()
            audit.status, audit.pending = wf.AUDIT_STOPPED, None
            wf.save_audit(audit)
            return ok(stopped=True, summary=wf.stop_summary(audit), progress=audit.progress())
        if state not in wf.PROGRESS_STATES:
            raise ToolInputError(f"'state' must be one of {sorted(wf.PROGRESS_STATES)} or 'stop'.")
        number = args.get("item")
        if not isinstance(number, str) or not number.strip():
            raise ToolInputError("'item' is required (e.g. '1.2').")
        audit = self._require_audit()
        item = audit.item(number.strip())
        if item is None:
            raise ToolInputError(f"No item {number!r} in audit #{audit.id}.")
        note = args.get("note")
        item.state = str(state)
        if isinstance(note, str) and note.strip():
            item.note = note.strip()
        item.updated_at = self.runtime.now().timestamp()
        if state == "verified":  # one run check verifies every item retried with it
            for other in audit.items:
                if other.dag_id == item.dag_id and other.state == "retried":
                    other.state, other.updated_at = "verified", item.updated_at
        audit.status = wf.AUDIT_OPEN
        follow: wf.Item | None = item
        if not item.is_open:
            following = audit.open_items()
            audit.current = following[0].number if following and following[0].selected else None
            follow = None  # ask "What next?"
        else:
            audit.current = item.number
        wf.save_audit(audit)
        return ok(
            item=item.to_dict(),
            progress=audit.progress(),
            **wf.next_step(audit, follow, sandbox_enabled=self._sandbox_enabled()),
        )

    # ------------------------------------------------------------------ observers

    def observe_sandbox(
        self, dag_id: str, task_id: str | None, level: str, result: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Record a sandbox test on the matching code-fix item; never raises."""
        try:
            audit = wf.load_current()
            if audit is None or audit.status != wf.AUDIT_OPEN:
                return None
            item = self._code_item(audit, dag_id, task_id)
            if item is None:
                return None
            passed = bool(result.get("passed"))
            if passed and level == "import":
                item.note = "sandbox: import passed"
                wf.save_audit(audit)
                return {
                    "item": item.number,
                    "state": item.state,
                    "next_action": "Import passed: now test level='structure'.",
                }
            item.state = "sandbox_passed" if passed else "sandbox_failed"
            item.note = f"sandbox {level}: {'passed' if passed else 'failed'}"
            item.updated_at = self.runtime.now().timestamp()
            audit.current = item.number
            wf.save_audit(audit)
            return {
                "item": item.number,
                "state": item.state,
                **wf.next_step(audit, item, sandbox_enabled=True),
            }
        except Exception:
            logger.warning("airflow-mlops: could not record the sandbox test", exc_info=True)
            return None

    def observe_clear(
        self, dag_id: str, task_ids: list[str], result: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Mark the matching items as retried after a real clear; never raises."""
        try:
            if result.get("outcome") != "cleared":
                return None
            audit = wf.load_current()
            if audit is None or audit.status != wf.AUDIT_OPEN:
                return None
            items = [
                i
                for i in audit.items
                if i.dag_id == dag_id
                and i.is_open
                and (i.task_id in task_ids or (i.task_id is None and i.state == "applied"))
            ]
            if not items:
                return None
            for item in items:
                item.state = "retried"
                item.updated_at = self.runtime.now().timestamp()
            audit.current = items[0].number
            wf.save_audit(audit)
            return {
                "items": [i.number for i in items],
                "state": "retried",
                **wf.next_step(audit, items[0], sandbox_enabled=self._sandbox_enabled()),
            }
        except Exception:
            logger.warning("airflow-mlops: could not record the retry", exc_info=True)
            return None

    def context(self, session_id: str | None) -> str | None:
        """Dossier reminder for ``pre_llm_call``; never raises."""
        try:
            audit = wf.load_current()
            if audit is None or not wf.context_is_relevant(audit, session_id):
                return None
            return wf.context_summary(audit)
        except Exception:
            logger.warning("airflow-mlops: could not read the audit", exc_info=True)
            return None

    # ------------------------------------------------------------------ helpers

    def _plan_dags(self, audit: wf.Audit, dag_ids: list[str]) -> list[str]:
        settings = self.runtime.settings()
        if len(dag_ids) > MAX_DAGS_PER_PLAN:
            raise ToolInputError(f"At most {MAX_DAGS_PER_PLAN} DAGs per call.")
        client = self.runtime.client(settings)
        diagnoser = RunDiagnoser(client, settings, load_rules(settings.error_patterns_path))
        notes = []
        for dag_id in dict.fromkeys(dag_ids):
            _check_allowed(settings, dag_id)
            if audit.items_of(dag_id):
                continue  # already planned: keep its numbers
            run_id = self._run_to_diagnose(audit, client, dag_id)
            if run_id is None:
                audit.dag_index(dag_id)
                notes.append(f"{dag_id}: no run to diagnose.")
                continue
            diagnosis = diagnoser.diagnose(dag_id, run_id)
            items = wf.items_from_diagnosis(audit, diagnosis)
            if not items:
                notes.append(f"{dag_id}: no failure in run {run_id}.")
            audit.items += items
        return notes

    @staticmethod
    def _run_to_diagnose(audit: wf.Audit, client: Any, dag_id: str) -> str | None:
        cand = next((c for c in audit.candidates if c["dag_id"] == dag_id), None)
        if cand and cand.get("run_id"):
            return str(cand["run_id"])
        runs = client.list_dag_runs(dag_id, limit=_RUNS_SCANNED)
        failed = next((r for r in runs if r.get("state") == "failed"), None)
        chosen = failed or (runs[0] if runs else None)
        return str(chosen["run_id"]) if chosen else None

    @staticmethod
    def _code_item(audit: wf.Audit, dag_id: str, task_id: str | None) -> wf.Item | None:
        current = audit.item(audit.current) if audit.current else None
        if current and current.dag_id == dag_id and current.kind != "retry" and current.is_open:
            return current
        candidates = [
            i
            for i in audit.ordered_items()
            if i.dag_id == dag_id and i.kind != "retry" and i.is_open
        ]
        exact = [i for i in candidates if task_id and i.task_id == task_id]
        found = exact or candidates
        return found[0] if found else None

    def _require_audit(self) -> wf.Audit:
        audit = wf.load_current()
        if audit is None:
            raise ToolInputError("No audit in progress: call airflow_audit_plan with dag_ids.")
        return audit

    def _state(self, audit: wf.Audit, question: dict[str, Any] | None) -> dict[str, Any]:
        state = {**audit.to_dict(), "plan_text": wf.plan_text(audit)}
        if question:
            state["next_question"] = question
        elif audit.unplanned_candidates() or audit.items:
            state["next_question"] = wf.question_what_next(audit)
        return state

    def _period(self) -> str:
        return self.runtime.settings().report_default_period

    def _sandbox_enabled(self) -> bool:
        try:
            return self.runtime.settings().sandbox_enabled
        except Exception:
            return False


class AuditCommands:
    """``/airflow-audit [scope] [period]`` and ``/airflow-status``."""

    def __init__(self, ctx: Any, reads: ReadTools) -> None:
        self._ctx = ctx
        self.reads = reads

    def audit(self, raw_args: str = "", **kwargs: Any) -> str:
        session_id = _session(kwargs)
        current = wf.load_current()
        if current is not None and not (raw_args or "").strip() and _resumable(current):
            return self._resume(current, session_id, kwargs)
        scope, period = parse_command_args(raw_args)
        try:
            report = self.reads._report(scope, period, True)
        except Exception as exc:  # report errors become plain text, like /airflow-report
            message = getattr(exc, "message", None) or str(exc)
            return f"❌ /airflow-audit: {message}"
        candidates = wf.candidates_from_report(report["report"])
        audit = wf.create_audit(
            scope, report["report"]["scope_period"]["period"], candidates, session_id
        )
        markdown = str(report["markdown"])
        posted = self._post(markdown)
        head = "" if posted else markdown + "\n"
        question = wf.question_after_report(candidates)
        if question is None:
            audit.status = wf.AUDIT_DONE
            wf.save_audit(audit)
            return head + "No incident in this period: nothing to diagnose."
        return head + self._ask_and_hand_over(audit, question, kwargs)

    def status(self, raw_args: str = "", **kwargs: Any) -> str:
        audit = wf.load_current()
        if audit is None:
            return "No audit in progress. Start one with /airflow-audit."
        lines = [
            f"Audit #{audit.id} — {audit.status} — scope {audit.scope} {audit.period}",
            audit.progress()["line"],
        ]
        if audit.items:
            lines += ["", wf.plan_text(audit)]
        unplanned = audit.unplanned_candidates()
        if unplanned:
            lines += ["", "Not diagnosed yet: " + ", ".join(c["dag_id"] for c in unplanned)]
        return "\n".join(lines)

    # ------------------------------------------------------------------ helpers

    def _resume(self, audit: wf.Audit, session_id: str | None, kwargs: dict[str, Any]) -> str:
        left = len(audit.open_items()) + len(audit.unplanned_candidates())
        question = wf.make_question(
            f"Resume audit #{audit.id} ({left} left)?",
            [
                wf.option(
                    f"Resume ({left} left)",
                    "Call airflow_audit_plan with no arguments and ask the question it returns.",
                    audit.progress()["line"],
                ),
                wf.option("New audit", "NEW"),
                wf.option(wf.STOP_LABEL, wf.STOP_THEN),
            ],
        )
        picked = self._ask(question)
        if picked is not None and picked["then"] == "NEW":
            audit.status = wf.AUDIT_SUPERSEDED
            wf.save_audit(audit)
            return self.audit("all", **kwargs)
        audit.session_id = session_id or audit.session_id
        if picked is None:
            audit.status = wf.AUDIT_OPEN
            wf.save_audit(audit)
            return (
                self.status() + "\n\nReply 'resume' to continue this audit, or run "
                "/airflow-audit all 24h for a new one."
            )
        if picked["then"] == wf.STOP_THEN:
            audit.status = wf.AUDIT_STOPPED
            wf.save_audit(audit)
            return wf.stop_summary(audit)
        audit.status, audit.pending = (
            wf.AUDIT_OPEN,
            {"label": picked["label"], "then": picked["then"]},
        )
        wf.save_audit(audit)
        return self.status() + "\n\n" + self._hand_over(picked, kwargs)

    def _ask_and_hand_over(
        self, audit: wf.Audit, question: dict[str, Any], kwargs: dict[str, Any]
    ) -> str:
        picked = self._ask(question)
        if picked is None:  # no menu on this surface: the model gets the question as context
            audit.pending = {"label": "(not answered yet)", "then": question["question"]}
            wf.save_audit(audit)
            return text_question(question)
        if picked["then"] == wf.STOP_THEN:
            audit.status = wf.AUDIT_STOPPED
            wf.save_audit(audit)
            return wf.stop_summary(audit)
        audit.pending = {"label": picked["label"], "then": picked["then"]}
        wf.save_audit(audit)
        return self._hand_over(picked, kwargs)

    def _hand_over(self, picked: dict[str, str], kwargs: dict[str, Any]) -> str:
        message = (
            f"[airflow-mlops] In the audit menu the user chose: {picked['label']}. "
            f"{picked['then']} Then follow the airflow-mlops:airflow-audit skill."
        )
        if self._inject(message, kwargs):
            return f"➡️ {picked['label']}: starting."
        return (
            f"Choice saved: {picked['label']}. Send any message (e.g. 'go') and I will "
            "continue from there."
        )

    def _ask(self, question: dict[str, Any]) -> dict[str, str] | None:
        ask = getattr(self._ctx, "ask_choice", None)
        if ask is None:
            return None
        choices = [
            {
                "value": o["label"],
                "label": o["label"],
                **({"description": o["description"]} if o.get("description") else {}),
            }
            for o in question["options"]
        ]
        try:
            answer = ask(question["question"], choices)
        except Exception:
            logger.warning("airflow-mlops: ask_choice failed", exc_info=True)
            return None
        return next((o for o in question["options"] if o["label"] == answer), None)

    def _post(self, text: str) -> bool:
        post = getattr(self._ctx, "post_message", None)
        if post is None:
            return False
        try:
            return bool(getattr(post(text), "ok", False))
        except Exception:
            logger.warning("airflow-mlops: post_message failed", exc_info=True)
            return False

    def _inject(self, message: str, kwargs: dict[str, Any]) -> bool:
        inject = getattr(self._ctx, "inject_message", None)
        if inject is None:
            return False
        key = kwargs.get("session_key") or None
        try:
            return bool(inject(message, session_key=key) if key else inject(message))
        except Exception:
            logger.warning("airflow-mlops: inject_message failed", exc_info=True)
            return False


def text_question(question: dict[str, Any]) -> str:
    """The question as numbered text, for surfaces without a menu."""
    lines = [question["question"]]
    for number, opt in enumerate(question["options"], start=1):
        extra = f" ({opt['description']})" if opt.get("description") else ""
        lines.append(f"  {number}. {opt['label']}{extra}")
    lines.append("Reply with a number or a name.")
    return "\n".join(lines)


def _session(kwargs: dict[str, Any]) -> str | None:
    value = kwargs.get("session_id") or kwargs.get("session_key")
    return str(value) if value else None


def _resumable(audit: wf.Audit) -> bool:
    return bool(audit.open_items() or audit.unplanned_candidates())
