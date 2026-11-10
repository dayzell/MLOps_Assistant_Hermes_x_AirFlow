"""Guided audit: the ``airflow_audit_*`` tools, ``/airflow-audit``, ``/airflow-status`` and the
dossier reminder given to the model before each reply.

Rule: an audit never changes code. Report → diagnosis → test of each proposal (a real retry
for a transient failure, a sandbox test for a code fix) → leaving the audit archives it.
The dossier lives in ``plugin_db`` (see :mod:`workflow`). The plugin decides every question;
the model writes it at the end of its reply (numbered text) and reports each step back.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime
from typing import Any

from . import storage
from . import workflow as wf
from .diagnosis import RunDiagnoser, load_rules
from .display import FAIL, STATUS_ICONS
from .runtime import PluginRuntime
from .tools import (
    Handler,
    ReadTools,
    ToolInputError,
    _check_allowed,
    _context_str,
    _optional_int,
    _optional_str_list,
    ok,
    parse_command_args,
    safe_handler,
)

logger = logging.getLogger(__name__)

MAX_DAGS_PER_PLAN = 10
_RUNS_SCANNED = 10


class AuditTools:
    """``airflow_audit_plan`` / ``_select`` / ``_progress`` / ``_timeline``."""

    def __init__(self, runtime: PluginRuntime, reads: ReadTools) -> None:
        self.runtime = runtime
        self.reads = reads
        self.board = ProgressBoard(runtime.ctx)

    def handlers(self) -> dict[str, Handler]:
        return {
            "airflow_audit_plan": safe_handler(self.plan, context_aware=True),
            "airflow_audit_select": safe_handler(self.select, context_aware=True),
            "airflow_audit_progress": safe_handler(self.progress, context_aware=True),
            "airflow_audit_timeline": safe_handler(self.timeline),
        }

    # ------------------------------------------------------------------ tools

    def plan(self, args: dict[str, Any], **context: Any) -> str:
        dag_ids = _optional_str_list(args, "dag_ids")
        session_id = _context_str(context, "session_id")
        audit = wf.load_current()
        if not dag_ids:
            if audit is None:
                raise ToolInputError(
                    "No audit in progress. Start one with /airflow-audit (report first), or "
                    "give dag_ids to diagnose."
                )
            if not audit.items:  # report made, nothing diagnosed yet
                return ok(
                    **audit.to_dict(),
                    candidates=audit.candidates,
                    next_question=wf.question_after_report(audit.unplanned_candidates()),
                )
            return ok(**self._state(audit, wf.question_after_plan(audit)))
        if audit is None:
            audit = wf.create_audit("selected", self._period(), [], session_id)
        audit.session_id = session_id or audit.session_id
        notes = self._plan_dags(audit, dag_ids)
        audit.pending = None
        wf.save_audit(audit)
        return ok(**self._state(audit, wf.question_after_plan(audit)), notes=notes)

    def select(self, args: dict[str, Any], **context: Any) -> str:
        selection = args.get("selection")
        if not isinstance(selection, str) or not selection.strip():
            raise ToolInputError("'selection' is required, e.g. '1.1, 2.3', 'DAG 2' or 'all'.")
        audit = self._require_audit()
        return ok(**self._select_core(audit, selection, _context_str(context, "session_id")))

    def _select_core(
        self, audit: wf.Audit, selection: str, session_id: str | None
    ) -> dict[str, Any]:
        try:
            numbers, to_plan = wf.parse_selection(selection, audit)
        except wf.SelectionError as exc:
            raise ToolInputError(str(exc)) from exc
        notes: list[str] = []
        if to_plan:
            notes = self._plan_dags(audit, to_plan)
            numbers += [i.number for d in to_plan for i in audit.items_of(d) if i.is_open]
        numbers = [n for n in numbers if (i := audit.item(n)) is not None and i.is_open]
        if not numbers:
            raise ToolInputError(f"Nothing left to test in {selection!r}. " + " ".join(notes))
        chosen = set(numbers)
        for entry in audit.items:
            entry.selected = entry.number in chosen
        audit.current = numbers[0]
        audit.pending = None
        audit.session_id = session_id or audit.session_id
        wf.save_audit(audit)
        item = audit.item(numbers[0])
        if len(numbers) > 1:
            self.board.show(audit, f"starting with {numbers[0]}", start=True)
        return {
            "selected": numbers,
            "current": item.to_dict() if item else None,
            "queue": [i.to_dict() for i in audit.ordered_items() if i.number in numbers[1:]],
            "progress": audit.progress(),
            "notes": notes,
            **wf.next_step(audit, item, sandbox_enabled=self._sandbox_enabled()),
        }

    def progress(self, args: dict[str, Any], **context: Any) -> str:
        state = args.get("state")
        audit = self._require_audit()
        if state == "stop":
            return ok(**self.archive(audit))
        if state not in wf.PROGRESS_STATES:
            raise ToolInputError(f"'state' must be one of {sorted(wf.PROGRESS_STATES)} or 'stop'.")
        raw = args.get("item")
        if not isinstance(raw, str) or not raw.strip():
            raise ToolInputError("'item' is required (e.g. '1.2', or '1.1, 1.2' for a group).")
        numbers = _split(raw)
        items = [audit.item(n) for n in numbers]
        missing = [n for n, i in zip(numbers, items, strict=True) if i is None]
        if missing:
            raise ToolInputError(f"No item {', '.join(missing)} in audit #{audit.id}.")
        note = args.get("note")
        ts = self.runtime.now().timestamp()
        found = [i for i in items if i is not None]
        for item in found:
            item.state = str(state)
            if isinstance(note, str) and note.strip():
                item.note = note.strip()
            item.updated_at = ts
        first = found[0]
        audit.current = first.number if first.is_open else None
        wf.save_audit(audit)
        self.board.show(audit, f"{', '.join(numbers)}: {wf.STATE_TEXT[str(state)]}")
        return ok(**self._after(audit, first, items=[i.to_dict() for i in found]))

    def timeline(self, args: dict[str, Any]) -> str:
        """Facts for a postmortem: the dossier and the journal of its DAGs, in time order."""
        audit_id = _optional_int(args, "audit_id", None, 1)
        audit = wf.load_audit(audit_id)
        if audit is None:
            raise ToolInputError("No audit found: run /airflow-audit first.")
        dags = sorted({i.dag_id for i in audit.items} | {c["dag_id"] for c in audit.candidates})
        journal = storage.list_audit_entries(audit.created_at - 6 * 3600, dag_ids=dags, limit=300)
        events = [
            {
                "at": _iso(e["ts"]),
                "what": _event_label(e),
                "dag_id": e["dag_id"],
                "run_id": e["run_id"],
                "tasks": e["task_ids"],
                "outcome": e["outcome"],
                "reason": e["reason"],
                "approval": "auto policy"
                if e["auto"]
                else ("preview" if e["dry_run"] else "human"),
            }
            for e in reversed(journal)
        ]
        items = [{**i.to_dict(), "updated_at": _iso(i.updated_at)} for i in audit.ordered_items()]
        return ok(
            audit_id=audit.id,
            status=audit.status,
            scope=audit.scope,
            period=audit.period,
            started_at=_iso(audit.created_at),
            last_update=_iso(audit.updated_at),
            progress=audit.progress(),
            plan_text=wf.plan_text(audit),
            items=items,
            events=events,
            note="Facts only: use them as they are, never invent a time, a cause or an approval.",
        )

    # ------------------------------------------------------------------ observers

    def observe_sandbox_start(self, dag_id: str, task_id: str | None, level: str) -> None:
        """Before a sandbox run: show what is being tested on the board; never raises."""
        try:
            audit = wf.load_current()
            if audit is None:
                return
            item = self._code_item(audit, dag_id, task_id)
            if item is not None:
                group = ", ".join(i.number for i in wf.group_of(audit, item))
                self.board.show(audit, f"testing {group} in the sandbox ({level})...")
        except Exception:
            logger.warning("airflow-mlops: could not update the board", exc_info=True)

    def observe_sandbox(
        self, dag_id: str, task_id: str | None, level: str, result: dict[str, Any]
    ) -> dict[str, Any] | None:
        """The sandbox test IS the test of a code fix: record passed / failed; never raises."""
        try:
            audit = wf.load_current()
            if audit is None:
                return None
            item = self._code_item(audit, dag_id, task_id)
            if item is None:
                return None
            group = wf.group_of(audit, item)
            numbers = [i.number for i in group]
            passed = bool(result.get("passed"))
            if passed and level == "import":
                self.board.show(audit, f"{', '.join(numbers)}: import passed")
                return {
                    "items": numbers,
                    "next_action": "Import passed: now test level='structure' (same diff).",
                }
            ts = self.runtime.now().timestamp()
            commands = result.get("apply_commands") or {}
            for member in group:
                member.state = "passed" if passed else "failed"
                member.updated_at = ts
                member.note = f"sandbox {level}: {'passed' if passed else 'failed'}"
                if passed:
                    member.result = {
                        "level": level,
                        "patch_file": result.get("patch_file"),
                        "apply": commands.get("apply"),
                        "check": commands.get("check"),
                        "undo": commands.get("undo"),
                    }
            audit.current = None
            wf.save_audit(audit)
            self.board.show(audit, f"{', '.join(numbers)}: {wf.STATE_TEXT[item.state]}")
            warnings = result.get("masking_warnings") or []
            if passed and warnings:  # loads fine, but may hide the error: say it first
                return {
                    "items": numbers,
                    "state": item.state,
                    "next_question": wf.question_masking(audit, item, warnings),
                }
            return {"items": numbers, "state": item.state, **self._after(audit, item)}
        except Exception:
            logger.warning("airflow-mlops: could not record the sandbox test", exc_info=True)
            return None

    def observe_clear(
        self, dag_id: str, task_ids: list[str], result: dict[str, Any]
    ) -> dict[str, Any] | None:
        """A real retry of a transient failure: the item waits for the run check."""
        try:
            if result.get("outcome") != "cleared":
                return None
            audit = wf.load_current()
            if audit is None:
                return None
            items = [
                i
                for i in audit.items
                if i.dag_id == dag_id and i.kind == "retry" and i.is_open and i.task_id in task_ids
            ]
            if not items:
                return None
            for item in items:
                item.state = "retried"
                item.updated_at = self.runtime.now().timestamp()
            audit.current = items[0].number
            wf.save_audit(audit)
            self.board.show(audit, f"{dag_id} retried, checking the run...")
            return {
                "items": [i.number for i in items],
                "state": "retried",
                **wf.next_step(audit, items[0], sandbox_enabled=self._sandbox_enabled()),
            }
        except Exception:
            logger.warning("airflow-mlops: could not record the retry", exc_info=True)
            return None

    def context(self, session_id: str | None) -> str | None:
        """Reminder for ``pre_llm_call``: the audit in progress, or the results of the last
        archived one in the same conversation; never raises."""
        try:
            audit = wf.load_current() or wf.load_audit()
            if audit is None or not wf.context_is_relevant(audit, session_id):
                return None
            return wf.context_summary(audit)
        except Exception:
            logger.warning("airflow-mlops: could not read the audit", exc_info=True)
            return None

    def audit_in_progress(self, session_id: str | None = None) -> bool:
        """For the DAG write lock: no code change during an audit. The audit locks its own
        conversation until it ends, and every other one while it is active (30 min): a
        forgotten audit no longer blocks DAG writes for a day."""
        try:
            audit = wf.load_current()
            if audit is None:
                return False
            if session_id and audit.session_id and session_id == audit.session_id:
                return True
            return time.time() - audit.updated_at <= wf.CONTEXT_ACTIVE_SECONDS
        except Exception:
            return True  # cannot tell: keep the lock (a human can still end the audit)

    # ------------------------------------------------------------------ helpers

    def _after(self, audit: wf.Audit, item: wf.Item, **extra: Any) -> dict[str, Any]:
        """Next step after a change; leaving the audit when everything is tested."""
        step = wf.next_step(audit, item, sandbox_enabled=self._sandbox_enabled())
        payload = {"item": item.to_dict(), "progress": audit.progress(), **extra}
        if step.get("finished"):
            return {**payload, **self.archive(audit)}
        return {**payload, **step}

    def archive(self, audit: wf.Audit) -> dict[str, Any]:
        audit.status, audit.pending, audit.current = wf.AUDIT_ARCHIVED, None, None
        wf.save_audit(audit)
        self.board.show(audit, "archived", final=True)
        result: dict[str, Any] = {
            "archived": True,
            "summary": wf.stop_summary(audit),
            "progress": audit.progress(),
        }
        question = wf.question_apply(audit)
        if question:  # fixes passed their test: offer to have Hermes apply them
            result["next_question"] = question
            result["next_action"] = "Show the summary, then end your reply with next_question."
        else:
            result["next_action"] = "Show the summary. The audit is over: no question to ask."
        return result

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
            raise ToolInputError("No audit in progress: start one with /airflow-audit.")
        return audit

    def _state(self, audit: wf.Audit, question: dict[str, Any] | None) -> dict[str, Any]:
        state = {**audit.to_dict(), "plan_text": wf.plan_text(audit)}
        if question:
            state["next_question"] = question
        elif audit.has_work_left():
            state["next_question"] = wf.question_remaining(audit, "What next?")
        return state

    def _period(self) -> str:
        return self.runtime.settings().report_default_period

    def _sandbox_enabled(self) -> bool:
        try:
            return self.runtime.settings().sandbox_enabled
        except Exception:
            return False


class ProgressBoard:
    """One message in the conversation, updated in place while a batch is handled
    (``ctx.post_message`` + ``edit``). Silent where Hermes cannot post or edit."""

    def __init__(self, ctx: Any) -> None:
        self._ctx = ctx
        self._handles: dict[int, Any] = {}

    def show(
        self, audit: wf.Audit, activity: str, *, start: bool = False, final: bool = False
    ) -> None:
        try:
            text = wf.board_text(audit, activity)
            handle = self._handles.get(audit.id)
            if handle is None:
                post = getattr(self._ctx, "post_message", None)
                if not start or post is None:
                    return
                handle = post(text)
                if getattr(handle, "ok", False):
                    self._handles[audit.id] = handle
                return
            handle.edit(text)
            if final:
                self._handles.pop(audit.id, None)
        except Exception:
            logger.warning("airflow-mlops: could not update the progress board", exc_info=True)


class AuditCommands:
    """``/airflow-audit [scope] [period]`` and ``/airflow-status``.

    The command answers at once: the report, then the first question as numbered text. It
    never waits for a menu, never calls the model and never posts a separate message: clients
    give up on a slash command after a deadline (Hermes Desktop: 30 s, then they run it a
    second time), and in a command Hermes cannot tell a Desktop from a terminal. The user's
    reply starts a normal turn: the model continues with the ``airflow_audit_*`` tools.

    Every ``/airflow-audit`` starts a new audit from the report (an audit left open is
    archived: no resume).
    """

    def __init__(self, reads: ReadTools, audits: AuditTools) -> None:
        self.reads = reads
        self.audits = audits

    def audit(self, raw_args: str = "", **kwargs: Any) -> str:
        scope, period = parse_command_args(raw_args)
        try:
            report = self.reads.report(scope, period, update_baseline=True)
        except Exception as exc:  # report errors become plain text, like /airflow-report
            message = getattr(exc, "message", None) or str(exc)
            return f"{FAIL} /airflow-audit: {message}"
        health = report["report"]["health"]
        candidates = wf.candidates_from_report(report["report"])
        audit = wf.create_audit(
            scope, report["report"]["scope_period"]["period"], candidates, _session(kwargs)
        )
        markdown = str(report["markdown"])
        state_of_play = (
            f"{STATUS_ICONS.get(health['status'], '')} {health['status']}: "
            f"{health['status_reason']}"
        )
        question = wf.question_after_report(candidates, state_of_play)
        if question is None:
            self.audits.archive(audit)
            return _join([markdown, "No incident in this period: nothing to diagnose."])
        audit.pending = wf.pending_question(question)  # the model reads it in its reminder
        wf.save_audit(audit)
        return _join([markdown, text_question(question)])

    def status(self, raw_args: str = "", **kwargs: Any) -> str:
        audit = wf.load_current()
        if audit is None:
            last = wf.load_audit()
            if last is None:
                return "No audit yet. Start one with /airflow-audit."
            return "No audit in progress. Last one:\n" + wf.stop_summary(last)
        lines = [
            f"Audit #{audit.id} — in progress — scope {audit.scope} {audit.period}",
            audit.progress()["line"],
        ]
        if audit.items:
            lines += ["", wf.plan_text(audit)]
        unplanned = audit.unplanned_candidates()
        if unplanned:
            lines += ["", "Not diagnosed yet: " + ", ".join(c["dag_id"] for c in unplanned)]
        return "\n".join(lines)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _event_label(entry: dict[str, Any]) -> str:
    tool = entry["tool"]
    if tool == "airflow_sandbox_test":
        return f"sandbox test ({entry['reason'] or 'level ?'})"
    if tool == "dag_write":
        return f"DAG file written: {entry['reason']}"
    if tool == "airflow_clear_tasks":
        return "retry preview" if entry["dry_run"] else "retry"
    return str(tool)


def _join(parts: list[str]) -> str:
    return "\n\n".join(p for p in parts if p)


def text_question(question: dict[str, Any]) -> str:
    """The question as numbered text, for surfaces without a menu."""
    lines = [question["question"]]
    for number, opt in enumerate(question["options"], start=1):
        extra = f" ({opt['description']})" if opt.get("description") else ""
        lines.append(f"  {number}. {opt['label']}{extra}")
    lines.append("Reply with a number or a name.")
    return "\n".join(lines)


def _split(value: str) -> list[str]:
    return [n for n in re.split(r"[\s,;+]+", value or "") if n]


def _session(kwargs: dict[str, Any]) -> str | None:
    value = kwargs.get("session_id") or kwargs.get("session_key")
    return str(value) if value else None
