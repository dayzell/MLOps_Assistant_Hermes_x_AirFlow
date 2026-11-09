"""Guided audit (plan v2, step 1): the ``airflow_audit_*`` tools, ``/airflow-audit``,
``/airflow-status`` and the dossier reminder given to the model before each reply.

The dossier lives in ``plugin_db`` (see :mod:`workflow`). The plugin decides every question;
the model displays it with Hermes' ``clarify`` tool and reports each step back.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
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
# Surfaces that print command output in a terminal (ANSI colors work there).
TERMINAL_SURFACES = frozenset({"cli", "tui", "local"})
YELLOW_BOLD = "\x1b[1;33m"
RESET = "\x1b[0m"
GO = "➡️ {label}: ready. Send any message (e.g. {go}) and the agent continues from here."
_RUNS_SCANNED = 10


class AuditTools:
    """``airflow_audit_plan`` / ``airflow_audit_select`` / ``airflow_audit_progress``."""

    def __init__(self, runtime: PluginRuntime, reads: ReadTools) -> None:
        self.runtime = runtime
        self.reads = reads
        self.board = ProgressBoard(runtime.ctx)

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
        if not numbers:
            raise ToolInputError(f"Nothing left to handle in {selection!r}. " + " ".join(notes))
        chosen = set(numbers)
        for entry in audit.items:
            entry.selected = entry.number in chosen
        audit.current = numbers[0]
        audit.status, audit.pending = wf.AUDIT_OPEN, None
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
        if state == "stop":
            audit = self._require_audit()
            audit.status, audit.pending = wf.AUDIT_STOPPED, None
            wf.save_audit(audit)
            self.board.show(audit, "stopped", final=True)
            return ok(stopped=True, summary=wf.stop_summary(audit), progress=audit.progress())
        if state not in wf.PROGRESS_STATES:
            raise ToolInputError(f"'state' must be one of {sorted(wf.PROGRESS_STATES)} or 'stop'.")
        raw = args.get("item")
        if not isinstance(raw, str) or not raw.strip():
            raise ToolInputError("'item' is required (e.g. '1.2', or '1.1, 1.2' for a group).")
        audit = self._require_audit()
        numbers = [n for n in re.split(r"[\s,;+]+", raw.strip()) if n]
        items = [audit.item(n) for n in numbers]
        missing = [n for n, i in zip(numbers, items, strict=True) if i is None]
        if missing:
            raise ToolInputError(f"No item {', '.join(missing)} in audit #{audit.id}.")
        note = args.get("note")
        ts = self.runtime.now().timestamp()
        for item in items:
            assert item is not None
            item.state = str(state)
            if isinstance(note, str) and note.strip():
                item.note = note.strip()
            item.updated_at = ts
            if state == "verified":  # one run check verifies every item retried with it
                for other in audit.items:
                    if other.dag_id == item.dag_id and other.state == "retried":
                        other.state, other.updated_at = "verified", ts
        first = items[0]
        assert first is not None
        audit.status = wf.AUDIT_OPEN
        follow: wf.Item | None = first
        if not first.is_open:
            following = audit.open_items()
            audit.current = following[0].number if following and following[0].selected else None
            follow = None  # ask "What next?"
        else:
            audit.current = first.number
        wf.save_audit(audit)
        self.board.show(audit, f"{', '.join(numbers)}: {wf.STATE_TEXT[str(state)]}")
        return ok(
            item=first.to_dict(),
            items=[i.to_dict() for i in items if i is not None],
            progress=audit.progress(),
            **wf.next_step(audit, follow, sandbox_enabled=self._sandbox_enabled()),
        )

    # ------------------------------------------------------------------ observers

    def observe_sandbox_start(self, dag_id: str, task_id: str | None, level: str) -> None:
        """Before a sandbox run: show what is being tested on the board; never raises."""
        try:
            audit = wf.load_current()
            if audit is None or audit.status != wf.AUDIT_OPEN:
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
        """Record a sandbox test on the matching code fix (or its whole group, tested in one
        run); never raises."""
        try:
            audit = wf.load_current()
            if audit is None or audit.status != wf.AUDIT_OPEN:
                return None
            item = self._code_item(audit, dag_id, task_id)
            if item is None:
                return None
            group = wf.group_of(audit, item)
            numbers = [i.number for i in group]
            passed = bool(result.get("passed"))
            if passed and level == "import":
                for member in group:
                    member.note = "sandbox: import passed"
                wf.save_audit(audit)
                self.board.show(audit, f"{', '.join(numbers)}: import passed")
                return {
                    "items": numbers,
                    "state": item.state,
                    "next_action": "Import passed: now test level='structure'.",
                }
            ts = self.runtime.now().timestamp()
            for member in group:
                member.state = "sandbox_passed" if passed else "sandbox_failed"
                member.note = f"sandbox {level}: {'passed' if passed else 'failed'}"
                member.updated_at = ts
            audit.current = item.number
            wf.save_audit(audit)
            self.board.show(audit, f"{', '.join(numbers)}: {wf.STATE_TEXT[item.state]}")
            return {
                "items": numbers,
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
            self.board.show(audit, f"{dag_id} retried, checking the run...")
            return {
                "items": [i.number for i in items],
                "state": "retried",
                **wf.next_step(audit, items[0], sandbox_enabled=self._sandbox_enabled()),
            }
        except Exception:
            logger.warning("airflow-mlops: could not record the retry", exc_info=True)
            return None

    def observe_applied(self, dag_id: str, paths: list[str]) -> None:
        """A sandbox-tested fix was written to the DAG file: its items become 'applied'."""
        try:
            audit = wf.load_current()
            if audit is None or audit.status != wf.AUDIT_OPEN:
                return
            items = [i for i in audit.items if i.dag_id == dag_id and i.state == "sandbox_passed"]
            if not items:
                return
            ts = self.runtime.now().timestamp()
            for item in items:
                item.state, item.updated_at = "applied", ts
                item.note = "written: " + ", ".join(Path(p).name for p in paths)
            audit.current = items[0].number
            wf.save_audit(audit)
            numbers = ", ".join(i.number for i in items)
            self.board.show(audit, f"{numbers}: applied to the file")
        except Exception:
            logger.warning("airflow-mlops: could not record the applied fix", exc_info=True)

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

    The command runs the deterministic steps itself (diagnose and number, select, stop,
    resume), menu after menu, until a step needs the model; then it saves what to do and the
    user sends a message ("go"). Nothing is sent to the model behind the user's back: a turn
    started by a plugin runs unseen in Hermes v0.23 and can wait forever on a question.
    """

    MAX_MENUS = 6

    def __init__(self, ctx: Any, reads: ReadTools, audits: AuditTools) -> None:
        self._ctx = ctx
        self.reads = reads
        self.audits = audits

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
        lines = [] if self._post(markdown) else [markdown]
        question = wf.question_after_report(candidates)
        if question is None:
            audit.status = wf.AUDIT_DONE
            wf.save_audit(audit)
            return _join([*lines, "No incident in this period: nothing to diagnose."])
        return self._drive(audit, question, kwargs, lines)

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

    # ------------------------------------------------------------------ the menus

    def _resume(self, audit: wf.Audit, session_id: str | None, kwargs: dict[str, Any]) -> str:
        left = len(audit.open_items()) + len(audit.unplanned_candidates())
        question = wf.make_question(
            f"Resume audit #{audit.id} ({left} left)?",
            [
                wf.option(
                    f"Resume ({left} left)",
                    "Call airflow_audit_plan with no arguments and ask the question it returns.",
                    audit.progress()["line"],
                    kind="resume",
                ),
                wf.option("New audit", "Run /airflow-audit all 24h.", kind="new"),
                wf.stop_option(),
            ],
        )
        audit.session_id = session_id or audit.session_id
        audit.status = wf.AUDIT_OPEN
        wf.save_audit(audit)
        return self._drive(audit, question, kwargs, [])

    def _drive(
        self,
        audit: wf.Audit,
        question: dict[str, Any],
        kwargs: dict[str, Any],
        lines: list[str],
    ) -> str:
        for _ in range(self.MAX_MENUS):
            picked = self._ask(question)
            if picked is None:  # no menu on this surface: the model gets the question as context
                audit.pending = {"label": "(not answered yet)", "then": question["question"]}
                wf.save_audit(audit)
                return _join([*lines, text_question(question)])
            kind = picked.get("kind", "model")
            if kind == "stop":
                audit.status, audit.pending = wf.AUDIT_STOPPED, None
                wf.save_audit(audit)
                self.audits.board.show(audit, "stopped", final=True)
                return _join([*lines, wf.stop_summary(audit)])
            if kind == "new":
                audit.status = wf.AUDIT_SUPERSEDED
                wf.save_audit(audit)
                return _join([*lines, self.audit("all", **kwargs)])
            if kind == "model":  # the agent must do this step (retry preview, code...)
                audit.pending = {"label": picked["label"], "then": picked["then"]}
                wf.save_audit(audit)
                return _join([*lines, self._go(picked["label"], kwargs)])
            try:
                following = self._run_here(audit, picked, kwargs, lines)
            except Exception as exc:  # Airflow down, DAG not allowed...: say it, keep the audit
                message = getattr(exc, "message", None) or str(exc)
                return _join([*lines, f"❌ {picked['label']}: {message}"])
            if following is None:  # the next step needs the model
                return _join(lines)
            question = following
            refreshed = wf.load_current()
            audit = refreshed if refreshed is not None else audit
        return _join([*lines, "Send any message to go on."])

    def _run_here(
        self,
        audit: wf.Audit,
        picked: dict[str, str],
        kwargs: dict[str, Any],
        lines: list[str],
    ) -> dict[str, Any] | None:
        """Run one deterministic step in the command; the next question, or None when the
        model must take over (then the pending instruction is saved and explained)."""
        kind, value = picked["kind"], picked.get("value", "")
        if kind == "resume":
            lines.append(self.status())
            if audit.open_items():
                return wf.question_after_plan(audit)
            return wf.question_after_report(audit.unplanned_candidates())
        if kind == "plan":
            notes = self.audits._plan_dags(audit, [d for d in value.split(",") if d])
            audit.pending = None
            wf.save_audit(audit)
            lines.append(wf.plan_text(audit))
            lines += notes
            return wf.question_after_plan(audit) or wf.question_what_next(audit)
        # select
        step = self.audits._select_core(audit, value, _session(kwargs))
        lines.append(f"➡️ {picked['label']} — {step['progress']['line']}")
        if step.get("next_question"):
            return dict(step["next_question"])
        refreshed = wf.load_current() or audit
        refreshed.pending = {"label": picked["label"], "then": str(step.get("next_action"))}
        wf.save_audit(refreshed)
        lines.append(self._go(picked["label"], kwargs))
        return None

    # ------------------------------------------------------------------ Hermes features

    def _go(self, label: str, kwargs: dict[str, Any]) -> str:
        """The "send 'go'" line, with 'go' in bold yellow in a terminal (ANSI) and in bold
        Markdown elsewhere (Desktop, messaging), so it cannot be missed."""
        surface = ""
        getter = getattr(self._ctx, "session_surface", None)
        try:
            surface = str(getter() or "") if getter is not None else ""
        except Exception:
            surface = ""
        platform = str(kwargs.get("platform") or "")
        logger.info("airflow-mlops: /airflow-audit surface=%r platform=%r", surface, platform)
        terminal = surface in TERMINAL_SURFACES or (not surface and platform in TERMINAL_SURFACES)
        word = f"{YELLOW_BOLD}'go'{RESET}" if terminal else "**'go'**"
        return GO.format(label=label, go=word)

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


def _session(kwargs: dict[str, Any]) -> str | None:
    value = kwargs.get("session_id") or kwargs.get("session_key")
    return str(value) if value else None


def _resumable(audit: wf.Audit) -> bool:
    return bool(audit.open_items() or audit.unplanned_candidates())
