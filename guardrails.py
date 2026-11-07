"""Protected remediation: hard-coded guardrails, auto-retry policy, approval hook, audit log.

Safety model for ``airflow_clear_tasks``:

* Hard-coded in the handler (cannot be configured away): explicit non-empty ``task_ids``,
  one run only, never the whole DAG, ``dry_run=True`` by default, a ``reason`` for real
  clears, ``critical`` failures refused without ``confirm_critical=true``, DAG allowlist,
  ``read_only`` mode.
* Human approval: the ``pre_tool_call`` hook escalates every real clear to Hermes' approval
  gate (``{"action": "approve"}``) unless the auto-retry policy allows it.
* Auto-retry policy (off by default): only transient failures (from the cached diagnosis),
  at most ``auto_retry_max_attempts`` automatic retries per (dag_id, run_id, task_id), with an
  exponential backoff. Beyond that the call is escalated to a human — never a loop.
* Every call (dry runs, refusals, clears, errors) is written to the audit log.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import storage
from .airflow_client import AirflowClient, AirflowError
from .config import Settings
from .diagnosis import CRITICAL, TRANSIENT, UNKNOWN, RuleSet, read_and_classify

logger = logging.getLogger(__name__)

CLEAR_TOOL = "airflow_clear_tasks"
MAX_TASKS_PER_CLEAR = 20


class GuardrailError(Exception):
    """A hard-coded safety rule refused the action (reported as ``guardrail``)."""


# ============================================================================ auto policy


def required_backoff(settings: Settings, auto_attempts: int) -> float:
    """Seconds to wait after ``auto_attempts`` automatic retries (0 before the first one)."""
    if auto_attempts <= 0:
        return 0.0
    return float(settings.auto_retry_backoff_seconds) * (2.0 ** (auto_attempts - 1))


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    per_task: dict[str, dict[str, Any]] = field(default_factory=dict)


def evaluate_auto_policy(
    settings: Settings,
    dag_id: str,
    run_id: str,
    task_ids: list[str],
    *,
    only_failed: bool,
    now: float | None = None,
) -> PolicyDecision:
    """May this clear run WITHOUT human approval? Uses only local state (no network call)."""
    if not settings.auto_remediation_enabled:
        return PolicyDecision(False, "auto-remediation is disabled (auto_remediation_enabled)")
    if not task_ids:
        return PolicyDecision(False, "no task_ids")
    if not only_failed:
        return PolicyDecision(False, "automatic retries require only_failed=true")
    moment = time.time() if now is None else now
    per_task: dict[str, dict[str, Any]] = {}
    refusals: list[str] = []
    for task_id in task_ids:
        cached = storage.get_diagnosis(dag_id, run_id, task_id)
        state = storage.get_retry_state(dag_id, run_id, task_id)
        info: dict[str, Any] = {
            "category": cached.category if cached else None,
            "auto_attempts": state.auto_attempts,
            "max_auto_attempts": settings.auto_retry_max_attempts,
        }
        if cached is None:
            refusals.append(f"{task_id}: not diagnosed yet (run airflow_diagnose_run)")
        elif cached.category != TRANSIENT:
            refusals.append(f"{task_id}: classified {cached.category}, not transient")
        elif state.auto_attempts >= settings.auto_retry_max_attempts:
            refusals.append(
                f"{task_id}: automatic retry limit reached "
                f"({state.auto_attempts}/{settings.auto_retry_max_attempts}) - escalate to a human"
            )
        else:
            wait = required_backoff(settings, state.auto_attempts)
            elapsed = moment - (state.last_attempt_at or 0.0)
            if state.last_attempt_at is not None and elapsed < wait:
                info["retry_after_s"] = round(wait - elapsed, 1)
                refusals.append(f"{task_id}: backoff not elapsed (retry in {wait - elapsed:.0f}s)")
        per_task[task_id] = info
    if refusals:
        return PolicyDecision(False, "; ".join(refusals), per_task)
    return PolicyDecision(True, "transient failure within the automatic retry policy", per_task)


# ============================================================================ approval hook


def _summary(args: dict[str, Any]) -> str:
    tasks = args.get("task_ids")
    task_text = ", ".join(str(t) for t in tasks) if isinstance(tasks, list) else "?"
    options = []
    if args.get("include_downstream"):
        options.append("+downstream")
    if args.get("only_failed", True) is False:
        options.append("including non-failed tasks")
    if args.get("confirm_critical"):
        options.append("CRITICAL failure confirmed")
    suffix = f" ({', '.join(options)})" if options else ""
    reason = str(args.get("reason") or "").strip()[:200] or "no reason given"
    return (
        f"Clear Airflow task(s) [{task_text}] in {args.get('dag_id')} / {args.get('run_id')}"
        f"{suffix}. Reason: {reason}"
    )


def make_pre_tool_call_hook(settings_provider: Callable[[], Settings]) -> Callable[..., Any]:
    """Build the ``pre_tool_call`` callback guarding ``airflow_clear_tasks``."""

    def pre_tool_call(
        tool_name: str = "", args: dict[str, Any] | None = None, **kwargs: Any
    ) -> dict[str, Any] | None:
        if tool_name != CLEAR_TOOL:
            return None
        try:
            call = args if isinstance(args, dict) else {}
            if call.get("dry_run", True) is not False:
                return None  # dry runs are read-only: no approval needed
            settings = settings_provider()
            if settings.read_only:
                return {"action": "block", "message": "airflow-mlops is in read_only mode."}
            dag_id = call.get("dag_id")
            if not isinstance(dag_id, str) or not settings.dag_allowed(dag_id):
                return {
                    "action": "block",
                    "message": f"DAG {dag_id!r} is outside the airflow-mlops allowlist.",
                }
            task_ids = call.get("task_ids")
            run_id = call.get("run_id")
            if isinstance(task_ids, list) and isinstance(run_id, str):
                decision = evaluate_auto_policy(
                    settings,
                    dag_id,
                    run_id,
                    [str(t) for t in task_ids],
                    only_failed=call.get("only_failed", True) is not False,
                )
                if decision.allowed:
                    return None  # bounded automatic retry of a transient failure
                note = decision.reason
            else:
                note = "malformed arguments"
            return {
                "action": "approve",
                "message": f"{_summary(call)} [auto-retry not applicable: {note}]",
                "rule_key": f"{CLEAR_TOOL}:{dag_id}:{run_id}",
            }
        except Exception:  # never let a guard bug turn into an unapproved write
            logger.exception("airflow-mlops: pre_tool_call guard failed")
            return {
                "action": "block",
                "message": "airflow-mlops guard failed unexpectedly; the clear was blocked.",
            }

    return pre_tool_call


# ============================================================================ clear action


@dataclass
class ClearRequest:
    dag_id: str
    run_id: str
    task_ids: list[str]
    include_downstream: bool = False
    only_failed: bool = True
    dry_run: bool = True
    reason: str = ""
    confirm_critical: bool = False


class ClearTasksAction:
    """Execute ``airflow_clear_tasks`` with every guardrail, and journal it."""

    def __init__(self, client: AirflowClient, settings: Settings, ruleset: RuleSet) -> None:
        self.client = client
        self.settings = settings
        self.ruleset = ruleset

    def run(
        self,
        request: ClearRequest,
        *,
        session_id: str | None = None,
        conversation_id: str | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        audit = {
            "dag_id": request.dag_id,
            "run_id": request.run_id,
            "task_ids": request.task_ids,
            "dry_run": request.dry_run,
            "reason": request.reason,
            "session_id": session_id,
            "conversation_id": conversation_id,
            "now": now,
        }
        # Decide "auto vs human" from local state BEFORE anything below refreshes the cache,
        # so it matches what the pre_tool_call hook decided.
        decision = evaluate_auto_policy(
            self.settings,
            request.dag_id,
            request.run_id,
            request.task_ids,
            only_failed=request.only_failed,
            now=now,
        )
        self._pending_id: int | None = None
        try:
            result = self._execute(request, decision, audit)
        except GuardrailError as exc:
            self._journal(audit, auto=False, outcome="refused", details={"error": str(exc)})
            raise
        except AirflowError as exc:
            if self._pending_id is not None:
                self._complete(self._pending_id, "error", exc.to_dict())
            else:
                self._journal(audit, auto=False, outcome="error", details=exc.to_dict())
            raise
        details = {
            "task_instances": [
                f"{ti['task_id']}[{ti['map_index']}]" for ti in result["task_instances"]
            ][:100],
            "classification": result["classification"],
            "include_downstream": request.include_downstream,
            "only_failed": request.only_failed,
        }
        if self._pending_id is not None:
            self._complete(self._pending_id, result["outcome"], details)
            result["audit_id"] = self._pending_id
        else:
            result["audit_id"] = self._journal(
                audit, auto=False, outcome=result["outcome"], details=details
            )
        return result

    # ------------------------------------------------------------------ internals

    def _execute(
        self, request: ClearRequest, decision: PolicyDecision, audit: dict[str, Any]
    ) -> dict[str, Any]:
        self._static_checks(request)
        dag_tasks = {str(t["task_id"]) for t in self.client.get_tasks(request.dag_id)}
        unknown = sorted(set(request.task_ids) - dag_tasks)
        if unknown:
            raise GuardrailError(
                f"Unknown task(s) in DAG '{request.dag_id}': {', '.join(unknown)}."
            )
        if len(dag_tasks) > 1 and set(request.task_ids) >= dag_tasks:
            raise GuardrailError(
                "Refusing to clear every task of the DAG: re-running a whole DAG is never done "
                "by this plugin. Clear only the failed root task(s)."
            )

        classification = self._classifications(request)
        critical = sorted(t for t, c in classification.items() if c == CRITICAL)
        if critical and not request.confirm_critical and not request.dry_run:
            raise GuardrailError(
                f"Task(s) {', '.join(critical)} failed with a CRITICAL error: a retry will most "
                "likely fail again. Fix the cause first (see the airflow-fix-proposal skill), or "
                "pass confirm_critical=true if a human explicitly confirmed the retry."
            )

        # Always preview with Airflow's own dry run: this is the exact list that would change.
        preview = self.client.clear_task_instances(
            request.dag_id,
            request.run_id,
            request.task_ids,
            dry_run=True,
            only_failed=request.only_failed,
            include_downstream=request.include_downstream,
        )
        cleared_ids = {str(ti["task_id"]) for ti in preview}
        if len(dag_tasks) > 1 and cleared_ids >= dag_tasks:
            raise GuardrailError(
                "Refusing: with these options the whole DAG would be re-run "
                f"({len(cleared_ids)} tasks). Drop include_downstream or clear fewer tasks."
            )

        base: dict[str, Any] = {
            "dag_id": request.dag_id,
            "run_id": request.run_id,
            "task_ids": request.task_ids,
            "include_downstream": request.include_downstream,
            "only_failed": request.only_failed,
            "classification": classification,
            "auto_policy": {
                "allowed": decision.allowed,
                "reason": decision.reason,
                "per_task": decision.per_task,
            },
        }
        if not preview:
            return {
                **base,
                "dry_run": request.dry_run,
                "outcome": "nothing_to_clear",
                "count": 0,
                "task_instances": [],
                "message": "Nothing to clear (with only_failed=true, no matching failed task "
                "instance in this run).",
            }
        if request.dry_run:
            warnings = []
            if critical:
                warnings.append(f"critical failure(s): {', '.join(critical)}")
            if UNKNOWN in classification.values():
                warnings.append("some failures are unclassified: human review advised")
            return {
                **base,
                "dry_run": True,
                "outcome": "dry_run",
                "count": len(preview),
                "task_instances": preview,
                "warnings": warnings,
                "next_step": "Show this exact list to the user. To execute, call again with "
                "dry_run=false and a reason; the call goes through human approval unless the "
                "auto-retry policy allows it.",
            }

        # No write without a journal entry: record it as pending first, refuse if impossible.
        self._pending_id = self._journal(
            audit,
            auto=decision.allowed,
            outcome="pending",
            details={"preview": [ti["task_id"] for ti in preview][:100]},
        )
        if self._pending_id is None:
            raise GuardrailError("The audit log is unavailable: refusing to modify Airflow.")
        cleared = self.client.clear_task_instances(
            request.dag_id,
            request.run_id,
            request.task_ids,
            dry_run=False,
            only_failed=request.only_failed,
            include_downstream=request.include_downstream,
        )
        states = storage.record_retry(
            request.dag_id,
            request.run_id,
            request.task_ids,
            auto=decision.allowed,
            now=audit["now"],
        )
        return {
            **base,
            "dry_run": False,
            "outcome": "cleared",
            "approval": "auto-policy" if decision.allowed else "human (Hermes approval gate)",
            "count": len(cleared),
            "task_instances": cleared,
            "retry_attempts": {
                task: {"attempts": s.attempts, "auto_attempts": s.auto_attempts}
                for task, s in states.items()
            },
            "next_step": "Follow up with airflow_get_run_status until the task(s) succeed.",
        }

    def _static_checks(self, request: ClearRequest) -> None:
        if self.settings.read_only:
            raise GuardrailError("The plugin is in read_only mode: write tools are disabled.")
        if not request.task_ids:
            raise GuardrailError(
                "task_ids must list at least one task; whole-DAG clears are not supported."
            )
        if len(request.task_ids) > MAX_TASKS_PER_CLEAR:
            raise GuardrailError(f"At most {MAX_TASKS_PER_CLEAR} tasks per clear.")
        if not request.dry_run and not request.reason.strip():
            raise GuardrailError("A 'reason' is required when dry_run=false (it is journaled).")

    def _classifications(self, request: ClearRequest) -> dict[str, str]:
        """Category per requested task: cached diagnosis, else classify the latest try now."""
        result: dict[str, str] = {}
        missing: list[str] = []
        for task_id in request.task_ids:
            cached = storage.get_diagnosis(request.dag_id, request.run_id, task_id)
            if cached is not None:
                result[task_id] = cached.category
            else:
                missing.append(task_id)
        if not missing:
            return result
        instances = {
            str(ti["task_id"]): ti
            for ti in self.client.list_task_instances(request.dag_id, request.run_id)
        }
        for task_id in missing:
            ti = instances.get(task_id)
            if ti is None or ti.get("state") not in ("failed", "up_for_retry"):
                result[task_id] = "not_failed"
                continue
            evidence = read_and_classify(
                self.client, self.settings, self.ruleset, request.dag_id, request.run_id, ti
            )
            category = evidence.classification.category
            result[task_id] = category
            try:
                storage.save_diagnosis(
                    request.dag_id,
                    request.run_id,
                    task_id,
                    map_index=int(ti.get("map_index", -1)),
                    try_number=ti.get("try_number"),
                    category=category,
                    rule_ids=evidence.classification.rule_ids,
                    justification=evidence.classification.justification,
                )
            except Exception:
                logger.warning("airflow-mlops: could not cache classification", exc_info=True)
        return result

    @staticmethod
    def _complete(entry_id: int, outcome: str, details: dict[str, Any]) -> None:
        try:
            storage.update_audit_entry(entry_id, outcome=outcome, details=details)
        except Exception:
            logger.error(
                "airflow-mlops: AUDIT LOG UPDATE FAILED (entry %s)", entry_id, exc_info=True
            )

    @staticmethod
    def _journal(
        audit: dict[str, Any], *, auto: bool, outcome: str, details: dict[str, Any]
    ) -> int | None:
        try:
            return storage.add_audit_entry(
                tool=CLEAR_TOOL,
                dag_id=audit["dag_id"],
                run_id=audit["run_id"],
                task_ids=audit["task_ids"],
                dry_run=audit["dry_run"],
                auto=auto,
                reason=audit["reason"],
                outcome=outcome,
                details=details,
                session_id=audit["session_id"],
                conversation_id=audit["conversation_id"],
                now=audit["now"],
            )
        except Exception:
            logger.error("airflow-mlops: AUDIT LOG WRITE FAILED", exc_info=True)
            return None
