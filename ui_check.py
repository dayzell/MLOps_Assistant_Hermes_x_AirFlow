"""``/airflow-ui-check``: try the Hermes UI features the guided flow relies on (plan v2, step 0).

It checks, on the surface the user is on (terminal, Desktop, messaging):

1. ``ctx.post_message`` + ``edit``: a progress bar updated in place;
2. ``ctx.ask_choice``: a multiple-choice question;
3. ``ctx.inject_message``: the answer sent to the model as a message;
4. the ``pre_llm_call`` hook: a code the model can only know through the injected context.

Every step is guarded: a missing or refused feature is reported, never raised. Nothing touches
Airflow.
"""

from __future__ import annotations

import secrets
import threading
import time
from typing import Any

PROBE_TTL_SECONDS = 600
STOP = "stop"
QUESTION = "Which DAG should I diagnose? (ui-check: nothing will run)"
CHOICES: list[dict[str, str]] = [
    {
        "value": "demo_train_critical",
        "label": "demo_train_critical",
        "description": "recommended: hidden failure, critical",
    },
    {"value": "demo_fetch_transient", "label": "demo_fetch_transient"},
    {"value": "all", "label": "All (2)"},
    {"value": STOP, "label": "Stop here"},
]


def progress_bar(done: int, total: int, label: str) -> str:
    """``[###----] 3/7 · label`` (plain ASCII: same width everywhere)."""
    width = 7
    filled = round(width * done / total) if total else width
    return f"[{'#' * filled}{'-' * (width - filled)}] {done}/{total} · {label}"


class UiCheck:
    """Runs the probe and feeds its code to the ``pre_llm_call`` hook."""

    def __init__(self, ctx: Any, *, step_delay: float = 0.6) -> None:
        self._ctx = ctx
        self._delay = step_delay
        self._code: str | None = None
        self._code_expires = 0.0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ slash command

    def command(self, raw_args: str = "", **kwargs: Any) -> str:
        platform = kwargs.get("platform")
        session = "yes" if kwargs.get("session_id") else "no"
        lines = [
            "airflow-mlops UI check",
            f"Surface: platform={platform!r}, session={session}",
            "",
            f"1. Progress bar: {self._progress()}",
        ]
        answer, asked = self._ask()
        lines.append(f"2. Multiple-choice question: {asked}")
        lines.append(f"3. Answer sent to the model: {self._inject(answer, kwargs)}")
        code = self._new_code()
        lines += [
            f"4. Context before each reply (pre_llm_call): code {code} stored for 10 min.",
            f'   -> Ask the agent: "What is the airflow ui-check code?" It should answer {code}.',
        ]
        return "\n".join(lines)

    def _progress(self) -> str:
        post = getattr(self._ctx, "post_message", None)
        if post is None:
            return "❌ not available in this Hermes version"
        try:
            steps = ["reading runs", "diagnosing", "testing in sandbox", "done"]
            handle = post(progress_bar(0, len(steps), "starting..."))
            if not getattr(handle, "ok", False):
                return f"❌ refused here ({getattr(handle, 'error', 'unknown')})"
            for done, label in enumerate(steps, start=1):
                time.sleep(self._delay)
                outcome = handle.edit(progress_bar(done, len(steps), label))
                if not (outcome or {}).get("ok", False):
                    return f"⚠️ posted, but edit refused ({(outcome or {}).get('error')})"
            return "✅ a message above was updated in place (check it moved 0/4 -> 4/4)"
        except Exception as exc:  # report, never raise
            return f"❌ {type(exc).__name__}: {exc}"

    def _ask(self) -> tuple[str | None, str]:
        ask = getattr(self._ctx, "ask_choice", None)
        if ask is None:
            return None, "❌ not available in this Hermes version"
        try:
            answer = ask(QUESTION, CHOICES)
        except Exception as exc:
            return None, f"❌ {type(exc).__name__}: {exc}"
        if answer is None:
            return None, "⚠️ no answer (no picker on this surface, cancelled or timed out)"
        return str(answer), f"✅ you picked {answer!r}"

    def _inject(self, answer: str | None, context: dict[str, Any]) -> str:
        if answer is None or answer == STOP:
            return "ℹ️ skipped (no DAG picked)"
        inject = getattr(self._ctx, "inject_message", None)
        if inject is None:
            return "❌ not available in this Hermes version"
        message = (
            f"[airflow-mlops ui-check] The user picked {answer!r} in a menu. Do not call any "
            f"tool; reply with exactly one line: 'ui-check received: {answer}'."
        )
        try:
            key = context.get("session_key") or None
            accepted = inject(message, session_key=key) if key else inject(message)
        except Exception as exc:
            return f"❌ {type(exc).__name__}: {exc}"
        if accepted:
            return "✅ queued: the agent should reply 'ui-check received: ...'"
        return (
            "❌ refused (Desktop / gateway: set "
            "plugins.entries.airflow-mlops.allow_gateway_injection: true, then retry)"
        )

    # ------------------------------------------------------------------ pre_llm_call

    def _new_code(self) -> str:
        with self._lock:
            self._code = secrets.token_hex(3).upper()
            self._code_expires = time.monotonic() + PROBE_TTL_SECONDS
            return self._code

    def pre_llm_call(self, **kwargs: Any) -> dict[str, str] | None:
        """Hermes hook: add the probe code to the model's context while it is valid."""
        with self._lock:
            if self._code is None or time.monotonic() > self._code_expires:
                return None
            code = self._code
        return {
            "context": f"[airflow-mlops] The airflow ui-check code is {code}. Give it only if "
            "the user asks for the ui-check code."
        }
