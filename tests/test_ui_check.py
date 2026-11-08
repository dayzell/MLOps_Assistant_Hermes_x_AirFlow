"""/airflow-ui-check: every Hermes UI feature is tried, a missing or refused one is reported.
Nothing is ever sent to the model behind the user's back."""

from __future__ import annotations

from typing import Any

from airflow_mlops.ui_check import STOP, UiCheck, progress_bar
from harness import make_context


class Handle:
    def __init__(self, log: list[str], ok: bool = True) -> None:
        self.log, self.ok, self.error = log, ok, None if ok else "no_conversation"

    def edit(self, text: str) -> dict[str, Any]:
        self.log.append(text)
        return {"ok": True}


class HermesUi:
    """Stand-in for the Hermes context methods the probe uses."""

    def __init__(self, answer: str | None = "demo_train_critical") -> None:
        self.posted: list[str] = []
        self.questions: list[tuple[str, list[Any]]] = []
        self.injected: list[str] = []
        self.answer = answer

    def post_message(self, text: str) -> Handle:
        self.posted.append(text)
        return Handle(self.posted)

    def ask_choice(self, question: str, choices: list[Any]) -> str | None:
        self.questions.append((question, choices))
        return self.answer

    def inject_message(self, content: str, **kwargs: Any) -> bool:
        self.injected.append(content)
        return True


def test_progress_bar() -> None:
    assert progress_bar(0, 4, "starting") == "[-------] 0/4 · starting"
    assert progress_bar(3, 7, "testing 1.2") == "[###----] 3/7 · testing 1.2"
    assert progress_bar(4, 4, "done").startswith("[#######] 4/4")


def test_full_probe_on_a_capable_surface() -> None:
    ui = HermesUi()
    probe = UiCheck(ui, step_delay=0)
    text = probe.command("", platform="cli", session_id="s-1", session_key="k-1")
    assert ui.posted[0].endswith("starting...") and ui.posted[-1].startswith("[#######] 4/4")
    question, choices = ui.questions[0]
    assert question.startswith("Which DAG should I diagnose?")
    assert len(choices) <= 4 and choices[-1]["value"] == STOP  # Hermes menus: 4 options max
    assert ui.injected == []  # an injected turn would run unseen and could wait forever
    assert "1. Progress bar: ✅" in text and "2. Multiple-choice question: ✅" in text
    # the code announced to the user is the one given to the model
    code = text.split("code ")[1].split(" ")[0]
    assert code in probe.pre_llm_call(session_id="s-1")["context"]


def test_probe_reports_missing_features() -> None:
    probe = UiCheck(object(), step_delay=0)  # an old Hermes: none of the methods exist
    text = probe.command("")
    assert "1. Progress bar: ❌ not available" in text
    assert "2. Multiple-choice question: ❌ not available" in text


def test_pre_llm_call_is_silent_without_a_probe() -> None:
    assert UiCheck(object()).pre_llm_call(session_id="s") is None


def test_command_and_hook_are_registered() -> None:
    ctx = make_context()
    assert "airflow-ui-check" in ctx.commands
    assert "pre_llm_call" in ctx.hooks
