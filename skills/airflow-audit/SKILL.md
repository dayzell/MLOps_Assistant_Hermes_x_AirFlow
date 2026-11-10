---
name: airflow-audit
description: Run a guided Apache Airflow audit that never changes code — report first, then the diagnosis of the chosen DAGs as numbered problems, then the test of each proposal (a real retry for a transient failure, a fix tested in the sandbox for a code error), with a multiple-choice question between steps and "Stop here" always possible; leaving the audit archives it. Use when the user runs /airflow-audit, asks to audit or check their Airflow, answers one of the audit questions, or an "[airflow-mlops audit ...]" reminder is in context.
version: 3.0.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, audit, incident, guided]
    related_skills: [airflow-mlops:airflow-report, airflow-mlops:airflow-triage, airflow-mlops:airflow-fix-proposal]
    requires_toolsets: [airflow]
---

# Guided Airflow audit

**An audit never changes code.** It does three things, in this order:

1. **Report** — the state of play.
2. **Diagnosis** — the chosen DAGs, as numbered problems (1.1, 1.2, 2.1…).
3. **Test of each proposal** — a transient failure is retried for real (the user approves);
   a code error gets a fix tested in the sandbox (a copy: the user's files never change).

After each test, the plugin offers the problems left, or "Stop here". Leaving the audit
archives it (no resume). The tested fixes stay known: after the audit, if the user asks, the
tested patch can be applied (the DAG write lock lets only that change through, with the
user's approval). During the audit the lock blocks every write to a DAG file.

`/airflow-audit` only prints the report and the first question ("Which DAG should I
diagnose?") as numbered text, then stops. **You run every step after that**, with the
`airflow_audit_*` tools. The plugin still decides every question; you run each step, show the
result, then **end your reply with the plugin's question and wait**. Reply in the user's
language; the plugin's texts stay in English.

## When to Use

- The user asks "audit my Airflow", "what is broken?" (outside the slash command).
- The user answers the question `/airflow-audit` printed (a number, a DAG name, "stop"): the
  reminder lists each choice with what to do for it.
- An `[airflow-mlops audit … in progress]` reminder is in context and the user answers
  briefly ("yes", "2", "1.2", "stop").

An explicit request of the user always comes first: the reminder only guides the audit.

## How to ask a question

Tool results carry `next_question` (`question`, `choices`, `options`, `recommended`).

1. **Write first, ask last, in the same reply.** Write what the step produced (the report,
   the plan, the fix, the test result), then end the reply with `question` and its `choices`
   as a numbered list, recommended first. Then stop and wait for the user.
   **Never call a menu tool (`clarify`) and never call another tool after that text**: a reply
   that ends with a tool call is often sent without its text, and the user sees nothing.
2. Do the `then` of the chosen option. A typed selection ("1.1, 2.3", "DAG 2", "all
   critical") goes to `airflow_audit_select(selection=…)`.
3. "Stop here" → `airflow_audit_progress(state="stop")`: show the `summary`; the audit is over.

A result with `next_action` instead of a question: do that action now.

## Procedure

Order inside one turn: **all the tool calls first, the text last** (the result, then the
question). Never write text and then call a tool.

1. **Report.** If the user did not run `/airflow-audit`, call `airflow_build_report`, show its
   `markdown` and stop: the user names the DAG(s) to diagnose.
2. **Diagnosis.** `airflow_audit_plan(dag_ids=[…])`: show `plan_text`, end with "Which problem
   first?".
3. **Test.** `airflow_audit_select(selection=…)` returns what to do:
   - **code error**: read the code (`airflow_get_dag_source`), write the diff and test it with
     `airflow_sandbox_test`, `level="structure"` (it includes the import check). The plugin
     records the result itself. Then, in one reply: the fix (template of
     `airflow-mlops:airflow-fix-proposal`, step A), the test result, and the question the test
     returned: passed → the problems left; failed → "Revise the fix / Next problem / Stop here";
   - **transient failure**: preview the retry (`airflow_clear_tasks`, `dry_run=true`), then
     the real retry after the user's approval; check the run (`airflow_get_run_status`) and
     report `passed` or `failed` with `airflow_audit_progress`.
4. **End.** "Stop here", or everything tested: the plugin archives the audit and returns a
   summary (results, tested patches and how to apply them). Show it. When fixes passed their
   test, the result also has a `next_question` ("Ask Hermes to apply the tested fixes … now?"):
   ask it. "Apply …" → run the given `git apply` command(s) with the terminal tool (the lock
   lets only the tested changes through and Hermes asks the user to approve), read the changed
   lines back, then offer the targeted retry (dry run first). "Not now" → the fixes stay
   available in this conversation.

## Pitfalls

- **Never edit a DAG file during an audit**, even if asked to "apply" mid-audit: say the
  audit must end first ("Stop here"), then apply the tested patch. The lock blocks it anyway.
- A transient failure on a DAG that also has a code error is not retried during the audit
  (the code is not changed, so the retry would fail): the plugin says so and moves on.
- A fix that **hides** the error (`.get()` instead of a key, a swallowed exception, a default
  value) passes every sandbox level: when `masking_warnings` is not empty, the plugin asks
  "it may hide the error… What now?": say why before anything else.
- Keep the plugin's numbers (1.1, 2.3…); never renumber.
- A tool error (`ok: false`) is not a step: explain it and ask the same question again.

## Verification

- Every step ended with a question from the plugin, or with the archive summary.
- No DAG file changed during the audit (`git status` of the DAGs folder is clean).
