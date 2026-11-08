---
name: airflow-audit
description: Run a guided Apache Airflow audit — state of play (report), then the user picks what to diagnose, then a numbered plan, a proposed fix, an optional sandbox test, the application and a targeted retry, with a multiple-choice question between every step and "Stop here" always possible. Use when the user runs /airflow-audit, asks to audit or check their Airflow, answers one of the audit questions, or an "[airflow-mlops audit ...]" reminder is in context.
version: 0.8.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, audit, incident, guided]
    related_skills: [airflow-mlops:airflow-report, airflow-mlops:airflow-triage, airflow-mlops:airflow-fix-proposal]
    requires_toolsets: [airflow]
---

# Guided Airflow audit

The plugin keeps a dossier of the audit (numbered items and their state) and decides every
question. Your job: run each step, show the result, then **ask the plugin's question and
wait**. Never skip a question, never invent options. Reply in the user's language; the
plugin's own texts stay in English.

## When to Use

- The user runs `/airflow-audit`, or asks "audit my Airflow", "what is broken?", "check my DAGs".
- A message says "[airflow-mlops] In the audit menu the user chose: …".
- An `[airflow-mlops audit #N …]` reminder is in your context and the user answers briefly
  ("yes", "go", "2", "1.2", "stop"): it refers to the audit's last question.

## How to ask a question

Tool results carry `next_question` (`question`, `choices`, `options`, `recommended`).

1. Call the `clarify` tool with **exactly** `question` and `choices` (single select, the
   recommended option first). If `clarify` is not available, write the choices as a numbered
   list and wait for the answer.
2. Find the chosen option in `options` and do its `then` instruction. If the user typed a
   selection instead ("1.1, 2.3", "DAG 2", "all critical", "all except 2.1"), call
   `airflow_audit_select(selection=<their text>)`.
3. "Stop here" → `airflow_audit_progress(state="stop")`, show its `summary`, stop.

A result with `next_action` instead of a question: do that action now, then continue.

## Procedure

1. **State of play.** If the user did not run `/airflow-audit`, call `airflow_build_report`
   (the configured period) and show its `markdown`; then ask its `next_question`
   ("Which DAG should I diagnose?"). Diagnose nothing before the answer.
2. **Plan.** `airflow_audit_plan(dag_ids=[…])` diagnoses the chosen DAGs and numbers the
   problems. Show `plan_text` as is, then ask `next_question` ("Which problem first?").
3. **Select.** `airflow_audit_select(selection=…)` returns the current item and its
   `next_action` / `next_question`.
4. **Handle the item**, following the `airflow-mlops:airflow-fix-proposal` skill for code
   fixes and `airflow-mlops:airflow-triage` for retries:
   - code fix → read the code, show the fix (step A), then
     `airflow_audit_progress(item, state="fix_proposed")` and ask its question
     ("Test fix N in the sandbox?");
   - the sandbox result is recorded automatically: its `audit` block holds the next question
     ("Apply it to your file?");
   - after the user's file is changed → `state="applied"`; after a real retry the plugin
     records `retried`; once the run is OK → `state="verified"`.
5. **Next.** When an item is closed the plugin asks "What next?" (continue, next DAG, stop).
6. **Resume.** `airflow_audit_plan()` with no arguments returns the audit in progress.

Always show the progress line (`progress.line`, e.g. `[###----] 3/7 done · next: 1.2`)
under each step.

## Pitfalls

- Never diagnose, test, edit or retry anything the user did not choose.
- Never retry a DAG that still has a problem to fix: the plugin then lists the problems left
  (same DAG first) instead of offering the retry; do not offer it yourself.
- Never change a DAG file before its sandbox test passed **and** the user picked "Apply it";
  the approval prompt of the file tool is the final human validation.
- Keep the plugin's numbers (1.1, 2.3…) for the whole audit; never renumber.
- "yes" / "go" answers the last question with its recommended option; ask again if unsure.
- A tool error (`ok: false`) is not a step: explain it and ask the same question again.

## Verification

- Every step ended with a question from the plugin, or with "Stop here" and its summary.
- `/airflow-status` shows the same numbers and states as your answers.
