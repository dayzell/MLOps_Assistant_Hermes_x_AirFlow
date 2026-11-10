---
name: airflow-watch
description: Scheduled Apache Airflow watch — check every allowed DAG, stay silent when nothing changed, alert only on what is new (a failed run, a failure hidden by a final all_done task, a DAG back to green), diagnose new incidents and retry transient ones within the plugin's policy. Use for the "Airflow night watch" scheduled job (blueprint) or when the user asks to monitor Airflow on a schedule.
version: 1.0.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, monitoring, cron, alerting]
    related_skills: [airflow-mlops:airflow-triage, airflow-mlops:airflow-audit]
    requires_toolsets: [airflow]
---

# Airflow watch (scheduled)

Run unattended, usually from the "Airflow night watch" scheduled job. Nobody reads the chat
while it runs: be short, and stay silent when there is nothing new.

## When to Use

- The scheduled job created from the `airflow-mlops:night-watch` blueprint (`/blueprint`).
- "Watch my DAGs every 30 minutes", "alert me when something breaks".

## Procedure

1. Call `airflow_watch_check`.
2. If `silent` is true: respond with exactly `[SILENT]` and nothing else.
3. For each entry of `new_incidents`: `airflow_diagnose_run(dag_id, run_id)`; keep the
   category (transient / critical / unknown) and the one-line cause from the error block.
4. Transient failure only: you may call `airflow_clear_tasks` on `summary.retry_candidates`
   (`dry_run=true` first, then `dry_run=false` with a `reason`). The plugin's auto-retry policy
   (`auto_remediation_enabled`, max attempts, backoff) decides whether it runs without a human;
   when it asks for an approval nobody can give, say "retry needs your approval" in the alert.
5. Send one short alert: `alert_text`, then per incident `<dag_id>: <category> — <cause>` and
   what you did (retried / waiting for approval / nothing: critical). End with "Run
   /airflow-audit to fix it step by step."

## Pitfalls

- Never retry a critical or unknown failure, never change a DAG file, never clear a whole DAG.
- Do not repeat incidents listed in `still_failing`: they were already reported.
- A recovery is good news: one line is enough.
- `first_check: true` reports every current incident once; that is expected.

## Verification

- A check with nothing new produced `[SILENT]` only.
- Every alert names the DAG, the run, the category and what was done.
