---
name: airflow-triage
description: Diagnose a failed or retrying Apache Airflow DAG run — find the root-cause task, read its error, XComs and dependencies, classify the failure (transient or critical) and propose the minimal remediation. Use whenever a DAG run, task or pipeline has failed, is stuck in retries, or a user asks "why did my DAG fail?".
version: 0.3.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, incident, triage, data-engineering]
    related_skills: [airflow-mlops:airflow-fix-proposal, airflow-mlops:airflow-report]
    requires_toolsets: [airflow]
---

# Airflow failure triage

Turn a failed Airflow run into a short, evidence-based diagnosis and the **smallest** safe
remediation. Reply in the user's language.

## When to Use

- A DAG run is `failed`, or tasks are `up_for_retry` / `upstream_failed`.
- The user asks why a pipeline failed, whether it is safe to re-run it, or what to fix.
- A report (`airflow_build_report`) lists incidents that need explaining.

## Quick Reference

| Need | Tool |
|---|---|
| Find the failed run | `airflow_get_dag_runs(dag_id, state=["failed"])` |
| Everything in one call (start here) | `airflow_diagnose_run(dag_id, run_id)` |
| More log context for one task | `airflow_get_task_logs(dag_id, run_id, task_id, try_number?)` |
| Inputs / outputs of a task | `airflow_get_xcoms(dag_id, run_id, task_id)` |
| Wider blast radius | `airflow_get_dependencies(dag_id, task_id, depth)` |
| Remediation (only if exposed) | `airflow_clear_tasks(..., dry_run=true)` first |

## Procedure

1. **Locate the run.** If the user gave no `run_id`, call `airflow_get_dag_runs` and take the
   most recent `failed` run. Never guess a run id.
2. **Diagnose in one call:** `airflow_diagnose_run(dag_id, run_id)`. Read, in order:
   `summary` → `root_failures` → `secondary_failures` → `impacted`.
3. **Identify the root cause.** Only `root_failures` are causes. `secondary_failures`
   (failed because an upstream failed, e.g. `all_done` tasks) and `impacted`
   (`upstream_failed`, `skipped`) are consequences: do not fix or re-run them on their own.
4. **Read the evidence of each root failure:** `error_block` first, then `log_tail`, the
   XComs of the task and of its upstream producers (bad or empty inputs?), and `dag_params`.
   If the error block is empty or unclear, call `airflow_get_task_logs` for that try.
5. **Use the classification, and say why.** Quote `classification.category` and its
   `justification` (matched rule and excerpt).
   - `transient` → infrastructure / external hiccup; a targeted retry is reasonable.
   - `critical` → code, data, schema, permission or configuration problem; a retry will
     fail again. Switch to the `airflow-mlops:airflow-fix-proposal` skill.
   - `unknown` → say so plainly, show the error block, and ask a human.
   You may disagree with the rules if the evidence clearly says otherwise — explain why.
6. **Propose the minimal action.**
   - Transient: retry **only** `summary.retry_candidates`, with `only_failed=true`. Downstream
     tasks are re-run by Airflow automatically once the root succeeds; add
     `include_downstream=true` only if the user asks.
   - Critical: no retry. Propose a fix (fix-proposal skill) and the retry to do *after* it.
7. **If remediation tools are available** (`airflow_clear_tasks` is hidden in read-only
   mode): always call it with `dry_run=true` first, show the exact task instances it would
   clear, and wait for the user's go-ahead before `dry_run=false`. Give a `reason`.

## Answer format

```
**Run** <dag_id> / <run_id> — <status>
**Root cause** <task_id> (try <n>/<max>) — <category>: <one-line cause>
**Evidence** `<key line of the error block>` (+ XCom/param facts if relevant)
**Impact** <secondary failures and impacted tasks, or "none">
**Recommended action** <minimal action> — <why it is safe / what to fix first>
```

## Pitfalls

- **Never re-run the whole DAG** or clear tasks that succeeded. Retry the minimum.
- Do not treat `upstream_failed` / `skipped` tasks as causes.
- Do not retry a `critical` failure "to see"; it hides the real problem and wastes compute.
- A `skipped` task not downstream of a failure is often intentional (branching).
- Logs and XComs are already secret-masked: never ask the user to paste credentials.
- A first OOM can be transient; an OOM on try ≥ 2 is critical (resources or data volume).
- If a tool returns `dag_not_allowed`, the DAG is outside the allowlist: stop, do not work
  around it.

## Verification

- After a retry: `airflow_get_run_status(dag_id, run_id)` until the root task is `success`,
  then confirm the formerly `upstream_failed` tasks ran.
- If the same task fails again with the same error, reclassify as critical and escalate.
