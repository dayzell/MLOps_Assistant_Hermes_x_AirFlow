---
name: airflow-fix-proposal
description: Write a fix proposal for a critical Apache Airflow failure — probable cause, suggested correction as a unified diff, risks, and how to verify — for a human to review and apply. Use after airflow-triage classified a failure as critical (code, data, schema, permission or configuration error). The agent proposes; it never patches DAG code itself.
version: 0.3.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, incident, fix, code-review]
    related_skills: [airflow-mlops:airflow-triage]
    requires_toolsets: [airflow]
---

# Airflow fix proposal

A critical failure will not go away with a retry. Give the human a precise, reviewable fix
proposal. **You propose; you do not apply.** Never edit, write or commit DAG files, Airflow
Variables or Connections yourself, even if a file or terminal tool is available. Reply in the
user's language.

## When to Use

- `airflow_diagnose_run` classified a root failure as `critical` (or `unknown` with a clear
  code/data cause).
- The user asks "how do I fix this DAG?" or "what should I change?".

## Procedure

1. **Gather evidence** (from `airflow_diagnose_run`, then targeted tools if needed):
   the `error_block` (file, line, exception), the task's XCom inputs, `dag_params`, the task
   `operator` and `trigger_rule`. If the user shares the DAG source, read the exact lines.
2. **State the probable cause** in one or two sentences, quoting the decisive log line. Say
   how confident you are and what would confirm it.
3. **Propose the correction as a unified diff** against the DAG file named in the traceback.
   Keep it minimal and focused on the cause. If you do not have the source, write the diff
   on the lines visible in the traceback and say it must be adapted.
4. **List the risks**: side effects, data already written by the failed run (idempotency),
   downstream consumers, backfill needs.
5. **Explain how to verify** before and after deploying (see Verification).
6. **Say what to re-run after the fix**: only the root task(s), with
   `airflow_clear_tasks(dry_run=true)` first if that tool is available; never the whole DAG.

## Template

````markdown
### Cause probable
<one or two sentences> — evidence: `<decisive log line>` (confidence: high/medium/low)

### Correctif proposé
```diff
--- a/dags/<file>.py
+++ b/dags/<file>.py
@@ -<line>,<n> +<line>,<n> @@
-    <old line>
+    <new line>
```

### Risques
- <risk 1>
- <risk 2>

### Vérification
1. `airflow dags list-import-errors` (the DAG still imports)
2. `airflow tasks test <dag_id> <task_id> <logical_date>` (the task now succeeds)
3. After deployment: re-run only `<task_id>` (dry run first), then check the run status.
````

## Fix patterns by error class

| Error | Typical fix |
|---|---|
| `KeyError` / `AttributeError` | Validate the input (XCom / params) and fail with a clear message; fix the producer if the key is really missing |
| `ImportError` / `ModuleNotFoundError` | Add the dependency to the image / requirements; never `pip install` at runtime |
| `SyntaxError` | Fix the DAG file; check `airflow dags list-import-errors` |
| Schema mismatch | Align the producer and consumer schema; add an explicit schema check task |
| Data-quality failure | Do not loosen the check by default; investigate the upstream data first |
| Missing connection / variable | Create it in Airflow (UI/CLI/secrets backend) — the human does it; give the exact conn_id |
| Permission denied | Name the exact permission / role / path needed; never suggest running as root |
| Recurrent OOM | Reduce batch size / chunking, or raise the task's resources (pool, executor config) |

## Pitfalls

- Do not invent file names or line numbers: take them from the traceback, or say they are
  unknown.
- Do not "fix" a failure by catching and ignoring the exception, or by deleting a data-quality
  check.
- Never put secrets in the diff; reference Airflow Connections / Variables instead.

## Verification

- The DAG imports without error (`airflow dags list-import-errors` is empty for it).
- `airflow tasks test` succeeds for the fixed task.
- After deployment and a targeted re-run, `airflow_get_run_status` shows the root task in
  `success` and the formerly impacted tasks completed.
