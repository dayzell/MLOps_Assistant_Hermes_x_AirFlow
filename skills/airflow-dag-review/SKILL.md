---
name: airflow-dag-review
description: Review an Apache Airflow DAG before it is deployed — final trigger_rule that hides failures, secret written in the code, heavy work at parse time, swallowed exceptions, missing retries, catchup — with exact lines, then propose fixes tested in the sandbox. Use when the user asks to review, check or validate a DAG before a pull request or a deployment, or after writing a new DAG.
version: 1.0.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, review, code-review, prevention]
    related_skills: [airflow-mlops:airflow-fix-proposal, airflow-mlops:airflow-audit]
    requires_toolsets: [airflow]
---

# Airflow DAG review

Prevent incidents instead of repairing them: review a DAG before it reaches production.
Reply in the user's language.

## When to Use

- "Review my DAG", "is this DAG ready?", "check my new DAG before the PR".
- A new or changed DAG file before deployment.
- After an incident, to check the other DAGs for the same mistake.

## Procedure

1. **Run the checks:** `airflow_review_dag(dag_id)`. It reads the code (local folder first)
   and the tasks, and returns `findings` (severity, rule, exact line, message, suggestion),
   `counts` and a `verdict`.
2. **Show the result** as a short table, most severe first:

   ```
   DAG review — <dag_id> (<path>) — <verdict>
   | Severity | Line | Problem | Suggested fix |
   ```

   Quote the line from the code when it helps (`airflow_get_dag_source` with
   `start_line` / `end_line` around it). Never print a secret: the plugin masks it.
3. **Ask what to fix** at the end of the same reply (numbered text, at most 4 choices,
   recommended first; no menu tool, no tool call after the text): the first `high`
   finding, "All high (n)", "Only show the report", "Stop here".
4. **Fix the chosen findings** with the `airflow-mlops:airflow-fix-proposal` procedure: read the
   code, show the fix, test it with `airflow_sandbox_test` (`level="import"`, then
   `"structure"`). The DAG write lock lets only the tested change be written, after the user's
   approval (`apply_commands.apply`).
5. **Review again** after the fixes: the finding must be gone.

## Rules and their usual fix

| Rule | Usual fix |
|---|---|
| `masking_trigger_rule` | Keep the final task; add a leaf "watcher" task with `trigger_rule="one_failed"` that raises |
| `hardcoded_secret` | Airflow Connection / Variable (secrets backend); rotate the secret |
| `top_level_work` | Move the call into a task; templates like `{{ var.value.x }}` |
| `swallowed_exception` | Let it fail, or log and re-raise with a clear message |
| `no_retries` | `retries` / `retry_delay` on tasks that call external systems |
| `catchup_enabled` | `catchup=False` unless a backfill is wanted |
| `syntax_error` | Fix the syntax first: nothing else can run |

## Pitfalls

- These are heuristics: an empty report is not a proof, a finding can be intended (say so and
  move on if the user confirms).
- Do not "fix" a finding by hiding it (e.g. catching the exception): the sandbox result's
  `masking_warnings` flags such changes.
- Never paste or guess a secret value; only point at its line.

## Verification

- `airflow_review_dag` returns no `high` finding after the fixes.
- Every applied fix passed `airflow_sandbox_test` at the `structure` level.
