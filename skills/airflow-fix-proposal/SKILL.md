---
name: airflow-fix-proposal
description: Write a fix proposal for a critical Apache Airflow failure — probable cause, the suggested correction (file, lines, before/after), risks and verification — then offer to test it in a disposable Docker sandbox. Use after airflow-triage classified a failure as critical (code, data, schema, permission or configuration error). The procedure ends once the fix is tested; editing the user's files is a separate step the user asks for.
version: 0.7.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, incident, fix, code-review]
    related_skills: [airflow-mlops:airflow-triage]
    requires_toolsets: [airflow]
---

# Airflow fix proposal

A critical failure will not go away with a retry. Give the human a precise, reviewable fix,
offer to test it in the sandbox, and stop there. Reply in the user's language.

**Where this procedure ends.** It diagnoses, proposes and tests; the plugin tools never write
the user's files. Once the fix is shown (and tested, if the user wanted it), the procedure is
over and the user decides what happens next. If the user then asks you to change the code,
you are back to normal coding work: use your usual file tools (read / patch / write) as for
any other code request; this skill does not restrict them.

## When to Use

- `airflow_diagnose_run` classified a root failure as `critical` (or `unknown` with a clear
  code/data cause).
- The user asks "how do I fix this DAG?" or "what should I change?".

## Procedure

0. **Several problems to fix?** First present the numbered plan of the
   `airflow-mlops:airflow-triage` skill ("Several problems at once": DAG 1 → 1.1, 1.2…, each
   with a one-sentence fix) and ask which one to start with. Then apply the steps below to
   the chosen item only, and keep its number in your answer ("Fix 2.1 — …").
1. **Gather evidence** (from `airflow_diagnose_run`, then targeted tools if needed):
   the `error_block` (file, line, exception), the task's XCom inputs, `dag_params`, the task
   `operator` and `trigger_rule`.
   **Then read the real code with `airflow_get_dag_source(dag_id)`** (for long files, a window
   around the traceback line with `start_line`/`end_line`). Never guess the code.
2. **State the probable cause** in one or two sentences, quoting the decisive log line. Say
   how confident you are and what would confirm it.
3. **Show the fix** (template "Step A"): the file (`path` from `airflow_get_dag_source`), the
   lines, the code before and after, the risks. Keep it minimal and focused on the cause. If
   the tool reports `differs_from_deployed`, say that the local code is not what Airflow runs.
   Prepare the unified diff (`--- a/<path>` / `+++ b/<path>`, context lines copied exactly
   from `content`) for the sandbox, but only show it if the user asks.
4. **Offer the sandbox test and wait for the answer** (`airflow_sandbox_test` is hidden when
   the sandbox is disabled: then say the fix could not be tested and go to step 6).
5. **If the user agrees, test it**:
   - always start with `level="import"`, then `level="structure"` with the `task_id`;
   - if a level fails, read `stages` / `log_excerpt`, correct the diff and test again: do not
     present a fix that failed;
   - offer `level="task"` (runs ONE task on a data sample, with `run_id` for input fixtures)
     only when it adds evidence and the user agrees: it asks for approval and is never
     allowed for heavy operators;
   - mind the per-session test budget.

   Show the result with template "Step B", built from the sandbox result: `changes` gives
   the exact file and line numbers, computed from the real file.
6. **Stop here.** End with the hand-off line of the template: the fix is ready, the user's
   file is unchanged, and the user chooses (ask you to edit the file, apply the patch
   themselves, or leave it). Then say what to re-run once the code is fixed: only the root
   task(s), with `airflow_clear_tasks(dry_run=true)` first if that tool is available.

## Template

Step A — the proposal (end of step 4):

````markdown
### Fix <number> — <short title>
**Probable cause**: <one or two sentences> — evidence: `<decisive log line>` (confidence: high/medium/low)

📄 **File**: `<path>`
📍 **Lines <first>–<last>**

Before:
```python
<current code>
```
After:
```python
<fixed code>
```

**Risks**
- <risk 1>

🧪 Do you want me to test this fix in a disposable Docker sandbox (a copy of your DAGs, no network, no secrets)? Your files will not be changed.
````

Step B — the sandbox result (end of step 6):

````markdown
🧪 **Sandbox** (temporary copy): import ✅ · structure ✅ (<changes or "no structure change">) · task <✅ / ❌ / not run>
📄 `<changes[].local_path>` · 📍 lines <lines_before[0]>–<lines_before[1]> (<modified | added after line N | removed>)
<only if the location differs from step A: show block.before / block.after again>

✅ The fix is ready and tested; your file is unchanged (`your_files_modified: false`). This is where the procedure stops.
Next, as you prefer:
- ask me to apply it to `<changes[].local_path>`;
- or apply it yourself: `<apply_commands.check>` (dry check), then `<apply_commands.apply>` (undo: `<apply_commands.undo>`);
- then retry only `<task_id>` (dry run first).
````

Without the sandbox, replace step B by: "⚠️ Not tested (sandbox disabled). The procedure stops
here; your file is unchanged." and the same "Next" list without the commands.

## If the user then asks you to apply the fix

This is ordinary code editing, outside the procedure: use your file tools as usual.
- Apply exactly the tested change, at the location given by `changes` (or the patch file);
  if the file changed since the test, read it again first.
- Read the file back and show the changed lines.
- Do not commit, push or deploy unless asked; never change Airflow Connections or Variables
  (the human does it in Airflow).
- Then offer the targeted retry (`airflow_clear_tasks`, dry run first).

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
| Failure masked by a final `all_done` task (run marked success) | Keep the final task (cleanup / notification must still run) and add a leaf "watcher" task with `trigger_rule="one_failed"` that raises, so the run fails. Switching the final task to `all_success` hides failures from notifications: only suggest it with that trade-off stated |

## Inside a guided audit

If an `[airflow-mlops audit …]` reminder is in context, or the user started with
`/airflow-audit`, the `airflow-mlops:airflow-audit` skill leads: report each step with
`airflow_audit_progress` and ask the `next_question` it returns (clarify tool, options as given)
instead of writing your own question.

## Pitfalls


- Do not invent file names, line numbers or code: read them with `airflow_get_dag_source`.
- Do not edit the user's files during the procedure: show, offer the test, stop. Edit only
  when the user asks for it afterwards.
- Do not "fix" a failure by catching and ignoring the exception, or by deleting a data-quality
  check.
- **Do not make the error disappear by changing what a task means.** Reading another key
  (`frame["features"]` instead of `frame["target"]`), returning a default or skipping the work
  makes the run green but the result wrong. When a producer and a consumer disagree on a data
  contract, fix the producer (or ask the user which side is right), and validate the input
  with a clear error.
- Never say the user's file was patched after a sandbox test: the sandbox only patches a
  temporary copy (`your_files_modified: false`). Take line numbers from `changes`.
- Never put secrets in the code; reference Airflow Connections / Variables instead.
- A sandbox pass is evidence, not a guarantee: it ran without network and on a sample, and
  proves the code loads and the structure holds, not that the fix is right. Say so.

## Verification

- The DAG imports without error (`airflow dags list-import-errors` is empty for it).
- `airflow tasks test` succeeds for the fixed task.
- After deployment and a targeted re-run, `airflow_get_run_status` shows the root task in
  `success` and the formerly impacted tasks completed.
