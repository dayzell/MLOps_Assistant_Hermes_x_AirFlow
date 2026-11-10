---
name: airflow-postmortem
description: Write the postmortem of an Apache Airflow incident in English from facts only — timeline, impact, root cause, fix and its sandbox evidence, who approved what, retries, prevention — using the guided-audit dossier and the plugin's journal. Use after an incident or an /airflow-audit, when the user asks for an incident report, a postmortem, a summary for the team or for an audit trail.
version: 1.0.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, incident, postmortem, compliance]
    related_skills: [airflow-mlops:airflow-audit, airflow-mlops:airflow-dag-review]
    requires_toolsets: [airflow]
---

# Airflow incident postmortem

A blameless incident sheet built from what the plugin recorded. The sheet is in English (like
every report of the plugin); talk to the user in their language.

## When to Use

- "Write the postmortem of Monday's incident", "incident report for the team".
- After `/airflow-audit` (fixed, stopped or still open).

## Procedure

1. `airflow_audit_timeline()` (or with `audit_id`): dossier items with their states and the
   journal events in time order (sandbox tests, DAG files written, retries, approvals).
2. For each root problem, if the cause needs detail: `airflow_diagnose_run(dag_id, run_id)`.
3. Write the sheet with the template below. **Facts only**: every time, cause and approval
   comes from the tool results; unknown → "unknown", never guessed.

## Template

```markdown
# Incident postmortem — <dag_id(s)> — <date of the first event>

**Status**: <resolved / mitigated / open> · **Audit**: #<audit_id> · **Period**: <started_at> → <last_update>

## Summary
<two sentences: what broke, the effect, how it was fixed>

## Impact
<runs failed or hidden (masked), data or model affected, duration>

## Timeline (UTC)
| Time | Event |
|---|---|
| <at> | <what> — <outcome> (<approval>) |

## Root cause
<the error (decisive log line), why it happened, why it was not seen earlier
(e.g. a final all_done task marking the run success)>

## Fix
<file and change, sandbox levels passed, applied with the user's approval at <time>>

## Recovery
<targeted retry: tasks, result>

## Prevention
- <e.g. watcher task with trigger_rule="one_failed"; run airflow-mlops:airflow-dag-review before merge; enable the night watch>

## Open items
- <items still open or deferred, with their number>
```

## Pitfalls

- Blameless: describe systems and decisions, not people.
- Never invent a time or an approval; the journal is the source of truth.
- Keep secrets out: the plugin already masks them; do not reconstruct them.

## Verification

- Every line of the timeline matches an event of `airflow_audit_timeline`.
- The open items list matches the items not `verified` / `skipped`.
