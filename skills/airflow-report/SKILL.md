---
name: airflow-report
description: Produce the standard Apache Airflow monitoring report (always the same six sections — scope and period, health, per-task performance, data quality, incidents and actions, recommendations) from airflow_build_report, without inventing numbers. Use for daily/weekly pipeline status, health checks, SLA or performance reviews, or when the user runs /airflow-report.
version: 0.5.0
author: Joseph Keita
license: MIT
metadata:
  hermes:
    tags: [airflow, mlops, monitoring, report, sla, data-quality]
    related_skills: [airflow-mlops:airflow-triage]
    requires_toolsets: [airflow]
---

# Airflow standard monitoring report

The report must look the same every time so that reports can be compared week after week.
The structure is produced by the tool; your job is to present it faithfully and add a short,
evidence-based reading. Reply in the user's language.

## When to Use

- "How are my pipelines doing?", "weekly Airflow report", "any SLA misses?", "status of
  DAG X over the last 7 days".
- Scheduled (cron) monitoring runs.
- The `/airflow-report [scope] [period]` slash command already returns the Markdown directly;
  use this skill when the report is requested in conversation.

## Quick Reference

- `airflow_build_report(scope="all" | "<dag_id>" | "tag:<tag>", period="24h" | "7d" | ...)`
  → `report` (JSON, six fixed keys) + `markdown` (fixed template).
- Sections, in this exact order:
  1. Périmètre et période — `scope_period`
  2. Santé globale — `health` (status, runs, success rate, incidents, per-DAG table)
  3. Performance par tâche — `performance` (p50/p95, baseline, trend, retries, queue, SLA)
  4. Qualité des données — `data_quality` (XCom convention `data_quality`)
  5. Incidents et actions — `incidents` (failed runs, cached diagnosis, audit log actions)
  6. Recommandations — `recommendations` (severity high/medium/low/info)

## Procedure

1. Call `airflow_build_report` with the scope and period the user asked for (default: the
   configured period). If the scope is unclear, use `all`.
2. Start your answer with a 2–3 line **executive summary**: health status, success rate,
   number of incidents, the single most important recommendation.
3. Then output the tool's `markdown` **as is** (all six sections, same headings, same order).
   Do not drop empty sections: an empty section still says "aucun" / "non configuré" /
   "non instrumenté".
4. If there are incidents, offer to triage the most recent one with the
   `airflow-mlops:airflow-triage` skill (`airflow_diagnose_run`).

## Writing rules

- **Never invent a number.** Every figure must come from the tool output. Missing data is
  written "—", "non configuré" or "non instrumenté", never estimated.
- Data quality comes only from the XCom convention (`rows`, `null_ratio`, `freshness`,
  `checks_passed`). Without it, say "non instrumenté" and suggest instrumenting it.
- Trends compare the p50 duration to the stored baseline: "no_baseline" on the first reports
  is normal (the baseline builds up over time).
- SLA misses are only reported when `sla_thresholds` is configured.
- Keep the recommendations' order (most severe first) and their evidence; do not soften a
  `high` item.
- Use absolute dates (UTC) as given by the report, not "yesterday".

## Pitfalls

- `scope_period.sampled_dags`: for very busy DAGs only the most recent runs were analysed
  in detail — mention it.
- `scope_period.errors`: some DAGs could not be read — mention them, never silently skip.
- Building a report refreshes the duration baselines; pass `update_baseline=false` for ad-hoc
  what-if reports that must not move the baseline.

## Verification

- The answer contains the six section headings, in order, exactly once.
- Every number in the summary appears in the tool output.
