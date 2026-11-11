# airflow-mlops: an MLOps assistant for Apache Airflow, inside Hermes Agent

`airflow-mlops` turns Hermes Agent into an on-call MLOps engineer for **Apache Airflow**. It
watches your DAGs, finds the real cause of a failed run, retries only what is safe to retry,
and tests every code fix in a throwaway Docker sandbox before anything touches your DAG files.

Tested against Airflow 2.9.3 (API v1) and Airflow 3.4.0 (API v2), with Hermes 0.23.0.

## Why it exists

ML pipelines (feature builds, training, scoring) run on Airflow overnight. When one breaks:

- **Someone wakes up for nothing.** Most failures are transient: a connection reset, an HTTP
  503, a full pool. A single targeted retry fixes them.
- **Some failures stay invisible.** A final task with `trigger_rule="all_done"` marks the run
  `success` even though `train` crashed, so the model silently goes stale.
- **The diagnosis is slow.** You open the UI, find the root task among the `upstream_failed`
  ones, scroll the logs, check the XComs, then guess whether a retry is safe.
- **Quick fixes hide the error.** `frame.get("target")` makes the `KeyError` disappear. It
  does not fix the producer that forgot the column.
- **An AI agent with write access is a new risk.** Nobody wants an untested edit to a
  production DAG, or a retry of a whole DAG.

The plugin answers each point. It stays silent unless something is new, and it shows hidden
failures. It also separates transient errors from code errors, with the matching log line as
proof. Fixes are tested on a copy first and flagged when they mask the error. Every write
needs a human approval.

## What it does

| When | What you use | What happens |
|---|---|---|
| Before deployment | `airflow-dag-review` skill | Reviews a DAG and names the exact lines: a final `all_done` with no watcher, a secret in the code, heavy work at parse time, a swallowed exception, no retries |
| At night, unattended | "Airflow night watch" blueprint (`/blueprint`) + `airflow-watch` skill | Checks every DAG on a schedule and answers `[SILENT]` when nothing changed. Otherwise it sends a short alert: new failure, hidden failure, DAG back to green |
| With you | `/airflow-audit` | A guided audit: report, then diagnosis as numbered problems (1.1, 1.2, 2.1…), then a test of each proposal. A transient failure gets a real retry after your approval; a code error gets a fix tested in the sandbox. One question between steps, `Stop here` at any time. **An audit never changes code** |
| Any time | `/airflow-report [all\|<dag_id>\|tag:<tag>] [24h\|7d]` | A standard 6-section monitoring report: scope, health, per-task performance (p50/p95 against a baseline), data quality, incidents and actions, recommendations. Numbers are never invented |
| After the incident | `airflow-postmortem` skill | A postmortem built only from recorded facts: timeline, cause, tested fix, approvals, open points |

`/airflow-status` shows the audit in progress, or the summary of the last one.

## Safety model

- **Read-only by default** (`read_only: true`). The only write to Airflow is
  `airflow_clear_tasks`, and it is hidden until you turn read-only off.
- **Targeted retries only.** The plugin always runs a dry run first. It never clears a whole
  DAG and never re-runs a task that succeeded. A real retry needs a reason and Hermes' human
  approval. A task classified `critical` (a code error) is refused unless explicitly confirmed.
- **Automatic retries are opt-in** (`auto_remediation_enabled: false`). When enabled, they
  only cover diagnosed transient errors, with a bounded attempt count and exponential backoff.
  Past that limit the plugin hands over to a human instead of looping.
- **Docker sandbox.** A fix is applied to a copy of your DAGs folder. The container has no
  network, no secrets, a read-only filesystem, CPU, memory and PID limits, and runs as a
  non-root user. The levels are `import`, `structure` (tasks and dependencies compared with
  production) and `task` (one task run on a data sample).
- **DAG write lock** (`dag_write_guard: sandbox_required`). In your DAGs folder, Hermes can
  only write the exact change that passed the sandbox, and only after your approval. Hermes'
  code tools work normally everywhere else.
- **Masking detection.** A fix that turns `x["k"]` into `x.get("k")`, swallows an exception
  or drops a `raise` is flagged before it can be applied.
- **Secret redaction** of logs, XComs, params and errors before the model sees them, plus a
  DAG allowlist (`dag_allowlist`).
- **Audit log** in the plugin's SQLite database: who did what, when, why, and with what result.

## Install

```bash
# from the Hermes plugin catalog, once the entry is listed
hermes plugins install airflow-mlops --enable

# or from Git, pinned to the reviewed commit of this branch
hermes plugins install https://github.com/dayzell/MLOps_Assistant_Hermes_x_AirFlow --ref <40-char commit sha> --enable
```

Requirements:

- Hermes `>=0.23.0`, Python `>=3.11`;
- Airflow 2.x (REST API v1) or 3.x (REST API v2);
- Docker, only for the optional sandbox.

The Python dependencies come from `pyproject.toml`: `httpx>=0.27,<0.29` and `PyYAML>=6.0,<7`.
The unit tests pass at both floors and at the latest versions.

### Credentials

You need one credential set; the install prompts for it. Secrets are read only through
`agent.secret_scope.get_secret`, and the plugin never reads `.env` or `auth.json`.

| `auth_mode` | Airflow | Secrets |
|---|---|---|
| `jwt` (default) | 3.x | `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` |
| `basic` | 2.x | `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` |
| `token` | 2.x / 3.x | `AIRFLOW_TOKEN` |

### Settings

Settings live under `plugins.entries.airflow-mlops.settings` in `config.yaml`, and the
Desktop settings form shows them too. A minimal example:

```yaml
plugins:
  entries:
    airflow-mlops:
      settings:
        base_url: https://airflow.example.com
        api_version: v2              # v1 = Airflow 2.x, v2 = Airflow 3.x
        auth_mode: jwt
        dag_allowlist: ["ml_*"]
        read_only: false             # expose the targeted retry tool
        sandbox_enabled: true        # needs Docker on Hermes' local terminal
        sandbox_image: apache/airflow:3.4.0   # Airflow + your DAG dependencies
        sandbox_dags_path: /path/to/your/dags
```

`plugin.yaml` describes every setting and its default: timeouts, log and XCom limits, SLA
thresholds, retry policy, sandbox limits and the lock mode.

## Tools, skills, hooks

- **Tools (toolset `airflow`):**
  - read: `airflow_list_dags`, `airflow_get_dag`, `airflow_get_dag_runs`,
    `airflow_get_run_status`, `airflow_get_task_logs`, `airflow_get_xcoms`,
    `airflow_get_dependencies`, `airflow_get_dag_source`;
  - analysis: `airflow_diagnose_run`, `airflow_build_report`, `airflow_review_dag`,
    `airflow_watch_check`;
  - actions: `airflow_clear_tasks`, `airflow_sandbox_test`;
  - audit: `airflow_audit_plan`, `airflow_audit_select`, `airflow_audit_progress`,
    `airflow_audit_timeline`.

  Every tool returns JSON and never raises.
- **Skills** (`airflow-mlops:<name>`): `airflow-triage`, `airflow-report`,
  `airflow-fix-proposal`, `airflow-audit`, `airflow-dag-review`, `airflow-watch`,
  `airflow-postmortem`.
- **Hooks:**
  - `pre_tool_call`: approval for real retries and higher sandbox levels, and the DAG write lock;
  - `post_tool_call`: records whether a written DAG file matches the tested fix;
  - `pre_llm_call`: a short reminder of the audit in progress, injected only into that audit's
    conversation.

**Diagnosis.** `airflow_diagnose_run` separates the root failures from the tasks they blocked.
It extracts the traceback and classifies each root failure as `transient`, `critical` or
`unknown`. It uses the rules in `data/error_patterns.yaml`, and each classification comes with
the line that matched. You can supply your own rules with `error_patterns_path`.

**Data quality.** The report reads a task's XCom `data_quality`, for example
`{"rows": 1200, "null_ratio": 0.01, "freshness": "PT1H", "checks_passed": true}`. Without it,
the section says "not instrumented".

## Disclosure (catalog rule 13)

- **Network:** the plugin calls only the REST API of the Airflow server you configure. There
  is no telemetry and no other third-party service.
- **Shell commands:** only the sandbox's `docker run`. It is sent through Hermes' `terminal`
  tool (`ctx.dispatch_tool`), so Hermes' own command checks apply. `sandbox_runner/check_dag.py`
  runs inside that container only. The sandbox is off by default (`sandbox_enabled: false`).
- **Reads outside the plugin's data:** the local DAGs folder (`sandbox_dags_path`). It is
  read to show code and copied for sandbox tests. The plugin itself never writes it.
- **Writes:** only in `plugin_data_dir("airflow-mlops")` and `plugin_db("airflow-mlops")`
  (audit log, diagnosis cache, baselines, tested changes, temporary sandbox copies).
- **Changes to Airflow:** only `airflow_clear_tasks`. It is hidden by default, needs approval
  and is logged.
- **Hooks on Hermes' own tools:** `pre_tool_call` inspects `write_file`, `patch`, `terminal`
  and `execute_code`, but only to protect the DAGs folder. It blocks or asks for approval and
  never auto-approves.
- **Context injection:** `pre_llm_call` adds the audit reminder (a few lines) only while an
  audit is running in that conversation.
- **No background process, no stored credentials, no self-update.**
- **Unattended runs:** with no human available (cron, gateway), approvals are refused, never
  skipped. One caveat: Hermes' own "yolo" mode skips approvals by Hermes' design, while the
  plugin's blocks still apply.

## Project

This branch contains only the plugin. The full project, on the `main` branch of
[the repository](https://github.com/dayzell/MLOps_Assistant_Hermes_x_AirFlow), adds:

- the unit tests, with Airflow v1 and v2 mocked;
- a disposable Airflow 2.9 / 3.4 demo stack with demo DAGs;
- end-to-end and sandbox smoke tests;
- the architecture notes.

To validate this folder (the same check the catalog CI runs):

```bash
hermes plugins validate . --install-deps
hermes plugins doctor . --ci
```

`.github/workflows/checks.yml` runs the release quarantine that catalog rule 10 recommends.
On each push, it checks that the dependencies resolve with `uv --exclude-newer "3 days"` and
at their lowest declared floors. It also checks that no bytecode or binary file is committed,
and runs the linter.

This project was assisted by Claude Opus 5.5
## License

MIT, © 2026 Joseph Keita
