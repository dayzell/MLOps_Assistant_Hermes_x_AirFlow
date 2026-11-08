"""Guided audit (plan v2, step 1): the incident dossier, its numbered items and the questions
asked between steps.

The plugin computes every question and its options (at most 4, the limit of Hermes' menus,
recommended option first); the model only displays them with Hermes' ``clarify`` tool and
follows the ``then`` instruction of the chosen option. Nothing here calls Airflow.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from . import storage

# Item states, in the order of the procedure.
STATES = (
    "new",
    "fix_proposed",
    "sandbox_passed",
    "sandbox_failed",
    "applied",
    "retried",
    "verified",
    "skipped",
    "deferred",
)
CLOSED_STATES = frozenset({"verified", "skipped"})
WAITING_STATES = CLOSED_STATES | {"deferred"}
PROGRESS_STATES = frozenset(STATES) - {"new"}
# Still to fix: a retry of the DAG cannot work while one of these is left on it.
TODO_STATES = frozenset({"new", "fix_proposed", "sandbox_passed", "sandbox_failed"})
KINDS = ("code_fix", "retry", "investigate")
AUDIT_OPEN, AUDIT_STOPPED, AUDIT_DONE, AUDIT_SUPERSEDED = "open", "stopped", "done", "superseded"
STOP_LABEL = "Stop here"
STOP_THEN = 'Call airflow_audit_progress with state="stop" and show its summary.'
MAX_CHOICES = 4  # Hermes' clarify tool: 4 options at most (+ an automatic "Other" row)
RESUMABLE_SECONDS = 7 * 24 * 3600
CONTEXT_ACTIVE_SECONDS = 30 * 60
_CATEGORY_RANK = {"critical": 0, "unknown": 1, "transient": 2}
_STATUS_RANK = {"failing": 0, "degraded": 1}


# ============================================================================ model


@dataclass
class Item:
    number: str
    dag_index: int
    position: int
    dag_id: str
    run_id: str | None
    task_id: str | None
    category: str
    kind: str
    title: str
    state: str = "new"
    selected: bool = False
    note: str | None = None
    updated_at: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.state not in WAITING_STATES

    def label(self) -> str:
        target = self.task_id or "run"
        return f"{self.number} {target} — {self.title}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "number": self.number,
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "category": self.category,
            "kind": self.kind,
            "title": self.title,
            "state": self.state,
            "selected": self.selected,
            "note": self.note,
        }


@dataclass
class Audit:
    id: int
    created_at: float
    updated_at: float
    session_id: str | None
    scope: str
    period: str
    status: str
    candidates: list[dict[str, Any]]
    pending: dict[str, Any] | None = None
    current: str | None = None
    items: list[Item] = field(default_factory=list)

    # -- lookups
    def item(self, number: str) -> Item | None:
        return next((i for i in self.items if i.number == number), None)

    def dag_index(self, dag_id: str) -> int:
        """1-based DAG number, stable for the whole audit (new DAGs are appended)."""
        for index, cand in enumerate(self.candidates, start=1):
            if cand["dag_id"] == dag_id:
                return index
        self.candidates.append({"dag_id": dag_id, "status": "unknown", "reason": "added"})
        return len(self.candidates)

    def items_of(self, dag_id: str) -> list[Item]:
        return [i for i in self.ordered_items() if i.dag_id == dag_id]

    def ordered_items(self) -> list[Item]:
        return sorted(self.items, key=lambda i: (i.dag_index, i.position))

    def planned_dags(self) -> list[str]:
        return list(dict.fromkeys(i.dag_id for i in self.ordered_items()))

    def unplanned_candidates(self) -> list[dict[str, Any]]:
        planned = set(self.planned_dags())
        return [c for c in self.candidates if c["dag_id"] not in planned and c.get("incident")]

    def open_items(self) -> list[Item]:
        """Items still to handle: selected ones first, then the plan order."""
        items = [i for i in self.ordered_items() if i.is_open]
        return sorted(items, key=lambda i: (not i.selected, i.dag_index, i.position))

    def progress(self) -> dict[str, Any]:
        total = len(self.items)
        done = sum(1 for i in self.items if i.state in CLOSED_STATES)
        deferred = sum(1 for i in self.items if i.state == "deferred")
        nxt = self.open_items()
        return {
            "done": done,
            "deferred": deferred,
            "total": total,
            "line": progress_line(done, total, nxt[0].number if nxt else None),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit_id": self.id,
            "status": self.status,
            "scope": self.scope,
            "period": self.period,
            "current": self.current,
            "progress": self.progress(),
            "items": [i.to_dict() for i in self.ordered_items()],
        }


def progress_line(done: int, total: int, next_number: str | None = None) -> str:
    """``[###----] 3/7 done · next: 1.2`` (plain ASCII: same width everywhere)."""
    width = 7
    filled = round(width * done / total) if total else 0
    text = f"[{'#' * filled}{'-' * (width - filled)}] {done}/{total} done"
    return text + (f" · next: {next_number}" if next_number else "")


# ============================================================================ persistence


def create_audit(
    scope: str,
    period: str,
    candidates: list[dict[str, Any]],
    session_id: str | None,
    now: float | None = None,
) -> Audit:
    """Open a new audit; any audit still open or stopped is superseded."""
    ts = time.time() if now is None else now
    with storage.connect() as conn:
        conn.execute(
            "UPDATE audits SET status = ?, updated_at = ? WHERE status IN (?, ?)",
            (AUDIT_SUPERSEDED, ts, AUDIT_OPEN, AUDIT_STOPPED),
        )
        cursor = conn.execute(
            """
            INSERT INTO audits (created_at, updated_at, session_id, scope, period, status,
                                candidates, pending, current)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (ts, ts, session_id, scope, period, AUDIT_OPEN, json.dumps(candidates)),
        )
        audit_id = int(cursor.lastrowid or 0)
    return Audit(audit_id, ts, ts, session_id, scope, period, AUDIT_OPEN, list(candidates))


def load_current(now: float | None = None) -> Audit | None:
    """The latest open or stopped audit still recent enough to resume."""
    ts = time.time() if now is None else now
    with storage.connect() as conn:
        row = conn.execute(
            """
            SELECT id, created_at, updated_at, session_id, scope, period, status, candidates,
                   pending, current
            FROM audits WHERE status IN (?, ?) AND updated_at >= ?
            ORDER BY updated_at DESC, id DESC LIMIT 1
            """,
            (AUDIT_OPEN, AUDIT_STOPPED, ts - RESUMABLE_SECONDS),
        ).fetchone()
        if row is None:
            return None
        rows = conn.execute(
            """
            SELECT number, dag_index, position, dag_id, run_id, task_id, category, kind, title,
                   state, selected, note, updated_at
            FROM audit_items WHERE audit_id = ?
            """,
            (row[0],),
        ).fetchall()
    audit = Audit(
        id=int(row[0]),
        created_at=float(row[1]),
        updated_at=float(row[2]),
        session_id=row[3],
        scope=str(row[4]),
        period=str(row[5]),
        status=str(row[6]),
        candidates=_loads(row[7], []),
        pending=_loads(row[8], None),
        current=row[9],
    )
    audit.items = [
        Item(
            number=str(r[0]),
            dag_index=int(r[1]),
            position=int(r[2]),
            dag_id=str(r[3]),
            run_id=r[4],
            task_id=r[5],
            category=str(r[6]),
            kind=str(r[7]),
            title=str(r[8]),
            state=str(r[9]),
            selected=bool(r[10]),
            note=r[11],
            updated_at=float(r[12]),
        )
        for r in rows
    ]
    return audit


def save_audit(audit: Audit, now: float | None = None) -> None:
    ts = time.time() if now is None else now
    audit.updated_at = ts
    with storage.connect() as conn:
        conn.execute(
            """
            UPDATE audits SET updated_at = ?, status = ?, candidates = ?, pending = ?,
                              current = ?, session_id = COALESCE(?, session_id)
            WHERE id = ?
            """,
            (
                ts,
                audit.status,
                json.dumps(audit.candidates),
                json.dumps(audit.pending) if audit.pending is not None else None,
                audit.current,
                audit.session_id,
                audit.id,
            ),
        )
        for item in audit.items:
            conn.execute(
                """
                INSERT INTO audit_items (audit_id, number, dag_index, position, dag_id, run_id,
                    task_id, category, kind, title, state, selected, note, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (audit_id, number) DO UPDATE SET
                    state = excluded.state, selected = excluded.selected,
                    note = excluded.note, updated_at = excluded.updated_at,
                    run_id = excluded.run_id, title = excluded.title
                """,
                (
                    audit.id,
                    item.number,
                    item.dag_index,
                    item.position,
                    item.dag_id,
                    item.run_id,
                    item.task_id,
                    item.category,
                    item.kind,
                    item.title[:300],
                    item.state,
                    int(item.selected),
                    (item.note or "")[:500] or None,
                    item.updated_at or ts,
                ),
            )


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


# ============================================================================ planning


def candidates_from_report(report: dict[str, Any]) -> list[dict[str, Any]]:
    """DAGs with an incident in the report, most urgent first (failing, critical, masked)."""
    runs_by_dag: dict[str, list[dict[str, Any]]] = {}
    for run in report["incidents"]["failed_runs"]:  # newest first
        runs_by_dag.setdefault(run["dag_id"], []).append(run)
    candidates = []
    for dag in report["health"]["per_dag"]:
        if dag["status"] not in _STATUS_RANK:
            continue
        runs = runs_by_dag.get(dag["dag_id"], [])
        latest = runs[0] if runs else {}
        categories = list((latest.get("diagnosis") or {}).values())
        category = min(categories, key=lambda c: _CATEGORY_RANK.get(c, 1)) if categories else None
        candidates.append(
            {
                "dag_id": dag["dag_id"],
                "status": dag["status"],
                "reason": dag["status_reason"],
                "run_id": latest.get("run_id"),
                "masked": bool(latest.get("masked")),
                "category": category,
                "incident": True,
            }
        )
    candidates.sort(
        key=lambda c: (
            _STATUS_RANK[c["status"]],
            0 if c["masked"] else 1,
            _CATEGORY_RANK.get(c["category"] or "unknown", 1),
        )
    )
    return candidates


def items_from_diagnosis(audit: Audit, diagnosis: dict[str, Any]) -> list[Item]:
    """Numbered items of one DAG from ``airflow_diagnose_run`` (roots, then the masking rule)."""
    dag_id = str(diagnosis["dag_id"])
    run_id = str(diagnosis["run_id"])
    dag_index = audit.dag_index(dag_id)
    roots = sorted(
        diagnosis.get("root_failures") or [],
        key=lambda r: _CATEGORY_RANK.get(_category(r), 1),
    )
    drafts: list[tuple[str | None, str, str, str]] = []
    for root in roots + list(diagnosis.get("retrying") or []):
        category = _category(root)
        kind = {"transient": "retry", "critical": "code_fix"}.get(category, "investigate")
        drafts.append((str(root["task_id"]), category, kind, short_cause(root)))
    if diagnosis.get("masked_failure"):
        drafts.append(
            (
                None,
                "critical",
                "code_fix",
                "run marked success: a final task's trigger_rule hides the failure",
            )
        )
    now = time.time()
    return [
        Item(
            number=f"{dag_index}.{position}",
            dag_index=dag_index,
            position=position,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            category=category,
            kind=kind,
            title=title,
            updated_at=now,
        )
        for position, (task_id, category, kind, title) in enumerate(drafts, start=1)
    ]


def _category(entry: dict[str, Any]) -> str:
    classification = entry.get("classification") or {}
    return str(classification.get("category") or "unknown")


def short_cause(entry: dict[str, Any], limit: int = 70) -> str:
    """One line for the plan: the exception line of the error block, else the rule."""
    block = str(entry.get("error_block") or "")
    lines = [line.strip() for line in block.splitlines() if line.strip()]
    for line in reversed(lines):
        if re.match(r"^[\w.]*(Error|Exception|Exit|Interrupt|Timeout)\b", line) or ": " in line:
            text = re.sub(r"^\[[^\]]*\]\s*", "", line)
            return text if len(text) <= limit else text[: limit - 1] + "…"
    rules = (entry.get("classification") or {}).get("matched_rules") or []
    if rules:
        return str(rules[0].get("rule_id"))
    return f"{_category(entry)} failure"


def plan_text(audit: Audit) -> str:
    """The numbered plan, grouped by DAG, as plain text (English, like every pre-made text)."""
    lines = []
    for dag_id in audit.planned_dags():
        items = audit.items_of(dag_id)
        cand = next((c for c in audit.candidates if c["dag_id"] == dag_id), {})
        reason = cand.get("reason")
        header = f"DAG {items[0].dag_index} — {dag_id}" + (f" ({reason})" if reason else "")
        lines.append(header)
        for item in items:
            action = {
                "code_fix": "code fix (sandbox test offered)",
                "retry": "retry only, no code change",
                "investigate": "investigate (unclear cause)",
            }[item.kind]
            lines.append(
                f"  {item.number} {item.task_id or 'run'} — {item.category} — {item.title}"
                f"\n      -> {action} [{item.state}]"
            )
    return "\n".join(lines)


# ============================================================================ selection

_ITEM = re.compile(r"^(\d+)\.(\d+)$")
_RANGE = re.compile(r"^(\d+)\.(\d+)\s*-\s*(\d+)\.(\d+)$")


class SelectionError(ValueError):
    """The typed selection does not match the plan."""


def parse_selection(text: str, audit: Audit) -> tuple[list[str], list[str]]:
    """Resolve a typed selection to ``(item numbers, DAG ids to plan first)``.

    Accepted: ``1.1, 2.3`` · ``1.1-1.4`` · ``DAG 2`` / ``2`` · a DAG id · ``all`` ·
    ``all critical`` / ``all transient`` · ``all except 3.2``. Order is kept.
    """
    raw = (text or "").strip().lower()
    if not raw:
        raise SelectionError("Empty selection.")
    excluded: set[str] = set()
    if " except " in f" {raw} ":
        raw, _, rest = raw.partition("except")
        excluded = set(parse_selection(rest, audit)[0])
        raw = raw.strip() or "all"
    numbers: list[str] = []
    to_plan: list[str] = []
    for token in re.split(r"\s*(?:,|;|\band\b|\bet\b)\s*", raw):
        token = token.strip()
        if not token:
            continue
        found, plan = _resolve_token(token, audit)
        numbers += found
        to_plan += plan
    numbers = [n for n in dict.fromkeys(numbers) if n not in excluded]
    if not numbers and not to_plan:
        raise SelectionError(f"Nothing matches {text!r}.")
    return numbers, list(dict.fromkeys(to_plan))


def _resolve_token(token: str, audit: Audit) -> tuple[list[str], list[str]]:
    ordered = audit.ordered_items()
    if token in ("all", "tout", "tous", "everything"):
        return [i.number for i in ordered if i.is_open], [
            c["dag_id"] for c in audit.unplanned_candidates()
        ]
    if token.startswith("all "):
        category = token[4:].strip()
        if category not in _CATEGORY_RANK:
            raise SelectionError(f"Unknown category {category!r} (critical, transient, unknown).")
        return [i.number for i in ordered if i.is_open and i.category == category], []
    if match := _RANGE.match(token):
        start = (int(match[1]), int(match[2]))
        end = (int(match[3]), int(match[4]))
        found = [i.number for i in ordered if start <= (i.dag_index, i.position) <= end]
        if not found:
            raise SelectionError(f"No item in range {token!r}.")
        return found, []
    if _ITEM.match(token):
        if audit.item(token) is None:
            raise SelectionError(f"No item {token!r} in the plan.")
        return [token], []
    dag_number = re.fullmatch(r"(?:dag\s*)?(\d+)", token)
    if dag_number:
        index = int(dag_number[1])
        if not 1 <= index <= len(audit.candidates):
            raise SelectionError(f"No DAG {index} in this audit.")
        return _dag_selection(audit, audit.candidates[index - 1]["dag_id"])
    for cand in audit.candidates:
        if cand["dag_id"].lower() == token:
            return _dag_selection(audit, cand["dag_id"])
    if re.fullmatch(r"[\w.\-]+", token):
        return [], [token]  # a DAG id not in the audit yet: plan it (allowlist checked later)
    raise SelectionError(f"Cannot understand {token!r}.")


def _dag_selection(audit: Audit, dag_id: str) -> tuple[list[str], list[str]]:
    items = [i.number for i in audit.items_of(dag_id) if i.is_open]
    return (items, []) if audit.items_of(dag_id) else ([], [dag_id])


# ============================================================================ questions


def option(label: str, then: str, description: str | None = None) -> dict[str, str]:
    entry = {"label": label, "then": then}
    if description:
        entry["description"] = description
    return entry


def make_question(
    text: str, options: list[dict[str, str]], typed: str | None = None
) -> dict[str, Any]:
    """A question for Hermes' clarify tool: ``choices`` are the labels, recommended first."""
    options = options[:MAX_CHOICES]
    question: dict[str, Any] = {
        "question": text,
        "choices": [o["label"] for o in options],
        "recommended": options[0]["label"],
        "options": options,
        "how_to_ask": "Call the clarify tool with exactly this question and these choices "
        "(single select, recommended first), then do the 'then' of the chosen option. If "
        "clarify is not available, list the choices numbered and wait for the answer.",
    }
    if typed:
        question["typed_answers"] = typed
    return question


def _plan_then(dag_ids: list[str]) -> str:
    return f"Call airflow_audit_plan with dag_ids={json.dumps(dag_ids)}."


def question_after_report(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Which DAG should I diagnose? (None when nothing needs a diagnosis)."""
    if not candidates:
        return None
    ids = [c["dag_id"] for c in candidates]
    first = candidates[0]
    options = [
        option(
            first["dag_id"],
            _plan_then([first["dag_id"]]),
            f"recommended: {_why(first)}",
        )
    ]
    if len(candidates) == 2:
        second = candidates[1]
        options.append(option(second["dag_id"], _plan_then([second["dag_id"]]), _why(second)))
    elif len(candidates) > 2:
        critical = [c["dag_id"] for c in candidates if c["category"] == "critical" or c["masked"]]
        if 1 < len(critical) < len(candidates):
            options.append(option(f"All critical ({len(critical)})", _plan_then(critical)))
        else:
            second = candidates[1]
            options.append(option(second["dag_id"], _plan_then([second["dag_id"]]), _why(second)))
    if len(candidates) > 1:
        options.append(option(f"All ({len(candidates)})", _plan_then(ids)))
    options.append(option(STOP_LABEL, STOP_THEN))
    return make_question(
        "Which DAG should I diagnose?",
        options,
        typed="DAG numbers or names, e.g. '1, 3' or 'demo_fetch_transient' "
        "(then call airflow_audit_select with that text).",
    )


def _why(cand: dict[str, Any]) -> str:
    parts = [cand.get("reason") or cand["status"]]
    if cand.get("category"):
        parts.append(str(cand["category"]))
    return ", ".join(parts)


def question_after_plan(audit: Audit) -> dict[str, Any] | None:
    """Which problem first?"""
    items = [i for i in audit.open_items()]
    if not items:
        return None
    options = [
        option(
            items[0].label(),
            f'Call airflow_audit_select with selection="{items[0].number}".',
            "recommended",
        )
    ]
    if len(items) > 1:
        options.append(
            option(
                items[1].label(), f'Call airflow_audit_select with selection="{items[1].number}".'
            )
        )
    first_dag = [i for i in items if i.dag_id == items[0].dag_id]
    if len(first_dag) > 1 and len(first_dag) < len(items):
        options.append(
            option(
                f"All of DAG {items[0].dag_index} ({len(first_dag)})",
                f'Call airflow_audit_select with selection="DAG {items[0].dag_index}".',
            )
        )
    elif len(items) > 2 or (len(items) == 2 and len(first_dag) == 2):
        options.append(
            option(f"All ({len(items)})", 'Call airflow_audit_select with selection="all".')
        )
    options.append(option(STOP_LABEL, STOP_THEN))
    return make_question(
        "Which problem first?",
        options,
        typed="item numbers, e.g. '1.1, 2.3', '1.1-1.3', 'DAG 2', 'all critical', "
        "'all except 2.1' (then call airflow_audit_select with that text).",
    )


def next_step(audit: Audit, item: Item | None, *, sandbox_enabled: bool) -> dict[str, Any]:
    """What happens after an item changed state: an instruction or a question."""
    if item is None or not item.is_open:
        return (
            {"next_question": question_what_next(audit)}
            if audit.open_items() or (audit.unplanned_candidates())
            else {"next_action": "Everything in this audit is handled. " + STOP_THEN}
        )
    n, dag, run = item.number, item.dag_id, item.run_id
    task = item.task_id
    progress = f'airflow_audit_progress with item="{n}"'
    if item.state == "new":
        blockers = blocking_fixes(audit, item)
        if item.kind == "retry" and blockers:
            return {
                "next_question": question_remaining(
                    audit,
                    f"{n}: {task} can only be retried once {dag} is fixed "
                    f"({len(blockers)} problem(s) left on this DAG). Which one first?",
                    ref=item,
                )
            }
        if item.kind == "retry":
            return {
                "next_question": make_question(
                    f"{n}: {task} failed for a transient reason. Retry it?",
                    [
                        option(
                            "Preview the retry (dry run)",
                            f'Call airflow_clear_tasks with dag_id="{dag}", run_id="{run}", '
                            f'task_ids=["{task}"], dry_run=true; show the list, then ask before '
                            f'the real retry. After it, call {progress}, state="retried". '
                            "In read-only mode, explain how to clear it in the Airflow UI.",
                        ),
                        option(
                            "Diagnose deeper",
                            f"Read the logs of {task} (airflow_get_task_logs) and explain; "
                            "then ask this question again.",
                        ),
                        option("Skip it", f'Call {progress}, state="skipped".'),
                        option(STOP_LABEL, STOP_THEN),
                    ],
                )
            }
        return {
            "next_action": (
                f'Fix {n}: read the code with airflow_get_dag_source(dag_id="{dag}"), then '
                "present the fix with the airflow-fix-proposal template (step A: cause, file, "
                f'lines, before/after, risks). Then call {progress}, state="fix_proposed", and '
                "ask the question it returns. Do not edit any file."
            )
        }
    if item.state == "fix_proposed":
        if not sandbox_enabled:
            return {
                "next_question": make_question(
                    f"Fix {n} cannot be tested here (sandbox disabled). What now?",
                    [
                        option("Show the raw diff", "Show the unified diff, then ask again."),
                        option("Keep it for later", f'Call {progress}, state="deferred".'),
                        option(STOP_LABEL, STOP_THEN),
                    ],
                )
            }
        return {
            "next_question": make_question(
                f"Test fix {n} in the sandbox?",
                [
                    option(
                        "Test it (copy, no network)",
                        f'Call airflow_sandbox_test with dag_id="{dag}", the diff, '
                        'level="import", then level="structure"'
                        + (f' with task_id="{task}"' if task else "")
                        + ". The plugin records the result in the audit.",
                        "your files are not changed",
                    ),
                    option("Show the raw diff", "Show the unified diff, then ask this again."),
                    option("Skip the test", f'Call {progress}, state="deferred".'),
                    option(STOP_LABEL, STOP_THEN),
                ],
            )
        }
    if item.state == "sandbox_passed":
        return {
            "next_question": make_question(
                f"Fix {n} passed the sandbox test. Apply it to your file?",
                [
                    option(
                        "Apply it (your approval required)",
                        "Edit the file with your file tools exactly as tested (sandbox result: "
                        "changes / patch_file), read it back and show the changed lines; then "
                        f'call {progress}, state="applied".',
                    ),
                    option(
                        "I'll apply it myself",
                        "Show apply_commands (check, apply, undo) from the sandbox result; "
                        f'call {progress}, state="applied" once the user confirms.',
                    ),
                    option("Not now", f'Call {progress}, state="deferred".'),
                    option(STOP_LABEL, STOP_THEN),
                ],
            )
        }
    if item.state == "sandbox_failed":
        return {
            "next_question": make_question(
                f"The sandbox test of fix {n} failed.",
                [
                    option(
                        "Revise the fix",
                        "Read stages / log_excerpt, correct the diff and test it again.",
                    ),
                    option("Show the sandbox log", "Show log_excerpt, then ask this again."),
                    option("Keep it for later", f'Call {progress}, state="deferred".'),
                    option(STOP_LABEL, STOP_THEN),
                ],
            )
        }
    if item.state == "applied":
        blockers = blocking_fixes(audit, item)
        if blockers:
            return {
                "next_question": question_remaining(
                    audit,
                    f"Fix {n} is applied, but {dag} still has {len(blockers)} problem(s) to fix "
                    "before a retry can work. Which one next?",
                    ref=item,
                )
            }
        retry = retry_option(audit, dag, run, recommended=True)
        if remaining(audit, exclude=n):
            return {
                "next_question": question_remaining(
                    audit,
                    f"Fix {n} is applied and {dag} has nothing left to fix. Retry it now, or "
                    "handle another problem first?",
                    lead=[retry],
                    ref=item,
                )
            }
        return {
            "next_question": make_question(
                f"Fix {n} is applied. Retry the failed tasks of {dag}?",
                [
                    retry,
                    option("Not now", f'Call {progress}, state="deferred".'),
                    option(STOP_LABEL, STOP_THEN),
                ],
            )
        }
    if item.state == "retried":
        return {
            "next_action": (
                f'Check the run with airflow_get_run_status(dag_id="{dag}", run_id="{run}") '
                f'until the tasks finish. Success: call {progress}, state="verified". Same '
                f'error again: explain it and call {progress}, state="new" (it is no longer '
                "transient: propose a code fix)."
            )
        }
    return {"next_question": question_what_next(audit)}


def question_what_next(audit: Audit) -> dict[str, Any]:
    """The problems left, as after the diagnosis; a DAG whose fixes are all applied can be
    retried first."""
    lead = []
    for dag_id in audit.planned_dags():
        applied = [i for i in audit.items_of(dag_id) if i.state == "applied"]
        if applied and not blocking_fixes(audit, applied[0]):
            lead.append(retry_option(audit, dag_id, applied[0].run_id, recommended=True))
            break
    return question_remaining(audit, "What next? Problems left:", lead=lead)


def remaining(audit: Audit, exclude: str | None = None) -> list[Item]:
    """Items still to fix (not applied, retried or closed), in the plan order."""
    return [i for i in audit.open_items() if i.state in TODO_STATES and i.number != exclude]


def blocking_fixes(audit: Audit, item: Item) -> list[Item]:
    """Code fixes left on the same DAG: retrying it before they are applied fails again."""
    return [
        i
        for i in remaining(audit, exclude=item.number)
        if i.dag_id == item.dag_id and i.kind != "retry"
    ]


def retry_option(
    audit: Audit, dag_id: str, run_id: str | None, *, recommended: bool = False
) -> dict[str, str]:
    roots = [i.task_id for i in audit.items_of(dag_id) if i.task_id and i.state == "applied"]
    tasks = json.dumps(roots) if roots else "the failed root task(s)"
    return option(
        f"Preview the retry of {dag_id} (dry run)",
        f'Call airflow_clear_tasks with dag_id="{dag_id}", run_id="{run_id}", task_ids={tasks}, '
        "dry_run=true; show the list, then ask before the real retry (the plugin records it).",
        "recommended: every fix of this DAG is applied" if recommended else None,
    )


def question_remaining(
    audit: Audit,
    text: str,
    *,
    lead: list[dict[str, str]] | None = None,
    ref: Item | None = None,
) -> dict[str, Any]:
    """Same menu as after the diagnosis: the problems left (same DAG as ``ref`` first),
    "All remaining" when they do not fit, a DAG still to diagnose, "Stop here"."""
    options = list(lead or [])
    pool = remaining(audit, exclude=ref.number if ref else None)
    if ref is not None:
        pool.sort(key=lambda i: i.dag_id != ref.dag_id)  # stable: plan order kept
    room = MAX_CHOICES - 1 - len(options)
    shown = pool[: room - 1] if len(pool) > room >= 2 else pool[:room]
    for index, entry in enumerate(shown):
        if ref is not None and entry.dag_id == ref.dag_id:
            where = "same DAG: needed before the retry"
        else:
            where = f"DAG {entry.dag_index} — {entry.dag_id}"
        recommended = not lead and index == 0
        options.append(
            option(
                entry.label(),
                f'Call airflow_audit_select with selection="{entry.number}".',
                f"recommended, {where}" if recommended else where,
            )
        )
    if len(pool) > len(shown):
        options.append(
            option(
                f"All remaining ({len(pool)})", 'Call airflow_audit_select with selection="all".'
            )
        )
    unplanned = audit.unplanned_candidates()
    if unplanned and len(options) < MAX_CHOICES - 1:
        options.append(
            option(
                f"Diagnose {unplanned[0]['dag_id']}",
                _plan_then([unplanned[0]["dag_id"]]),
                _why(unplanned[0]),
            )
        )
    options.append(option(STOP_LABEL, STOP_THEN))
    return make_question(
        text,
        options,
        typed="item numbers, e.g. '1.2, 2.1', 'DAG 2', 'all' (then call airflow_audit_select).",
    )


# ============================================================================ summaries


def stop_summary(audit: Audit) -> str:
    done = [i for i in audit.ordered_items() if i.state in CLOSED_STATES]
    left = [i for i in audit.ordered_items() if i.state not in CLOSED_STATES]
    lines = [f"⏸ Audit #{audit.id} stopped. {audit.progress()['line']}"]
    if done:
        lines.append("Done: " + ", ".join(f"{i.number} ({i.state})" for i in done))
    if left:
        lines.append(
            "Left: " + ", ".join(f"{i.number} {i.task_id or 'run'} ({i.state})" for i in left)
        )
    unplanned = audit.unplanned_candidates()
    if unplanned:
        lines.append("Not diagnosed: " + ", ".join(c["dag_id"] for c in unplanned))
    lines.append("Resume any time with /airflow-audit.")
    return "\n".join(lines)


def context_summary(audit: Audit, limit: int = 1200) -> str:
    """Short reminder injected before each model reply (``pre_llm_call``)."""
    lines = [
        f"[airflow-mlops audit #{audit.id} — {audit.status} — scope {audit.scope} "
        f"{audit.period} — {audit.progress()['line']}]"
    ]
    if audit.pending:
        lines.append(
            f"Pending choice from a menu: {audit.pending.get('label')} -> "
            f"{audit.pending.get('then')}"
        )
    if audit.items:
        lines.append(
            "Items: "
            + " · ".join(
                f"{i.number} {i.task_id or 'run'} ({i.category}, {i.kind}) {i.state}"
                for i in audit.ordered_items()
            )
        )
    unplanned = audit.unplanned_candidates()
    if unplanned:
        lines.append("Not diagnosed yet: " + ", ".join(c["dag_id"] for c in unplanned))
    if audit.current:
        lines.append(f"Current item: {audit.current}")
    lines.append(
        "Follow the airflow-mlops:airflow-audit skill: ask the next question with the clarify "
        "tool (options from next_question), report each step with airflow_audit_progress."
    )
    text = "\n".join(lines)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def context_is_relevant(audit: Audit, session_id: str | None, now: float | None = None) -> bool:
    """Inject the reminder in the audit's session, or in any session right after activity."""
    if audit.status != AUDIT_OPEN:
        return False
    ts = time.time() if now is None else now
    if session_id and audit.session_id and session_id == audit.session_id:
        return True
    return ts - audit.updated_at <= CONTEXT_ACTIVE_SECONDS
