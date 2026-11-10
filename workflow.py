"""Guided audit: the incident dossier, its numbered items and the questions between steps.

The rule (user's decision, 2026-11-09): **an audit never changes code.**

1. report (state of play) → 2. diagnosis of the chosen DAGs, numbered problems →
3. test of each proposal: a transient failure is retried for real (after the user's approval);
   a code error gets a fix tested in the sandbox (a copy) → after each test, the problems left
   are offered, or "Stop here".

Leaving the audit ("Stop here", or everything tested) archives it: no resume, the next
``/airflow-audit`` starts again from the report. A short summary of the tested fixes stays
known in the conversation, so Hermes can apply one later, on request, under the DAG write lock.

The plugin computes every question (at most 4 options, recommended first); the model only
writes them as numbered text at the end of its reply. Nothing here calls Airflow.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any

from . import storage
from .display import ARCHIVE, FAIL, LATER, OK, WAITING, WORKING

# Item states.
STATES = ("new", "fix_proposed", "retried", "passed", "failed", "skipped")
OPEN_STATES = frozenset({"new", "fix_proposed", "retried"})  # still to test
DONE_STATES = frozenset({"passed", "failed", "skipped"})
PROGRESS_STATES = frozenset(STATES) - {"new"}
AUDIT_OPEN, AUDIT_ARCHIVED = "open", "archived"
STOP_LABEL = "Stop here"
STOP_THEN = (
    'Call airflow_audit_progress with state="stop": the audit is archived; show its summary.'
)
MAX_CHOICES = 4  # a short list the user answers with a number
OPEN_SECONDS = 24 * 3600  # an open audit older than this is ignored
CONTEXT_ACTIVE_SECONDS = 30 * 60
ARCHIVE_CONTEXT_SECONDS = 3 * 3600
_CATEGORY_RANK = {"critical": 0, "unknown": 1, "transient": 2}
_STATUS_RANK = {"failing": 0, "degraded": 1}

STATE_TEXT = {
    "new": "to test",
    "fix_proposed": "fix proposed",
    "retried": "retried, checking the run",
    "passed": "test passed",
    "failed": "test failed",
    "skipped": "skipped",
}


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
    result: dict[str, Any] | None = None  # tested fix: patch_file, apply, check, level

    @property
    def is_open(self) -> bool:
        return self.state in OPEN_STATES

    @property
    def crashes(self) -> bool:
        """A code error that makes a task fail (not the masking trigger_rule item)."""
        return self.kind != "retry" and self.task_id is not None

    def label(self) -> str:
        return f"{self.number} {self.task_id or 'run'} — {self.title}"

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
            "result": self.result,
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
        """Problems still to test: selected ones first, then the plan order."""
        items = [i for i in self.ordered_items() if i.is_open]
        return sorted(items, key=lambda i: (not i.selected, i.dag_index, i.position))

    def has_work_left(self) -> bool:
        return bool(self.open_items() or self.unplanned_candidates())

    def progress(self) -> dict[str, Any]:
        total = len(self.items)
        done = sum(1 for i in self.items if i.state in DONE_STATES)
        nxt = self.open_items()
        return {
            "done": done,
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
    """``[###----] 3/7 tested · next: 1.2`` (plain ASCII: same width everywhere)."""
    width = 7
    filled = round(width * done / total) if total else 0
    text = f"[{'#' * filled}{'-' * (width - filled)}] {done}/{total} tested"
    return text + (f" · next: {next_number}" if next_number else "")


# ============================================================================ persistence

_AUDIT_COLUMNS = (
    "id, created_at, updated_at, session_id, scope, period, status, candidates, pending, current"
)


def create_audit(
    scope: str,
    period: str,
    candidates: list[dict[str, Any]],
    session_id: str | None,
    now: float | None = None,
) -> Audit:
    """Open a new audit; an audit still open is archived (no resume)."""
    ts = time.time() if now is None else now
    with storage.connect() as conn:
        conn.execute(
            "UPDATE audits SET status = ?, updated_at = ? WHERE status = ?",
            (AUDIT_ARCHIVED, ts, AUDIT_OPEN),
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
    """The audit in progress (open), if any."""
    ts = time.time() if now is None else now
    return _load(
        f"SELECT {_AUDIT_COLUMNS} FROM audits WHERE status = ? AND updated_at >= ? "
        "ORDER BY updated_at DESC, id DESC LIMIT 1",
        (AUDIT_OPEN, ts - OPEN_SECONDS),
    )


def load_audit(audit_id: int | None = None) -> Audit | None:
    """One audit by id (any status), or the most recent one: for the postmortem."""
    if audit_id is None:
        return _load(f"SELECT {_AUDIT_COLUMNS} FROM audits ORDER BY id DESC LIMIT 1", ())
    return _load(f"SELECT {_AUDIT_COLUMNS} FROM audits WHERE id = ?", (audit_id,))


def _load(query: str, params: tuple[Any, ...]) -> Audit | None:
    with storage.connect() as conn:
        row = conn.execute(query, params).fetchone()
        if row is None:
            return None
        rows = conn.execute(
            """
            SELECT number, dag_index, position, dag_id, run_id, task_id, category, kind, title,
                   state, selected, note, updated_at, result
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
        status=AUDIT_OPEN if str(row[6]) == AUDIT_OPEN else AUDIT_ARCHIVED,
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
            state=str(r[9]) if str(r[9]) in STATES else "skipped",
            selected=bool(r[10]),
            note=r[11],
            updated_at=float(r[12]),
            result=_loads(r[13], None),
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
                    task_id, category, kind, title, state, selected, note, updated_at, result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (audit_id, number) DO UPDATE SET
                    state = excluded.state, selected = excluded.selected,
                    note = excluded.note, updated_at = excluded.updated_at,
                    run_id = excluded.run_id, title = excluded.title,
                    result = excluded.result
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
                    json.dumps(item.result) if item.result else None,
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
            test = {
                "code_fix": "test: a fix in the sandbox (copy, no code change)",
                "retry": "test: a real retry, after your approval",
                "investigate": "test: a fix in the sandbox (unclear cause)",
            }[item.kind]
            lines.append(
                f"  {item.number} {item.task_id or 'run'} — {item.category} — {item.title}"
                f"\n      -> {test} [{STATE_TEXT[item.state]}]"
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
    raw = (text or "").strip()
    if not raw:
        raise SelectionError("Empty selection.")
    excluded: set[str] = set()
    exception = re.search(r"(?i)(?:^|\s)except\s", raw)
    if exception:
        excluded = set(parse_selection(raw[exception.end() :], audit)[0])
        raw = raw[: exception.start()].strip() or "all"
    numbers: list[str] = []
    to_plan: list[str] = []
    # "and" / "et" separate only between spaces: DAG ids such as extract-and-load stay whole.
    for token in re.split(r"(?i)\s*[,;]\s*|\s+(?:and|et)\s+", raw):
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
    typed, token = token, token.lower()  # keywords in any case; a new DAG id keeps its case
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
        return [], [typed]  # a DAG id not in the audit yet: plan it (allowlist checked later)
    raise SelectionError(f"Cannot understand {token!r}.")


def _dag_selection(audit: Audit, dag_id: str) -> tuple[list[str], list[str]]:
    items = [i.number for i in audit.items_of(dag_id) if i.is_open]
    return (items, []) if audit.items_of(dag_id) else ([], [dag_id])


# ============================================================================ questions


def option(
    label: str,
    then: str,
    description: str | None = None,
    *,
    kind: str = "model",
    value: str | None = None,
) -> dict[str, str]:
    """One choice: ``then`` is the instruction for the model; ``kind`` / ``value`` let the
    slash command run the deterministic steps itself (plan, select, stop)."""
    entry = {"label": label, "then": then, "kind": kind}
    if description:
        entry["description"] = description
    if value is not None:
        entry["value"] = value
    return entry


def stop_option() -> dict[str, str]:
    return option(STOP_LABEL, STOP_THEN, "archives the audit", kind="stop")


def make_question(
    text: str, options: list[dict[str, str]], typed: str | None = None
) -> dict[str, Any]:
    """A question the model writes at the end of its reply: ``choices`` are the labels,
    recommended first. Never through a menu tool in the same turn: a model that calls a tool
    right after its text often sends the tool call alone, and the text is never displayed."""
    options = options[:MAX_CHOICES]
    question: dict[str, Any] = {
        "question": text,
        "choices": [o["label"] for o in options],
        "recommended": options[0]["label"],
        "options": options,
        "how_to_ask": "First write what this step produced (report, plan, fix, result). END "
        "your reply with this question and its choices as a numbered list, then stop: no "
        "tool call after the text, no menu tool. When the user answers (a number, a label "
        "or their own words), do the 'then' of the chosen option.",
    }
    if typed:
        question["typed_answers"] = typed
    return question


def pending_question(question: dict[str, Any]) -> dict[str, str]:
    """A question shown as text by a slash command, as the model must know it when the user
    answers: every choice with what to do for it."""
    choices = " | ".join(
        f"{number}. {o['label']} -> {o['then']}"
        for number, o in enumerate(question["options"], start=1)
    )
    return {"label": str(question["question"]).splitlines()[-1], "then": choices}


def _plan_then(dag_ids: list[str]) -> str:
    return f"Call airflow_audit_plan with dag_ids={json.dumps(dag_ids)}."


def _plan_option(label: str, dag_ids: list[str], description: str | None = None) -> dict[str, str]:
    return option(label, _plan_then(dag_ids), description, kind="plan", value=",".join(dag_ids))


def _select_option(label: str, selection: str, description: str | None = None) -> dict[str, str]:
    return option(
        label,
        f'Call airflow_audit_select with selection="{selection}".',
        description,
        kind="select",
        value=selection,
    )


def question_after_report(
    candidates: list[dict[str, Any]], state_of_play: str | None = None
) -> dict[str, Any] | None:
    """Which DAG should I diagnose? (None when nothing needs a diagnosis). The state of play
    leads the question, so it is seen even where a separate message is not shown."""
    if not candidates:
        return None
    ids = [c["dag_id"] for c in candidates]
    first = candidates[0]
    options = [_plan_option(first["dag_id"], [first["dag_id"]], f"recommended: {_why(first)}")]
    if len(candidates) == 2:
        second = candidates[1]
        options.append(_plan_option(second["dag_id"], [second["dag_id"]], _why(second)))
    elif len(candidates) > 2:
        critical = [c["dag_id"] for c in candidates if c["category"] == "critical" or c["masked"]]
        if 1 < len(critical) < len(candidates):
            options.append(_plan_option(f"All critical ({len(critical)})", critical))
        else:
            second = candidates[1]
            options.append(_plan_option(second["dag_id"], [second["dag_id"]], _why(second)))
    if len(candidates) > 1:
        options.append(_plan_option(f"All ({len(candidates)})", ids))
    options.append(stop_option())
    text = "Which DAG should I diagnose?"
    if state_of_play:
        text = f"{state_of_play}\n{text}"
    return make_question(
        text,
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
    items = audit.open_items()
    if not items:
        return None
    options = [_select_option(items[0].label(), items[0].number, "recommended")]
    if len(items) > 1:
        options.append(_select_option(items[1].label(), items[1].number))
    first_dag = [i for i in items if i.dag_id == items[0].dag_id]
    if len(first_dag) > 1 and len(first_dag) < len(items):
        options.append(
            _select_option(
                f"All of DAG {items[0].dag_index} ({len(first_dag)})",
                f"DAG {items[0].dag_index}",
            )
        )
    elif len(items) > 2 or (len(items) == 2 and len(first_dag) == 2):
        options.append(_select_option(f"All ({len(items)})", "all"))
    options.append(stop_option())
    return make_question(
        "Which problem first?",
        options,
        typed="item numbers, e.g. '1.1, 2.3', '1.1-1.3', 'DAG 2', 'all critical', "
        "'all except 2.1' (then call airflow_audit_select with that text).",
    )


def group_of(audit: Audit, item: Item) -> list[Item]:
    """Selected code fixes of the same DAG at the same step: proposed and tested together
    (one read of the code, one sandbox run)."""
    if item.kind == "retry" or not item.selected:
        return [item]
    members = [
        i
        for i in audit.ordered_items()
        if i.selected
        and i.is_open
        and i.dag_id == item.dag_id
        and i.kind != "retry"
        and i.state == item.state
    ]
    return members if any(i.number == item.number for i in members) else [item]


def code_errors(audit: Audit, dag_id: str) -> list[Item]:
    """Code errors that make a task of this DAG fail: no code changes during an audit, so a
    retry of the DAG cannot work while one of them is there."""
    return [i for i in audit.items_of(dag_id) if i.crashes and i.state != "skipped"]


def retry_group(audit: Audit, item: Item) -> list[Item]:
    """Selected transient failures ready to retry now (no code error on their DAG)."""
    if item.kind != "retry" or not item.selected:
        return [item]
    members = [
        i
        for i in audit.ordered_items()
        if i.selected
        and i.kind == "retry"
        and i.state == "new"
        and not code_errors(audit, i.dag_id)
    ]
    return members if any(i.number == item.number for i in members) else [item]


def _numbers(items: list[Item]) -> tuple[str, str]:
    """``("1.1 + 1.2", "1.1, 1.2")``: the label shown, the value given to the tools."""
    numbers = [i.number for i in items]
    return " + ".join(numbers), ", ".join(numbers)


def next_step(audit: Audit, item: Item | None, *, sandbox_enabled: bool) -> dict[str, Any]:
    """What happens next for an item: an instruction for the model, or a question."""
    if item is None or not item.is_open:
        if item is not None and item.state == "failed":
            return {"next_question": question_failed(audit, item)}
        return after_test(audit, item)
    dag, run, task = item.dag_id, item.run_id, item.task_id
    group = group_of(audit, item)
    many = len(group) > 1
    n, values = _numbers(group)
    progress = f'airflow_audit_progress with item="{values}"'
    if item.kind == "retry":
        if item.state == "retried":
            return {
                "next_action": (
                    f'Check the run with airflow_get_run_status(dag_id="{dag}", run_id="{run}") '
                    f'until the tasks finish. Success: call {progress}, state="passed". Failed '
                    f'again: call {progress}, state="failed".'
                )
            }
        blockers = code_errors(audit, dag)
        if blockers:
            return {
                "next_question": question_remaining(
                    audit,
                    f"{n} cannot be retried during the audit: {dag} has a code error "
                    f"({', '.join(b.number for b in blockers)}) and the code is not changed "
                    "during an audit. Retry it once the fix is applied. What next?",
                    exclude={item.number},
                )
            }
        return _retry_question(audit, item)
    # code fix / investigation: the tools first (read, test on a copy), the text last. A reply
    # whose text is followed by a tool call is often sent without the text.
    present = (
        "Only then write ONE reply: the fix (airflow-fix-proposal step A: cause, file, lines, "
        "before/after, risks), the result, and END with the next_question that call returned. "
        "No text before the tools. Never edit a file during an audit."
    )
    if not sandbox_enabled:
        return {
            "next_action": (
                f"{'Fixes' if many else 'Fix'} {n}: read the code with airflow_get_dag_source("
                f'dag_id="{dag}"). The sandbox is disabled, so the fix cannot be tested: call '
                f'{progress}, state="skipped", note="not tested (sandbox disabled)". {present}'
            )
        }
    diff = "ONE diff containing every fix" if many else "the diff"
    task_arg = f', task_id="{task}"' if task and not many else ""
    # "structure" loads the patched DAG first: it includes the "import" level.
    test = (
        f'test it with airflow_sandbox_test(dag_id="{dag}", {diff}, level="structure"'
        f"{task_arg}). The plugin records the result. {present}"
    )
    if item.state == "fix_proposed":
        return {"next_action": f"Now {test}"}
    return {
        "next_action": (
            f"{'Fixes' if many else 'Fix'} {n}: read the code with airflow_get_dag_source("
            f'dag_id="{dag}"), write the diff, then {test}'
        )
    }


def _retry_question(audit: Audit, item: Item) -> dict[str, Any]:
    """The test of a transient failure is a real retry: one preview, one decision."""
    group = retry_group(audit, item)
    n, values = _numbers(group)
    progress = f'airflow_audit_progress with item="{values}"'
    calls = "; ".join(
        f'dag_id="{i.dag_id}", run_id="{i.run_id}", task_ids=["{i.task_id}"]' for i in group
    )
    many = len(group) > 1
    return {
        "next_question": make_question(
            f"Retry the {len(group)} transient failures ({n})?"
            if many
            else f"{n}: {item.task_id} failed for a transient reason. Retry it?",
            [
                option(
                    "Preview all retries (dry run)" if many else "Preview the retry (dry run)",
                    f"Call airflow_clear_tasks with dry_run=true for: {calls}. Show the list, "
                    "then ask once before the real retry (Hermes asks the user to approve). The "
                    "plugin records it; then check the run.",
                    "recommended",
                    kind="retry",
                    value=values,
                ),
                option(
                    "Skip it" if not many else "Skip them",
                    f'Call {progress}, state="skipped".',
                    kind="progress",
                    value=f"skipped:{values}",
                ),
                stop_option(),
            ],
        )
    }


def after_test(audit: Audit, item: Item | None) -> dict[str, Any]:
    """After a test (or a skip): the problems left, or the end of the audit."""
    if not audit.has_work_left():
        return {"finished": True, "next_action": "Everything is tested. " + STOP_THEN}
    header = "What next?"
    if item is not None and item.state == "passed":
        header = f"{OK} {item.number} works ({_how(item)}). Problems left:"
    return {"next_question": question_remaining(audit, header)}


def _how(item: Item) -> str:
    if item.kind == "retry":
        return "the retry succeeded"
    level = (item.result or {}).get("level", "structure")
    return f"sandbox '{level}' passed, your file is unchanged"


def question_failed(audit: Audit, item: Item) -> dict[str, Any]:
    """The test failed: revise, move on, or stop."""
    options = []
    if item.kind != "retry":
        options.append(
            option(
                "Revise the fix",
                f'Call airflow_audit_progress with item="{item.number}", state="fix_proposed"; '
                "read stages / log_excerpt of the sandbox result, correct the diff and test it "
                "again.",
                "recommended",
                kind="revise",
                value=item.number,
            )
        )
    left = [i for i in audit.open_items() if i.number != item.number]
    if left:
        options.append(_select_option(f"Next problem: {left[0].label()}", left[0].number))
    elif audit.unplanned_candidates():
        dag = audit.unplanned_candidates()[0]["dag_id"]
        options.append(_plan_option(f"Diagnose {dag}", [dag]))
    options.append(stop_option())
    why = (
        "the retry failed again: probably not transient; it needs a code fix after the audit"
        if item.kind == "retry"
        else "the sandbox test failed"
    )
    return make_question(f"{FAIL} {item.number}: {why}. What now?", options)


def question_remaining(
    audit: Audit, text: str, *, exclude: set[str] | None = None
) -> dict[str, Any]:
    """The problems left (same menu as after the diagnosis), a DAG still to diagnose,
    "Stop here"."""
    pool = [i for i in audit.open_items() if i.number not in (exclude or set())]
    options: list[dict[str, str]] = []
    room = MAX_CHOICES - 1
    unplanned = audit.unplanned_candidates()
    if unplanned:
        room -= 1
    shown = pool[: room - 1] if len(pool) > room >= 2 else pool[:room]
    for index, entry in enumerate(shown):
        where = f"DAG {entry.dag_index} — {entry.dag_id}"
        options.append(
            _select_option(
                entry.label(), entry.number, f"recommended, {where}" if index == 0 else where
            )
        )
    if len(pool) > len(shown):
        options.append(_select_option(f"All remaining ({len(pool)})", "all"))
    if unplanned:
        options.append(
            _plan_option(
                f"Diagnose {unplanned[0]['dag_id']}",
                [unplanned[0]["dag_id"]],
                _why(unplanned[0]),
            )
        )
    options.append(stop_option())
    return make_question(
        text,
        options,
        typed="item numbers, e.g. '1.2, 2.1', 'DAG 2', 'all' (then call airflow_audit_select).",
    )


def question_masking(audit: Audit, item: Item, warnings: list[dict[str, Any]]) -> dict[str, Any]:
    """The fix passed the sandbox but looks like it hides the error: say it first.
    (Real case: `frame["target"]` -> `frame.get("target")` loads and keeps the structure, so
    every sandbox level passes, while the model would train without its target.)"""
    first = warnings[0]
    left = [i for i in audit.open_items() if i.number != item.number]
    options = [
        option(
            "Revise the fix",
            f'Call airflow_audit_progress with item="{item.number}", state="fix_proposed"; '
            "explain the risk in one sentence, propose a fix of the cause (fix the producer, or "
            "validate the input and fail with a clear message) and test it again.",
            "recommended",
            kind="revise",
            value=item.number,
        ),
        option(
            "Keep it and go on",
            "Say that the fix hides the error, then ask the question returned by "
            f'airflow_audit_progress with item="{item.number}", state="passed".',
            kind="progress",
            value=f"passed:{item.number}",
        ),
    ]
    if left:
        options.append(_select_option(f"Next problem: {left[0].label()}", left[0].number))
    options.append(stop_option())
    return make_question(
        f"{item.number} passed the sandbox, but the fix may hide the error instead of fixing it "
        f"({first['file']}:{first['line']}: {first['why']}). What now?",
        options,
    )


# ============================================================================ summaries


def board_text(audit: Audit, activity: str) -> str:
    """The live progress board of the batch being handled (the selected items)."""
    items = [i for i in audit.ordered_items() if i.selected] or audit.ordered_items()
    done = sum(1 for i in items if i.state in DONE_STATES)
    lines = [f"Audit #{audit.id} {progress_line(done, len(items))} · {activity}"]
    for item in items:
        if item.state == "passed":
            icon = OK
        elif item.state == "failed":
            icon = FAIL
        elif item.number == audit.current:
            icon = WORKING
        elif item.state == "skipped":
            icon = LATER
        else:
            icon = WAITING
        lines.append(f"  {icon} {item.label()} — {STATE_TEXT[item.state]}")
    return "\n".join(lines)


def summary_lines(audit: Audit) -> list[str]:
    lines = []
    for item in audit.ordered_items():
        label = item.label()
        if item.state == "passed" and item.kind == "retry":
            lines.append(f"{OK} {label} — retried: the run succeeded")
        elif item.state == "passed":
            apply = (item.result or {}).get("apply")
            text = f"{OK} {label} — fix tested in the sandbox ({_how(item)})"
            if apply:
                text += f"\n     apply it on request: {apply}"
            lines.append(text)
        elif item.state == "failed":
            lines.append(f"{FAIL} {label} — test failed (to review)")
        elif item.state == "skipped":
            lines.append(f"{LATER} {label} — skipped{f' ({item.note})' if item.note else ''}")
        else:
            lines.append(f"{WAITING} {label} — not tested")
    return lines


def stop_summary(audit: Audit) -> str:
    lines = [f"{ARCHIVE} Audit #{audit.id} archived. {audit.progress()['line']}"]
    lines += summary_lines(audit)
    unplanned = audit.unplanned_candidates()
    if unplanned:
        lines.append("Not diagnosed: " + ", ".join(c["dag_id"] for c in unplanned))
    lines.append(
        "No code was changed during the audit. Ask me to apply a tested fix: the lock lets "
        "only that change through, after your approval. /airflow-audit starts a new audit."
    )
    return "\n".join(lines)


def tested_fixes(audit: Audit) -> list[Item]:
    """Code fixes whose sandbox test passed, with their apply command."""
    return [
        i
        for i in audit.ordered_items()
        if i.state == "passed" and i.kind != "retry" and (i.result or {}).get("apply")
    ]


def question_apply(audit: Audit) -> dict[str, Any] | None:
    """At the end of the audit (archived): offer to have Hermes apply the tested fixes.
    None when no fix passed its test."""
    fixes = tested_fixes(audit)
    if not fixes:
        return None
    numbers = " + ".join(i.number for i in fixes)
    commands = list(dict.fromkeys(str((i.result or {})["apply"]) for i in fixes))
    dags = ", ".join(dict.fromkeys(i.dag_id for i in fixes))
    many = len(fixes) > 1
    return make_question(
        f"{OK} Audit #{audit.id} archived. Ask Hermes to apply the tested "
        f"{'fixes' if many else 'fix'} {numbers} now?",
        [
            option(
                f"Apply {numbers} (your approval required)",
                "Apply exactly the tested patch(es) with the terminal tool, one by one: "
                + " ; ".join(commands)
                + ". The airflow-mlops lock lets only these tested changes through and Hermes "
                "asks the user to approve each. Then read the changed lines back and offer the "
                f"targeted retry of {dags} (airflow_clear_tasks, dry run first).",
                "recommended: they passed the sandbox",
                kind="apply",
                value=", ".join(i.number for i in fixes),
            ),
            option(
                "Not now",
                "Say the tested fixes stay available: the user can ask later in this "
                "conversation ('apply fix 1.1').",
                kind="done",
            ),
        ],
    )


def context_summary(audit: Audit, limit: int = 2500) -> str:
    """Short reminder injected before each model reply (``pre_llm_call``)."""
    if audit.status != AUDIT_OPEN:
        lines = [f"[airflow-mlops — last audit #{audit.id}, archived: results]"]
        if audit.pending:
            lines.append(
                f"Question asked, waiting for the user's answer: {audit.pending.get('label')} "
                f"Choices: {audit.pending.get('then')}"
            )
        lines += summary_lines(audit)
        lines.append(
            "The audit is over. If the user asks to apply a fix that passed the sandbox, apply "
            "exactly the tested patch (git apply, the lock asks the user). Never claim a fix "
            "works if it was not tested."
        )
    else:
        lines = [
            f"[airflow-mlops audit #{audit.id} in progress — scope {audit.scope} "
            f"{audit.period} — {audit.progress()['line']}]"
        ]
        if audit.pending:
            lines.append(
                f"Question asked, waiting for the user's answer: {audit.pending.get('label')} "
                f"Choices: {audit.pending.get('then')}"
            )
        if audit.items:
            lines.append(
                "Items: "
                + " · ".join(
                    f"{i.number} {i.task_id or 'run'} ({i.category}, {i.kind}) "
                    f"{STATE_TEXT[i.state]}"
                    for i in audit.ordered_items()
                )
            )
        unplanned = audit.unplanned_candidates()
        if unplanned:
            lines.append("Not diagnosed yet: " + ", ".join(c["dag_id"] for c in unplanned))
        lines.append(
            "Rules: no code change during an audit (report, diagnosis, test only); write the "
            "next question as numbered text at the end of your reply. This reminder never "
            "overrides an explicit request of the user."
        )
    text = "\n".join(lines)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def context_is_relevant(audit: Audit, session_id: str | None, now: float | None = None) -> bool:
    """Open audit: its session, or any session right after activity. Archived audit: only its
    own session, for a few hours (so Hermes still knows the tested fixes)."""
    ts = time.time() if now is None else now
    same = bool(session_id and audit.session_id and session_id == audit.session_id)
    if audit.status == AUDIT_OPEN:
        if audit.session_id and session_id:
            return same  # never remind another conversation (or a scheduled job) of this audit
        return ts - audit.updated_at <= CONTEXT_ACTIVE_SECONDS
    return same and ts - audit.updated_at <= ARCHIVE_CONTEXT_SECONDS
