"""DAG write lock (plan v2, step 3).

A file of the DAGs folder (``sandbox_dags_path``) is changed only with **the exact change
that passed a sandbox test**, and after a **human approval**. Everywhere else Hermes' own file
and shell tools are untouched.

How it works:

* every passing ``airflow_sandbox_test`` records, per file, the fingerprint of the file before
  the fix and of the fixed file (``tested_changes`` table) and the tested ``.patch``;
* ``pre_tool_call`` on Hermes' ``write_file`` / ``patch`` (and on ``terminal`` /
  ``execute_code`` commands that write in the DAGs folder) compares the content the call would
  produce with the tested one: same content → human approval; untested or different → blocked,
  with the command that applies the tested patch;
* ``post_tool_call`` checks the file once written and journals whether it is the tested change.

Fingerprints ignore line endings (CRLF / LF) and trailing blank lines only.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shlex
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import storage
from .config import Settings

logger = logging.getLogger(__name__)

FILE_TOOLS = frozenset({"write_file", "patch"})
SHELL_TOOLS = frozenset({"terminal", "execute_code"})
TESTED_TTL_SECONDS = 7 * 24 * 3600
# A DAG file is written only once the change passed at least these levels ("import" alone
# only proves that the file loads, not that tasks and dependencies are unchanged).
PROVEN_LEVELS = frozenset({"structure", "task"})
AUDIT_BLOCK = (
    "BLOCKED by airflow-mlops: an audit is in progress, and an audit never changes code "
    "(report, diagnosis, test only). Finish it ('Stop here' archives it); the fix tested in "
    "the sandbox can then be applied, after the user's approval."
)
_V4A_HEADER = re.compile(r"^\*\*\*\s*(?:Update|Add|Delete|Move) File:\s*(.+?)\s*$", re.M)
_GIT_APPLY = re.compile(r"\bgit\b[^\n;&|]*?\bapply\b([^\n;&|]*)")
_WRITE_HINTS = re.compile(
    r"(?<![0-9&])>{1,2}(?!&)|\btee\b|\bsed\s+-i|\b(?:mv|cp|rm|del|copy|move|touch|truncate)\b|"
    r"Set-Content|Add-Content|Out-File|New-Item|Remove-Item|Copy-Item|Move-Item|"
    r"\bpython\b|\bperl\b|\bopen\(|write_text|write_bytes|\.write\(|"
    r"\bgit\s+(?:checkout|restore|reset|stash|apply|am|merge|pull)\b|\bpatch\s+-p",
    re.I,
)


# ============================================================================ fingerprints


def fingerprint(content: str | bytes) -> str:
    """Content identity that ignores line endings and trailing blank lines."""
    text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    text = text.replace("\r\n", "\n").replace("\r", "\n").rstrip("\n") + "\n"
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def path_key(path: str | Path) -> str:
    """Comparable real path (links resolved, case-insensitive on Windows, forward slashes):
    a write through a symbolic link to the DAGs folder is a write in the DAGs folder."""
    return os.path.normcase(os.path.realpath(os.path.expanduser(str(path)))).replace("\\", "/")


@dataclass(frozen=True)
class TestedChange:
    path: str
    dag_id: str
    level: str
    patch_file: str
    before: str | None
    after: str
    tested_at: float

    @property
    def when(self) -> str:
        return time.strftime("%H:%M", time.localtime(self.tested_at))


def record_tested(
    dag_id: str,
    level: str,
    patch_file: str,
    files: list[tuple[Path, bytes | None, bytes]],
    now: float | None = None,
) -> None:
    """Remember a passing sandbox test: ``files`` = (real path, content before, content after)."""
    ts = time.time() if now is None else now
    with storage.connect() as conn:
        for path, before, after in files:
            conn.execute(
                """
                INSERT INTO tested_changes
                    (path, dag_id, level, patch_file, before_fp, after_fp, tested_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    path_key(path),
                    dag_id,
                    level,
                    patch_key(patch_file),
                    fingerprint(before) if before is not None else None,
                    fingerprint(after),
                    ts,
                ),
            )
        conn.execute("DELETE FROM tested_changes WHERE tested_at < ?", (ts - TESTED_TTL_SECONDS,))


def tested_changes(
    path: str | Path | None = None, *, patch_file: str | None = None
) -> list[TestedChange]:
    """Recent passing tests for one file or one patch, most recent first."""
    where: list[str] = ["tested_at >= ?"]
    params: list[Any] = [time.time() - TESTED_TTL_SECONDS]
    if path is not None:
        where.append("path = ?")
        params.append(path_key(path))
    if patch_file is not None:
        where.append("patch_file = ?")
        params.append(patch_key(patch_file))
    with storage.connect() as conn:
        rows = conn.execute(
            "SELECT path, dag_id, level, patch_file, before_fp, after_fp, tested_at "
            f"FROM tested_changes WHERE {' AND '.join(where)} ORDER BY tested_at DESC, id DESC",
            params,
        ).fetchall()
    return [TestedChange(r[0], r[1], r[2], r[3], r[4], r[5], float(r[6])) for r in rows]


def patch_key(patch_file: str | Path) -> str:
    return path_key(patch_file)


def _agent_cwd() -> str:
    """Hermes' working directory (``agent.runtime_cwd``, as the plugin guide asks), else the
    process's one (outside Hermes, e.g. in tests)."""
    try:
        from agent.runtime_cwd import resolve_agent_cwd

        return str(resolve_agent_cwd())
    except Exception:
        return os.getcwd()


def _read(path: str) -> bytes | None:
    try:
        return Path(path).read_bytes()
    except OSError:
        return None


# ============================================================================ the guard


class DagWriteGuard:
    """Decisions for Hermes' ``pre_tool_call`` / ``post_tool_call`` on DAG files."""

    def __init__(
        self,
        settings_provider: Callable[[], Settings],
        cwd_provider: Callable[[], str] | None = None,
        audit_in_progress: Callable[[str | None], bool] | None = None,
    ) -> None:
        self._settings = settings_provider
        self._cwd = cwd_provider or _agent_cwd
        self._audit_in_progress = audit_in_progress

    def _busy(self, session_id: str | None) -> bool:
        """An airflow-mlops audit is in progress: no code change until it is over."""
        try:
            return bool(self._audit_in_progress and self._audit_in_progress(session_id))
        except Exception:
            return False

    # ------------------------------------------------------------------ setup

    def root_text(self) -> str:
        root, _ = self._root()
        return root or ""

    def _root(self) -> tuple[str | None, str]:
        settings = self._settings()
        mode = settings.dag_write_guard
        if mode == "off" or not settings.sandbox_dags_path:
            return None, "off"
        if mode == "sandbox_required" and not settings.sandbox_enabled:
            mode = "approval_only"  # nothing can be tested: a human decides every write
        return path_key(settings.sandbox_dags_path), mode

    def _abs(self, path: str) -> str:
        expanded = os.path.expanduser(path)
        if not os.path.isabs(expanded):
            try:
                base = self._cwd() or _agent_cwd()
            except Exception:
                base = _agent_cwd()
            expanded = os.path.join(base, expanded)
        return path_key(expanded)

    @staticmethod
    def _inside(path: str, root: str) -> bool:
        return path == root or path.startswith(root.rstrip("/") + "/")

    # ------------------------------------------------------------------ pre_tool_call

    def decide(
        self, tool_name: str, args: dict[str, Any], session_id: str | None = None
    ) -> dict[str, Any] | None:
        if tool_name not in FILE_TOOLS | SHELL_TOOLS:
            return None
        root, mode = self._root()
        if root is None:
            return None
        if tool_name in SHELL_TOOLS:
            text = str(args.get("command") if tool_name == "terminal" else args.get("code") or "")
            decision = self._decide_shell(text, root, mode)
            if decision is not None and self._busy(session_id):
                return {"action": "block", "message": AUDIT_BLOCK}
            return decision
        targets = self._file_targets(tool_name, args)
        inside = [(path, result) for path, result in targets if self._inside(path, root)]
        if not inside:
            return None
        if self._busy(session_id):
            return {"action": "block", "message": AUDIT_BLOCK}
        names = ", ".join(Path(p).name for p, _ in inside)
        if mode == "approval_only":
            return _approve(
                f"Change DAG file(s) {names} (dag_write_guard: approval_only, no sandbox check).",
                f"dag_write:{fingerprint(repr(sorted(args.items())))[:12]}",
            )
        approved: list[TestedChange] = []
        for path, result in inside:
            verdict = self._check_file(path, result)
            if isinstance(verdict, str):
                return {"action": "block", "message": verdict}
            approved.append(verdict)
        first = approved[0]
        return _approve(
            f"Apply the sandbox-tested fix to {names} (DAG {first.dag_id}, sandbox "
            f"'{first.level}' passed at {first.when}).",
            f"dag_write:{first.after[:12]}",
        )

    def _file_targets(self, tool_name: str, args: dict[str, Any]) -> list[tuple[str, str | None]]:
        """(absolute path, content after the call or None when it cannot be computed)."""
        if tool_name == "write_file":
            path = args.get("path")
            content = args.get("content")
            return (
                [(self._abs(path), content if isinstance(content, str) else None)] if path else []
            )
        if args.get("mode") == "patch" or (args.get("patch") and not args.get("path")):
            return [(self._abs(p), None) for p in _V4A_HEADER.findall(str(args.get("patch") or ""))]
        path = args.get("path")
        if not isinstance(path, str) or not path:
            return []
        absolute = self._abs(path)
        return [(absolute, _simulate_replace(absolute, args))]

    def _check_file(self, path: str, result: str | None) -> TestedChange | str:
        tests = tested_changes(path)
        name = Path(path).name
        if not tests:
            return (
                f"BLOCKED by airflow-mlops: {name} is a DAG file and this change was not tested. "
                "Test it first with airflow_sandbox_test (a copy, no network); a change that "
                "passed can then be applied, after the user's approval."
            )
        latest = tests[0]
        apply_hint = (
            f"Apply the tested fix exactly with the terminal: git apply {latest.patch_file} "
            f"(run from the DAGs folder; the user approves it)."
        )
        if result is None:
            return (
                f"BLOCKED by airflow-mlops: cannot check that this edit of {name} is the change "
                f"tested in the sandbox. {apply_hint}"
            )
        produced = fingerprint(result)
        matches = [t for t in tests if t.after == produced]
        if not matches:
            return (
                f"BLOCKED by airflow-mlops: this change of {name} differs from the one that "
                f"passed the sandbox at {latest.when}. Test this exact change again with "
                f"airflow_sandbox_test, or apply the tested one. {apply_hint}"
            )
        match = next((t for t in matches if t.level in PROVEN_LEVELS), None)
        if match is None:
            return (
                f"BLOCKED by airflow-mlops: this change of {name} only passed the "
                f"'{matches[0].level}' level. Run airflow_sandbox_test with the same diff and "
                "level='structure' (tasks and dependencies checked) before it can be written."
            )
        current = _read(path)
        if (
            match.before is not None
            and current is not None
            and fingerprint(current)
            not in (
                match.before,
                match.after,
            )
        ):
            return (
                f"BLOCKED by airflow-mlops: {name} changed since the sandbox test at "
                f"{match.when}. Test the fix again on the current file."
            )
        return match

    def _decide_shell(self, text: str, root: str, mode: str) -> dict[str, Any] | None:
        normalized = text.replace("\\", "/")
        # The command concerns the DAGs folder when it names it, or when it runs from inside
        # it (relative paths: ``sed -i ... x.py``, ``git apply fix.patch``).
        touches_root = root.lower() in normalized.lower() or self._inside(self._abs("."), root)
        apply = _GIT_APPLY.search(normalized)
        if apply:
            options = apply.group(1)
            if "--check" in options or "--stat" in options:
                return None  # dry check: reads only
            patch = _patch_argument(options)
            tests = tested_changes(patch_file=patch) if patch else []
            if tests:
                unproven = [t for t in tests if not _proven(t)]
                if unproven and not _reverse(options) and mode == "sandbox_required":
                    return {
                        "action": "block",
                        "message": f"BLOCKED by airflow-mlops: this patch only passed the "
                        f"'{unproven[0].level}' level. Run airflow_sandbox_test with "
                        "level='structure' first, then apply the patch it saves.",
                    }
                stale = [t for t in tests if not _still_applies(t, reverse=_reverse(options))]
                if stale and mode == "sandbox_required":
                    return {
                        "action": "block",
                        "message": "BLOCKED by airflow-mlops: the DAG file changed since this "
                        "patch was tested. Test the fix again on the current file.",
                    }
                verb = "Undo" if _reverse(options) else "Apply"
                names = ", ".join(sorted({Path(t.path).name for t in tests}))
                return _approve(
                    f"{verb} the sandbox-tested patch on {names} (DAG {tests[0].dag_id}, "
                    f"tested at {tests[0].when}).",
                    f"dag_patch:{fingerprint(tests[0].patch_file)[:12]}:{verb.lower()}",
                )
            if touches_root and mode == "sandbox_required":
                return {
                    "action": "block",
                    "message": "BLOCKED by airflow-mlops: this patch was not tested in the "
                    "sandbox. Test it with airflow_sandbox_test first.",
                }
        if touches_root and _WRITE_HINTS.search(normalized):
            return _approve(
                "This command may change files of the DAGs folder; airflow-mlops cannot check "
                "it against a sandbox test. Approve only if you know what it does.",
                f"dag_shell:{fingerprint(text)[:12]}",
            )
        return None

    # ------------------------------------------------------------------ post_tool_call

    def after(self, tool_name: str, args: dict[str, Any], result: Any = None) -> None:
        """Once a DAG file is written: journal whether it is the tested change."""
        try:
            root, _ = self._root()
            if root is None:
                return
            paths: list[str] = []
            if tool_name in FILE_TOOLS:
                paths = [p for p, _ in self._file_targets(tool_name, args) if self._inside(p, root)]
            elif tool_name == "terminal":
                text = str(args.get("command") or "").replace("\\", "/")
                apply = _GIT_APPLY.search(text)
                if apply and "--check" not in apply.group(1):
                    patch = _patch_argument(apply.group(1))
                    paths = [t.path for t in tested_changes(patch_file=patch)] if patch else []
            for path in dict.fromkeys(paths):
                self._observe_written(path)
        except Exception:
            logger.warning("airflow-mlops: could not check a DAG write", exc_info=True)

    def _observe_written(self, path: str) -> None:
        content = _read(path)
        tests = tested_changes(path)
        if content is None or not tests:
            return
        produced = fingerprint(content)
        match = next((t for t in tests if t.after == produced), None)
        storage.add_audit_entry(
            tool="dag_write",
            dag_id=(match or tests[0]).dag_id,
            run_id=None,
            task_ids=[],
            dry_run=False,
            auto=False,
            reason=Path(path).name,
            outcome="matches_tested_fix" if match else "differs_from_tested_fix",
        )


def _proven(test: TestedChange) -> bool:
    """The same change (same file, same result) passed at least the 'structure' level, in
    this run or another one (each sandbox run saves its own patch file)."""
    if test.level in PROVEN_LEVELS:
        return True
    return any(
        t.after == test.after and t.level in PROVEN_LEVELS for t in tested_changes(test.path)
    )


def _approve(message: str, rule_key: str) -> dict[str, Any]:
    return {"action": "approve", "message": message, "rule_key": rule_key}


def _simulate_replace(path: str, args: dict[str, Any]) -> str | None:
    """Content after Hermes' ``patch`` (replace mode), or None if it cannot be reproduced."""
    old, new = args.get("old_string"), args.get("new_string")
    if not isinstance(old, str) or not isinstance(new, str) or not old:
        return None
    raw = _read(path)
    if raw is None:
        return None
    text = raw.decode("utf-8", errors="replace").replace("\r\n", "\n")
    old, new = old.replace("\r\n", "\n"), new.replace("\r\n", "\n")
    if old not in text:
        return None
    return text.replace(old, new) if args.get("replace_all") else text.replace(old, new, 1)


def _patch_argument(options: str) -> str | None:
    try:
        tokens = shlex.split(options)
    except ValueError:
        tokens = [t.strip("'\"") for t in options.split()]
    candidates = [t for t in tokens if t.lower().endswith(".patch") or t.lower().endswith(".diff")]
    return candidates[-1] if candidates else None


def _reverse(options: str) -> bool:
    return bool(re.search(r"(^|\s)(-R|--reverse)(\s|$)", options))


def _still_applies(test: TestedChange, *, reverse: bool) -> bool:
    current = _read(test.path)
    if current is None:
        return test.before is None and not reverse
    fp = fingerprint(current)
    return fp == (test.after if reverse else test.before) or (not reverse and fp == test.after)
