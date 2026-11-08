"""Throw-away Docker sandbox to test a proposed DAG fix before presenting it.

Flow of ``airflow_sandbox_test``:
    1. guardrails  (enabled, allowlist, budget, one test at a time, heavy operators)
    2. workspace   copy of ``sandbox_dags_path`` in ``plugin_data_dir``; the user's files
                   are never modified; the unified diff is applied to the copy
    3. container   ``docker run`` with hard limits, no network, no secrets, read-only,
                   launched through ``ctx.dispatch_tool("terminal", ...)`` so Hermes'
                   approval pipeline applies
    4. result      parsed from the ``SANDBOX_RESULT`` line; OOM / timeout detected
    5. cleanup     workspace removed, test journaled in the audit log

Levels: ``import`` < ``structure`` < ``task`` (see sandbox_runner/check_dag.py).
"""

from __future__ import annotations

import json
import logging
import re
import shlex
import shutil
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from plugins.plugin_storage import plugin_data_dir

from . import storage
from .airflow_client import AirflowClient, AirflowError
from .config import PLUGIN_NAME, SANDBOX_LEVELS, Settings
from .redaction import redact_text
from .textutils import excerpt_log
from .views import collect_xcoms

logger = logging.getLogger(__name__)

TOOL_NAME = "airflow_sandbox_test"
RUNNER_SOURCE = Path(__file__).resolve().parent / "sandbox_runner" / "check_dag.py"
SENTINEL = "SANDBOX_RESULT "
_NETWORK_ERROR = re.compile(
    r"Temporary failure in name resolution|Name or service not known|Network is unreachable"
    r"|Connection refused|getaddrinfo failed|nodename nor servname"
)
CONTAINER_ROOT = "/sandbox"

MAX_COPY_FILES = 2000
MAX_COPY_BYTES = 50 * 1024 * 1024
MAX_PATCH_CHARS = 200_000
SKIPPED_DIRS = frozenset(
    {
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        "node_modules",
        "logs",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
    }
)

Dispatcher = Callable[[str, dict[str, Any]], str]


class SandboxError(Exception):
    """The sandbox refused or could not prepare the test (reported as ``sandbox``)."""


# ============================================================================ unified diff


@dataclass
class _Hunk:
    old_start: int
    lines: list[str]


@dataclass
class _FilePatch:
    path: str
    is_new: bool
    hunks: list[_Hunk]


_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,\d+)? \+\d+(?:,\d+)? @@")


def _clean_diff_path(raw: str) -> str:
    path = raw.split("\t", 1)[0].strip()
    if path.startswith(("a/", "b/")):
        path = path[2:]
    return path


def parse_unified_diff(patch: str) -> list[_FilePatch]:
    """Parse a unified diff (``---`` / ``+++`` / ``@@`` hunks). Deletions are refused."""
    files: list[_FilePatch] = []
    lines = patch.splitlines()
    index = 0
    current: _FilePatch | None = None
    while index < len(lines):
        line = lines[index]
        if (
            line.startswith("--- ")
            and index + 1 < len(lines)
            and lines[index + 1].startswith("+++ ")
        ):
            source = _clean_diff_path(line[4:])
            target = _clean_diff_path(lines[index + 1][4:])
            if target == "/dev/null":
                raise SandboxError("The patch deletes a file; deletions are not supported.")
            current = _FilePatch(path=target, is_new=source == "/dev/null", hunks=[])
            files.append(current)
            index += 2
            continue
        match = _HUNK_RE.match(line)
        if match:
            if current is None:
                raise SandboxError("Malformed patch: hunk before any '---'/'+++' header.")
            hunk = _Hunk(old_start=int(match.group(1)), lines=[])
            index += 1
            while index < len(lines):
                body = lines[index]
                next_is_header = index + 1 < len(lines) and lines[index + 1].startswith("+++ ")
                if body.startswith("@@") or (body.startswith("--- ") and next_is_header):
                    break  # next hunk or next file
                if body.startswith("\\"):  # "\ No newline at end of file"
                    index += 1
                    continue
                if body == "" or body[0] in " +-":
                    hunk.lines.append(body if body else " ")
                else:
                    break
                index += 1
            current.hunks.append(hunk)
            continue
        index += 1
    if not files:
        raise SandboxError("The patch contains no file change (expected a unified diff).")
    return files


def _safe_relative(root: Path, raw: str) -> PurePosixPath:
    rel = PurePosixPath(raw.replace("\\", "/"))
    if rel.is_absolute() or ".." in rel.parts or re.match(r"^[A-Za-z]:", raw):
        raise SandboxError(f"Refusing patch path outside the DAGs folder: {raw!r}")
    if (root / rel).resolve().is_relative_to(root.resolve()):
        return rel
    raise SandboxError(f"Refusing patch path outside the DAGs folder: {raw!r}")


def _resolve_target(root: Path, raw: str, is_new: bool) -> Path:
    """Map a diff path onto the copied folder, tolerating extra leading components
    (``dags/train.py`` when the root already is the dags folder)."""
    rel = _safe_relative(root, raw)
    if is_new:
        return root / rel
    for start in range(len(rel.parts)):
        candidate = root.joinpath(*rel.parts[start:])
        if candidate.is_file():
            return candidate
    raise SandboxError(f"Patched file {raw!r} does not exist in the DAGs folder.")


def _find_block(lines: list[str], block: list[str], hint: int, start: int) -> int | None:
    """Index where ``block`` matches (trailing whitespace ignored), nearest to ``hint``."""
    if not block:
        return min(max(hint, start), len(lines))
    wanted = [b.rstrip() for b in block]
    candidates = [
        i
        for i in range(start, len(lines) - len(block) + 1)
        if [line.rstrip() for line in lines[i : i + len(block)]] == wanted
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda i: abs(i - hint))


def apply_unified_diff(root: Path, patch: str) -> list[str]:
    """Apply ``patch`` to files under ``root``; returns the changed relative paths."""
    if len(patch) > MAX_PATCH_CHARS:
        raise SandboxError("The patch is too large.")
    changed: list[str] = []
    for file_patch in parse_unified_diff(patch):
        target = _resolve_target(root, file_patch.path, file_patch.is_new)
        if file_patch.is_new:
            original, newline, trailing = [], "\n", True
        else:
            text = target.read_bytes().decode("utf-8")  # read_text() would drop CRLF
            newline = "\r\n" if "\r\n" in text else "\n"
            trailing = text.endswith(("\n", "\r"))
            original = text.splitlines()
        lines = list(original)
        cursor = 0
        for number, hunk in enumerate(file_patch.hunks, start=1):
            old = [line[1:] for line in hunk.lines if line[0] in " -"]
            new = [line[1:] for line in hunk.lines if line[0] in " +"]
            position = _find_block(lines, old, max(0, hunk.old_start - 1), cursor)
            if position is None:
                raise SandboxError(
                    f"Hunk {number} of {file_patch.path!r} does not apply: its context lines "
                    "were not found. Regenerate the diff from the current file."
                )
            lines[position : position + len(old)] = new
            cursor = position + len(new)
        target.parent.mkdir(parents=True, exist_ok=True)
        body = newline.join(lines) + (newline if trailing and lines else "")
        target.write_text(body, encoding="utf-8", newline="")
        changed.append(target.relative_to(root).as_posix())
    return changed


# ============================================================================ workspace


def copy_dags_folder(source: Path, destination: Path) -> int:
    """Copy the DAGs folder (bounded, without VCS / caches / venvs); returns files copied."""
    count, total = 0, 0
    for path in sorted(source.rglob("*")):
        rel = path.relative_to(source)
        if any(part in SKIPPED_DIRS for part in rel.parts) or path.is_symlink():
            continue
        if path.is_dir():
            continue
        count += 1
        total += path.stat().st_size
        if count > MAX_COPY_FILES or total > MAX_COPY_BYTES:
            raise SandboxError(
                f"The DAGs folder is too large to copy (> {MAX_COPY_FILES} files or "
                f"{MAX_COPY_BYTES // (1024 * 1024)} MB). Point sandbox_dags_path to a smaller "
                "folder."
            )
        target = destination / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    return count


STALE_WORKSPACE_SECONDS = 3600


def purge_stale_workspaces(root: Path, max_age_s: float = STALE_WORKSPACE_SECONDS) -> int:
    """Remove workspaces older than ``max_age_s`` (left behind when the process was killed
    mid-test, so its ``finally`` cleanup never ran). Returns how many were removed."""
    if not root.is_dir():
        return 0
    cutoff = time.time() - max_age_s
    removed = 0
    for entry in root.iterdir():
        try:
            if entry.is_dir() and not entry.is_symlink() and entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                removed += 1
        except OSError:
            continue
    return removed


def host_path(path: Path) -> str:
    """Path as Docker on the host expects it (``C:/x/y`` on Windows, ``/x/y`` elsewhere)."""
    return path.resolve().as_posix()


def build_docker_command(
    settings: Settings,
    workspace: Path,
    *,
    level: str,
    dag_id: str,
    task_id: str | None,
    container_name: str,
) -> str:
    """The exact, bounded ``docker run`` command (POSIX shell; Git Bash on Windows)."""
    budget = max(20, settings.sandbox_timeout_seconds - 15)
    argv = [
        "docker",
        "run",
        "--rm",
        "--name",
        container_name,
        "--network",
        "none",
        "--cpus",
        f"{settings.sandbox_cpus:g}",
        "--memory",
        settings.sandbox_memory,
        "--memory-swap",
        settings.sandbox_memory,
        "--pids-limit",
        "256",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,exec,size=512m",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "--user",
        "50000:0",
        "-v",
        f"{host_path(workspace)}:{CONTAINER_ROOT}:ro",
        "-e",
        "HOME=/tmp",
        "-e",
        "AIRFLOW_HOME=/tmp/airflow",
        "-e",
        f"AIRFLOW__CORE__DAGS_FOLDER={CONTAINER_ROOT}/dags",
        "-e",
        "AIRFLOW__CORE__LOAD_EXAMPLES=False",
        "-e",
        "AIRFLOW__CORE__UNIT_TEST_MODE=False",
        "-e",
        "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=False",
        "-e",
        f"PYTHONPATH={CONTAINER_ROOT}/dags",
        "-e",
        "PYTHONDONTWRITEBYTECODE=1",
        "-e",
        "AIRFLOW_SANDBOX=1",
        "-e",
        f"AIRFLOW_SANDBOX_SAMPLE_ROWS={settings.sandbox_sample_rows}",
        "-e",
        f"AIRFLOW_SANDBOX_FIXTURES={CONTAINER_ROOT}/fixtures/xcoms.json",
        "--entrypoint",
        "timeout",
        settings.sandbox_image,
        "--signal",
        "KILL",
        str(settings.sandbox_timeout_seconds),
        "python",
        f"{CONTAINER_ROOT}/check_dag.py",
        "--level",
        level,
        "--dag-id",
        dag_id,
        "--budget",
        str(budget),
    ]
    if task_id:
        argv += ["--task-id", task_id]
    # MSYS_NO_PATHCONV stops Git Bash (Windows) from rewriting /sandbox paths; harmless elsewhere.
    return "MSYS_NO_PATHCONV=1 " + " ".join(shlex.quote(part) for part in argv)


# ============================================================================ result parsing


def parse_terminal_result(raw: str, settings: Settings) -> dict[str, Any]:
    """Interpret the ``terminal`` tool JSON (``output``/``exit_code``/``error``)."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        data = {"output": str(raw), "exit_code": None, "error": None}
    if not isinstance(data, dict):
        data = {"output": str(data), "exit_code": None, "error": None}
    output = redact_text(str(data.get("output") or ""))
    exit_code = data.get("exit_code")
    terminal_error = data.get("error") or data.get("message")
    result: dict[str, Any] = {"exit_code": exit_code, "timed_out": False, "oom": False}

    parsed: dict[str, Any] | None = None
    for line in reversed(output.splitlines()):
        if line.startswith(SENTINEL):
            try:
                candidate = json.loads(line[len(SENTINEL) :])
                parsed = candidate if isinstance(candidate, dict) else None
            except ValueError:
                parsed = None
            break

    if parsed is not None:
        result.update(passed=bool(parsed.get("passed")), stages=parsed.get("stages", {}))
        task_stage = result["stages"].get("task") if isinstance(result["stages"], dict) else None
        if isinstance(task_stage, dict) and not task_stage.get("passed"):
            task_output = str(task_stage.get("output") or "")
            if task_stage.get("exit_code") in (-9, 137):
                result["oom"] = True
                result["error"] = (
                    "The task was killed: it exceeded the sandbox memory limit "
                    f"({settings.sandbox_memory}). Reduce the data volume (sample) or raise "
                    "sandbox_memory if the task legitimately needs more."
                )
            elif task_stage.get("timed_out"):
                result["timed_out"] = True
                result["error"] = "The task exceeded the sandbox time limit."
            elif _NETWORK_ERROR.search(task_output):
                result["hint"] = (
                    "The task tried to reach the network, which the sandbox never allows. This "
                    "is a sandbox limitation, not necessarily a bug in the fix: feed the task "
                    "from AIRFLOW_SANDBOX_FIXTURES / AIRFLOW_SANDBOX_SAMPLE_ROWS, or rely on the "
                    "'import' and 'structure' levels for this task."
                )
    else:
        result.update(passed=False, stages={})
        if exit_code in (137, -9):
            result["oom"] = True
            result["error"] = (
                f"Killed: the sandbox exceeded its memory limit ({settings.sandbox_memory}) "
                f"or the time limit ({settings.sandbox_timeout_seconds}s)."
            )
        elif exit_code == 124:
            result["timed_out"] = True
            result["error"] = f"Timed out after {settings.sandbox_timeout_seconds}s."
        elif exit_code in (125, 126, 127):
            result["error"] = (
                "Docker could not start the sandbox (is Docker running, is the image "
                f"{settings.sandbox_image!r} available, and is Hermes' terminal backend local?)."
            )
        elif terminal_error:
            result["error"] = (
                f"The terminal tool refused or failed: {redact_text(str(terminal_error))}"
            )
        else:
            result["error"] = "The sandbox produced no result line."
    excerpt = excerpt_log(output, max_lines=60, max_chars=6000, context_lines=30)
    result["log_excerpt"] = excerpt.error_block or excerpt.tail
    return result


# ============================================================================ runner


@dataclass
class SandboxRequest:
    dag_id: str
    patch: str
    level: str = "import"
    task_id: str | None = None
    run_id: str | None = None


def level_rank(level: str) -> int:
    return SANDBOX_LEVELS.index(level)


class SandboxRunner:
    """Prepare, run and journal one sandbox test."""

    def __init__(
        self,
        client: AirflowClient,
        settings: Settings,
        dispatch: Dispatcher,
        *,
        session_id: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        self.client = client
        self.settings = settings
        self.dispatch = dispatch
        self.session_id = session_id
        self.conversation_id = conversation_id

    def run(self, request: SandboxRequest) -> dict[str, Any]:
        try:
            result = self._run(request)
        except SandboxError as exc:
            self._journal(request, "refused", {"error": str(exc)})
            raise
        except AirflowError as exc:
            self._journal(request, "error", exc.to_dict())
            raise
        outcome = "passed" if result.get("passed") else "failed"
        result["audit_id"] = self._journal(
            request,
            outcome,
            {
                "level": request.level,
                "exit_code": result.get("exit_code"),
                "oom": result.get("oom"),
                "timed_out": result.get("timed_out"),
                "patched_files": result.get("patched_files"),
            },
        )
        return result

    # ------------------------------------------------------------------ steps

    def _checks(self, request: SandboxRequest) -> Path:
        settings = self.settings
        if not settings.sandbox_enabled:
            raise SandboxError("The sandbox is disabled (sandbox_enabled: false).")
        if not settings.sandbox_image:
            raise SandboxError("sandbox_image is not configured.")
        dags_path = Path(settings.sandbox_dags_path).expanduser()
        if not settings.sandbox_dags_path or not dags_path.is_dir():
            raise SandboxError("sandbox_dags_path is not configured or is not a folder.")
        if request.level not in SANDBOX_LEVELS:
            raise SandboxError(f"level must be one of {', '.join(SANDBOX_LEVELS)}.")
        if request.level == "task" and not request.task_id:
            raise SandboxError("level 'task' requires task_id.")
        used = storage.count_audit_entries(TOOL_NAME, self.session_id)
        if used >= settings.sandbox_max_tests_per_session:
            raise SandboxError(
                f"Sandbox budget reached for this session ({used}/"
                f"{settings.sandbox_max_tests_per_session} tests). Ask the user before going on."
            )
        return dags_path

    def _run(self, request: SandboxRequest) -> dict[str, Any]:
        dags_path = self._checks(request)
        tasks = self.client.get_tasks(request.dag_id)
        operators = {str(t["task_id"]): str(t.get("operator") or "") for t in tasks}
        if request.level == "task":
            operator = operators.get(str(request.task_id), "")
            if operator in self.settings.sandbox_heavy_operators:
                raise SandboxError(
                    f"Task {request.task_id!r} uses {operator}, listed in "
                    "sandbox_heavy_operators: it is never executed in the sandbox. Use the "
                    "'import' or 'structure' level instead."
                )

        sandbox_root = plugin_data_dir(PLUGIN_NAME) / "sandbox"
        purge_stale_workspaces(sandbox_root)  # leftovers of an interrupted process
        workspace = sandbox_root / uuid.uuid4().hex[:12]
        try:
            dags_copy = workspace / "dags"
            dags_copy.mkdir(parents=True)
            copied = copy_dags_folder(dags_path, dags_copy)
            patched = apply_unified_diff(dags_copy, request.patch)
            shutil.copy2(RUNNER_SOURCE, workspace / "check_dag.py")
            expected = {str(t["task_id"]): list(t.get("downstream_task_ids") or []) for t in tasks}
            (workspace / "expected_structure.json").write_text(json.dumps(expected), "utf-8")
            (workspace / "fixtures").mkdir()
            (workspace / "fixtures" / "xcoms.json").write_text(
                json.dumps(self._fixtures(request), default=str), "utf-8"
            )

            command = build_docker_command(
                self.settings,
                workspace,
                level=request.level,
                dag_id=request.dag_id,
                task_id=request.task_id,
                container_name=f"airflow-mlops-sbx-{workspace.name}",
            )
            started = time.monotonic()
            raw = self.dispatch(
                "terminal",
                {"command": command, "timeout": self.settings.sandbox_timeout_seconds + 60},
            )
            result = parse_terminal_result(raw, self.settings)
            result.update(
                dag_id=request.dag_id,
                level=request.level,
                task_id=request.task_id,
                duration_s=round(time.monotonic() - started, 1),
                patched_files=patched,
                files_copied=copied,
                limits={
                    "cpus": self.settings.sandbox_cpus,
                    "memory": self.settings.sandbox_memory,
                    "timeout_s": self.settings.sandbox_timeout_seconds,
                    "network": "none",
                    "secrets": "none",
                },
            )
            return result
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    def _fixtures(self, request: SandboxRequest) -> dict[str, Any]:
        """XComs of the task's direct upstream producers in the failed run (masked)."""
        if request.level != "task" or not request.run_id or not request.task_id:
            return {}
        fixtures: dict[str, Any] = {}
        try:
            tasks = self.client.get_tasks(request.dag_id)
            upstream = [
                str(t["task_id"])
                for t in tasks
                if request.task_id in (t.get("downstream_task_ids") or [])
            ]
            for task_id in upstream[:5]:
                entries, _ = collect_xcoms(
                    self.client,
                    request.dag_id,
                    request.run_id,
                    task_id,
                    max_chars=self.settings.xcom_max_chars,
                    max_entries=10,
                )
                fixtures[task_id] = {e["key"]: e.get("value") for e in entries}
        except AirflowError:
            logger.info("airflow-mlops: sandbox fixtures unavailable", exc_info=True)
        return fixtures

    def _journal(
        self, request: SandboxRequest, outcome: str, details: dict[str, Any]
    ) -> int | None:
        try:
            return storage.add_audit_entry(
                tool=TOOL_NAME,
                dag_id=request.dag_id,
                run_id=request.run_id,
                task_ids=[request.task_id] if request.task_id else [],
                dry_run=True,  # the sandbox never modifies Airflow
                auto=False,
                reason=f"sandbox {request.level}",
                outcome=outcome,
                details=details,
                session_id=self.session_id,
                conversation_id=self.conversation_id,
            )
        except Exception:
            logger.error("airflow-mlops: could not journal the sandbox test", exc_info=True)
            return None
