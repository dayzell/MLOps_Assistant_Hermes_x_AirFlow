"""Read the Python source of a DAG so the agent can write fixes that actually apply.

Origin, in order of preference:
    1. the local DAGs folder (``sandbox_dags_path``) - the code the sandbox patches and the
       user commits; the tool also says whether it differs from what Airflow runs;
    2. the Airflow REST API (the deployed code).

The text is secret-masked and size-bounded. Masked lines contain ``[REDACTED]`` and cannot be
used as diff context.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

from .airflow_client import AirflowClient, AirflowError
from .config import Settings
from .redaction import redact_code

MAX_SOURCE_LINES = 600
MAX_SOURCE_CHARS = 60_000
_SKIPPED_PARTS = frozenset({".git", "__pycache__", ".venv", "venv", "node_modules"})


def locate_local_file(root: Path, fileloc: str | None, relative_fileloc: str | None) -> Path | None:
    """Find the DAG file inside the local folder ``root``.

    Tries ``relative_fileloc`` (Airflow 3), then the longest suffix of ``fileloc`` (e.g.
    ``/opt/airflow/dags/team/x.py`` -> ``team/x.py`` then ``x.py``). Never leaves ``root``.
    """
    resolved_root = root.resolve()
    candidates: list[PurePosixPath] = []
    if relative_fileloc:
        candidates.append(PurePosixPath(relative_fileloc.replace("\\", "/")))
    if fileloc:
        parts = PurePosixPath(fileloc.replace("\\", "/")).parts
        candidates += [PurePosixPath(*parts[i:]) for i in range(len(parts)) if parts[i:]]
    for rel in candidates:
        if rel.is_absolute() or ".." in rel.parts or _SKIPPED_PARTS.intersection(rel.parts):
            continue
        path = (root / rel).resolve()
        if path.is_file() and path.is_relative_to(resolved_root):
            return path
    return None


def read_dag_source(
    client: AirflowClient,
    settings: Settings,
    dag_id: str,
    *,
    start_line: int | None = None,
    end_line: int | None = None,
) -> dict[str, Any]:
    details = client.get_dag_details(dag_id)
    fileloc = details.get("fileloc")
    relative_fileloc = details.get("relative_fileloc")

    text: str | None = None
    origin = "airflow_api"
    path = relative_fileloc or (PurePosixPath(fileloc).name if fileloc else f"{dag_id}.py")
    differs: bool | None = None

    if settings.sandbox_dags_path:
        root = Path(settings.sandbox_dags_path).expanduser()
        local = locate_local_file(root, fileloc, relative_fileloc) if root.is_dir() else None
        if local is not None:
            text = local.read_text(encoding="utf-8", errors="replace")
            origin = "local_dags_folder"
            path = local.relative_to(root.resolve()).as_posix()
            try:
                deployed = client.get_dag_source(dag_id)
                differs = deployed.strip() != text.strip()
            except AirflowError:
                differs = None  # deployed code unavailable: cannot compare

    if text is None:
        text = client.get_dag_source(dag_id)

    lines = text.splitlines()
    total = len(lines)
    first = max(1, start_line or 1)
    last = min(total, end_line or total)
    if first > max(total, 1) or (end_line is not None and end_line < first):
        raise ValueError(f"Line range {start_line}-{end_line} is outside the file (1-{total}).")
    window = lines[first - 1 : last]
    truncated = False
    if len(window) > MAX_SOURCE_LINES:
        window, truncated = window[:MAX_SOURCE_LINES], True
        last = first + MAX_SOURCE_LINES - 1
    content = "\n".join(window)
    if len(content) > MAX_SOURCE_CHARS:
        kept = content[:MAX_SOURCE_CHARS].rsplit("\n", 1)[0]
        last = first + kept.count("\n")
        content, truncated = kept, True
    masked = redact_code(content)

    result: dict[str, Any] = {
        "dag_id": dag_id,
        "path": path,
        "origin": origin,
        "fileloc": fileloc,
        "total_lines": total,
        "first_line": first if total else 0,
        "last_line": last if total else 0,
        "truncated": truncated,
        "redacted": masked != content,
        "content": masked,
        "diff_guidance": (
            f"Use '--- a/{path}' / '+++ b/{path}' in diff headers and copy context lines exactly "
            "from 'content'. Lines containing [REDACTED] cannot be used as diff context."
        ),
    }
    if truncated:
        result["next"] = f"Call again with start_line={last + 1} to read the rest."
    if differs is not None:
        result["differs_from_deployed"] = differs
        if differs:
            result["warning"] = (
                "The local file differs from the code Airflow currently runs: the failure may "
                "come from the deployed version. Mention it to the user."
            )
    return result
