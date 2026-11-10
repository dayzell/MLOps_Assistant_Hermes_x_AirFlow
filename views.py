"""Read helpers shared by the tools and the diagnosis: XCom collection and task graph views."""

from __future__ import annotations

from collections import deque
from typing import Any

from .airflow_client import AirflowClient, AirflowError
from .redaction import redact_value
from .textutils import truncate_value


def collect_xcoms(
    client: AirflowClient,
    dag_id: str,
    run_id: str,
    task_id: str,
    *,
    max_chars: int,
    key: str | None = None,
    map_index: int | None = None,
    max_entries: int = 50,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch the XComs of one task instance, secret-masked and size-bounded.

    Returns ``(entries, entries_truncated)``. A failure on one value is reported on that
    entry instead of failing the whole collection.
    """
    entries = client.list_xcom_entries(
        dag_id, run_id, task_id, key=key, map_index=map_index, max_items=max_entries
    )
    results: list[dict[str, Any]] = []
    for entry in entries:
        entry_key = str(entry.get("key"))
        entry_map_index = entry.get("map_index", map_index)
        item: dict[str, Any] = {
            "key": entry_key,
            "map_index": entry_map_index,
            "timestamp": entry.get("timestamp"),
        }
        try:
            value = client.get_xcom_value(
                dag_id,
                run_id,
                task_id,
                entry_key,
                entry_map_index if isinstance(entry_map_index, int) else None,
            )
            masked = redact_value({entry_key: value})[entry_key]
            item["value"], item["truncated"] = truncate_value(masked, max_chars)
        except AirflowError as exc:
            item["value"], item["truncated"] = None, False
            item["error"] = exc.to_dict()
        results.append(item)
    return results, len(entries) >= max_entries


class TaskGraph:
    """Static task graph of a DAG built from ``get_tasks`` output."""

    def __init__(self, tasks: list[dict[str, Any]]) -> None:
        self.tasks: dict[str, dict[str, Any]] = {
            str(task["task_id"]): task for task in tasks if task.get("task_id")
        }
        self.downstream: dict[str, list[str]] = {
            tid: [str(child) for child in task.get("downstream_task_ids") or []]
            for tid, task in self.tasks.items()
        }
        self.upstream: dict[str, list[str]] = {tid: [] for tid in self.tasks}
        for tid, children in self.downstream.items():
            for child in children:
                self.upstream.setdefault(child, []).append(tid)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self.tasks

    def _walk(self, start: str, edges: dict[str, list[str]], depth: int | None) -> dict[str, int]:
        seen: dict[str, int] = {}
        queue: deque[tuple[str, int]] = deque([(start, 0)])
        while queue:
            current, distance = queue.popleft()
            if depth is not None and distance >= depth:
                continue
            for neighbour in edges.get(current, []):
                if neighbour not in seen and neighbour != start:
                    seen[neighbour] = distance + 1
                    queue.append((neighbour, distance + 1))
        return seen

    def ancestors(self, task_id: str, depth: int | None = None) -> dict[str, int]:
        """Upstream tasks of ``task_id`` with their distance (all of them by default)."""
        return self._walk(task_id, self.upstream, depth)

    def descendants(self, task_id: str, depth: int | None = None) -> dict[str, int]:
        return self._walk(task_id, self.downstream, depth)

    def describe(self, task_id: str, distance: int | None = None) -> dict[str, Any]:
        task = self.tasks.get(task_id, {})
        info: dict[str, Any] = {
            "task_id": task_id,
            "trigger_rule": task.get("trigger_rule"),
            "operator": task.get("operator"),
        }
        if distance is not None:
            info["distance"] = distance
        return info


def build_dependency_view(
    tasks: list[dict[str, Any]], task_id: str, depth: int
) -> dict[str, Any] | None:
    """Upstream/downstream neighbourhood of ``task_id`` (BFS, ``depth`` hops each way)."""
    graph = TaskGraph(tasks)
    if task_id not in graph:
        return None

    def listing(found: dict[str, int]) -> list[dict[str, Any]]:
        return [
            {**graph.describe(tid, dist), "direct_upstream": graph.upstream.get(tid, [])}
            for tid, dist in sorted(found.items(), key=lambda kv: (kv[1], kv[0]))
        ]

    task = graph.tasks[task_id]
    return {
        "task": {
            "task_id": task_id,
            "operator": task.get("operator"),
            "trigger_rule": task.get("trigger_rule"),
            "retries": task.get("retries"),
        },
        "upstream": listing(graph.ancestors(task_id, depth)),
        "downstream": listing(graph.descendants(task_id, depth)),
    }
