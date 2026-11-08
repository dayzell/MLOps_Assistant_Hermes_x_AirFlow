"""Failure diagnosis: root-cause vs impacted tasks, error extraction and rule-based classification.

The plugin returns structured facts plus a justified classification
(``transient`` / ``critical`` / ``unknown``). Turning them into a natural-language
explanation is the LLM's job, guided by the ``airflow-triage`` skill.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from .airflow_client import AirflowClient, AirflowError
from .config import Settings
from .redaction import redact_text, redact_value
from .storage import save_diagnosis
from .textutils import excerpt_log, truncate_value
from .views import TaskGraph, collect_xcoms

logger = logging.getLogger(__name__)

TRANSIENT = "transient"
CRITICAL = "critical"
UNKNOWN = "unknown"
_RULE_CATEGORIES = (TRANSIENT, CRITICAL)

BUNDLED_RULES_PATH = Path(__file__).resolve().parent / "data" / "error_patterns.yaml"

FAILED_STATES = frozenset({"failed"})
RETRYING_STATES = frozenset({"up_for_retry"})
IMPACTED_STATES = frozenset({"upstream_failed", "skipped"})

_MAX_DETAILED_FAILURES = 5
_MAX_XCOM_TASKS = 3  # the failed task + up to 2 direct upstream producers
_MAX_XCOM_ENTRIES = 10
_LOG_TAIL_LINES = 30
_EXCERPT_CHARS = 240


# ============================================================================ rules


@dataclass(frozen=True)
class Rule:
    id: str
    category: str
    description: str
    patterns: tuple[re.Pattern[str], ...]
    keywords: tuple[str, ...]
    min_tries: int = 1
    below_min_tries_category: str | None = None


@dataclass(frozen=True)
class RuleSet:
    rules: tuple[Rule, ...]
    source: str
    warnings: tuple[str, ...] = ()


def parse_rules(document: Any, source: str) -> RuleSet:
    """Validate a rules document. Invalid rules are skipped and reported as warnings."""
    warnings: list[str] = []
    raw_rules = document.get("rules") if isinstance(document, dict) else None
    if not isinstance(raw_rules, list):
        return RuleSet((), source, ("rules file has no 'rules' list",))

    rules: list[Rule] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_rules):
        where = f"rules[{index}]"
        if not isinstance(raw, dict):
            warnings.append(f"{where}: not a mapping, skipped")
            continue
        rule_id = str(raw.get("id") or "").strip()
        category = str(raw.get("category") or "").strip().lower()
        if not rule_id or rule_id in seen:
            warnings.append(f"{where}: missing or duplicate id, skipped")
            continue
        if category not in _RULE_CATEGORIES:
            warnings.append(f"{rule_id}: category must be transient or critical, skipped")
            continue
        flags = re.IGNORECASE if raw.get("ignore_case") else 0
        patterns: list[re.Pattern[str]] = []
        for pattern in raw.get("patterns") or []:
            try:
                patterns.append(re.compile(str(pattern), flags | re.MULTILINE))
            except re.error as exc:
                warnings.append(f"{rule_id}: invalid regex {pattern!r} ({exc}), ignored")
        keywords = tuple(str(k).lower() for k in raw.get("keywords") or [] if str(k).strip())
        if not patterns and not keywords:
            warnings.append(f"{rule_id}: no usable pattern or keyword, skipped")
            continue
        below = raw.get("below_min_tries_category")
        below_category = str(below).lower() if below else None
        if below_category is not None and below_category not in (*_RULE_CATEGORIES, UNKNOWN):
            warnings.append(f"{rule_id}: invalid below_min_tries_category, ignored")
            below_category = None
        try:
            min_tries = max(1, int(raw.get("min_tries") or 1))
        except (TypeError, ValueError):
            min_tries = 1
            warnings.append(f"{rule_id}: invalid min_tries, using 1")
        seen.add(rule_id)
        rules.append(
            Rule(
                id=rule_id,
                category=category,
                description=str(raw.get("description") or ""),
                patterns=tuple(patterns),
                keywords=keywords,
                min_tries=min_tries,
                below_min_tries_category=below_category,
            )
        )
    return RuleSet(tuple(rules), source, tuple(warnings))


@lru_cache(maxsize=8)
def _load_rules_cached(path: str, mtime: float) -> RuleSet:
    del mtime  # part of the cache key only: an edited file is reloaded
    with open(path, encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    return parse_rules(document, path)


def load_rules(custom_path: str | None = None) -> RuleSet:
    """Load the custom rules file when configured and readable, else the bundled rules."""
    fallback_warning: tuple[str, ...] = ()
    if custom_path:
        candidate = Path(custom_path).expanduser()
        try:
            ruleset = _load_rules_cached(str(candidate), candidate.stat().st_mtime)
            if ruleset.rules:
                return ruleset
            fallback_warning = (
                f"custom rules {candidate.name} contain no valid rule; using bundled rules",
                *ruleset.warnings,
            )
        except (OSError, yaml.YAMLError) as exc:
            fallback_warning = (
                f"cannot read custom rules ({type(exc).__name__}); using bundled rules",
            )
    bundled = _load_rules_cached(str(BUNDLED_RULES_PATH), BUNDLED_RULES_PATH.stat().st_mtime)
    if fallback_warning:
        return RuleSet(bundled.rules, bundled.source, fallback_warning + bundled.warnings)
    return bundled


# ============================================================================ classification


@dataclass(frozen=True)
class RuleMatch:
    rule_id: str
    category: str
    rule_category: str
    description: str
    matched: str
    excerpt: str
    source: str
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "rule_id": self.rule_id,
            "category": self.category,
            "description": self.description,
            "matched": self.matched,
            "excerpt": self.excerpt,
            "source": self.source,
        }
        if self.category != self.rule_category:
            data["rule_category"] = self.rule_category
        if self.note:
            data["note"] = self.note
        return data


@dataclass(frozen=True)
class Classification:
    category: str
    justification: str
    matches: tuple[RuleMatch, ...] = field(default_factory=tuple)

    @property
    def rule_ids(self) -> list[str]:
        return [match.rule_id for match in self.matches]

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "justification": self.justification,
            "matched_rules": [match.to_dict() for match in self.matches],
        }


def _line_at(text: str, position: int) -> str:
    start = text.rfind("\n", 0, position) + 1
    end = text.find("\n", position)
    line = text[start : end if end != -1 else len(text)].strip()
    return line[:_EXCERPT_CHARS]


def _match_rule(rule: Rule, text: str, source: str, try_number: int | None) -> RuleMatch | None:
    found: tuple[str, int] | None = None
    for pattern in rule.patterns:
        hit = pattern.search(text)
        if hit:
            found = (hit.group(0), hit.start())
            break
    if found is None and rule.keywords:
        lowered = text.lower()
        for keyword in rule.keywords:
            position = lowered.find(keyword)
            if position != -1:
                found = (text[position : position + len(keyword)], position)
                break
    if found is None:
        return None

    category, note = rule.category, None
    tries = try_number or 1
    if tries < rule.min_tries and rule.below_min_tries_category:
        category = rule.below_min_tries_category
        note = (
            f"try {tries} < min_tries {rule.min_tries}: counted as {category}; "
            f"becomes {rule.category} if it recurs"
        )
    return RuleMatch(
        rule_id=rule.id,
        category=category,
        rule_category=rule.category,
        description=rule.description,
        matched=found[0][:_EXCERPT_CHARS],
        excerpt=_line_at(text, found[1]),
        source=source,
        note=note,
    )


def classify(
    error_block: str | None,
    log_tail: str | None,
    ruleset: RuleSet,
    try_number: int | None = None,
) -> Classification:
    """Classify a failure from its error block (preferred) or, failing that, its log tail.

    A matching critical rule always wins; ``unknown`` means no rule matched (human review).
    """
    if not (error_block or log_tail):
        return Classification(UNKNOWN, "No log text available to classify the failure.")

    matches: list[RuleMatch] = []
    for source, text in (("error_block", error_block), ("log_tail", log_tail)):
        if not text:
            continue
        for rule in ruleset.rules:
            match = _match_rule(rule, text, source, try_number)
            if match is not None:
                matches.append(match)
        if matches:
            break

    if not matches:
        return Classification(
            UNKNOWN,
            "No classification rule matched the error block or the log tail; "
            "the failure needs human review.",
        )

    critical = [m for m in matches if m.category == CRITICAL]
    transient = [m for m in matches if m.category == TRANSIENT]
    if critical:
        reason = _describe(critical)
        if transient:
            reason += (
                f"; transient signal(s) {', '.join(m.rule_id for m in transient)} also present "
                "but critical rules take precedence"
            )
        return Classification(CRITICAL, f"Critical: {reason}.", tuple(critical + transient))
    if transient:
        return Classification(TRANSIENT, f"Transient: {_describe(transient)}.", tuple(transient))
    return Classification(
        UNKNOWN, f"Only inconclusive rules matched: {_describe(matches)}.", tuple(matches)
    )


def _describe(matches: list[RuleMatch]) -> str:
    parts = []
    for match in matches:
        where = "error block" if match.source == "error_block" else "log tail"
        text = f"rule '{match.rule_id}' matched {match.matched!r} in the {where}"
        if match.note:
            text += f" ({match.note})"
        parts.append(text)
    return "; ".join(parts)


@dataclass(frozen=True)
class TaskEvidence:
    """Log excerpt and classification of the latest try of one task instance."""

    classification: Classification
    error_block: str | None = None
    log_tail: str | None = None
    log_error: dict[str, Any] | None = None


def read_and_classify(
    client: AirflowClient,
    settings: Settings,
    ruleset: RuleSet,
    dag_id: str,
    run_id: str,
    ti: dict[str, Any],
) -> TaskEvidence:
    """Fetch the latest try's log of ``ti`` (secret-masked, bounded) and classify it."""
    task_id = str(ti["task_id"])
    map_index = ti.get("map_index", -1)
    map_arg = map_index if isinstance(map_index, int) and map_index >= 0 else None
    try_number = int(ti.get("try_number") or 0)
    if try_number < 1:
        return TaskEvidence(
            classify(None, None, ruleset),
            log_error={"type": "not_found", "message": "task never started"},
        )
    try:
        text = redact_text(client.get_task_log(dag_id, run_id, task_id, try_number, map_arg))
    except AirflowError as exc:
        return TaskEvidence(classify(None, None, ruleset), log_error=exc.to_dict())
    excerpt = excerpt_log(
        text,
        max_lines=settings.log_max_lines,
        max_chars=settings.log_max_chars,
        context_lines=settings.error_context_lines,
    )
    return TaskEvidence(
        classify(excerpt.error_block, excerpt.tail, ruleset, try_number),
        error_block=excerpt.error_block,
        log_tail=excerpt.tail,
    )


# ============================================================================ run diagnosis


def _worst(categories: list[str]) -> str:
    if CRITICAL in categories:
        return CRITICAL
    if not categories or UNKNOWN in categories:
        return UNKNOWN
    return TRANSIENT


class RunDiagnoser:
    """Aggregate everything needed to interpret a failed DAG run."""

    def __init__(self, client: AirflowClient, settings: Settings, ruleset: RuleSet) -> None:
        self.client = client
        self.settings = settings
        self.ruleset = ruleset

    def diagnose(self, dag_id: str, run_id: str) -> dict[str, Any]:
        run = self.client.get_dag_run(dag_id, run_id)
        tis = self.client.list_task_instances(dag_id, run_id)
        graph = TaskGraph(self.client.get_tasks(dag_id))
        state_counts = dict(Counter(str(ti.get("state") or "none") for ti in tis))

        failed = [ti for ti in tis if ti.get("state") in FAILED_STATES]
        retrying = [ti for ti in tis if ti.get("state") in RETRYING_STATES]
        failed_ids = {str(ti["task_id"]) for ti in failed}

        roots, secondary = [], []
        for ti in failed:
            failed_ancestors = sorted(set(graph.ancestors(str(ti["task_id"]))) & failed_ids)
            if failed_ancestors:
                secondary.append({**self._brief(ti, graph), "failed_upstream": failed_ancestors})
            else:
                roots.append(ti)

        base: dict[str, Any] = {
            "dag_id": dag_id,
            "run_id": run_id,
            "run": run,
            "state_counts": state_counts,
            "rules_source": "bundled"
            if Path(self.ruleset.source) == BUNDLED_RULES_PATH
            else "custom",
        }
        if self.ruleset.warnings:
            base["rules_warnings"] = list(self.ruleset.warnings)

        if not failed and not retrying:
            base.update(
                outcome="no_failure",
                summary={
                    "category": None,
                    "message": "No failed or retrying task instance in this run.",
                },
            )
            return base

        # Bound the work and the payload: only the first failures get logs, XComs and a
        # classification; the others are listed briefly.
        # Roots first, so they get the detail budget before retrying tasks.
        examined = [
            self._detail(dag_id, run_id, ti, graph)
            if index < _MAX_DETAILED_FAILURES
            else {**self._brief(ti, graph), "classification": None, "details_omitted": True}
            for index, ti in enumerate(roots + retrying)
        ]
        detailed_roots, detailed_retrying = examined[: len(roots)], examined[len(roots) :]

        impacted = []
        root_ids = {str(ti["task_id"]) for ti in roots}
        for ti in tis:
            if ti.get("state") not in IMPACTED_STATES:
                continue
            causes = sorted(set(graph.ancestors(str(ti["task_id"]))) & root_ids)
            entry = {**self._brief(ti, graph), "caused_by": causes}
            if not causes:
                entry["note"] = (
                    "not downstream of a failed task: possibly intentional (branching / "
                    "short-circuit)"
                )
            impacted.append(entry)

        categories = [
            item["classification"]["category"] for item in detailed_roots if item["classification"]
        ]
        if not roots:  # only retrying tasks so far
            categories = [
                item["classification"]["category"]
                for item in detailed_retrying
                if item["classification"]
            ]
        overall = _worst(categories)
        retry_candidates = [
            {"task_id": item["task_id"], "map_index": item["map_index"]}
            for item in detailed_roots
            if item["classification"] and item["classification"]["category"] == TRANSIENT
        ]

        if run.get("state") == "success" and failed:
            base["masked_failure"] = (
                "The run is marked 'success' although tasks failed: a final task with a "
                "permissive trigger_rule (e.g. all_done) masked the failure. Treat it as a "
                "real incident and review the trigger_rule of the final tasks."
            )
        base.update(
            outcome="failed" if failed else "retrying",
            summary={
                "category": overall,
                "root_failure_count": len(roots),
                "secondary_failure_count": len(secondary),
                "retrying_count": len(retrying),
                "impacted_count": len(impacted),
                "retry_candidates": retry_candidates,
                "requires_human": overall != TRANSIENT,
                "guidance": _guidance(overall),
            },
            root_failures=detailed_roots,
            secondary_failures=secondary,
            retrying=detailed_retrying,
            impacted=impacted,
            dag_params=self._params(dag_id),
        )
        self._cache(dag_id, run_id, detailed_roots + detailed_retrying, base)
        return base

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _brief(ti: dict[str, Any], graph: TaskGraph) -> dict[str, Any]:
        task_id = str(ti["task_id"])
        return {
            "task_id": task_id,
            "map_index": ti.get("map_index", -1),
            "state": ti.get("state"),
            "trigger_rule": graph.tasks.get(task_id, {}).get("trigger_rule"),
        }

    def _detail(
        self, dag_id: str, run_id: str, ti: dict[str, Any], graph: TaskGraph
    ) -> dict[str, Any]:
        task_id = str(ti["task_id"])
        map_index = ti.get("map_index", -1)
        map_arg = map_index if isinstance(map_index, int) and map_index >= 0 else None
        try_number = ti.get("try_number") or 0
        detail: dict[str, Any] = {
            **self._brief(ti, graph),
            "try_number": try_number,
            "max_tries": ti.get("max_tries"),
            "duration_s": ti.get("duration_s"),
            "operator": ti.get("operator") or graph.tasks.get(task_id, {}).get("operator"),
            "upstream": [graph.describe(t) for t in graph.upstream.get(task_id, [])],
            "downstream": [graph.describe(t) for t in graph.downstream.get(task_id, [])],
        }

        evidence = read_and_classify(self.client, self.settings, self.ruleset, dag_id, run_id, ti)
        if evidence.log_tail is not None:
            detail["log_tail"] = "\n".join(evidence.log_tail.splitlines()[-_LOG_TAIL_LINES:])
        if evidence.log_error is not None:
            detail["log_error"] = evidence.log_error
        detail["error_block"] = evidence.error_block
        detail["classification"] = evidence.classification.to_dict()
        detail["xcoms"] = self._xcoms(dag_id, run_id, task_id, map_arg, graph)
        return detail

    def _xcoms(
        self, dag_id: str, run_id: str, task_id: str, map_index: int | None, graph: TaskGraph
    ) -> dict[str, Any]:
        """XComs of the failed task and of its direct upstream producers (its inputs)."""
        targets = [(task_id, map_index)] + [
            (up, None) for up in graph.upstream.get(task_id, [])[: _MAX_XCOM_TASKS - 1]
        ]
        result: dict[str, Any] = {}
        for target, target_map in targets:
            try:
                entries, truncated = collect_xcoms(
                    self.client,
                    dag_id,
                    run_id,
                    target,
                    max_chars=self.settings.xcom_max_chars,
                    map_index=target_map,
                    max_entries=_MAX_XCOM_ENTRIES,
                )
                result[target] = {"entries": entries, "truncated": truncated}
            except AirflowError as exc:
                result[target] = {"error": exc.to_dict()}
        return result

    def _params(self, dag_id: str) -> Any:
        try:
            params = self.client.get_dag_details(dag_id).get("params") or {}
        except AirflowError as exc:
            return {"error": exc.to_dict()}
        value, _ = truncate_value(redact_value(params), self.settings.xcom_max_chars * 2)
        return value

    @staticmethod
    def _cache(
        dag_id: str, run_id: str, details: list[dict[str, Any]], result: dict[str, Any]
    ) -> None:
        """Persist classifications for the approval hook; never fail the diagnosis."""
        try:
            for item in details:
                classification = item["classification"]
                if not classification:
                    continue
                save_diagnosis(
                    dag_id,
                    run_id,
                    item["task_id"],
                    map_index=int(item.get("map_index", -1)),
                    try_number=item.get("try_number"),
                    category=classification["category"],
                    rule_ids=[m["rule_id"] for m in classification["matched_rules"]],
                    justification=classification["justification"],
                )
            result["cached"] = True
        except Exception:
            logger.warning("airflow-mlops: could not cache the diagnosis", exc_info=True)
            result["cached"] = False


def _guidance(category: str) -> str:
    if category == TRANSIENT:
        return (
            "Transient failure: a targeted retry of the listed retry_candidates only (never the "
            "whole DAG) is usually enough; check the dry run first."
        )
    if category == CRITICAL:
        return (
            "Critical failure: do not retry blindly. Fix the cause first (code, data, "
            "configuration or permissions); propose a fix for human review."
        )
    return "Unclassified failure: read the error block and logs; escalate to a human."
