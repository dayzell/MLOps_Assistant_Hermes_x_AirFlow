"""Secret masking applied to everything returned to the LLM (logs, XComs, params, errors).

The rules are deliberately conservative: a false positive costs a masked value, a false
negative leaks a credential into the model context.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

MASK = "[REDACTED]"

# Key names whose *value* must never be shown (dict keys, key=value pairs, JSON fields).
_SENSITIVE_KEY = (
    r"(?:passw(?:or)?d|passwd|pwd|secret|token|api[_-]?key|apikey|access[_-]?key"
    r"|private[_-]?key|credentials?|authorization|auth[_-]?token|session[_-]?id"
    r"|cookie|dsn|conn(?:ection)?[_-]?(?:uri|url|string|str)|sas[_-]?token"
    r"|client[_-]?secret|signature)"
)
_SENSITIVE_KEY_RE = re.compile(_SENSITIVE_KEY, re.IGNORECASE)

# Ordered (pattern, replacement) rules for free text.
_TEXT_RULES: list[tuple[re.Pattern[str], str]] = [
    # PEM private keys (multi-line).
    (
        re.compile(
            r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
            re.DOTALL,
        ),
        "[REDACTED PRIVATE KEY]",
    ),
    # Airflow connection / variable env vars: AIRFLOW_CONN_X=..., AIRFLOW_VAR_Y=...
    (
        re.compile(r"\b((?:AIRFLOW_CONN|AIRFLOW_VAR)_[A-Z0-9_]+\s*[=:]\s*)(['\"]?)[^\s'\"]+\2"),
        r"\1" + MASK,
    ),
    # Credentials embedded in URIs: scheme://user:password@host -> scheme://user:[REDACTED]@host
    (
        re.compile(r"\b([a-zA-Z][a-zA-Z0-9+.\-]*://)([^/\s:@'\"]*):([^@\s/'\"]+)@"),
        r"\1\2:" + MASK + "@",
    ),
    # HTTP Authorization headers.
    (
        re.compile(
            r"(?i)\b(authorization\s*[:=]\s*['\"]?)(?:(bearer|basic|token|digest)\s+)?"
            r"[A-Za-z0-9._~+/=\-]+"
        ),
        r"\1\2 " + MASK,
    ),
    # Bare "Bearer <token>" occurrences.
    (re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=\-]{8,}"), r"\1" + MASK),
    # JSON Web Tokens.
    (
        re.compile(r"\beyJ[A-Za-z0-9_\-]{5,}\.eyJ[A-Za-z0-9_\-]{5,}\.[A-Za-z0-9_\-]+"),
        "[REDACTED JWT]",
    ),
    # Well-known provider token shapes.
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "[REDACTED AWS KEY]"),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{20,}"), MASK),
    (re.compile(r"\bglpat-[A-Za-z0-9_\-]{20,}"), MASK),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9\-]{10,}"), MASK),
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}"), MASK),
    # key = "quoted value" / key: 'quoted value' (JSON, YAML, Python reprs).
    (
        re.compile(r"(?i)([\"']?\b\w*" + _SENSITIVE_KEY + r"\w*[\"']?\s*[:=]\s*)\"[^\"\n]*\""),
        r'\1"' + MASK + '"',
    ),
    (
        re.compile(r"(?i)([\"']?\b\w*" + _SENSITIVE_KEY + r"\w*[\"']?\s*[:=]\s*)'[^'\n]*'"),
        r"\1'" + MASK + "'",
    ),
    # key=value / key: value (unquoted, up to a delimiter).
    (
        re.compile(r"(?i)(\b\w*" + _SENSITIVE_KEY + r"\w*\s*[:=]\s*)(?![\"'\[{]|\s)[^\s,;&}\])]+"),
        r"\1" + MASK,
    ),
]


def is_sensitive_key(key: str) -> bool:
    """True when a mapping key names a secret (password, token, conn uri, ...)."""
    return bool(_SENSITIVE_KEY_RE.search(key))


def redact_text(text: str) -> str:
    """Mask secrets inside free text (log lines, error messages, stringified values)."""
    if not text:
        return text
    for pattern, replacement in _TEXT_RULES:
        text = pattern.sub(replacement, text)
    return text


def redact_value(value: Any, *, _depth: int = 0) -> Any:
    """Recursively mask secrets in JSON-like data (dicts, lists, strings).

    Values stored under a sensitive key are replaced entirely, whatever their type.
    """
    if _depth > 50:  # pathological nesting: refuse to walk further, mask instead
        return MASK
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if is_sensitive_key(key_text) and item not in (None, "", [], {}):
                result[key_text] = MASK
            else:
                result[key_text] = redact_value(item, _depth=_depth + 1)
        return result
    if isinstance(value, list | tuple):
        return [redact_value(item, _depth=_depth + 1) for item in value]
    return value
