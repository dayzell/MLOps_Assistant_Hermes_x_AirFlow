"""Size limiting helpers: log tails, error-block extraction and value truncation.

Everything returned to the LLM goes through these helpers so a single huge log or XCom
cannot flood the model context.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

_TRACEBACK_START = re.compile(r"Traceback \(most recent call last\):")
_CHAIN_MARKERS = (
    "During handling of the above exception",
    "The above exception was the direct cause",
)
# Final line of a Python traceback: "pkg.module.SomeError: message" (not indented).
_EXCEPTION_LINE = re.compile(
    r"^(?:\[[^\]]*\]\s*)?(?:[\w.]+\.)?[A-Z]\w*(?:Error|Exception|Exit|Interrupt|Timeout|Warning)"
    r"\w*(?::|$)"
)
_ERROR_LINE = re.compile(
    r"\b(?:ERROR|CRITICAL|FATAL)\b|Exception\b|Error:|Traceback|Killed|OOM|exit code",
)


@dataclass(frozen=True)
class LogExcerpt:
    """Truncated view of a log."""

    tail: str
    error_block: str | None
    total_lines: int
    returned_lines: int
    truncated: bool


def _cap_chars(text: str, max_chars: int, *, keep_end: bool = True) -> tuple[str, bool]:
    if len(text) <= max_chars:
        return text, False
    marker = f"...[truncated {len(text) - max_chars} chars]...\n"
    if keep_end:
        return marker + text[-max_chars:], True
    return text[:max_chars] + "\n" + marker, True


def extract_error_block(lines: list[str], context_lines: int) -> str | None:
    """Return the most relevant error excerpt of a log.

    Preference order: the last Python traceback (including its chained causes, trimmed to
    ``context_lines`` keeping the end), else the last error-looking line with context.
    """
    if not lines:
        return None
    context_lines = max(1, context_lines)

    starts = [i for i, line in enumerate(lines) if _TRACEBACK_START.search(line)]
    if starts:
        start = starts[-1]
        # Walk back through chained tracebacks ("During handling of ...").
        for previous in reversed(starts[:-1]):
            between = lines[previous:start]
            if any(marker in line for line in between for marker in _CHAIN_MARKERS):
                start = previous
            else:
                break
        end = len(lines)
        for index in range(starts[-1] + 1, len(lines)):
            if _EXCEPTION_LINE.search(lines[index].strip()) and not lines[index].startswith(" "):
                end = index + 1
                # Keep indented continuation lines of a multi-line exception message.
                while end < len(lines) and end - index <= 3 and lines[end][:1] in (" ", "	"):
                    end += 1
                break
        block = lines[start:end]
        if len(block) > context_lines:
            omitted = len(block) - context_lines + 1
            block = [
                block[0],
                f"  ... [{omitted} traceback lines omitted] ...",
                *block[-(context_lines - 1) :],
            ]
        return "\n".join(block)

    matches = [i for i, line in enumerate(lines) if _ERROR_LINE.search(line)]
    if not matches:
        return None
    last = matches[-1]
    half = max(1, context_lines // 2)
    window = lines[max(0, last - half) : min(len(lines), last + half + 1)]
    return "\n".join(window)


def excerpt_log(text: str, max_lines: int, max_chars: int, context_lines: int) -> LogExcerpt:
    """Build a bounded excerpt of ``text``: tail of ``max_lines`` lines plus the error block."""
    lines = text.splitlines()
    total = len(lines)
    max_lines = max(1, max_lines)
    tail_lines = lines[-max_lines:]
    tail, chars_cut = _cap_chars("\n".join(tail_lines), max_chars)
    error_block = extract_error_block(lines, context_lines)
    if error_block is not None:
        error_block, _ = _cap_chars(error_block, max(200, max_chars // 2))
    return LogExcerpt(
        tail=tail,
        error_block=error_block,
        total_lines=total,
        returned_lines=len(tail_lines),
        truncated=chars_cut or total > len(tail_lines),
    )


def truncate_value(value: Any, max_chars: int) -> tuple[Any, bool]:
    """Return ``value`` unchanged when its JSON form fits in ``max_chars``.

    Otherwise return a truncated string preview and ``True``.
    """
    try:
        serialized = json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        serialized = str(value)
    if len(serialized) <= max_chars:
        return value, False
    preview, _ = _cap_chars(serialized, max_chars, keep_end=False)
    return preview, True


def truncate_text(text: str | None, max_chars: int) -> str | None:
    """Truncate free text from the end (keeps the beginning)."""
    if text is None:
        return None
    result, _ = _cap_chars(text, max_chars, keep_end=False)
    return result
