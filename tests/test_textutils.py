"""Log tail / error block extraction and value truncation."""

from __future__ import annotations

from airflow_mlops.textutils import excerpt_log, extract_error_block, truncate_value


def test_extract_last_traceback_with_chain() -> None:
    lines = [
        "INFO start",
        "Traceback (most recent call last):",
        '  File "a.py", line 1, in f',
        "ConnectionResetError: [Errno 104] Connection reset by peer",
        "",
        "During handling of the above exception, another exception occurred:",
        "",
        "Traceback (most recent call last):",
        '  File "b.py", line 2, in g',
        "requests.exceptions.ConnectionError: ('Connection aborted.')",
        "INFO Marking task as FAILED",
    ]
    block = extract_error_block(lines, context_lines=40)
    assert block is not None
    assert block.splitlines()[0] == "Traceback (most recent call last):"
    assert "ConnectionResetError" in block
    assert block.splitlines()[-1].startswith("requests.exceptions.ConnectionError")
    assert "Marking task" not in block


def test_long_traceback_is_trimmed_keeping_the_end() -> None:
    frames = [f'  File "m{i}.py", line {i}, in f{i}' for i in range(100)]
    lines = ["Traceback (most recent call last):", *frames, "ValueError: final"]
    block = extract_error_block(lines, context_lines=10)
    assert block is not None
    out = block.splitlines()
    assert len(out) == 11
    assert out[0].startswith("Traceback")
    assert "omitted" in out[1]
    assert out[-1] == "ValueError: final"


def test_error_line_without_traceback() -> None:
    lines = [f"INFO {i}" for i in range(50)] + ["ERROR - Process killed (OOM)"] + ["INFO end"]
    block = extract_error_block(lines, context_lines=4)
    assert block is not None and "OOM" in block
    assert len(block.splitlines()) <= 5


def test_no_error_returns_none() -> None:
    assert extract_error_block(["INFO all good", "INFO done"], 10) is None


def test_excerpt_is_bounded_by_chars() -> None:
    text = "\n".join("x" * 100 for _ in range(100))
    excerpt = excerpt_log(text, max_lines=100, max_chars=500, context_lines=10)
    assert len(excerpt.tail) < 600
    assert excerpt.truncated is True


def test_truncate_value() -> None:
    assert truncate_value({"a": 1}, 100) == ({"a": 1}, False)
    preview, truncated = truncate_value(list(range(1000)), 50)
    assert truncated is True and isinstance(preview, str) and "truncated" in preview
