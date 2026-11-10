"""Fixes that hide the error instead of fixing it: spotted after the sandbox run, and the audit
asks before offering to apply them."""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow_mlops import workflow as wf
from airflow_mlops.audit_tools import AuditTools
from airflow_mlops.runtime import PluginRuntime
from airflow_mlops.sandbox import masking_warnings
from airflow_mlops.tools import ReadTools
from harness import make_context


def warnings_for(tmp_path: Path, before: str, after: str) -> list[str]:
    src, copy = tmp_path / "src", tmp_path / "copy"
    src.mkdir()
    copy.mkdir()
    (src / "d.py").write_text(before, encoding="utf-8")
    (copy / "d.py").write_text(after, encoding="utf-8")
    return [w["rule"] for w in masking_warnings(src, copy, ["d.py"])]


@pytest.mark.parametrize(
    ("before", "after", "rule"),
    [
        ('y = frame["target"]\n', 'y = frame.get("target")\n', "key_lookup_to_get"),
        (
            "def f():\n    load()\n",
            "def f():\n    try:\n        load()\n    except Exception:\n        pass\n",
            "swallowed_exception",
        ),
        ("rows = fetch()\n", "rows = fetch() or []\n", "silent_default"),
        # real case (09/11): the model "fixed" the KeyError by reading another column
        ('    print(frame["target"])\n', '    print(frame["features"])\n', "key_swapped"),
        (
            "if df.empty:\n    raise ValueError('no rows')\nsave(df)\n",
            "save(df)\n",
            "removed_check",
        ),
    ],
)
def test_masking_patterns(tmp_path: Path, before: str, after: str, rule: str) -> None:
    assert rule in warnings_for(tmp_path, before, after)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        # a real fix of the cause: validate the input and fail with a clear message
        (
            'y = frame["target"]\n',
            'if "target" not in frame:\n'
            '    raise KeyError("target column missing: check the producer")\n'
            'y = frame["target"]\n',
        ),
        ("x = 1\n", "x = 2\n"),
        # renaming a variable while reading the same key is not a swap
        ('y = frame["target"]\n', 'labels = frame["target"]\n'),
    ],
)
def test_real_fixes_are_not_flagged(tmp_path: Path, before: str, after: str) -> None:
    assert warnings_for(tmp_path, before, after) == []


def test_the_audit_asks_before_going_on_with_a_masking_fix() -> None:
    ctx = make_context()
    runtime = PluginRuntime(ctx)
    tools = AuditTools(runtime, ReadTools(runtime))
    audit = wf.create_audit("all", "24h", [], "s-1")
    audit.items.append(
        wf.Item("1.1", 1, 1, "train", "r", "train", "critical", "code_fix", "t", "fix_proposed")
    )
    audit.current = "1.1"
    wf.save_audit(audit)
    warning = {
        "file": "train.py",
        "line": 5,
        "rule": "key_lookup_to_get",
        "code": 'frame.get("target")',
        "why": "a key lookup became .get(): a missing key no longer fails",
    }
    step = tools.observe_sandbox(
        "train", "train", "structure", {"passed": True, "masking_warnings": [warning]}
    )
    assert step is not None and step["state"] == "passed"
    question = step["next_question"]
    assert question["question"].startswith("1.1 passed the sandbox, but the fix may hide the error")
    assert "train.py:5" in question["question"]
    assert question["choices"][0] == "Revise the fix"  # recommended
    assert question["choices"][1] == "Keep it and go on"
    assert question["choices"][-1] == wf.STOP_LABEL
