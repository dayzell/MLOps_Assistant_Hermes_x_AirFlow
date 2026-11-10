"""Icons of the plugin's answers, in one place.

The answers use emoji (the user's choice): ✅ healthy / passed, 🟠 degraded, 🔴 failing,
⚪ no data, ⚠️ warning, ❌ failed, ℹ️ information. No terminal color code anywhere: in a
slash command Hermes cannot tell a terminal from the Desktop app.
"""

from __future__ import annotations

STATUS_ICONS = {"healthy": "✅", "degraded": "🟠", "failing": "🔴", "no_data": "⚪"}
SEVERITY_ICONS = {"high": "🔴", "medium": "🟠", "low": "🟡", "info": "ℹ️"}
OK = "✅"
FAIL = "❌"
WARN = "⚠️"
NEXT = "➡️"
WORKING = "⏳"
WAITING = "•"
LATER = "⏸"
ARCHIVE = "🗂️"
