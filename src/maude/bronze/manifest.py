"""Append-only JSON Lines log of everything that happens to a source file.

Each landed file gets a "landed" event and, once parsed into its Bronze table,
a "parsed" event. The manifest is the single place that answers "where did
this row come from, when, and in which run?".
"""

from __future__ import annotations

import json
from pathlib import Path


class Manifest:
    def __init__(self, path: Path) -> None:
        self.path = path

    def events(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def append(self, event: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True) + "\n")

    def landed(self) -> list[dict]:
        return [e for e in self.events() if e["event"] == "landed"]

    def pending_parse(self) -> list[dict]:
        events = self.events()
        parsed = {e["file_id"] for e in events if e["event"] == "parsed"}
        return [e for e in events if e["event"] == "landed" and e["file_id"] not in parsed]
