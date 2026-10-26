"""Identity of one pipeline run, stamped on every row and log line it produces."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class RunContext:
    run_id: str
    env: str
    started_at: datetime

    @classmethod
    def start(cls, env: str) -> RunContext:
        started = datetime.now(UTC).replace(microsecond=0)
        run_id = f"{env}-{started:%Y%m%dT%H%M%SZ}"
        # Link runs to their GitHub Actions run when there is one.
        if gh_run := os.environ.get("GITHUB_RUN_ID"):
            run_id += f"-gh{gh_run}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
        return cls(run_id=run_id, env=env, started_at=started)

    @property
    def started_iso(self) -> str:
        return self.started_at.isoformat()
