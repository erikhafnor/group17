"""Quality checks and the per-run quality report."""

from __future__ import annotations

import json
from pathlib import Path


class QualityCheckError(RuntimeError):
    """A table failed a check and must not be published."""


def write_run_report(reports_dir: Path, run_id: str, status: str, metrics: dict) -> Path:
    """Write metrics.json and a Markdown summary (also used as the GitHub job summary)."""
    out = reports_dir / run_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "metrics.json").write_text(
        json.dumps({"run_id": run_id, "status": status, "metrics": metrics}, indent=2, default=str),
        encoding="utf-8",
    )

    lines = [f"## MAUDE pipeline run `{run_id}`", "", f"**Status:** {status}", ""]
    for stage, value in metrics.items():
        lines += [f"### {stage}", ""]
        rows = value if isinstance(value, list) else [value]
        if rows and isinstance(rows[0], dict):
            keys = list(dict.fromkeys(k for row in rows for k in row))
            lines += ["| " + " | ".join(keys) + " |", "|" + "---|" * len(keys)]
            lines += ["| " + " | ".join(str(r.get(k, "")) for k in keys) + " |" for r in rows]
        else:
            lines.append(str(value))
        lines.append("")
    summary = out / "summary.md"
    summary.write_text("\n".join(lines), encoding="utf-8")
    return summary
