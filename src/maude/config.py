"""Environment configuration loaded from config/<env>.toml.

Two environment variables override the file, so tests and ad-hoc runs never
touch the dev or prod data areas:

- MAUDE_DATA_ROOT        where Bronze/Silver/Gold, logs and reports are written
- MAUDE_SOURCE_BASE_URL  where source zip files are downloaded from
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
ENVIRONMENTS = ("dev", "prod", "test")


@dataclass(frozen=True)
class Config:
    env: str
    data_root: Path
    source_base_url: str
    classification_base_url: str
    file_sets: tuple[str, ...]
    history_through: int
    history_years: tuple[int, ...]
    device_problem_codes: str
    patient_problem_codes: str
    spark: dict[str, str]

    @property
    def landing_dir(self) -> Path:
        return self.data_root / "bronze" / "landing"

    @property
    def manifest_path(self) -> Path:
        return self.data_root / "bronze" / "_manifest" / "events.jsonl"

    @property
    def staging_dir(self) -> Path:
        return self.data_root / "_staging"

    @property
    def logs_dir(self) -> Path:
        return self.data_root / "logs"

    @property
    def reports_dir(self) -> Path:
        return self.data_root / "reports"

    def bronze_table(self, family: str) -> Path:
        return self.data_root / "bronze" / "tables" / family

    def silver_table(self, name: str) -> Path:
        return self.data_root / "silver" / name

    def quarantine_table(self, name: str) -> Path:
        return self.data_root / "silver" / "_quarantine" / name

    def gold_table(self, name: str) -> Path:
        return self.data_root / "gold" / name


def _as_url(value: str) -> str:
    """Accept an http(s)/file URL, or a local path relative to the repo root."""
    if "://" in value:
        return value.rstrip("/")
    path = Path(os.path.expanduser(value))
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve().as_uri()


def load_config(env: str) -> Config:
    if env not in ENVIRONMENTS:
        raise ValueError(f"unknown environment {env!r}; expected one of {ENVIRONMENTS}")
    with (CONFIG_DIR / f"{env}.toml").open("rb") as fh:
        raw = tomllib.load(fh)

    data_root = os.environ.get("MAUDE_DATA_ROOT") or raw["data_root"]
    source_override = os.environ.get("MAUDE_SOURCE_BASE_URL")
    return Config(
        env=raw["env"],
        data_root=Path(os.path.expanduser(data_root)).resolve(),
        source_base_url=_as_url(source_override or raw["source_base_url"]),
        classification_base_url=_as_url(source_override or raw["classification_base_url"]),
        file_sets=tuple(raw["file_sets"]),
        history_through=int(raw["history_through"]),
        history_years=tuple(int(y) for y in raw["history_years"]),
        device_problem_codes=raw["device_problem_codes"],
        patient_problem_codes=raw["patient_problem_codes"],
        spark={k: str(v) for k, v in raw.get("spark", {}).items()},
    )
