from pathlib import Path

import pytest

from maude.config import REPO_ROOT, load_config


def test_dev_and_prod_configs_load():
    dev, prod = load_config("dev"), load_config("prod")
    assert dev.file_sets == ("lookups", "monthly")
    assert "history" in prod.file_sets
    assert dev.source_base_url.startswith("https://")
    assert dev.spark["spark.sql.ansi.enabled"] == "false"


def test_unknown_environment_is_rejected():
    with pytest.raises(ValueError, match="unknown environment"):
        load_config("staging")


def test_environment_variables_override_paths(cfg, tmp_path: Path, source_dir: Path):
    assert cfg.data_root == (tmp_path / "data").resolve()
    assert cfg.source_base_url == source_dir.resolve().as_uri()
    assert cfg.bronze_table("master") == cfg.data_root / "bronze" / "tables" / "master"


def test_relative_source_path_resolves_against_repo_root(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("MAUDE_SOURCE_BASE_URL", raising=False)
    expected = (REPO_ROOT / "tests" / "fixtures" / "source").resolve().as_uri()
    assert load_config("test").source_base_url == expected
