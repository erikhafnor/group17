from dataclasses import replace

import pytest

from maude.config import load_config
from maude.sources import files_for


def test_dev_ingests_lookups_and_monthly_files_only():
    names = [src.name for src in files_for(load_config("dev"))]
    assert names[:3] == ["deviceproblemcodes2025", "patientproblemcodes2026", "foiclass"]
    assert {"mdrfoiadd", "mdrfoichange", "deviceadd", "foitextchange"} <= set(names)
    assert not [n for n in names if "thru" in n]


def test_prod_history_follows_config():
    prod = load_config("prod")
    names = {src.name for src in files_for(prod)}
    assert f"mdrfoithru{prod.history_through}" in names
    for year in prod.history_years:
        assert {f"device{year}", f"foitext{year}"} <= names


def test_unknown_file_set_is_rejected(cfg):
    with pytest.raises(ValueError, match="unknown file sets"):
        files_for(replace(cfg, file_sets=("weekly",)))
