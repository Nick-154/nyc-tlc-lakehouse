import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Redirect every lakehouse path into a throwaway directory."""
    from tlc import config

    monkeypatch.setattr(config, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(config, "BRONZE", tmp_path / "bronze")
    monkeypatch.setattr(config, "SILVER", tmp_path / "silver")
    monkeypatch.setattr(config, "QUARANTINE", tmp_path / "quarantine")
    monkeypatch.setattr(config, "GOLD", tmp_path / "gold")
    monkeypatch.setattr(config, "WAREHOUSE", tmp_path / "gold" / "warehouse.duckdb")
    monkeypatch.setattr(config, "MIN_ROWS_PER_MONTH", 1)
    config.ensure_dirs()
    return tmp_path
