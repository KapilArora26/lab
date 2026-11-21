from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from signal_service.demo.generate import build_demo
from signal_service.warehouse.duckdb import DuckDBWarehouse


@pytest.fixture(scope="session")
def demo_db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("seed") / "demo.duckdb"
    build_demo(path)
    return path


@pytest.fixture
def demo_wh(demo_db_path: Path, tmp_path: Path) -> DuckDBWarehouse:
    destination = tmp_path / "demo.duckdb"
    shutil.copy(demo_db_path, destination)
    warehouse = DuckDBWarehouse(destination)
    yield warehouse
    warehouse.close()
