from __future__ import annotations

from pathlib import Path

import duckdb


class DuckDBWarehouse:
    backend = "duckdb"

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(self.path)

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        cursor = self.connection.execute(sql, params or [])
        if cursor.description is None:
            return []
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def execute(self, sql: str, params: list | None = None) -> None:
        self.connection.execute(sql, params or [])

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if rows:
            self.connection.executemany(sql, rows)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> DuckDBWarehouse:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
