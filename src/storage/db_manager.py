from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

from src.utils.config import CONFIG
from src.utils.paths import ensure_project_directories


class DuckDBManager:
    """Lightweight connection manager for the project DuckDB database."""

    def __init__(self, db_path: Path | None = None, read_only: bool = False) -> None:
        ensure_project_directories()
        self.db_path = Path(db_path or CONFIG.duckdb_path)
        self.read_only = read_only

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path), read_only=self.read_only)

    @contextmanager
    def connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, parameters: tuple | None = None) -> None:
        with self.connection() as conn:
            if parameters:
                conn.execute(sql, parameters)
            else:
                conn.execute(sql)

    def fetch_df(self, sql: str, parameters: tuple | None = None):
        with self.connection() as conn:
            if parameters:
                return conn.execute(sql, parameters).df()
            return conn.execute(sql).df()
