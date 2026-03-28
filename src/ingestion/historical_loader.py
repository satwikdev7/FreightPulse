from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database
from src.utils.config import CONFIG


@dataclass(frozen=True)
class HistoricalLoadValidation:
    table_name: str
    expected_rows: int
    actual_rows: int

    @property
    def is_valid(self) -> bool:
        return self.expected_rows == self.actual_rows


class FAFHistoricalLoader:
    """Load the FAF 1997-2012 reprocessed state dataset into the raw layer."""

    table_name = "raw_faf_historical"

    def __init__(self, csv_path: Path | None = None, db_manager: DuckDBManager | None = None) -> None:
        self.csv_path = Path(csv_path or CONFIG.faf_historical_csv)
        self.db_manager = db_manager or DuckDBManager()

    def load(self) -> HistoricalLoadValidation:
        initialize_database(self.db_manager)
        with self.db_manager.connection() as conn:
            conn.execute(f"DELETE FROM {self.table_name}")
            conn.execute(
                f"""
                INSERT INTO {self.table_name}
                SELECT *
                FROM read_csv_auto(
                    ?,
                    header = true,
                    sample_size = -1,
                    ignore_errors = false
                )
                """,
                [str(self.csv_path)],
            )
            expected_rows = conn.execute(
                "SELECT COUNT(*) FROM read_csv_auto(?, header = true, sample_size = -1)",
                [str(self.csv_path)],
            ).fetchone()[0]
            actual_rows = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]

        validation = HistoricalLoadValidation(self.table_name, expected_rows, actual_rows)
        if not validation.is_valid:
            raise ValueError(
                f"Row-count validation failed for {self.table_name}: "
                f"expected {expected_rows}, got {actual_rows}"
            )
        return validation
