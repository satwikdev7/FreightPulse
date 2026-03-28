from __future__ import annotations

from dataclasses import dataclass

from src.ingestion.faf_loader import FAFHiLoLoader, LoadValidation
from src.ingestion.fred_loader import FredLoader, FredLoadValidation
from src.ingestion.historical_loader import FAFHistoricalLoader, HistoricalLoadValidation
from src.ingestion.metadata_loader import MetadataLoader
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


@dataclass(frozen=True)
class MetadataLoadValidation:
    table_name: str
    expected_rows: int
    actual_rows: int

    @property
    def is_valid(self) -> bool:
        return self.expected_rows == self.actual_rows


class RawLayerLoader:
    """Project-level orchestration for raw dataset ingestion into DuckDB."""

    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager or DuckDBManager()
        initialize_database(self.db_manager)

    def load_hilo(self) -> LoadValidation:
        return FAFHiLoLoader(db_manager=self.db_manager).load()

    def load_historical(self) -> HistoricalLoadValidation:
        return FAFHistoricalLoader(db_manager=self.db_manager).load()

    def load_fred(self, series_ids: list[str] | None = None) -> FredLoadValidation:
        return FredLoader(db_manager=self.db_manager).load(series_ids=series_ids)

    def load_metadata(self) -> list[MetadataLoadValidation]:
        frames = MetadataLoader().load_all()
        validations: list[MetadataLoadValidation] = []

        with self.db_manager.connection() as conn:
            for table_name, frame in frames.items():
                conn.execute(f"DELETE FROM {table_name}")
                conn.register("metadata_frame", frame)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM metadata_frame")
                actual_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                expected_rows = len(frame)
                conn.unregister("metadata_frame")

                validation = MetadataLoadValidation(table_name, expected_rows, actual_rows)
                if not validation.is_valid:
                    raise ValueError(
                        f"Metadata row-count validation failed for {table_name}: "
                        f"expected {expected_rows}, got {actual_rows}"
                    )
                validations.append(validation)

        return validations
