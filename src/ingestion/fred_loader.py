from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from fredapi import Fred

from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database
from src.utils.config import CONFIG


FRED_SERIES: dict[str, str] = {
    "GDPC1": "gdp",
    "GASDESW": "diesel_price",
    "INDPRO": "industrial_production",
    "UNRATE": "unemployment_rate",
    "PCU484484": "ppi_transportation",
    "TRFVOLUSM227NFWA": "vehicle_miles",
}


@dataclass(frozen=True)
class FredLoadValidation:
    requested_series: int
    loaded_rows: int
    distinct_series: int

    @property
    def is_valid(self) -> bool:
        return self.loaded_rows > 0 and self.distinct_series == self.requested_series


class FredLoader:
    """Fetch FRED series and store them in a normalized raw DuckDB table."""

    table_name = "raw_fred_series"

    def __init__(
        self,
        api_key: str | None = None,
        db_manager: DuckDBManager | None = None,
    ) -> None:
        self.api_key = api_key or CONFIG.fred_api_key
        self.db_manager = db_manager or DuckDBManager()

    def _client(self) -> Fred:
        if not self.api_key:
            raise ValueError("FRED_API_KEY is not configured. Add it to your .env before ingestion.")
        return Fred(api_key=self.api_key)

    def fetch_series(self, series_ids: list[str] | None = None) -> pd.DataFrame:
        client = self._client()
        requested = series_ids or list(FRED_SERIES.keys())
        frames: list[pd.DataFrame] = []

        for series_id in requested:
            series = client.get_series(series_id)
            frame = series.rename("value").reset_index()
            frame.columns = ["date", "value"]
            frame["series_id"] = series_id
            frame = frame[["series_id", "date", "value"]]
            frame["date"] = pd.to_datetime(frame["date"]).dt.date
            frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
            frame = frame.dropna(subset=["value"])
            frames.append(frame)

        if not frames:
            return pd.DataFrame(columns=["series_id", "date", "value"])
        return pd.concat(frames, ignore_index=True)

    def load(self, series_ids: list[str] | None = None) -> FredLoadValidation:
        initialize_database(self.db_manager)
        frame = self.fetch_series(series_ids)

        with self.db_manager.connection() as conn:
            conn.execute(f"DELETE FROM {self.table_name}")
            conn.register("fred_frame", frame)
            conn.execute(
                f"""
                INSERT INTO {self.table_name}
                SELECT series_id, CAST(date AS DATE), value
                FROM fred_frame
                """
            )
            loaded_rows = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
            distinct_series = conn.execute(
                f"SELECT COUNT(DISTINCT series_id) FROM {self.table_name}"
            ).fetchone()[0]
            conn.unregister("fred_frame")

        validation = FredLoadValidation(
            requested_series=len(series_ids or list(FRED_SERIES.keys())),
            loaded_rows=loaded_rows,
            distinct_series=distinct_series,
        )
        if not validation.is_valid:
            raise ValueError(
                "FRED ingestion validation failed: "
                f"loaded_rows={loaded_rows}, distinct_series={distinct_series}"
            )
        return validation
