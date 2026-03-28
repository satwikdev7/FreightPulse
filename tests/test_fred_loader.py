import pandas as pd

from src.ingestion.fred_loader import FredLoader
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


def test_fred_loader_writes_normalized_rows(monkeypatch, tmp_path):
    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path)
    initialize_database(manager)

    sample = pd.DataFrame(
        {
            "series_id": ["GDPC1", "GDPC1", "UNRATE"],
            "date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-04-01"), pd.Timestamp("2020-01-01")],
            "value": [100.0, 101.0, 5.0],
        }
    )

    monkeypatch.setattr(FredLoader, "fetch_series", lambda self, series_ids=None: sample)

    validation = FredLoader(api_key="dummy", db_manager=manager).load(series_ids=["GDPC1", "UNRATE"])

    assert validation.is_valid
    assert validation.loaded_rows == 3
    assert validation.distinct_series == 2

    frame = manager.fetch_df("SELECT series_id, COUNT(*) AS n FROM raw_fred_series GROUP BY 1 ORDER BY 1")
    assert frame["series_id"].tolist() == ["GDPC1", "UNRATE"]
    assert frame["n"].tolist() == [2, 1]
