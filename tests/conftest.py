from src.storage.db_manager import DuckDBManager


def test_db_manager_smoke_fixture():
    manager = DuckDBManager()
    assert manager.db_path.name == "freightpulse.duckdb"
