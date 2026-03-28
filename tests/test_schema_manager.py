from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


def test_initialize_database_creates_core_tables(tmp_path):
    manager = DuckDBManager(db_path=tmp_path / "schema_test.duckdb")
    initialize_database(manager)

    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """
    tables = manager.fetch_df(query)["table_name"].tolist()

    assert "raw_faf_hilo" in tables
    assert "staged_faf_domestic" in tables
    assert "feature_corridor_enriched" in tables
