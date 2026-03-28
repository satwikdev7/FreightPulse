from pathlib import Path

from src.ingestion.historical_loader import FAFHistoricalLoader
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


def test_historical_loader_loads_csv_into_raw_table(tmp_path: Path):
    csv_path = tmp_path / "sample_historical.csv"
    csv_path.write_text(
        "fr_orig,dms_origst,dms_destst,fr_dest,fr_inmode,dms_mode,fr_outmode,sctg2,trade_type,"
        "tons_1997,tons_2002,tons_2007,tons_2012,value_1997,value_2002,value_2007,value_2012,"
        "current_value_1997,current_value_2002,current_value_2007,current_value_2012,tmiles_1997,"
        "tmiles_2002,tmiles_2007,tmiles_2012\n"
        ",1,2,,,1,,3,1,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25\n"
        ",2,3,,,2,,4,1,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35\n"
    )

    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path)
    initialize_database(manager)

    validation = FAFHistoricalLoader(csv_path=csv_path, db_manager=manager).load()

    assert validation.is_valid
    assert validation.actual_rows == 2
    count = manager.fetch_df("SELECT COUNT(*) AS count FROM raw_faf_historical")["count"].iloc[0]
    assert count == 2
