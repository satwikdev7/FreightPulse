from pathlib import Path

from src.ingestion.faf_loader import FAFHiLoLoader
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database


def test_faf_hilo_loader_loads_csv_into_raw_table(tmp_path: Path):
    csv_path = tmp_path / "sample_hilo.csv"
    csv_path.write_text(
        "fr_orig,dms_orig,dms_dest,fr_dest,fr_inmode,dms_mode,fr_outmode,sctg2,trade_type,"
        "dist_band,tons_2017,tons_2018,tons_2019,tons_2020,tons_2021,tons_2022,tons_2023,"
        "tons_2024,tons_2030,tons_2035,tons_2040,tons_2045,tons_2050,value_2017,value_2018,"
        "value_2019,value_2020,value_2021,value_2022,value_2023,value_2024,value_2030,"
        "value_2035,value_2040,value_2045,value_2050,current_value_2018,current_value_2019,"
        "current_value_2020,current_value_2021,current_value_2022,current_value_2023,"
        "current_value_2024,tmiles_2017,tmiles_2018,tmiles_2019,tmiles_2020,tmiles_2021,"
        "tmiles_2022,tmiles_2023,tmiles_2024,tmiles_2030,tmiles_2035,tmiles_2040,tmiles_2045,"
        "tmiles_2050,tons_2030_low,tons_2035_low,tons_2040_low,tons_2045_low,tons_2050_low,"
        "tons_2030_high,tons_2035_high,tons_2040_high,tons_2045_high,tons_2050_high,"
        "value_2030_low,value_2035_low,value_2040_low,value_2045_low,value_2050_low,"
        "value_2030_high,value_2035_high,value_2040_high,value_2045_high,value_2050_high\n"
        + ",".join([""] * 10 + ["1"] * 66)
        + "\n"
        + ",".join([""] * 10 + ["2"] * 66)
        + "\n"
    )

    db_path = tmp_path / "test.duckdb"
    manager = DuckDBManager(db_path=db_path)
    initialize_database(manager)

    validation = FAFHiLoLoader(csv_path=csv_path, db_manager=manager).load()

    assert validation.is_valid
    assert validation.actual_rows == 2
    count = manager.fetch_df("SELECT COUNT(*) AS count FROM raw_faf_hilo")["count"].iloc[0]
    assert count == 2
