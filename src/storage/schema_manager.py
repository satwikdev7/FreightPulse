from __future__ import annotations

import argparse
from textwrap import dedent

from src.storage.db_manager import DuckDBManager
from src.utils.paths import ensure_project_directories


RAW_TABLES_SQL = dedent(
    """
    CREATE TABLE IF NOT EXISTS raw_faf_hilo (
        fr_orig DOUBLE,
        dms_orig INTEGER,
        dms_dest INTEGER,
        fr_dest DOUBLE,
        fr_inmode DOUBLE,
        dms_mode INTEGER,
        fr_outmode DOUBLE,
        sctg2 INTEGER,
        trade_type INTEGER,
        dist_band INTEGER,
        tons_2017 DOUBLE,
        tons_2018 DOUBLE,
        tons_2019 DOUBLE,
        tons_2020 DOUBLE,
        tons_2021 DOUBLE,
        tons_2022 DOUBLE,
        tons_2023 DOUBLE,
        tons_2024 DOUBLE,
        tons_2030 DOUBLE,
        tons_2035 DOUBLE,
        tons_2040 DOUBLE,
        tons_2045 DOUBLE,
        tons_2050 DOUBLE,
        value_2017 DOUBLE,
        value_2018 DOUBLE,
        value_2019 DOUBLE,
        value_2020 DOUBLE,
        value_2021 DOUBLE,
        value_2022 DOUBLE,
        value_2023 DOUBLE,
        value_2024 DOUBLE,
        value_2030 DOUBLE,
        value_2035 DOUBLE,
        value_2040 DOUBLE,
        value_2045 DOUBLE,
        value_2050 DOUBLE,
        current_value_2018 DOUBLE,
        current_value_2019 DOUBLE,
        current_value_2020 DOUBLE,
        current_value_2021 DOUBLE,
        current_value_2022 DOUBLE,
        current_value_2023 DOUBLE,
        current_value_2024 DOUBLE,
        tmiles_2017 DOUBLE,
        tmiles_2018 DOUBLE,
        tmiles_2019 DOUBLE,
        tmiles_2020 DOUBLE,
        tmiles_2021 DOUBLE,
        tmiles_2022 DOUBLE,
        tmiles_2023 DOUBLE,
        tmiles_2024 DOUBLE,
        tmiles_2030 DOUBLE,
        tmiles_2035 DOUBLE,
        tmiles_2040 DOUBLE,
        tmiles_2045 DOUBLE,
        tmiles_2050 DOUBLE,
        tons_2030_low DOUBLE,
        tons_2035_low DOUBLE,
        tons_2040_low DOUBLE,
        tons_2045_low DOUBLE,
        tons_2050_low DOUBLE,
        tons_2030_high DOUBLE,
        tons_2035_high DOUBLE,
        tons_2040_high DOUBLE,
        tons_2045_high DOUBLE,
        tons_2050_high DOUBLE,
        value_2030_low DOUBLE,
        value_2035_low DOUBLE,
        value_2040_low DOUBLE,
        value_2045_low DOUBLE,
        value_2050_low DOUBLE,
        value_2030_high DOUBLE,
        value_2035_high DOUBLE,
        value_2040_high DOUBLE,
        value_2045_high DOUBLE,
        value_2050_high DOUBLE
    );

    CREATE TABLE IF NOT EXISTS raw_faf_historical (
        fr_orig DOUBLE,
        dms_origst INTEGER,
        dms_destst INTEGER,
        fr_dest DOUBLE,
        fr_inmode DOUBLE,
        dms_mode INTEGER,
        fr_outmode DOUBLE,
        sctg2 INTEGER,
        trade_type INTEGER,
        tons_1997 DOUBLE,
        tons_2002 DOUBLE,
        tons_2007 DOUBLE,
        tons_2012 DOUBLE,
        value_1997 DOUBLE,
        value_2002 DOUBLE,
        value_2007 DOUBLE,
        value_2012 DOUBLE,
        current_value_1997 DOUBLE,
        current_value_2002 DOUBLE,
        current_value_2007 DOUBLE,
        current_value_2012 DOUBLE,
        tmiles_1997 DOUBLE,
        tmiles_2002 DOUBLE,
        tmiles_2007 DOUBLE,
        tmiles_2012 DOUBLE
    );

    CREATE TABLE IF NOT EXISTS raw_fred_series (
        series_id VARCHAR,
        date DATE,
        value DOUBLE
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_states (
        numeric_label INTEGER,
        description VARCHAR
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_zones (
        numeric_label INTEGER,
        short_description VARCHAR,
        long_description VARCHAR
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_commodities (
        numeric_label INTEGER,
        description VARCHAR
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_modes (
        numeric_label INTEGER,
        description VARCHAR
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_trade_type (
        numeric_label INTEGER,
        description VARCHAR
    );

    CREATE TABLE IF NOT EXISTS raw_faf_metadata_distance_band (
        numeric_label INTEGER,
        description VARCHAR
    );
    """
)


STAGED_TABLES_SQL = dedent(
    """
    CREATE TABLE IF NOT EXISTS staged_faf_domestic (
        dms_orig INTEGER,
        dms_dest INTEGER,
        dms_mode INTEGER,
        sctg2 INTEGER,
        trade_type INTEGER,
        dist_band INTEGER,
        tons_2017 DOUBLE,
        tons_2018 DOUBLE,
        tons_2019 DOUBLE,
        tons_2020 DOUBLE,
        tons_2021 DOUBLE,
        tons_2022 DOUBLE,
        tons_2023 DOUBLE,
        tons_2024 DOUBLE,
        tons_2030 DOUBLE,
        tons_2035 DOUBLE,
        tons_2040 DOUBLE,
        tons_2045 DOUBLE,
        tons_2050 DOUBLE,
        value_2017 DOUBLE,
        value_2018 DOUBLE,
        value_2019 DOUBLE,
        value_2020 DOUBLE,
        value_2021 DOUBLE,
        value_2022 DOUBLE,
        value_2023 DOUBLE,
        value_2024 DOUBLE,
        value_2030 DOUBLE,
        value_2035 DOUBLE,
        value_2040 DOUBLE,
        value_2045 DOUBLE,
        value_2050 DOUBLE,
        tmiles_2017 DOUBLE,
        tmiles_2018 DOUBLE,
        tmiles_2019 DOUBLE,
        tmiles_2020 DOUBLE,
        tmiles_2021 DOUBLE,
        tmiles_2022 DOUBLE,
        tmiles_2023 DOUBLE,
        tmiles_2024 DOUBLE,
        tmiles_2030 DOUBLE,
        tmiles_2035 DOUBLE,
        tmiles_2040 DOUBLE,
        tmiles_2045 DOUBLE,
        tmiles_2050 DOUBLE,
        tons_2030_low DOUBLE,
        tons_2035_low DOUBLE,
        tons_2040_low DOUBLE,
        tons_2045_low DOUBLE,
        tons_2050_low DOUBLE,
        tons_2030_high DOUBLE,
        tons_2035_high DOUBLE,
        tons_2040_high DOUBLE,
        tons_2045_high DOUBLE,
        tons_2050_high DOUBLE,
        value_2030_low DOUBLE,
        value_2035_low DOUBLE,
        value_2040_low DOUBLE,
        value_2045_low DOUBLE,
        value_2050_low DOUBLE,
        value_2030_high DOUBLE,
        value_2035_high DOUBLE,
        value_2040_high DOUBLE,
        value_2045_high DOUBLE,
        value_2050_high DOUBLE
    );

    CREATE TABLE IF NOT EXISTS staged_faf_historical_domestic (
        dms_origst INTEGER,
        dms_destst INTEGER,
        dms_mode INTEGER,
        sctg2 INTEGER,
        trade_type INTEGER,
        tons_1997 DOUBLE,
        tons_2002 DOUBLE,
        tons_2007 DOUBLE,
        tons_2012 DOUBLE,
        value_1997 DOUBLE,
        value_2002 DOUBLE,
        value_2007 DOUBLE,
        value_2012 DOUBLE,
        tmiles_1997 DOUBLE,
        tmiles_2002 DOUBLE,
        tmiles_2007 DOUBLE,
        tmiles_2012 DOUBLE
    );

    CREATE TABLE IF NOT EXISTS staged_fred_annual (
        year INTEGER,
        gdp DOUBLE,
        diesel_price DOUBLE,
        industrial_production DOUBLE,
        unemployment_rate DOUBLE,
        ppi_transportation DOUBLE,
        vehicle_miles DOUBLE
    );

    CREATE TABLE IF NOT EXISTS staged_zone_to_state (
        zone_id INTEGER,
        zone_short_description VARCHAR,
        zone_long_description VARCHAR,
        state_fips INTEGER,
        state_name VARCHAR
    );
    """
)


FEATURE_TABLES_SQL = dedent(
    """
    CREATE TABLE IF NOT EXISTS feature_corridor_annual (
        corridor_id INTEGER,
        corridor_name VARCHAR,
        year INTEGER,
        total_tons DOUBLE,
        total_value DOUBLE,
        total_tmiles DOUBLE,
        gdp DOUBLE,
        diesel_price DOUBLE,
        industrial_production DOUBLE,
        unemployment_rate DOUBLE,
        ppi_transportation DOUBLE,
        vehicle_miles DOUBLE
    );

    CREATE TABLE IF NOT EXISTS feature_corridor_enriched (
        corridor_id INTEGER,
        corridor_name VARCHAR,
        year INTEGER,
        total_tons DOUBLE,
        total_value DOUBLE,
        total_tmiles DOUBLE,
        gdp DOUBLE,
        diesel_price DOUBLE,
        industrial_production DOUBLE,
        unemployment_rate DOUBLE,
        ppi_transportation DOUBLE,
        vehicle_miles DOUBLE,
        tons_lag1 DOUBLE,
        tons_lag2 DOUBLE,
        tons_lag3 DOUBLE,
        value_lag1 DOUBLE,
        diesel_lag1 DOUBLE,
        tons_rolling_mean_3yr DOUBLE,
        tons_rolling_std_3yr DOUBLE,
        tons_yoy_change DOUBLE,
        gdp_yoy_change DOUBLE,
        diesel_yoy_change DOUBLE,
        value_per_ton DOUBLE,
        tmiles_per_ton DOUBLE
    );

    CREATE TABLE IF NOT EXISTS feature_corridor_historical (
        corridor_id INTEGER,
        corridor_name VARCHAR,
        year INTEGER,
        total_tons DOUBLE,
        total_value DOUBLE,
        total_tmiles DOUBLE,
        geography_level VARCHAR
    );

    CREATE TABLE IF NOT EXISTS feature_bts_forecast (
        corridor_id INTEGER,
        corridor_name VARCHAR,
        year INTEGER,
        bts_tons DOUBLE,
        bts_tons_low DOUBLE,
        bts_tons_high DOUBLE
    );
    """
)


def initialize_database(db_manager: DuckDBManager | None = None) -> None:
    ensure_project_directories()
    manager = db_manager or DuckDBManager()
    with manager.connection() as conn:
        conn.execute(RAW_TABLES_SQL)
        conn.execute(STAGED_TABLES_SQL)
        conn.execute(FEATURE_TABLES_SQL)


def main() -> None:
    parser = argparse.ArgumentParser(description="FreightPulse DuckDB schema manager")
    parser.add_argument(
        "--init",
        action="store_true",
        help="Initialize the DuckDB database and create all base tables.",
    )
    args = parser.parse_args()

    if args.init:
        initialize_database()
        print("Initialized FreightPulse DuckDB schema.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
