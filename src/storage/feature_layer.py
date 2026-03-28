from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.features.corridor_builder import corridor_definitions_frame
from src.features.derived_features import add_derived_features
from src.features.economic_features import add_economic_change_features
from src.features.lag_features import add_lag_features
from src.features.rolling_features import add_rolling_features
from src.storage.db_manager import DuckDBManager
from src.storage.schema_manager import initialize_database
from src.utils.corridor_config import LOCKED_CORRIDORS


ANNUAL_YEARS = [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
BTS_YEARS = [2030, 2035, 2040, 2045, 2050]
HISTORICAL_YEARS = [1997, 2002, 2007, 2012]


@dataclass(frozen=True)
class FeatureBuildResult:
    table_name: str
    row_count: int


def _annual_union_query(years: list[int], low_high: bool = False) -> str:
    selects: list[str] = []
    for year in years:
        if low_high:
            selects.append(
                f"""
                SELECT
                    dms_orig,
                    dms_dest,
                    {year} AS year,
                    tons_{year} AS bts_tons,
                    tons_{year}_low AS bts_tons_low,
                    tons_{year}_high AS bts_tons_high
                FROM staged_faf_domestic
                """
            )
        else:
            selects.append(
                f"""
                SELECT
                    dms_orig,
                    dms_dest,
                    {year} AS year,
                    tons_{year} AS total_tons,
                    value_{year} AS total_value,
                    tmiles_{year} AS total_tmiles
                FROM staged_faf_domestic
                """
            )
    return "\nUNION ALL\n".join(selects)


def _historical_union_query(years: list[int]) -> str:
    selects: list[str] = []
    for year in years:
        selects.append(
            f"""
            SELECT
                dms_origst,
                dms_destst,
                {year} AS year,
                tons_{year} AS total_tons,
                value_{year} AS total_value,
                tmiles_{year} AS total_tmiles
            FROM staged_faf_historical_domestic
            """
        )
    return "\nUNION ALL\n".join(selects)


class FeatureLayerBuilder:
    """Build model-ready feature tables from the staged DuckDB layer."""

    def __init__(self, db_manager: DuckDBManager | None = None) -> None:
        self.db_manager = db_manager or DuckDBManager()
        initialize_database(self.db_manager)

    def build_feature_corridor_annual(self) -> FeatureBuildResult:
        corridor_frame = corridor_definitions_frame()
        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM feature_corridor_annual")
            conn.register("corridor_definitions", corridor_frame)
            conn.execute(
                f"""
                INSERT INTO feature_corridor_annual
                WITH annualized AS (
                    {_annual_union_query(ANNUAL_YEARS)}
                ),
                corridor_agg AS (
                    SELECT
                        c.corridor_id,
                        c.corridor_name,
                        a.year,
                        SUM(a.total_tons) AS total_tons,
                        SUM(a.total_value) AS total_value,
                        SUM(a.total_tmiles) AS total_tmiles
                    FROM annualized a
                    INNER JOIN corridor_definitions c
                        ON a.dms_orig = c.origin
                       AND a.dms_dest = c.destination
                    GROUP BY 1, 2, 3
                )
                SELECT
                    c.corridor_id,
                    c.corridor_name,
                    c.year,
                    c.total_tons,
                    c.total_value,
                    c.total_tmiles,
                    f.gdp,
                    f.diesel_price,
                    f.industrial_production,
                    f.unemployment_rate,
                    f.ppi_transportation,
                    f.vehicle_miles
                FROM corridor_agg c
                LEFT JOIN staged_fred_annual f
                    ON c.year = f.year
                ORDER BY c.corridor_id, c.year
                """
            )
            conn.unregister("corridor_definitions")
            row_count = conn.execute("SELECT COUNT(*) FROM feature_corridor_annual").fetchone()[0]
        return FeatureBuildResult("feature_corridor_annual", row_count)

    def build_feature_corridor_enriched(self) -> FeatureBuildResult:
        annual = self.db_manager.fetch_df(
            """
            SELECT *
            FROM feature_corridor_annual
            ORDER BY corridor_id, year
            """
        )
        enriched = add_lag_features(annual)
        enriched = add_rolling_features(enriched)
        enriched = add_derived_features(enriched)
        enriched = add_economic_change_features(enriched)
        enriched = enriched[
            [
                "corridor_id",
                "corridor_name",
                "year",
                "total_tons",
                "total_value",
                "total_tmiles",
                "gdp",
                "diesel_price",
                "industrial_production",
                "unemployment_rate",
                "ppi_transportation",
                "vehicle_miles",
                "tons_lag1",
                "tons_lag2",
                "tons_lag3",
                "value_lag1",
                "diesel_lag1",
                "tons_rolling_mean_3yr",
                "tons_rolling_std_3yr",
                "tons_yoy_change",
                "gdp_yoy_change",
                "diesel_yoy_change",
                "value_per_ton",
                "tmiles_per_ton",
            ]
        ]

        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM feature_corridor_enriched")
            conn.register("feature_corridor_enriched_frame", enriched)
            conn.execute("INSERT INTO feature_corridor_enriched SELECT * FROM feature_corridor_enriched_frame")
            conn.unregister("feature_corridor_enriched_frame")
            row_count = conn.execute("SELECT COUNT(*) FROM feature_corridor_enriched").fetchone()[0]
        return FeatureBuildResult("feature_corridor_enriched", row_count)

    def build_feature_bts_forecast(self) -> FeatureBuildResult:
        corridor_frame = corridor_definitions_frame()
        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM feature_bts_forecast")
            conn.register("corridor_definitions", corridor_frame)
            conn.execute(
                f"""
                INSERT INTO feature_bts_forecast
                WITH annualized AS (
                    {_annual_union_query(BTS_YEARS, low_high=True)}
                )
                SELECT
                    c.corridor_id,
                    c.corridor_name,
                    a.year,
                    SUM(a.bts_tons) AS bts_tons,
                    SUM(a.bts_tons_low) AS bts_tons_low,
                    SUM(a.bts_tons_high) AS bts_tons_high
                FROM annualized a
                INNER JOIN corridor_definitions c
                    ON a.dms_orig = c.origin
                   AND a.dms_dest = c.destination
                GROUP BY 1, 2, 3
                ORDER BY 1, 3
                """
            )
            conn.unregister("corridor_definitions")
            row_count = conn.execute("SELECT COUNT(*) FROM feature_bts_forecast").fetchone()[0]
        return FeatureBuildResult("feature_bts_forecast", row_count)

    def build_feature_corridor_historical(self) -> FeatureBuildResult:
        zone_state = self.db_manager.fetch_df(
            """
            SELECT zone_id, state_fips, state_name
            FROM staged_zone_to_state
            """
        )
        corridor_states = []
        for corridor in LOCKED_CORRIDORS:
            origin_state = zone_state.loc[zone_state["zone_id"] == corridor.origin]
            dest_state = zone_state.loc[zone_state["zone_id"] == corridor.destination]
            if origin_state.empty or dest_state.empty:
                raise ValueError(f"Missing zone-to-state mapping for corridor {corridor.name}")
            corridor_states.append(
                {
                    "corridor_id": corridor.corridor_id,
                    "corridor_name": corridor.name,
                    "origin_state_fips": int(origin_state["state_fips"].iloc[0]),
                    "dest_state_fips": int(dest_state["state_fips"].iloc[0]),
                }
            )

        corridor_states_frame = pd.DataFrame(corridor_states)

        with self.db_manager.connection() as conn:
            conn.execute("DELETE FROM feature_corridor_historical")
            conn.register("corridor_state_pairs", corridor_states_frame)
            conn.execute(
                f"""
                INSERT INTO feature_corridor_historical
                WITH annualized AS (
                    {_historical_union_query(HISTORICAL_YEARS)}
                )
                SELECT
                    c.corridor_id,
                    c.corridor_name,
                    a.year,
                    SUM(a.total_tons) AS total_tons,
                    SUM(a.total_value) AS total_value,
                    SUM(a.total_tmiles) AS total_tmiles,
                    'state' AS geography_level
                FROM annualized a
                INNER JOIN corridor_state_pairs c
                    ON a.dms_origst = c.origin_state_fips
                   AND a.dms_destst = c.dest_state_fips
                GROUP BY 1, 2, 3, 7
                ORDER BY 1, 3
                """
            )
            conn.unregister("corridor_state_pairs")
            row_count = conn.execute("SELECT COUNT(*) FROM feature_corridor_historical").fetchone()[0]
        return FeatureBuildResult("feature_corridor_historical", row_count)

    def build_all(self) -> list[FeatureBuildResult]:
        return [
            self.build_feature_corridor_annual(),
            self.build_feature_corridor_enriched(),
            self.build_feature_bts_forecast(),
            self.build_feature_corridor_historical(),
        ]
